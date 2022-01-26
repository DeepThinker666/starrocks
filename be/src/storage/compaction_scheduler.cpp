// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/compaction_scheduler.h"

#include <chrono>
#include <thread>

#include "storage/compaction_manager.h"
#include "storage/compaction_task.h"
#include "storage/data_dir.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"

using namespace std::chrono_literals;

namespace starrocks {

// compaction task执行结束之后，回调函数唤醒scheduler
void CompactionScheduler::schedule() {
    LOG(INFO) << "start compaction scheduler";
    while (true) {
        // check current running tasks
        // check running tasks of each DataDir
        _wait_to_run();
        // 如果空转，怎么办？找不到能够compaction的任务怎么？
        // 如果candidat为空怎么办？如果candidate中没有满足条件的，比如所有的candidate对应的datadir都有最大的running
        // task
        // 如果candidate为空，是不是考虑wait住？
        // 如果所有的candidate都不满足条件，就
        Tablet* selected = try_get_next_tablet();
        if (!selected) {
            // 遍历所有的tablet，都找不到符合条件的tablet
            // 有可能compaction task完成了，可以重新调度
            // 有可能加入新的candidate，可以重新调度
            // 如果采用唤醒机制，需要在比较多的地方掉用notify
            // 暂时先sleep
            // 可以改成notify的机制，最长等待10s，避免bug，导致无法调度compaction
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        } else {
            // 考虑并发
            LOG(INFO) << "selected tablet:" << selected->tablet_id()
                      << ", compaction score:" << selected->compaction_score();
            std::shared_ptr<CompactionTask> compaction_task = selected->get_compaction(true);
            PriorityThreadPool::Task task;
            task.work_function = [compaction_task] { compaction_task->start(); };
            // make this configurable
            LOG(INFO) << "input rows num:" << compaction_task->input_rows_num()
                      << ", rowsets size:" << compaction_task->input_rowsets_size();
            if (compaction_task->input_rows_num() > 1000000 ||
                compaction_task->input_rowsets_size() > 1024 * 1024 * 1024) {
                // submit task to low priority pool
                LOG(INFO) << "submit compaction task to low priority pool. tablet:"
                          << compaction_task->tablet()->tablet_id();
                _low_priority_compaction_pool.offer(task);
            } else {
                // submit task to normal pool
                LOG(INFO) << "submit compaction task to normal priority pool. tablet:"
                          << compaction_task->tablet()->tablet_id();
                _normal_compaction_pool.offer(task);
            }
        }
    }
}

bool CompactionScheduler::_can_schedule_next() {
    int32_t max_task_num = std::min(
            config::max_compaction_task_num,
            static_cast<int32_t>(StorageEngine::instance()->get_store_num() * config::max_compaction_task_per_disk));
    return config::enable_compaction && CompactionManager::instance()->running_tasks_num() < max_task_num;
}

void CompactionScheduler::_wait_to_run() {
    std::unique_lock<std::mutex> lk(_mutex);
    int round = 0;
    // check _can_schedule_next every one second to avoid deadlock and support modifying config online
    while (!_cv.wait_for(lk, 1000ms, [this] { return _can_schedule_next(); })) {
        // TODO: remove this log
        LOG(INFO) << "compaction scheduler wait to run, round:" << round++;
    }
}

Tablet* CompactionScheduler::try_get_next_tablet() {
    LOG(INFO) << "try to get next qualified tablet.";
    // tmp_tablets save the tmp picked candidates tablets
    std::vector<Tablet*> tmp_tablets;
    Tablet* tablet = nullptr;
    int64_t now_ms = UnixMillis();
    while (true) {
        tablet = CompactionManager::instance()->pick_candidate();
        if (!tablet) {
            // means there no candidate tablet, break
            break;
        }
        LOG(INFO) << "try tablet:" << tablet->tablet_id();
        if (!tablet->need_compaction()) {
            // check need compaction
            // if it is false, skip this tablet and remove it from candidate
            LOG(WARNING) << "skip tablet:" << tablet->tablet_id() << " for need_compaction is false";
            continue;
        }

        if (tablet->tablet_state() != TABLET_RUNNING) {
            LOG(WARNING) << "skip tablet:" << tablet->tablet_id() << " for tablet state is not RUNNING";
            continue;
        }

        // check for alter task for safety
        // maybe this logic can be removed if tablet is the schema change dest tablet, need_compaction will never return true
        AlterTabletTaskSharedPtr cur_alter_task = tablet->alter_task();
        if (cur_alter_task != nullptr && cur_alter_task->alter_state() != ALTER_FINISHED &&
            cur_alter_task->alter_state() != ALTER_FAILED) {
            TabletManager* tablet_manager = StorageEngine::instance()->tablet_manager();
            TabletSharedPtr related_tablet = tablet_manager->get_tablet(cur_alter_task->related_tablet_id());
            if (related_tablet != nullptr && tablet->creation_time() > related_tablet->creation_time()) {
                // Current tablet is newly created during schema-change or rollup, skip it
                continue;
            }
        }

        std::shared_ptr<CompactionTask> compaction_task = tablet->get_compaction(false);
        if (compaction_task) {
            // tablet already has a running compaction task, skip it
            LOG(WARNING) << "skip tablet:" << tablet->tablet_id()
                         << " for there is running compaction task:" << compaction_task->task_id();
            continue;
        }
        tmp_tablets.push_back(tablet);
        DataDir* data_dir = tablet->data_dir();
        if (data_dir->reach_capacity_limit(0)) {
            LOG(WARNING) << "skip tablet:" << tablet->tablet_id() << " for data dir reach capacity limit";
            continue;
        }

        // A not-ready tablet maybe a newly created tablet under schema-change, skip it
        if (tablet->tablet_state() == TABLET_NOTREADY) {
            continue;
        }

        // create a new compaction task
        bool need_reset_task = true;
        compaction_task = tablet->get_compaction(true);
        if (compaction_task) {
            // create new compaction task successfully

            // to compatible with old compaction framework
            // TODO: can be optimized to use just one lock
            int64_t last_failure_ms = 0;
            if (compaction_task->compaction_level() == 0) {
                std::unique_lock lk(tablet->get_cumulative_lock(), std::try_to_lock);
                if (!lk.owns_lock()) {
                    LOG(INFO) << "skip tablet:" << tablet->tablet_id() << " for lock";
                    continue;
                }
                last_failure_ms = tablet->last_cumu_compaction_failure_time();
            } else {
                std::unique_lock lk(tablet->get_base_lock(), std::try_to_lock);
                if (!lk.owns_lock()) {
                    LOG(INFO) << "skip tablet:" << tablet->tablet_id() << " for lock";
                    continue;
                }
                last_failure_ms = tablet->last_base_compaction_failure_time();
            }
            if (now_ms - last_failure_ms <= config::min_compaction_failure_interval_sec * 1000) {
                LOG(INFO) << "Too often to schedule compaction, skip it."
                          << "compaction_level=" << compaction_task->compaction_level()
                          << ", last_failure_time_ms=" << last_failure_ms << ", tablet_id=" << tablet->tablet_id();
                continue;
            }
            DataDir* data_dir = tablet->data_dir();
            // control the concurrent running tasks's limit
            // just try best here for that there may be concurrent CompactionSchedulers
            // hard limit will be checked when CompactionManager::register()
            uint16_t num = CompactionManager::instance()->running_tasks_num_for_dir(data_dir);
            if (num < config::max_compaction_task_per_disk) {
                // found a qualified tablet
                // qualified tablet will be removed from candidates
                tmp_tablets.pop_back();
                break;
            }
        } else {
            LOG(INFO) << "skip tablet:" << tablet->tablet_id() << " for create compaction task failed.";
        }
        if (need_reset_task) {
            tablet->reset_compaction();
        }
    }
    LOG(INFO) << "pick next candidates. tmp tablets size:" << tmp_tablets.size();
    CompactionManager::instance()->insert_candidates(tmp_tablets);
    if (tablet) {
        LOG(INFO) << "get a qualified tablet:" << tablet->tablet_id();
    } else {
        LOG(INFO) << "no qualified tablet.";
    }

    return tablet;
}

} // namespace starrocks
