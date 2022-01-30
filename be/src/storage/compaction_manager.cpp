// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/compaction_manager.h"

#include "storage/compaction_scheduler.h"
#include "storage/data_dir.h"
#include "util/thread.h"
namespace starrocks {

std::unique_ptr<CompactionManager> CompactionManager::_instance(new CompactionManager());

CompactionManager* CompactionManager::instance() {
    return _instance.get();
}

void CompactionManager::print_log() {
    while (true) {
        {
            std::lock_guard lg(_tasks_mutex);
            if (_stop_log) {
                break;
            }
            LOG(INFO) << "there are " << _running_tasks.size() << " compaction tasks";
            for (auto& compaction_task : _running_tasks) {
                LOG(INFO) << compaction_task->get_task_info();
            }
        }
        LOG(INFO) << "task info printed";
        sleep(2);
    }
}

void CompactionManager::update_candidate_async(Tablet* tablet) {
    PriorityThreadPool::Task task;
    task.work_function = [tablet, this] {
        update_candidate(tablet);
    };
    bool ret = _update_candidate_pool.try_offer(task);
    if (!ret) {
        LOG(WARNING) << "update candidate failed for queue is full. capacity:"
                     << _update_candidate_pool.get_queue_capacity()
                     << ", queue size:" << _update_candidate_pool.get_queue_size();
    }
}

void CompactionManager::update_candidate(Tablet* tablet) {
    bool should_notify = false;
    {
        std::lock_guard lg(_candidates_mutex);
        size_t num = _candidate_tablets.erase(tablet);
        should_notify = num == 0;
        _candidate_tablets.insert(tablet);
        LOG(INFO) << "current _candidate_tablets size:" << _candidate_tablets.size()
                  << ", should_notify:" << should_notify;
    }
    if (should_notify) {
        _notify_schedulers();
    }
}

void CompactionManager::insert_candidates(const std::vector<Tablet*>& tablets) {
    std::lock_guard lg(_candidates_mutex);
    _candidate_tablets.insert(tablets.begin(), tablets.end());
}

Tablet* CompactionManager::pick_candidate() {
    std::lock_guard lg(_candidates_mutex);
    size_t original_num = _candidate_tablets.size();
    if (_candidate_tablets.empty()) {
        // return nullptr if _candidate_tablets is empty
        return nullptr;
    }

    auto iter = _candidate_tablets.begin();
    Tablet* ret = *iter;
    _candidate_tablets.erase(iter);
    LOG(INFO) << "candidate tablets size:" << _candidate_tablets.size() << ", original num:" << original_num;
    return ret;
}

bool CompactionManager::register_task(CompactionTask* compaction_task) {
    if (!compaction_task) {
        return false;
    }
    LOG(INFO) << "register compaction task:" << compaction_task->task_id()
              << ", tablet:" << compaction_task->tablet()->tablet_id();
    std::lock_guard lg(_tasks_mutex);
    if (!_log_thread_inited) {
        _log_thread = std::thread([this]() {
            LOG(INFO) << "start compaction manager log printer";
            print_log();
        });
        Thread::set_thread_name(_log_thread, "compaction_log");
        _log_thread_inited = true;
    }
    if (config::max_compaction_task_num >= 0 && _running_tasks.size() >= config::max_compaction_task_num) {
        LOG(WARNING) << "register compaction task failed for running tasks reach max limit:"
                     << config::max_compaction_task_num;
        return false;
    }
    if (compaction_task->compaction_level() == 0 && config::max_level_0_compaction_task >= 0 &&
        _level_to_task_num_map[0] >= config::max_level_0_compaction_task) {
        LOG(WARNING) << "register compaction task failed for level 0 limit:" << config::max_level_0_compaction_task;
        return false;
    } else if (compaction_task->compaction_level() == 1 && config::max_level_1_compaction_task >= 0 &&
               _level_to_task_num_map[1] >= config::max_level_1_compaction_task) {
        LOG(WARNING) << "register compaction task failed for level 1 limit:" << config::max_level_1_compaction_task;
        return false;
    }
    Tablet* tablet = compaction_task->tablet();
    DataDir* data_dir = tablet->data_dir();
    if (config::max_compaction_task_per_disk >= 0 &&
        _data_dir_to_task_num_map[data_dir] >= config::max_compaction_task_per_disk) {
        LOG(WARNING) << "register compaction task failed for disk's running tasks reach limit:"
                     << config::max_compaction_task_per_disk;
        return false;
    }
    auto p = _running_tasks.insert(compaction_task);
    if (!p.second) {
        // duplicate task
        LOG(WARNING) << "duplicate task, compaction_task:" << compaction_task->task_id()
                     << ", tablet:" << compaction_task->tablet()->tablet_id();
        return false;
    }
    _level_to_task_num_map[compaction_task->compaction_level()]++;
    _data_dir_to_task_num_map[data_dir]++;
    _running_tasks_num++;
    LOG(INFO) << "registered compaction task:" << compaction_task->task_id()
              << ", tablet:" << compaction_task->tablet()->tablet_id()
              << ", data dir task num:" << _data_dir_to_task_num_map[data_dir] << ", data_dir:" << data_dir->path()
              << ", running task:" << _running_tasks_num
              << ", compaction level:" << (int)compaction_task->compaction_level()
              << ", level num:" << _level_to_task_num_map[compaction_task->compaction_level()];
    return true;
}

void CompactionManager::unregister_task(CompactionTask* compaction_task) {
    if (!compaction_task) {
        return;
    }
    LOG(INFO) << "unregister compaction task:" << compaction_task->task_id()
              << ", tablet:" << compaction_task->tablet()->tablet_id();
    std::lock_guard lg(_tasks_mutex);
    auto size = _running_tasks.erase(compaction_task);
    if (size > 0) {
        Tablet* tablet = compaction_task->tablet();
        DataDir* data_dir = tablet->data_dir();
        _level_to_task_num_map[compaction_task->compaction_level()]--;
        _data_dir_to_task_num_map[data_dir]--;
        _running_tasks_num--;
        LOG(INFO) << "unregister compaction task:" << compaction_task->task_id()
                  << ", tablet:" << compaction_task->tablet()->tablet_id() << ", data dir:" << data_dir->path()
                  << ", data dir task num:" << _data_dir_to_task_num_map[data_dir]
                  << ", running task num:" << _running_tasks_num
                  << ", compaction level:" << (int)compaction_task->compaction_level()
                  << ", level num:" << _level_to_task_num_map[compaction_task->compaction_level()];
    }
}

void CompactionManager::clear_tasks() {
    std::lock_guard lg(_tasks_mutex);
    _running_tasks.clear();
    _running_tasks_num = 0;
    _data_dir_to_task_num_map.clear();
    _level_to_task_num_map.clear();
}

void CompactionManager::_notify_schedulers() {
    LOG(INFO) << "CompactionManager notify schedulers";
    std::lock_guard lg(_scheduler_mutex);
    for (auto& scheduler : _schedulers) {
        scheduler->notify();
    }
}

} // namespace starrocks
