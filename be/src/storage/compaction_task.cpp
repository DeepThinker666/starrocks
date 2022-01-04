// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#include "storage/compaction_task.h"

#include "storage/compaction_manager.h"
#include "storage/storage_engine.h"

namespace starrocks {

void CompactionTask::run() {
    LOG(INFO) << "start compaction. task_id:" << _task_id << ", tablet:" << _tablet->tablet_id()
              << ", _algorithm:" << _algorithm << ", _compaction_level:" << (int)_compaction_level;
    std::stringstream ss;
    for (auto& rowset : _input_rowsets) {
        ss << rowset->version() << ";";
    }
    LOG(INFO) << "input rowsets:" << ss.str();

    DeferOp op([&] {
        LOG(INFO) << "do some compaction callback.";
        // reset compaction before judge need_compaction again
        // because if there is a compaction task in a tablet, it will be able to run another one
        _tablet->reset_compaction();
        CompactionManager::instance()->unregister_task(this);
        if (_tablet->need_compaction()) {
            LOG(INFO) << "tablet:" << _tablet->tablet_id() << " compaction finished. and should do compaction again";
            CompactionManager::instance()->update_candidate(_tablet);
        }
    });

    bool registered = CompactionManager::instance()->register_task(this);
    if (!registered) {
        LOG(WARNING) << "register compaction task failed.";
        return;
    }

    Status status = run_impl();
    if (status.ok()) {
        _success_callback();
    } else {
        _failure_callback();
    }
    LOG(INFO) << "compaction finish. status:" << status.to_string() << ", task_id:" << _task_id
              << ", tablet:" << _tablet->tablet_id();
}

bool CompactionTask::should_stop() {
    return StorageEngine::instance()->bg_worker_stopped() || !config::enable_compaction || BackgroudTask::should_stop();
}

void CompactionTask::_success_callback() {
    // for compatible, update compaction time
    if (_compaction_level == 0) {
        _tablet->set_last_cumu_compaction_success_time(UnixMillis());
    } else {
        _tablet->set_last_base_compaction_success_time(UnixMillis());
    }

    // preload the rowset
    // warm-up this rowset
    auto st = _output_rowset->load();
    if (!st.ok()) {
        // only log load failure
        LOG(WARNING) << "ignore load rowset error tablet:" << _tablet->tablet_id()
                     << ", rowset:" << _output_rowset->rowset_id() << ", status:" << st;
    }
}

void CompactionTask::_failure_callback() {
    if (_compaction_level == 0) {
        _tablet->set_last_cumu_compaction_failure_time(UnixMillis());
    } else {
        _tablet->set_last_base_compaction_failure_time(UnixMillis());
    }
}

} // namespace starrocks
