// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.
#include "storage/compaction_task.h"

#include "runtime/current_thread.h"
#include "storage/compaction_manager.h"
#include "storage/storage_engine.h"
#include "util/scoped_cleanup.h"
#include "util/time.h"
#include "util/trace.h"

namespace starrocks {

void CompactionTask::run() {
    _task_info.start_time = UnixMillis();
    scoped_refptr<Trace> trace(new Trace);
    SCOPED_CLEANUP({
        if (_watch.elapsed_time() / 1e9 > config::cumulative_compaction_trace_threshold) {
            LOG(INFO) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
        }
    });
    ADOPT_TRACE(trace.get());
    TRACE("[Compaction]start to perform compaction. task_id:$0, tablet:$1, _algorithm::$2, _compaction_level:$3",
          _task_info.task_id, _task_info.tablet_id, ToString(_task_info.algorithm), (int)_task_info.compaction_level);
    LOG(INFO) << "start compaction. task_id:" << _task_info.task_id << ", tablet:" << _task_info.tablet_id
              << ", _algorithm:" << ToString(_task_info.algorithm)
              << ", _compaction_level:" << (int)_task_info.compaction_level;
    std::stringstream ss;
    ss << "output version:" << _task_info.output_version << ", input rowsets size:" << _input_rowsets.size()
       << ", input versions:";

    for (int i = 0; i < 5 && i < _input_rowsets.size(); ++i) {
        ss << _input_rowsets[i]->version() << ";";
    }
    if (_input_rowsets.size() > 5) {
        ss << ".." << (*_input_rowsets.rbegin())->version();
    }
    LOG(INFO) << ss.str();
    TRACE("[Compaction]$0", ss.str());
    TRACE_COUNTER_INCREMENT("input_rowsets_count", _input_rowsets.size());
    TRACE_COUNTER_INCREMENT("input_rowsets_data_size", _task_info.input_rowsets_size);
    TRACE_COUNTER_INCREMENT("input_row_num", _task_info.input_rows_num);
    TRACE_COUNTER_INCREMENT("input_segments_num", _task_info.input_segments_num);
    SCOPED_THREAD_LOCAL_MEM_TRACKER_SETTER(_mem_tracker);

    DeferOp op([&] {
        LOG(INFO) << "do some compaction callback.";
        TRACE("[Compaction]do compaction callback.");
        // reset compaction before judge need_compaction again
        // because if there is a compaction task in a tablet, it will be able to run another one
        _tablet->reset_compaction();
        _task_info.end_time = UnixMillis();
        CompactionManager::instance()->unregister_task(this);
        if (_tablet->need_compaction()) {
            LOG(INFO) << "tablet:" << _tablet->tablet_id() << " compaction finished. and should do compaction again";
            TRACE("[Compaction] compaction task finished. and will do compaction again.");
            CompactionManager::instance()->update_candidate(_tablet);
        }
        LOG(INFO) << _task_info.to_string();
        TRACE("[Compaction] $0", _task_info.to_string());
    });

    bool registered = CompactionManager::instance()->register_task(this);
    if (!registered) {
        LOG(WARNING) << "register compaction task failed. task_id:" << _task_info.task_id
                     << ", tablet:" << _task_info.tablet_id;
        return;
    }
    TRACE("[Compaction] compaction registered");

    Status status = run_impl();
    if (status.ok()) {
        _success_callback();
    } else {
        _failure_callback();
        LOG(INFO) << " Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
    }
    _watch.stop();
    _task_info.end_time = UnixMillis();
    // get elapsed_time in us
    _task_info.elapsed_time = _watch.elapsed_time() / 1000;
    LOG(INFO) << "compaction finish. status:" << status.to_string() << ", task info:" << _task_info.to_string();
}

bool CompactionTask::should_stop() {
    return StorageEngine::instance()->bg_worker_stopped() || !config::enable_compaction || BackgroudTask::should_stop();
}

void CompactionTask::_success_callback() {
    // for compatible, update compaction time
    if (_task_info.compaction_level == 0) {
        _tablet->set_last_cumu_compaction_success_time(UnixMillis());
    } else {
        _tablet->set_last_base_compaction_success_time(UnixMillis());
    }

    // for compatible
    if (_task_info.compaction_level == 0) {
        StarRocksMetrics::instance()->cumulative_compaction_deltas_total.increment(_input_rowsets.size());
        StarRocksMetrics::instance()->cumulative_compaction_bytes_total.increment(_task_info.input_rowsets_size);
    } else {
        StarRocksMetrics::instance()->base_compaction_deltas_total.increment(_input_rowsets.size());
        StarRocksMetrics::instance()->base_compaction_bytes_total.increment(_task_info.input_rowsets_size);
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
    if (_task_info.compaction_level == 0) {
        _tablet->set_last_cumu_compaction_failure_time(UnixMillis());
    } else {
        _tablet->set_last_base_compaction_failure_time(UnixMillis());
    }
    LOG(WARNING) << "compaction task" << _task_info.task_id << ", tablet:" << _task_info.tablet_id << " failed.";
}

} // namespace starrocks
