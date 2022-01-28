// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <mutex>
#include <sstream>
#include <vector>

#include "storage/backgroud_task.h"
#include "storage/compaction_utils.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"
#include "util/runtime_profile.h"
#include "util/time.h"

namespace starrocks {

class CompactionScheduler;

struct CompactionTaskInfo {
    CompactionTaskInfo(CompactionAlgorithm algo)
            : algorithm(algo),
              task_id(0),
              elapsed_time(0),
              start_time(0),
              end_time(0),
              output_segments_num(0),
              output_rowset_size(0),
              merged_rows(0),
              filtered_rows(0),
              output_num_rows(0),
              column_group_size(0),
              total_output_num_rows(0),
              total_merged_rows(0),
              total_del_filtered_rows(0) {}
    CompactionAlgorithm algorithm;
    uint64_t task_id;
    Version output_version;
    uint64_t elapsed_time;
    int64_t tablet_id;
    int64_t start_time;
    int64_t end_time;
    uint32_t input_rows_num;
    uint32_t input_rowsets_num;
    size_t input_rowsets_size;
    uint32_t input_segments_num;
    uint32_t segment_iterator_num;
    uint32_t output_segments_num;
    uint32_t output_rowset_size;
    size_t merged_rows;
    size_t filtered_rows;
    size_t output_num_rows;
    uint8_t compaction_level;

    // for vertical compaction
    size_t column_group_size;
    size_t total_output_num_rows;
    size_t total_merged_rows;
    size_t total_del_filtered_rows;

    // return [0-100] to indicate progress
    int get_progress() {
        if (input_rows_num == 0) {
            return 100;
        }
        if (algorithm == HORIZONTAL_COMPACTION) {
            return (output_num_rows + merged_rows + filtered_rows) * 100 / input_rows_num;
        } else {
            if (column_group_size == 0) {
                return 0;
            }
            return (total_output_num_rows + total_merged_rows + total_del_filtered_rows) * 100 /
                   (input_rows_num * column_group_size);
        }
    }

    std::string to_string() {
        std::stringstream ss;
        ss << "[CompactionTaskInfo]";
        ss << " task_id:" << task_id;
        ss << ", tablet_id:" << tablet_id;
        ss << ", algorithm:" << algorithm_to_string(algorithm);
        ss << ", compaction_level:" << (int32_t)compaction_level;
        ss << ", output_version:" << output_version;
        ss << ", start_time:" << ToStringFromUnixMillis(start_time);
        ss << ", end_time:" << ToStringFromUnixMillis(end_time);
        ss << ", elapsed_time:" << elapsed_time << " us";
        ss << ", input_rowsets_size:" << input_rowsets_size;
        ss << ", input_segments_num:" << input_segments_num;
        ss << ", input_rowsets_num:" << input_rowsets_num;
        ss << ", input_rows_num:" << input_rows_num;
        ss << ", output_num_rows:" << output_num_rows;
        ss << ", merged_rows:" << merged_rows;
        ss << ", filtered_rows:" << filtered_rows;
        ss << ", output_segments_num:" << output_segments_num;
        ss << ", output_rowset_size:" << output_rowset_size;
        ss << ", column_group_size:" << column_group_size;
        ss << ", total_output_num_rows:" << total_output_num_rows;
        ss << ", total_merged_rows:" << total_merged_rows;
        ss << ", total_del_filtered_rows:" << total_del_filtered_rows;
        ss << ", progress:" << get_progress();
        return ss.str();
    }
};

class CompactionTask : public BackgroudTask {
public:
    CompactionTask(CompactionAlgorithm algorithm) : _task_info(algorithm), _runtime_profile("compaction") {
        _watch.start();
    }
    virtual ~CompactionTask() {
        if (_mem_tracker) {
            delete _mem_tracker;
        }
    }

    void run() override;

    bool should_stop() override;

    void set_input_rowsets(std::vector<RowsetSharedPtr>&& input_rowsets) {
        _input_rowsets = input_rowsets;
        _task_info.input_rowsets_num = _input_rowsets.size();
    }

    const std::vector<RowsetSharedPtr>& input_rowsets() const { return _input_rowsets; }

    void set_output_version(const Version& version) { _task_info.output_version = version; }

    const Version& output_version() const { return _task_info.output_version; }

    void set_input_rows_num(uint32_t input_rows_num) { _task_info.input_rows_num = input_rows_num; }

    uint32_t input_rows_num() const { return _task_info.input_rows_num; }

    void set_input_rowsets_size(size_t input_rowsets_size) { _task_info.input_rowsets_size = input_rowsets_size; }

    size_t input_rowsets_size() const { return _task_info.input_rowsets_size; }

    void set_compaction_level(uint8_t compaction_level) { _task_info.compaction_level = compaction_level; }

    uint8_t compaction_level() const { return _task_info.compaction_level; }

    void set_tablet(Tablet* tablet) {
        _tablet = tablet;
        _task_info.tablet_id = _tablet->tablet_id();
    }

    Tablet* tablet() { return _tablet; }

    void set_task_id(uint64_t task_id) { _task_info.task_id = task_id; }

    uint64_t task_id() { return _task_info.task_id; }

    void set_segment_iterator_num(size_t segment_iterator_num) {
        _task_info.segment_iterator_num = segment_iterator_num;
    }

    size_t segment_iterator_num() { return _task_info.segment_iterator_num; }

    void set_input_segments_num(uint32_t input_segments_num) { _task_info.input_segments_num = input_segments_num; }

    void set_start_time(int64_t start_time) { _task_info.start_time = start_time; }

    void set_end_time(int64_t end_time) { _task_info.end_time = end_time; }

    void set_output_segments_num(uint32_t output_segments_num) { _task_info.output_segments_num = output_segments_num; }

    void set_output_rowset_size(uint32_t output_rowset_size) { _task_info.output_rowset_size = output_rowset_size; }

    void set_merged_rows(size_t merged_rows) { _task_info.merged_rows = merged_rows; }

    void set_filtered_rows(size_t filtered_rows) { _task_info.filtered_rows = filtered_rows; }

    void set_output_num_rows(size_t output_num_rows) { _task_info.output_num_rows = output_num_rows; }

    void set_mem_tracker(MemTracker* mem_tracker) { _mem_tracker = mem_tracker; }

    std::string get_task_info() {
        _task_info.elapsed_time = _watch.elapsed_time() / 1000;
        return _task_info.to_string();
    }

    void set_compaction_scheduler(CompactionScheduler* scheduler) { _scheduler = scheduler; }

protected:
    virtual Status run_impl() = 0;

    void _try_lock() {
        if (_task_info.compaction_level == 0) {
            _compaction_lock = std::unique_lock(_tablet->get_cumulative_lock(), std::try_to_lock);
        } else {
            _compaction_lock = std::unique_lock(_tablet->get_base_lock(), std::try_to_lock);
        }
    }

    Status _validate_compaction(const Statistics& stats) {
        // check row number
        DCHECK(_output_rowset) << "_output_rowset is null";
        LOG(INFO) << "validate compaction, _input_rows_num:" << _task_info.input_rows_num
                  << ", output rowset rows:" << _output_rowset->num_rows() << ", merged_rows:" << stats.merged_rows
                  << ", filtered_rows:" << stats.filtered_rows;
        if (_task_info.input_rows_num != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
            LOG(WARNING) << "row_num does not match between cumulative input and output! "
                         << "input_row_num=" << _task_info.input_rows_num << ", merged_row_num=" << stats.merged_rows
                         << ", filtered_row_num=" << stats.filtered_rows
                         << ", output_row_num=" << _output_rowset->num_rows();

            return Status::InternalError("compaction check lines error.");
        }
        return Status::OK();
    }

    void _commit_compaction() {
        std::unique_lock wrlock(_tablet->get_header_lock());
        std::stringstream input_stream_info;
        for (auto& rowset : _input_rowsets) {
            input_stream_info << rowset->version() << ";";
        }
        LOG(INFO) << "commit compaction. output version:" << _task_info.output_version
                  << ", output rowset version:" << _output_rowset->version()
                  << ", input rowsets:" << input_stream_info.str();
        _tablet->modify_rowsets({_output_rowset}, _input_rowsets);
        LOG(INFO) << "start to save new tablet meta";
        _tablet->save_meta();
    }

    void _success_callback();

    void _failure_callback();

protected:
    CompactionTaskInfo _task_info;
    RuntimeProfile _runtime_profile;
    std::vector<RowsetSharedPtr> _input_rowsets;
    Tablet* _tablet;
    RowsetSharedPtr _output_rowset;
    std::unique_lock<std::mutex> _compaction_lock;
    MonotonicStopWatch _watch;
    MemTracker* _mem_tracker;
    CompactionScheduler* _scheduler;
};

} // namespace starrocks
