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
namespace starrocks {

// TODO: 优化统计信息，增加日志
struct CompactionStatistics {
    uint32_t _total_rows_read;
    uint32_t _total_rows_output;
    uint32_t _current_rows_read;
    uint32_t _current_rows_output;
    uint32_t _input_rowsets_num;
    uint32_t _input_segments_num;
    uint32_t _output_segments_num;
};

class CompactionTask : public BackgroudTask {
public:
    CompactionTask(CompactionAlgorithm algorithm)
            : _algorithm(algorithm), _task_id(0), _runtime_profile("compaction") {}
    virtual ~CompactionTask() = default;

    void run() override;

    bool should_stop() override;

    void set_input_rowsets(std::vector<RowsetSharedPtr>&& input_rowsets) { _input_rowsets = input_rowsets; }

    const std::vector<RowsetSharedPtr>& input_rowsets() const { return _input_rowsets; }

    void set_output_version(const Version& version) { _output_version = version; }

    const Version& output_version() const { return _output_version; }

    void set_input_rows_num(uint32_t input_rows_num) { _input_rows_num = input_rows_num; }

    uint32_t input_rows_num() const { return _input_rows_num; }

    void set_input_rowsets_size(size_t input_rowsets_size) { _input_rowsets_size = input_rowsets_size; }

    size_t input_rowsets_size() const { return _input_rowsets_size; }

    void set_compaction_level(uint8_t compaction_level) { _compaction_level = compaction_level; }

    uint8_t compaction_level() const { return _compaction_level; }

    void set_tablet(Tablet* tablet) { _tablet = tablet; }

    Tablet* tablet() { return _tablet; }

    void set_task_id(uint64_t task_id) { _task_id = task_id; }

    uint64_t task_id() { return _task_id; }

    void set_segment_iterator_num(size_t segment_iterator_num) { _segment_iterator_num = segment_iterator_num; }

    size_t segment_iterator_num() { return _segment_iterator_num; }

protected:
    virtual Status run_impl() = 0;

    void _try_lock() {
        if (_compaction_level == 0) {
            _compaction_lock = std::unique_lock(_tablet->get_cumulative_lock(), std::try_to_lock);
        } else {
            _compaction_lock = std::unique_lock(_tablet->get_base_lock(), std::try_to_lock);
        }
    }

    Status _validate_compaction(const Statistics& stats) {
        // check row number
        DCHECK(_output_rowset) << "_output_rowset is null";
        LOG(INFO) << "validate compaction, _input_rows_num:" << _input_rows_num
                  << ", output rowset rows:" << _output_rowset->num_rows() << ", merged_rows:" << stats.merged_rows
                  << ", filtered_rows:" << stats.filtered_rows;
        if (_input_rows_num != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
            LOG(WARNING) << "row_num does not match between cumulative input and output! "
                         << "input_row_num=" << _input_rows_num << ", merged_row_num=" << stats.merged_rows
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
        LOG(INFO) << "commit compaction. output version:" << _output_version
                  << ", output rowset version:" << _output_rowset->version()
                  << ", input rowsets:" << input_stream_info.str();
        _tablet->modify_rowsets({_output_rowset}, _input_rowsets);
        LOG(INFO) << "start to save new tablet meta";
        _tablet->save_meta();
    }

    void _success_callback();

    void _failure_callback();

protected:
    CompactionAlgorithm _algorithm;
    std::vector<RowsetSharedPtr> _input_rowsets;
    Version _output_version;
    uint64_t _task_id;
    uint32_t _input_rows_num;
    size_t _input_rowsets_size;
    uint8_t _compaction_level;
    size_t _segment_iterator_num;
    Tablet* _tablet;
    RowsetSharedPtr _output_rowset;
    RuntimeProfile _runtime_profile;
    std::unique_lock<std::mutex> _compaction_lock;
};

} // namespace starrocks
