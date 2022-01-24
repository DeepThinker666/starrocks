// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/compaction_task_factory.h"

#include "column/schema.h"
#include "storage/compaction_manager.h"
#include "storage/compaction_task.h"
#include "storage/compaction_utils.h"
#include "storage/horizontal_compaction_task.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/tablet_reader.h"
#include "storage/vectorized/tablet_reader_params.h"
#include "storage/vertical_compaction_task.h"

namespace starrocks {

std::shared_ptr<CompactionTask> CompactionTaskFactory::create_compaction_task() {
    // choose vertical or horizontal compaction algorithm
    LOG(INFO) << "start to create compaction task for tablet:" << _tablet->tablet_id();
    auto iterator_num_res = _get_segment_iterator_num();
    if (!iterator_num_res.ok()) {
        LOG(WARNING) << "fail to get segment iterator num. tablet=" << _tablet->tablet_id()
                     << ", err=" << iterator_num_res.status().to_string();
        return nullptr;
    }
    size_t segment_iterator_num = iterator_num_res.value();
    int64_t max_columns_per_group = config::vertical_compaction_max_columns_per_group;
    size_t num_columns = _tablet->num_columns();
    CompactionAlgorithm algorithm =
            CompactionUtils::choose_compaction_algorithm(num_columns, max_columns_per_group, segment_iterator_num);
    std::shared_ptr<CompactionTask> compaction_task;
    LOG(INFO) << "choose algorithm:" << algorithm << ", for tablet:" << _tablet->tablet_id()
              << ", segment_iterator_num:" << segment_iterator_num
              << ", max_columns_per_group:" << max_columns_per_group << ", num_columns:" << num_columns;
    if (algorithm == HORIZONTAL_COMPACTION) {
        compaction_task = std::make_shared<HorizontalCompactionTask>();
    } else if (algorithm == VERTICAL_COMPACTION) {
        compaction_task = std::make_shared<VerticalCompactionTask>();
    }
    // init the compaction task
    uint32_t input_rows_num = 0;
    size_t input_rowsets_size = 0;
    for (auto& rowset : _input_rowsets) {
        input_rows_num += rowset->num_rows();
        input_rowsets_size += rowset->data_disk_size();
    }
    compaction_task->set_task_id(CompactionManager::instance()->next_compaction_task_id());
    compaction_task->set_compaction_level(_compaction_level);
    compaction_task->set_input_rows_num(input_rows_num);
    compaction_task->set_input_rowsets(std::move(_input_rowsets));
    compaction_task->set_input_rowsets_size(input_rowsets_size);
    compaction_task->set_output_version(_output_version);
    compaction_task->set_tablet(_tablet);
    compaction_task->set_segment_iterator_num(segment_iterator_num);
    return compaction_task;
}

StatusOr<size_t> CompactionTaskFactory::_get_segment_iterator_num() {
    vectorized::Schema schema = vectorized::ChunkHelper::convert_schema_to_format_v2(_tablet->tablet_schema());
    vectorized::TabletReader reader(std::static_pointer_cast<Tablet>(_tablet->shared_from_this()), _output_version,
                                    schema);
    vectorized::TabletReaderParams reader_params;
    reader_params.reader_type = READER_LEVEL_COMPACTION;
    RETURN_IF_ERROR(reader.prepare());
    std::vector<ChunkIteratorPtr> seg_iters;
    RETURN_IF_ERROR(reader.get_segment_iterators(reader_params, &seg_iters));
    return seg_iters.size();
}

} // namespace starrocks
