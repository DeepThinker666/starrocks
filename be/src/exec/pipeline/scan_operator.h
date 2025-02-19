// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include "exec/pipeline/source_operator.h"
#include "exec/workgroup/work_group_fwd.h"

namespace starrocks {

class PriorityThreadPool;
class ScanNode;

namespace pipeline {

class ScanOperator : public SourceOperator {
public:
    ScanOperator(OperatorFactory* factory, int32_t id, ScanNode* scan_node);

    ~ScanOperator() override = default;

    Status prepare(RuntimeState* state) override;

    void close(RuntimeState* state) override;

    bool has_output() const override;

    bool pending_finish() const override;

    bool is_finished() const override;

    void set_finishing(RuntimeState* state) override;

    StatusOr<vectorized::ChunkPtr> pull_chunk(RuntimeState* state) override;

    void set_io_threads(PriorityThreadPool* io_threads) { _io_threads = io_threads; }
    void set_workgroup(workgroup::WorkGroupPtr wg);

    // interface for different scan node
    virtual Status do_prepare(RuntimeState* state) = 0;
    virtual void do_close(RuntimeState* state) = 0;
    virtual ChunkSourcePtr create_chunk_source(MorselPtr morsel) = 0;

protected:
    ScanNode* _scan_node = nullptr;

private:
    static constexpr int MAX_IO_TASKS_PER_OP = 4;

    const size_t _buffer_size = config::pipeline_io_buffer_size;

    // This method is only invoked when current morsel is reached eof
    // and all cached chunk of this morsel has benn read out
    Status _pickup_morsel(RuntimeState* state, int chunk_source_index);
    Status _trigger_next_scan(RuntimeState* state, int chunk_source_index);
    Status _try_to_trigger_next_scan(RuntimeState* state);

    bool _is_finished = false;

    int32_t _io_task_retry_cnt = 0;
    PriorityThreadPool* _io_threads = nullptr;
    std::atomic<int> _num_running_io_tasks = 0;
    std::vector<std::atomic<bool>> _is_io_task_running;
    std::vector<ChunkSourcePtr> _chunk_sources;

    workgroup::WorkGroupPtr _workgroup = nullptr;
};

class ScanOperatorFactory : public SourceOperatorFactory {
public:
    ScanOperatorFactory(int32_t id, ScanNode* scan_node);

    ~ScanOperatorFactory() override = default;

    OperatorPtr create(int32_t degree_of_parallelism, int32_t driver_sequence) override;

    bool with_morsels() const override { return true; }

    Status prepare(RuntimeState* state) override;
    void close(RuntimeState* state) override;

    // interface for different scan node
    virtual Status do_prepare(RuntimeState* state) = 0;
    virtual void do_close(RuntimeState* state) = 0;
    virtual OperatorPtr do_create(int32_t dop, int32_t driver_sequence) = 0;

protected:
    ScanNode* _scan_node;
};

pipeline::OpFactories decompose_scan_node_to_pipeline(std::shared_ptr<ScanOperatorFactory> factory, ScanNode* scan_node,
                                                      pipeline::PipelineBuilderContext* context);

} // namespace pipeline
} // namespace starrocks
