// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <vector>

#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {

class Tablet;
class DataDir;
class CompactionTask;

// 磁盘compaction任务并发控制
// maybe Scheduler should register to CompactionManager for concurrent scheduler
class CompactionScheduler {
public:
    CompactionScheduler();
    ~CompactionScheduler() = default;

    void schedule();

    void notify();

private:
    // wait until current running tasks are below max_concurrent_num
    // 有可能两个scheduler都被唤醒了，所以如果支持并发scheduler，这里需要考虑并发控制
    void _wait_to_run();

    bool _can_schedule_next();

    Tablet* try_get_next_tablet();

private:
    PriorityThreadPool _compaction_pool;

    std::mutex _mutex;
    std::condition_variable _cv;
    uint64_t _round;
};

} // namespace starrocks
