// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_set>
#include <vector>

#include "storage/compaction_task.h"
#include "storage/olap_common.h"
#include "storage/rowset/rowset.h"
#include "storage/tablet.h"
#include "util/priority_thread_pool.hpp"

namespace starrocks {

class CompactionScheduler;

class CompactionManager {
public:
    ~CompactionManager() = default;

    static CompactionManager* instance();

    size_t candidates_size() {
        std::lock_guard lg(_candidates_mutex);
        return _candidate_tablets.size();
    }

    void update_candidate_async(Tablet* tablet);

    void update_candidate(Tablet* tablet);

    void insert_candidates(const std::vector<Tablet*>& tablets);

    Tablet* pick_candidate();

    void register_scheduler(CompactionScheduler* scheduler) {
        std::lock_guard lg(_scheduler_mutex);
        _schedulers.push_back(scheduler);
    }

    bool register_task(CompactionTask* compaction_task);

    void unregister_task(CompactionTask* compaction_task);

    uint16_t running_tasks_num() { return _running_tasks_num; }

    uint16_t running_tasks_num_for_dir(DataDir* data_dir) {
        std::lock_guard lg(_tasks_mutex);
        return _data_dir_to_task_num_map[data_dir];
    }

    uint16_t running_tasks_num_for_level(uint8_t level) {
        std::lock_guard lg(_tasks_mutex);
        return _level_to_task_num_map[level];
    }

    uint64_t next_compaction_task_id() { return ++_next_task_id; }

    void print_log();

private:
    CompactionManager() : _update_candidate_pool("up_candidates", 1, 100000) {}
    CompactionManager(const CompactionManager& compaction_manager) = delete;
    CompactionManager(CompactionManager&& compaction_manager) = delete;
    CompactionManager& operator=(const CompactionManager& compaction_manager) = delete;
    CompactionManager& operator=(CompactionManager&& compaction_manager) = delete;

    void _notify_schedulers();

    // Comparator should compare tablet by compaction score
    // When compaction score is equal, use tablet id(to be unique) instead
    struct TabletCompactionComparator {
        bool operator()(const Tablet* left, const Tablet* right) const {
            int32_t left_score = static_cast<int32_t>(left->compaction_score() * 100);
            int32_t right_score = static_cast<int32_t>(right->compaction_score() * 100);
            return left_score > right_score || (left_score == right_score && left->tablet_id() < right->tablet_id());
        }
    };

    std::mutex _candidates_mutex;
    // protect by _mutex
    std::set<Tablet*, TabletCompactionComparator> _candidate_tablets;

    std::mutex _tasks_mutex;
    std::atomic<uint64_t> _next_task_id;
    std::atomic<uint16_t> _running_tasks_num;
    std::unordered_set<CompactionTask*> _running_tasks;
    std::unordered_map<DataDir*, uint16_t> _data_dir_to_task_num_map;
    std::unordered_map<uint8_t, uint16_t> _level_to_task_num_map;
    std::thread _log_thread;
    bool _log_thread_inited = false;
    PriorityThreadPool _update_candidate_pool;

    std::mutex _scheduler_mutex;
    std::vector<CompactionScheduler*> _schedulers;

    static std::unique_ptr<CompactionManager> _instance;
};

} // namespace starrocks
