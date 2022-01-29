// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/compaction_manager.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <memory>
#include <random>

#include "storage/tablet_updates.h"
#include "storage/tablet.h"
#include "storage/compaction_context.h"

namespace starrocks {

TEST(CompactionManagerTest, test_candidates) {
    std::vector<TabletSharedPtr> tablets;
    DataDir data_dir("./data_dir");
    for (int i = 0; i <= 10; i++) {
        TabletSharedPtr tablet = std::make_shared<Tablet>();
        TabletMetaSharedPtr tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->set_tablet_id(i);
        tablet->set_tablet_meta(tablet_meta);
        tablet->set_data_dir(&data_dir);
        std::unique_ptr<CompactionContext> compaction_context = std::make_unique<CompactionContext>();
        compaction_context->tablet = tablet.get();
        compaction_context->current_level = 0;
        // for i == 9 and i == 10, compaction scores are equal
        if (i == 10) {
            compaction_context->compaction_scores[0] = 10;
        } else {
            compaction_context->compaction_scores[0] = 1 + i;
        }
        compaction_context->compaction_scores[1] = 1 + 0.5 * i;
        tablet->set_compaction_context(compaction_context);
        tablets.push_back(tablet);
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(tablets.begin(), tablets.end(), g);

    for (auto& tablet : tablets) {
        CompactionManager::instance()->update_candidate(tablet.get());
    }

    ASSERT_EQ(11, CompactionManager::instance()->candidates_size());
    Tablet* candidate_1 = CompactionManager::instance()->pick_candidate();
    ASSERT_EQ(9, candidate_1->tablet_id());
    Tablet* candidate_2 = CompactionManager::instance()->pick_candidate();
    ASSERT_EQ(10, candidate_2->tablet_id());
    ASSERT_EQ(candidate_1->compaction_score(), candidate_2->compaction_score());
    double last_score = candidate_2->compaction_score();
    for (int i = 8; i >=0; i--) {
        Tablet* candidate = CompactionManager::instance()->pick_candidate();
        ASSERT_EQ(i, candidate->tablet_id());
        ASSERT_LT(candidate->compaction_score(), last_score);
        last_score = candidate->compaction_score();
    }
}

TEST(CompactionManagerTest, test_compaction_tasks) {
}

} // namespace starrocks
