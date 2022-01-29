// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/compaction_context.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <memory>
#include <random>

#include "storage/tablet_schema.h"
#include "storage/rowset/beta_rowset.h"

namespace starrocks {

// (k1 int, k2 varchar(20), k3 int) duplicated key (k1, k2)
void create_tablet_schema(TabletSchema* tablet_schema) {
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_keys_type(DUP_KEYS);
    tablet_schema_pb.set_num_short_key_columns(2);
    tablet_schema_pb.set_num_rows_per_row_block(1024);
    tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
    tablet_schema_pb.set_next_column_unique_id(4);

    ColumnPB* column_1 = tablet_schema_pb.add_column();
    column_1->set_unique_id(1);
    column_1->set_name("k1");
    column_1->set_type("INT");
    column_1->set_is_key(true);
    column_1->set_length(4);
    column_1->set_index_length(4);
    column_1->set_is_nullable(true);
    column_1->set_is_bf_column(false);

    ColumnPB* column_2 = tablet_schema_pb.add_column();
    column_2->set_unique_id(2);
    column_2->set_name("k2");
    column_2->set_type("INT"); // TODO change to varchar(20) when dict encoding for string is supported
    column_2->set_length(4);
    column_2->set_index_length(4);
    column_2->set_is_nullable(true);
    column_2->set_is_key(true);
    column_2->set_is_nullable(true);
    column_2->set_is_bf_column(false);

    ColumnPB* column_3 = tablet_schema_pb.add_column();
    column_3->set_unique_id(3);
    column_3->set_name("v1");
    column_3->set_type("INT");
    column_3->set_length(4);
    column_3->set_is_key(false);
    column_3->set_is_nullable(false);
    column_3->set_is_bf_column(false);
    column_3->set_aggregation("SUM");

    tablet_schema->init_from_pb(tablet_schema_pb);
}

TEST(CompactionContextTest, test_rowset_comparator) {
    std::set<Rowset*, RowsetComparator> sorted_rowsets_set;

    std::vector<RowsetSharedPtr> rowsets;
    TabletSchema tablet_schema;
    create_tablet_schema(&tablet_schema);

    RowsetMetaSharedPtr base_rowset_meta = std::make_shared<RowsetMeta>();
    base_rowset_meta->set_start_version(0);
    base_rowset_meta->set_end_version(9);
    RowsetSharedPtr base_rowset = std::make_shared<BetaRowset>(&tablet_schema, "./rowset_0", base_rowset_meta);
    rowsets.emplace_back(std::move(base_rowset));

    for (int i = 1; i <= 10; i++) {
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->set_start_version(i * 10);
        rowset_meta->set_end_version((i + 1) * 10 -1);
        RowsetSharedPtr rowset = std::make_shared<BetaRowset>(&tablet_schema, "./rowset" + std::to_string(i), rowset_meta);
        rowsets.emplace_back(std::move(rowset));
    }

    for (int i = 110; i < 120; i++) {
        RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
        rowset_meta->set_start_version(i);
        rowset_meta->set_end_version(i);
        RowsetSharedPtr rowset = std::make_shared<BetaRowset>(&tablet_schema, "./rowset" + std::to_string(i), rowset_meta);
        rowsets.emplace_back(std::move(rowset));
    }

    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(rowsets.begin(), rowsets.end(), g);

    for (auto& rowset : rowsets) {
        sorted_rowsets_set.insert(rowset.get());
    }

    bool is_sorted = true;
    Rowset * pre_rowset = nullptr;
    for (auto& rowset : sorted_rowsets_set) {
        if (pre_rowset != nullptr) {
            if (!(rowset->start_version() == pre_rowset->end_version() + 1 && rowset->start_version() <= rowset->end_version())) {
                is_sorted = false;
                break;
            }
        }
        pre_rowset = rowset;
    }
    ASSERT_EQ(true, is_sorted);
}

} // namespace starrocks
