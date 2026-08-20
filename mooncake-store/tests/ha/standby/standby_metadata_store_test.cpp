#include "ha/standby_metadata_store.h"

#include <gtest/gtest.h>

#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace mooncake::test {
namespace {

StandbyObjectMetadata MakeMetadata(uint64_t size) {
    StandbyObjectMetadata metadata;
    metadata.size = size;
    return metadata;
}

}  // namespace

static_assert(
    std::is_move_constructible_v<StandbyMetadataStore::SnapshotCursor>);
static_assert(
    !std::is_copy_constructible_v<StandbyMetadataStore::SnapshotCursor>);

TEST(StandbyMetadataStoreTest, EmptySnapshotCompletesWithoutObjects) {
    StandbyMetadataStore store;
    auto cursor = store.BeginSnapshotTraversal();
    std::vector<StandbyObjectEntry> chunk;

    ASSERT_TRUE(store.CopyNextSnapshotChunk(2, cursor, chunk));
    EXPECT_TRUE(chunk.empty());
    EXPECT_TRUE(cursor.done());
}

TEST(StandbyMetadataStoreTest, CopiesMultipleTenantsAcrossBoundedChunks) {
    StandbyMetadataStore store;
    ASSERT_TRUE(store.PutMetadata("tenant-a", "a1", MakeMetadata(1)));
    ASSERT_TRUE(store.PutMetadata("tenant-a", "a2", MakeMetadata(2)));
    ASSERT_TRUE(store.PutMetadata("tenant-b", "b1", MakeMetadata(3)));
    ASSERT_TRUE(store.PutMetadata("tenant-b", "b2", MakeMetadata(4)));
    ASSERT_TRUE(store.PutMetadata("tenant-c", "c1", MakeMetadata(5)));

    auto cursor = store.BeginSnapshotTraversal();
    std::set<std::pair<std::string, std::string>> copied;
    while (!cursor.done()) {
        std::vector<StandbyObjectEntry> chunk;
        ASSERT_TRUE(store.CopyNextSnapshotChunk(2, cursor, chunk));
        ASSERT_LE(chunk.size(), 2u);
        for (const auto& entry : chunk) {
            EXPECT_TRUE(copied.emplace(entry.tenant_id, entry.key).second);
        }
    }

    EXPECT_EQ(5u, copied.size());
    EXPECT_EQ(5u, store.GetKeyCount());
}

TEST(StandbyMetadataStoreTest, RejectsZeroSizedChunkWithoutAdvancing) {
    StandbyMetadataStore store;
    ASSERT_TRUE(store.PutMetadata("tenant", "key", MakeMetadata(1)));
    auto cursor = store.BeginSnapshotTraversal();
    std::vector<StandbyObjectEntry> chunk;

    EXPECT_FALSE(store.CopyNextSnapshotChunk(0, cursor, chunk));
    EXPECT_TRUE(chunk.empty());
    EXPECT_FALSE(cursor.done());
}

TEST(StandbyMetadataStoreTest, RestoreInsertRejectsDuplicateWithoutOverwrite) {
    StandbyMetadataStore store;
    ASSERT_TRUE(store.RestoreMetadata("tenant", "key", MakeMetadata(1)));

    EXPECT_FALSE(store.RestoreMetadata("tenant", "key", MakeMetadata(2)));
    auto restored = store.GetMetadata("tenant", "key");
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(1u, restored->size);
}

}  // namespace mooncake::test
