#include "storage/local/log_structured/store.h"

#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

namespace mooncake::logstructured {
namespace {

class StoreTempDirectory {
   public:
    StoreTempDirectory() {
        const auto id = next_id_.fetch_add(1, std::memory_order_relaxed);
        path_ = std::filesystem::temp_directory_path() /
                ("mooncake-log-store-test-" + std::to_string(getpid()) + "-" +
                 std::to_string(id));
        std::filesystem::create_directories(path_);
    }

    ~StoreTempDirectory() { std::filesystem::remove_all(path_); }

    const std::filesystem::path& path() const { return path_; }

   private:
    inline static std::atomic<uint64_t> next_id_{0};
    std::filesystem::path path_;
};

RecordIdentity StoreIdentity(std::string key, uint64_t incarnation) {
    return RecordIdentity{
        .tenant_id = "tenant-a",
        .object_key = std::move(key),
        .incarnation = ObjectIncarnation{.high = 5, .low = incarnation},
    };
}

LogStructuredStoreConfig Config(const StoreTempDirectory& temp,
                                uint64_t max_segment_bytes = 1024 * 1024) {
    return LogStructuredStoreConfig{.root_path = temp.path().string(),
                                    .max_segment_bytes = max_segment_bytes,
                                    .sync_data = true,
                                    .sync_wal = true};
}

TEST(LogStructuredStoreTest, PutIsInvisibleUntilMasterCommit) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp));
    ASSERT_TRUE(store.has_value());
    const auto identity = StoreIdentity("key", 1);

    auto prepared = (*store)->PreparePut(identity, "value");
    ASSERT_TRUE(prepared.has_value());
    EXPECT_EQ((*store)->Get(identity).error(), StoreError::kNotFound);
    ASSERT_TRUE((*store)->CommitPut(identity, prepared->sequence).has_value());
    EXPECT_EQ((*store)->Get(identity).value(), "value");
}

TEST(LogStructuredStoreTest, GeneratesIncarnationsForLatestLookup) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp));
    ASSERT_TRUE(store.has_value());

    auto first = (*store)->PreparePut("tenant-a", "key", "first");
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(first->identity, first->sequence).has_value());
    auto second = (*store)->PreparePut("tenant-a", "key", "second");
    ASSERT_TRUE(second.has_value());
    ASSERT_NE(first->identity.incarnation, second->identity.incarnation);
    ASSERT_TRUE(
        (*store)->CommitPut(second->identity, second->sequence).has_value());

    EXPECT_EQ((*store)->GetLatest("tenant-a", "key").value(), "second");
    EXPECT_TRUE((*store)->ContainsLatest("tenant-a", "key"));
    ASSERT_EQ((*store)->SnapshotCurrentIndex().size(), size_t{1});
}

TEST(LogStructuredStoreTest, AbortedUpdateDoesNotHideCommittedIncarnation) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp));
    ASSERT_TRUE(store.has_value());
    const auto old_identity = StoreIdentity("key", 1);
    const auto new_identity = StoreIdentity("key", 2);

    auto old_write = (*store)->PreparePut(old_identity, "old-value");
    ASSERT_TRUE(old_write.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(old_identity, old_write->sequence).has_value());
    auto new_write = (*store)->PreparePut(new_identity, "new-value");
    ASSERT_TRUE(new_write.has_value());
    ASSERT_TRUE(
        (*store)->AbortPut(new_identity, new_write->sequence).has_value());

    EXPECT_EQ((*store)->Get(old_identity).value(), "old-value");
    EXPECT_EQ((*store)->Get(new_identity).error(), StoreError::kNotFound);
}

TEST(LogStructuredStoreTest, RestartRecoversCommittedAndTombstonedState) {
    StoreTempDirectory temp;
    const auto live = StoreIdentity("live", 1);
    const auto deleted = StoreIdentity("deleted", 1);
    {
        auto store = LogStructuredStore::Open(Config(temp));
        ASSERT_TRUE(store.has_value());
        auto live_write = (*store)->PreparePut(live, "live-value");
        auto deleted_write = (*store)->PreparePut(deleted, "dead-value");
        ASSERT_TRUE(live_write.has_value());
        ASSERT_TRUE(deleted_write.has_value());
        ASSERT_TRUE(
            (*store)->CommitPut(live, live_write->sequence).has_value());
        ASSERT_TRUE(
            (*store)->CommitPut(deleted, deleted_write->sequence).has_value());
        ASSERT_TRUE((*store)->Delete(deleted).has_value());
    }

    auto recovered = LogStructuredStore::Open(Config(temp));
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ((*recovered)->Get(live).value(), "live-value");
    EXPECT_EQ((*recovered)->Get(deleted).error(), StoreError::kNotFound);
    EXPECT_GT((*recovered)->next_sequence(), uint64_t{3});
}

TEST(LogStructuredStoreTest, RotatesSegmentsWithoutChangingLookup) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp, 256));
    ASSERT_TRUE(store.has_value());
    const auto first = StoreIdentity("first", 1);
    const auto second = StoreIdentity("second", 1);

    auto first_write = (*store)->PreparePut(first, std::string(128, 'a'));
    ASSERT_TRUE(first_write.has_value());
    ASSERT_TRUE((*store)->CommitPut(first, first_write->sequence).has_value());
    const uint64_t first_segment = (*store)->active_segment_id();
    auto second_write = (*store)->PreparePut(second, std::string(128, 'b'));
    ASSERT_TRUE(second_write.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(second, second_write->sequence).has_value());

    EXPECT_GT((*store)->active_segment_id(), first_segment);
    EXPECT_EQ((*store)->Get(first).value(), std::string(128, 'a'));
    EXPECT_EQ((*store)->Get(second).value(), std::string(128, 'b'));
}

TEST(LogStructuredStoreTest, CheckpointPublishesManifestAndRotatesWal) {
    StoreTempDirectory temp;
    const auto before_checkpoint = StoreIdentity("before", 1);
    const auto after_checkpoint = StoreIdentity("after", 1);
    {
        auto store = LogStructuredStore::Open(Config(temp));
        ASSERT_TRUE(store.has_value());
        auto first = (*store)->PreparePut(before_checkpoint, "first");
        ASSERT_TRUE(first.has_value());
        ASSERT_TRUE((*store)
                        ->CommitPut(before_checkpoint, first->sequence)
                        .has_value());
        ASSERT_TRUE((*store)->Checkpoint().has_value());
        EXPECT_TRUE(std::filesystem::exists(temp.path() / "CURRENT"));
        EXPECT_FALSE(
            std::filesystem::exists(temp.path() / "WAL-00000000000000000001"));
        EXPECT_TRUE(
            std::filesystem::exists(temp.path() / "WAL-00000000000000000002"));

        auto second = (*store)->PreparePut(after_checkpoint, "second");
        ASSERT_TRUE(second.has_value());
        ASSERT_TRUE((*store)
                        ->CommitPut(after_checkpoint, second->sequence)
                        .has_value());
    }

    auto recovered = LogStructuredStore::Open(Config(temp));
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ((*recovered)->Get(before_checkpoint).value(), "first");
    EXPECT_EQ((*recovered)->Get(after_checkpoint).value(), "second");
}

TEST(LogStructuredStoreTest, RecoversSegmentCreatedAfterCheckpoint) {
    StoreTempDirectory temp;
    const auto first_identity = StoreIdentity("first", 1);
    const auto second_identity = StoreIdentity("second", 1);
    {
        auto store = LogStructuredStore::Open(Config(temp, 256));
        ASSERT_TRUE(store.has_value());
        auto first =
            (*store)->PreparePut(first_identity, std::string(128, 'a'));
        ASSERT_TRUE(first.has_value());
        ASSERT_TRUE(
            (*store)->CommitPut(first_identity, first->sequence).has_value());
        ASSERT_TRUE((*store)->Checkpoint().has_value());

        auto second =
            (*store)->PreparePut(second_identity, std::string(128, 'b'));
        ASSERT_TRUE(second.has_value());
        ASSERT_TRUE(
            (*store)->CommitPut(second_identity, second->sequence).has_value());
        EXPECT_EQ((*store)->active_segment_id(), uint64_t{2});
    }

    auto recovered = LogStructuredStore::Open(Config(temp, 256));
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ((*recovered)->active_segment_id(), uint64_t{2});
    EXPECT_EQ((*recovered)->Get(first_identity).value(), std::string(128, 'a'));
    EXPECT_EQ((*recovered)->Get(second_identity).value(),
              std::string(128, 'b'));
}

TEST(LogStructuredStoreTest, RepairsTornWalAndSegmentTailsOnRestart) {
    StoreTempDirectory temp;
    const auto identity = StoreIdentity("key", 1);
    uint64_t segment_id = 0;
    {
        auto store = LogStructuredStore::Open(Config(temp));
        ASSERT_TRUE(store.has_value());
        auto write = (*store)->PreparePut(identity, "value");
        ASSERT_TRUE(write.has_value());
        ASSERT_TRUE((*store)->CommitPut(identity, write->sequence).has_value());
        segment_id = (*store)->active_segment_id();
    }

    const auto segment_path = temp.path() / "segments" /
                              ("segment-" + std::string(19, '0') +
                               std::to_string(segment_id) + ".log");
    {
        std::ofstream segment(segment_path, std::ios::binary | std::ios::app);
        segment << "torn-segment";
        std::ofstream wal(temp.path() / "WAL-00000000000000000001",
                          std::ios::binary | std::ios::app);
        wal << "torn-wal";
    }

    auto recovered = LogStructuredStore::Open(Config(temp));
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ((*recovered)->Get(identity).value(), "value");
}

}  // namespace
}  // namespace mooncake::logstructured
