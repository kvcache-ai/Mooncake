#include "storage/local/log_structured/store.h"

#include <fcntl.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stop_token>
#include <string>

#include <gtest/gtest.h>

namespace mooncake::logstructured {
namespace {

class StoreTempDirectory {
   public:
    StoreTempDirectory() {
        const auto id = next_id_.fetch_add(1, std::memory_order_relaxed);
        const char* tmpdir = std::getenv("TMPDIR");
        const std::filesystem::path base =
            tmpdir == nullptr ? std::filesystem::temp_directory_path()
                              : std::filesystem::path(tmpdir);
        path_ = base / ("mooncake-log-store-test-" + std::to_string(getpid()) +
                        "-" + std::to_string(id));
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

std::filesystem::path SegmentFile(const StoreTempDirectory& temp,
                                  uint64_t segment_id) {
    std::ostringstream name;
    name << "segment-" << std::setw(20) << std::setfill('0') << segment_id
         << ".log";
    return temp.path() / "segments" / name.str();
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

TEST(LogStructuredStoreTest, ReclaimsFullyDeadSealedSegment) {
    StoreTempDirectory temp;
    const auto first = StoreIdentity("key", 1);
    const auto second = StoreIdentity("key", 2);
    auto store = LogStructuredStore::Open(Config(temp, 256));
    ASSERT_TRUE(store.has_value());

    auto old_write = (*store)->PreparePut(first, std::string(96, 'a'));
    ASSERT_TRUE(old_write.has_value());
    ASSERT_TRUE((*store)->CommitPut(first, old_write->sequence).has_value());
    const uint64_t dead_segment = old_write->physical.segment_id;

    auto new_write = (*store)->PreparePut(second, std::string(96, 'b'));
    ASSERT_TRUE(new_write.has_value());
    ASSERT_TRUE((*store)->CommitPut(second, new_write->sequence).has_value());
    ASSERT_NE(new_write->physical.segment_id, dead_segment);

    auto compacted = (*store)->CompactOnce({.max_source_segments = 1,
                                            .max_input_bytes = 1024 * 1024,
                                            .min_reclaim_ratio = 0.0,
                                            .stop_token = {}});
    ASSERT_TRUE(compacted.has_value());
    EXPECT_EQ(compacted->source_segments, size_t{1});
    EXPECT_EQ(compacted->target_segments, size_t{0});
    EXPECT_EQ(compacted->reclaimed_bytes, compacted->input_bytes);
    EXPECT_FALSE(
        std::filesystem::exists(temp.path() / "segments" /
                                ("segment-" + std::string(19, '0') +
                                 std::to_string(dead_segment) + ".log")));
    EXPECT_EQ((*store)->GetLatest("tenant-a", "key").value(),
              std::string(96, 'b'));
}

TEST(LogStructuredStoreTest, CompactsLiveRecordsAndRecoversAfterRestart) {
    StoreTempDirectory temp;
    const auto first = StoreIdentity("first", 1);
    const auto second = StoreIdentity("second", 1);
    const auto replacement = StoreIdentity("first", 2);
    {
        auto store = LogStructuredStore::Open(Config(temp, 512));
        ASSERT_TRUE(store.has_value());
        auto first_write = (*store)->PreparePut(first, std::string(80, 'a'));
        ASSERT_TRUE(first_write.has_value());
        ASSERT_TRUE(
            (*store)->CommitPut(first, first_write->sequence).has_value());
        auto second_write = (*store)->PreparePut(second, std::string(80, 'b'));
        ASSERT_TRUE(second_write.has_value());
        ASSERT_TRUE(
            (*store)->CommitPut(second, second_write->sequence).has_value());
        ASSERT_EQ(first_write->physical.segment_id,
                  second_write->physical.segment_id);
        const uint64_t source_segment = first_write->physical.segment_id;

        auto replacement_write =
            (*store)->PreparePut(replacement, std::string(160, 'c'));
        ASSERT_TRUE(replacement_write.has_value());
        ASSERT_TRUE((*store)
                        ->CommitPut(replacement, replacement_write->sequence)
                        .has_value());
        ASSERT_NE(replacement_write->physical.segment_id, source_segment);

        auto compacted = (*store)->CompactOnce({.max_source_segments = 1,
                                                .max_input_bytes = 1024 * 1024,
                                                .min_reclaim_ratio = 0.0,
                                                .stop_token = {}});
        ASSERT_TRUE(compacted.has_value());
        EXPECT_EQ(compacted->source_segments, size_t{1});
        EXPECT_EQ(compacted->target_segments, size_t{1});
        EXPECT_GT(compacted->reclaimed_bytes, uint64_t{0});
        EXPECT_EQ((*store)->Get(second).value(), std::string(80, 'b'));
        EXPECT_EQ((*store)->GetLatest("tenant-a", "first").value(),
                  std::string(160, 'c'));
    }

    auto recovered = LogStructuredStore::Open(Config(temp, 512));
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ((*recovered)->Get(second).value(), std::string(80, 'b'));
    EXPECT_EQ((*recovered)->GetLatest("tenant-a", "first").value(),
              std::string(160, 'c'));
}

TEST(LogStructuredStoreTest, PreparedValueRemainsInvisibleAfterRestart) {
    StoreTempDirectory temp;
    const auto identity = StoreIdentity("prepared", 1);
    {
        auto store = LogStructuredStore::Open(Config(temp));
        ASSERT_TRUE(store.has_value());
        ASSERT_TRUE((*store)->PreparePut(identity, "uncommitted").has_value());
    }

    auto recovered = LogStructuredStore::Open(Config(temp));
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ((*recovered)->Get(identity).error(), StoreError::kNotFound);
    const auto snapshot = (*recovered)->SnapshotIndex();
    ASSERT_EQ(snapshot.size(), size_t{1});
    EXPECT_EQ(snapshot[0].identity, identity);
    EXPECT_EQ(snapshot[0].version.state, VersionState::kPrepared);
}

TEST(LogStructuredStoreTest, MissingCommittedCompactionTargetFailsClosed) {
    StoreTempDirectory temp;
    const auto first = StoreIdentity("first", 1);
    const auto second = StoreIdentity("second", 1);
    uint64_t target_segment = 0;
    {
        auto store = LogStructuredStore::Open(Config(temp, 512));
        ASSERT_TRUE(store.has_value());
        auto first_write = (*store)->PreparePut(first, std::string(80, 'a'));
        auto second_write = (*store)->PreparePut(second, std::string(80, 'b'));
        ASSERT_TRUE(first_write.has_value());
        ASSERT_TRUE(second_write.has_value());
        ASSERT_TRUE(
            (*store)->CommitPut(first, first_write->sequence).has_value());
        ASSERT_TRUE(
            (*store)->CommitPut(second, second_write->sequence).has_value());
        ASSERT_TRUE((*store)->SealActiveSegment().has_value());
        auto compacted = (*store)->CompactOnce({.max_source_segments = 1,
                                                .max_input_bytes = 4096,
                                                .max_target_bytes = 4096,
                                                .fanout = 1,
                                                .max_levels = 2,
                                                .min_reclaim_ratio = 1.0,
                                                .enable_tiering = true,
                                                .stop_token = {}});
        ASSERT_TRUE(compacted.has_value());
        ASSERT_EQ(compacted->target_segments, size_t{1});
        for (const auto& entry : (*store)->SnapshotCurrentIndex()) {
            if (entry.identity == first) {
                target_segment = entry.version.physical.segment_id;
            }
        }
        ASSERT_NE(target_segment, uint64_t{0});
    }

    ASSERT_TRUE(std::filesystem::remove(SegmentFile(temp, target_segment)));
    auto recovered = LogStructuredStore::Open(Config(temp, 512));
    ASSERT_FALSE(recovered.has_value());
    EXPECT_EQ(recovered.error(), StoreError::kCorruptData);
}

TEST(LogStructuredStoreTest, CorruptCommittedCompactionTargetFailsClosed) {
    StoreTempDirectory temp;
    const auto identity = StoreIdentity("key", 1);
    uint64_t target_segment = 0;
    PhysicalRecord target_record;
    {
        auto store = LogStructuredStore::Open(Config(temp, 128));
        ASSERT_TRUE(store.has_value());
        auto write = (*store)->PreparePut(identity, std::string(96, 'a'));
        ASSERT_TRUE(write.has_value());
        ASSERT_TRUE((*store)->CommitPut(identity, write->sequence).has_value());
        ASSERT_TRUE((*store)->SealActiveSegment().has_value());
        auto compacted = (*store)->CompactOnce({.max_source_segments = 1,
                                                .max_input_bytes = 4096,
                                                .max_target_bytes = 4096,
                                                .fanout = 1,
                                                .max_levels = 2,
                                                .min_reclaim_ratio = 1.0,
                                                .enable_tiering = true,
                                                .stop_token = {}});
        ASSERT_TRUE(compacted.has_value());
        ASSERT_EQ(compacted->target_segments, size_t{1});
        const auto snapshot = (*store)->SnapshotCurrentIndex();
        ASSERT_EQ(snapshot.size(), size_t{1});
        target_record = snapshot[0].version.physical;
        target_segment = target_record.segment_id;
    }

    const int fd =
        open(SegmentFile(temp, target_segment).c_str(), O_RDWR | O_CLOEXEC);
    ASSERT_GE(fd, 0);
    char byte = 0;
    ASSERT_EQ(pread(fd, &byte, 1, target_record.value_offset), 1);
    byte ^= 0x40;
    ASSERT_EQ(pwrite(fd, &byte, 1, target_record.value_offset), 1);
    ASSERT_EQ(close(fd), 0);

    auto recovered = LogStructuredStore::Open(Config(temp, 128));
    ASSERT_FALSE(recovered.has_value());
    EXPECT_EQ(recovered.error(), StoreError::kCorruptData);
}

TEST(LogStructuredStoreTest, RemovesUnpublishedCompactionTargetOnRecovery) {
    StoreTempDirectory temp;
    const auto identity = StoreIdentity("key", 1);
    uint64_t orphan_segment = 0;
    {
        auto store = LogStructuredStore::Open(Config(temp));
        ASSERT_TRUE(store.has_value());
        auto write = (*store)->PreparePut(identity, "value");
        ASSERT_TRUE(write.has_value());
        ASSERT_TRUE((*store)->CommitPut(identity, write->sequence).has_value());
        ASSERT_TRUE((*store)->Checkpoint().has_value());
        orphan_segment = (*store)->active_segment_id() + 1;
    }

    const auto orphan_path = SegmentFile(temp, orphan_segment);
    auto writer = SegmentWriter::Create(orphan_path.string(), orphan_segment);
    ASSERT_TRUE(writer.has_value());
    ASSERT_TRUE(
        (*writer)
            ->Append(identity, "value", RecordKind::kCompactionCopy, 1, true)
            .has_value());
    writer.value().reset();

    auto recovered = LogStructuredStore::Open(Config(temp));
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ((*recovered)->Get(identity).value(), "value");
    EXPECT_FALSE(std::filesystem::exists(orphan_path));
}

TEST(LogStructuredStoreTest, ReportsPhysicalAndReclaimableBytes) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp, 128));
    ASSERT_TRUE(store.has_value());

    auto first = (*store)->PreparePut("tenant-a", "key", std::string(96, 'a'));
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(first->identity, first->sequence).has_value());
    auto second = (*store)->PreparePut("tenant-a", "key", std::string(96, 'b'));
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(second->identity, second->sequence).has_value());

    const auto stats = (*store)->SnapshotStats();
    EXPECT_GT(stats.physical_bytes, stats.live_record_bytes);
    EXPECT_EQ(stats.reclaimable_bytes,
              stats.physical_bytes - stats.live_record_bytes);
    EXPECT_EQ(stats.logical_value_bytes, uint64_t{96});
    EXPECT_EQ(stats.active_segments, size_t{1});
    EXPECT_GE(stats.sealed_segments, size_t{1});
}

TEST(LogStructuredStoreTest, CompactionHonorsTemporarySpaceBudget) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp, 512));
    ASSERT_TRUE(store.has_value());
    const auto stale = StoreIdentity("first", 1);
    const auto live = StoreIdentity("second", 1);
    const auto replacement = StoreIdentity("first", 2);

    auto stale_write = (*store)->PreparePut(stale, std::string(80, 'a'));
    ASSERT_TRUE(stale_write.has_value());
    ASSERT_TRUE((*store)->CommitPut(stale, stale_write->sequence).has_value());
    auto live_write = (*store)->PreparePut(live, std::string(80, 'b'));
    ASSERT_TRUE(live_write.has_value());
    ASSERT_TRUE((*store)->CommitPut(live, live_write->sequence).has_value());
    auto replacement_write =
        (*store)->PreparePut(replacement, std::string(160, 'c'));
    ASSERT_TRUE(replacement_write.has_value());
    ASSERT_TRUE((*store)
                    ->CommitPut(replacement, replacement_write->sequence)
                    .has_value());

    const auto before = (*store)->SnapshotStats();
    auto deferred = (*store)->CompactOnce({.max_source_segments = 1,
                                           .max_input_bytes = 4096,
                                           .max_target_bytes = 4096,
                                           .max_temporary_bytes = 0,
                                           .min_reclaim_ratio = 0.0,
                                           .stop_token = {}});
    ASSERT_TRUE(deferred.has_value());
    EXPECT_EQ(deferred->source_segments, size_t{0});
    EXPECT_EQ((*store)->SnapshotStats().physical_bytes, before.physical_bytes);
    EXPECT_EQ((*store)->Get(live).value(), std::string(80, 'b'));

    auto compacted = (*store)->CompactOnce({.max_source_segments = 1,
                                            .max_input_bytes = 4096,
                                            .max_target_bytes = 4096,
                                            .max_temporary_bytes = 4096,
                                            .min_reclaim_ratio = 0.0,
                                            .stop_token = {}});
    ASSERT_TRUE(compacted.has_value());
    EXPECT_EQ(compacted->source_segments, size_t{1});
    EXPECT_GT(compacted->reclaimed_bytes, uint64_t{0});
    EXPECT_EQ((*store)->Get(live).value(), std::string(80, 'b'));
    EXPECT_EQ((*store)->Get(replacement).value(), std::string(160, 'c'));
}

TEST(LogStructuredStoreTest, CancelledCompactionLeavesSourcesReadable) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp, 128));
    ASSERT_TRUE(store.has_value());

    auto first = (*store)->PreparePut("tenant-a", "key", std::string(96, 'a'));
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(first->identity, first->sequence).has_value());
    auto second = (*store)->PreparePut("tenant-a", "key", std::string(96, 'b'));
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(second->identity, second->sequence).has_value());
    ASSERT_TRUE((*store)->SealActiveSegment().has_value());

    std::stop_source stop_source;
    stop_source.request_stop();
    auto compacted =
        (*store)->CompactOnce({.max_source_segments = 8,
                               .max_input_bytes = 4096,
                               .max_target_bytes = 4096,
                               .min_reclaim_ratio = 0.0,
                               .stop_token = stop_source.get_token()});
    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error(), StoreError::kCancelled);
    EXPECT_EQ((*store)->GetLatest("tenant-a", "key").value(),
              std::string(96, 'b'));
}

TEST(LogStructuredStoreTest, CompactionHonorsBandwidthLimit) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp, 64 * 1024));
    ASSERT_TRUE(store.has_value());

    const auto live = StoreIdentity("live", 1);
    const auto stale = StoreIdentity("stale", 1);
    const auto replacement = StoreIdentity("stale", 2);
    for (const auto& identity : {live, stale}) {
        auto write = (*store)->PreparePut(identity, std::string(8 * 1024, 'a'));
        ASSERT_TRUE(write.has_value());
        ASSERT_TRUE((*store)->CommitPut(identity, write->sequence).has_value());
    }
    ASSERT_TRUE((*store)->SealActiveSegment().has_value());
    auto replacement_write =
        (*store)->PreparePut(replacement, std::string(8 * 1024, 'b'));
    ASSERT_TRUE(replacement_write.has_value());
    ASSERT_TRUE((*store)
                    ->CommitPut(replacement, replacement_write->sequence)
                    .has_value());

    const auto started = std::chrono::steady_clock::now();
    auto compacted = (*store)->CompactOnce({.max_source_segments = 1,
                                            .max_input_bytes = 64 * 1024,
                                            .max_target_bytes = 64 * 1024,
                                            .fanout = 2,
                                            .max_levels = 2,
                                            .min_reclaim_ratio = 0.0,
                                            .max_bytes_per_second = 32 * 1024,
                                            .stop_token = {}});
    const auto elapsed = std::chrono::steady_clock::now() - started;
    ASSERT_TRUE(compacted.has_value());
    EXPECT_EQ(compacted->source_segments, size_t{1});
    EXPECT_GE(elapsed, std::chrono::milliseconds(200));
    EXPECT_EQ((*store)->Get(live).value(), std::string(8 * 1024, 'a'));
}

TEST(LogStructuredStoreTest,
     ConcurrentMutationRejectsStaleCompactionPublication) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp, 256 * 1024));
    ASSERT_TRUE(store.has_value());
    const auto updated = StoreIdentity("updated", 1);
    const auto replacement_identity = StoreIdentity("updated", 2);
    const auto deleted = StoreIdentity("deleted", 1);
    const std::string old_value(32 * 1024, 'a');
    const std::string deleted_value(32 * 1024, 'b');

    auto old_write = (*store)->PreparePut(updated, old_value);
    ASSERT_TRUE(old_write.has_value());
    ASSERT_TRUE((*store)->CommitPut(updated, old_write->sequence).has_value());
    auto deleted_write = (*store)->PreparePut(deleted, deleted_value);
    ASSERT_TRUE(deleted_write.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(deleted, deleted_write->sequence).has_value());
    ASSERT_TRUE((*store)->SealActiveSegment().has_value());

    auto compaction = std::async(std::launch::async, [&]() {
        return (*store)->CompactOnce({.max_source_segments = 1,
                                      .max_input_bytes = 1024 * 1024,
                                      .max_target_bytes = 1024 * 1024,
                                      .fanout = 1,
                                      .max_levels = 2,
                                      .min_reclaim_ratio = 1.0,
                                      .max_bytes_per_second = 256 * 1024,
                                      .enable_tiering = true,
                                      .stop_token = {}});
    });

    const auto temporary_path = temp.path() / "tmp";
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    bool copy_started = false;
    do {
        for (const auto& entry :
             std::filesystem::directory_iterator(temporary_path)) {
            if (entry.is_regular_file() && entry.file_size() != 0) {
                copy_started = true;
                break;
            }
        }
        if (!copy_started) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    } while (!copy_started && std::chrono::steady_clock::now() < deadline);
    ASSERT_TRUE(copy_started);

    const std::string new_value(32 * 1024, 'c');
    auto replacement = (*store)->PreparePut(replacement_identity, new_value);
    ASSERT_TRUE(replacement.has_value());
    ASSERT_TRUE((*store)
                    ->CommitPut(replacement_identity, replacement->sequence)
                    .has_value());
    ASSERT_TRUE((*store)->Delete(deleted).has_value());

    auto compacted = compaction.get();
    ASSERT_FALSE(compacted.has_value());
    EXPECT_EQ(compacted.error(), StoreError::kInvalidTransition);
    EXPECT_EQ((*store)->Get(updated).error(), StoreError::kNotFound);
    EXPECT_EQ((*store)->Get(replacement_identity).value(), new_value);
    EXPECT_EQ((*store)->Get(deleted).error(), StoreError::kNotFound);
    EXPECT_TRUE(std::filesystem::is_empty(temporary_path));
}

TEST(LogStructuredStoreTest,
     CompactionInputBudgetAllowsOnlyOneOversizedSource) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp, 128));
    ASSERT_TRUE(store.has_value());

    for (int i = 0; i < 3; ++i) {
        const auto old_identity = StoreIdentity("key-" + std::to_string(i), 1);
        auto old_write =
            (*store)->PreparePut(old_identity, std::string(96, 'a' + i));
        ASSERT_TRUE(old_write.has_value());
        ASSERT_TRUE(
            (*store)->CommitPut(old_identity, old_write->sequence).has_value());

        const auto new_identity = StoreIdentity("key-" + std::to_string(i), 2);
        auto new_write =
            (*store)->PreparePut(new_identity, std::string(96, 'd' + i));
        ASSERT_TRUE(new_write.has_value());
        ASSERT_TRUE(
            (*store)->CommitPut(new_identity, new_write->sequence).has_value());
    }
    ASSERT_TRUE((*store)->SealActiveSegment().has_value());

    auto compacted = (*store)->CompactOnce({.max_source_segments = 3,
                                            .max_input_bytes = 1,
                                            .max_target_bytes = 1024,
                                            .fanout = 2,
                                            .max_levels = 2,
                                            .min_reclaim_ratio = 0.0,
                                            .stop_token = {}});
    ASSERT_TRUE(compacted.has_value());
    EXPECT_EQ(compacted->source_segments, size_t{1});
}

TEST(LogStructuredStoreTest, TieredCompactionMergesCleanSegments) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp, 256));
    ASSERT_TRUE(store.has_value());
    std::vector<RecordIdentity> identities;
    for (uint64_t i = 0; i < 5; ++i) {
        identities.push_back(StoreIdentity("key-" + std::to_string(i), 1));
        auto write =
            (*store)->PreparePut(identities.back(), std::string(96, 'a' + i));
        ASSERT_TRUE(write.has_value());
        ASSERT_TRUE((*store)
                        ->CommitPut(identities.back(), write->sequence)
                        .has_value());
    }

    auto compacted = (*store)->CompactOnce({.max_source_segments = 4,
                                            .max_input_bytes = 1024 * 1024,
                                            .max_target_bytes = 1024,
                                            .fanout = 4,
                                            .max_levels = 4,
                                            .min_reclaim_ratio = 1.0,
                                            .enable_tiering = true,
                                            .stop_token = {}});
    ASSERT_TRUE(compacted.has_value());
    EXPECT_EQ(compacted->source_segments, size_t{4});
    EXPECT_EQ(compacted->target_segments, size_t{1});
    EXPECT_EQ(compacted->reclaimed_bytes, uint64_t{0});
    for (size_t i = 0; i < identities.size(); ++i) {
        EXPECT_EQ((*store)->Get(identities[i]).value(),
                  std::string(96, 'a' + i));
    }
}

TEST(LogStructuredStoreTest, AppendFailurePreservesCommittedValueAfterRestart) {
    StoreTempDirectory temp;
    const auto old_identity = StoreIdentity("key", 1);
    const auto new_identity = StoreIdentity("key", 2);

    {
        auto store = LogStructuredStore::Open(Config(temp));
        ASSERT_TRUE(store.has_value());
        auto old_write = (*store)->PreparePut(old_identity, "old-value");
        ASSERT_TRUE(old_write.has_value());
        ASSERT_TRUE(
            (*store)->CommitPut(old_identity, old_write->sequence).has_value());

        std::atomic<size_t> writes{0};
        SegmentWriter::SetWriteFailurePredicateForTest(
            [&](std::string_view, uint64_t, size_t) {
                return writes.fetch_add(1, std::memory_order_relaxed) == 1;
            });
        auto failed = (*store)->PreparePut(new_identity, "new-value");
        SegmentWriter::SetWriteFailurePredicateForTest({});
        ASSERT_FALSE(failed.has_value());
        EXPECT_EQ(failed.error(), StoreError::kIoError);
        EXPECT_EQ((*store)->Get(old_identity).value(), "old-value");
        EXPECT_EQ((*store)->Get(new_identity).error(), StoreError::kNotFound);
    }

    auto recovered = LogStructuredStore::Open(Config(temp));
    ASSERT_TRUE(recovered.has_value());
    EXPECT_EQ((*recovered)->Get(old_identity).value(), "old-value");
    EXPECT_EQ((*recovered)->Get(new_identity).error(), StoreError::kNotFound);
}

TEST(LogStructuredStoreTest,
     CompactionTargetWriteFailurePreservesSourcesAndLatestValue) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp, 1024));
    ASSERT_TRUE(store.has_value());
    const auto old_identity = StoreIdentity("key", 1);
    const auto new_identity = StoreIdentity("key", 2);

    auto old_write = (*store)->PreparePut(old_identity, std::string(96, 'a'));
    ASSERT_TRUE(old_write.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(old_identity, old_write->sequence).has_value());
    auto new_write = (*store)->PreparePut(new_identity, std::string(96, 'b'));
    ASSERT_TRUE(new_write.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(new_identity, new_write->sequence).has_value());
    ASSERT_TRUE((*store)->SealActiveSegment().has_value());

    SegmentWriter::SetWriteFailurePredicateForTest([](std::string_view path,
                                                      uint64_t, size_t) {
        return std::filesystem::path(path).parent_path().filename() == "tmp";
    });
    auto failed = (*store)->CompactOnce({.max_source_segments = 1,
                                         .max_input_bytes = 4096,
                                         .max_target_bytes = 4096,
                                         .fanout = 1,
                                         .max_levels = 2,
                                         .min_reclaim_ratio = 0.0,
                                         .stop_token = {}});
    SegmentWriter::SetWriteFailurePredicateForTest({});
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), StoreError::kIoError);
    EXPECT_EQ((*store)->GetLatest("tenant-a", "key").value(),
              std::string(96, 'b'));
    EXPECT_TRUE(std::filesystem::is_empty(temp.path() / "tmp"));
    EXPECT_EQ((*store)->SnapshotStats().sealed_segments, size_t{1});

    auto compacted = (*store)->CompactOnce({.max_source_segments = 1,
                                            .max_input_bytes = 4096,
                                            .max_target_bytes = 4096,
                                            .fanout = 1,
                                            .max_levels = 2,
                                            .min_reclaim_ratio = 0.0,
                                            .stop_token = {}});
    ASSERT_TRUE(compacted.has_value());
    EXPECT_EQ(compacted->source_segments, size_t{1});
    EXPECT_EQ((*store)->GetLatest("tenant-a", "key").value(),
              std::string(96, 'b'));
}

TEST(LogStructuredStoreTest, RecoversAcrossCompactionPublicationCrashPoints) {
    constexpr std::array crash_points{
        CompactionCrashPoint::kBeforeTargetSync,
        CompactionCrashPoint::kAfterTargetSync,
        CompactionCrashPoint::kAfterTargetRename,
        CompactionCrashPoint::kBeforeManifestWrite,
        CompactionCrashPoint::kAfterManifestWrite,
        CompactionCrashPoint::kAfterManifestPublication,
        CompactionCrashPoint::kAfterSourceUnlink,
    };

    for (const auto crash_point : crash_points) {
        StoreTempDirectory temp;
        const auto old_identity = StoreIdentity("key", 1);
        const auto new_identity = StoreIdentity("key", 2);
        {
            auto store = LogStructuredStore::Open(Config(temp, 1024));
            ASSERT_TRUE(store.has_value());
            auto old_write =
                (*store)->PreparePut(old_identity, std::string(96, 'a'));
            ASSERT_TRUE(old_write.has_value());
            ASSERT_TRUE((*store)
                            ->CommitPut(old_identity, old_write->sequence)
                            .has_value());
            auto new_write =
                (*store)->PreparePut(new_identity, std::string(96, 'b'));
            ASSERT_TRUE(new_write.has_value());
            ASSERT_TRUE((*store)
                            ->CommitPut(new_identity, new_write->sequence)
                            .has_value());
            ASSERT_TRUE((*store)->SealActiveSegment().has_value());

            LogStructuredStore::SetCompactionCrashPredicateForTest(
                [crash_point](CompactionCrashPoint point) {
                    return point == crash_point;
                });
            auto interrupted = (*store)->CompactOnce({.max_source_segments = 1,
                                                      .max_input_bytes = 4096,
                                                      .max_target_bytes = 4096,
                                                      .fanout = 1,
                                                      .max_levels = 2,
                                                      .min_reclaim_ratio = 0.0,
                                                      .stop_token = {}});
            LogStructuredStore::SetCompactionCrashPredicateForTest({});
            ASSERT_FALSE(interrupted.has_value());
            EXPECT_EQ(interrupted.error(), StoreError::kIoError);
        }

        for (int restart = 0; restart < 3; ++restart) {
            auto recovered = LogStructuredStore::Open(Config(temp, 1024));
            ASSERT_TRUE(recovered.has_value()) << "restart=" << restart;
            EXPECT_EQ((*recovered)->GetLatest("tenant-a", "key").value(),
                      std::string(96, 'b'))
                << "restart=" << restart;
            EXPECT_EQ((*recovered)->Get(old_identity).error(),
                      StoreError::kNotFound)
                << "restart=" << restart;
            EXPECT_TRUE(std::filesystem::is_empty(temp.path() / "tmp"))
                << "restart=" << restart;
        }
    }
}

TEST(LogStructuredStoreTest, GetCompletesWhileCompactionWaitsToPublish) {
    StoreTempDirectory temp;
    auto store = LogStructuredStore::Open(Config(temp, 1024));
    ASSERT_TRUE(store.has_value());
    const auto old_identity = StoreIdentity("key", 1);
    const auto new_identity = StoreIdentity("key", 2);

    auto old_write = (*store)->PreparePut(old_identity, std::string(96, 'a'));
    ASSERT_TRUE(old_write.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(old_identity, old_write->sequence).has_value());
    auto new_write = (*store)->PreparePut(new_identity, std::string(96, 'b'));
    ASSERT_TRUE(new_write.has_value());
    ASSERT_TRUE(
        (*store)->CommitPut(new_identity, new_write->sequence).has_value());
    ASSERT_TRUE((*store)->SealActiveSegment().has_value());

    std::promise<void> reached_publish;
    std::promise<void> allow_publish;
    auto allow_publish_future = allow_publish.get_future().share();
    LogStructuredStore::SetCompactionCrashPredicateForTest(
        [&](CompactionCrashPoint point) {
            if (point != CompactionCrashPoint::kAfterTargetRename) return false;
            reached_publish.set_value();
            allow_publish_future.wait();
            return false;
        });
    auto compaction = std::async(std::launch::async, [&]() {
        return (*store)->CompactOnce({.max_source_segments = 1,
                                      .max_input_bytes = 4096,
                                      .max_target_bytes = 4096,
                                      .fanout = 1,
                                      .max_levels = 2,
                                      .min_reclaim_ratio = 0.0,
                                      .stop_token = {}});
    });
    ASSERT_EQ(reached_publish.get_future().wait_for(std::chrono::seconds(2)),
              std::future_status::ready);

    auto read = std::async(std::launch::async,
                           [&]() { return (*store)->Get(new_identity); });
    ASSERT_EQ(read.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    ASSERT_TRUE(read.get().has_value());

    allow_publish.set_value();
    auto compacted = compaction.get();
    LogStructuredStore::SetCompactionCrashPredicateForTest({});
    ASSERT_TRUE(compacted.has_value());
    EXPECT_EQ((*store)->GetLatest("tenant-a", "key").value(),
              std::string(96, 'b'));
}

}  // namespace
}  // namespace mooncake::logstructured
