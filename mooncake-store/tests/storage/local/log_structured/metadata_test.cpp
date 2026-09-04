#include "storage/local/log_structured/metadata.h"

#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

namespace mooncake::logstructured {
namespace {

class MetadataTempDirectory {
   public:
    MetadataTempDirectory() {
        const auto id = next_id_.fetch_add(1, std::memory_order_relaxed);
        path_ = std::filesystem::temp_directory_path() /
                ("mooncake-log-metadata-test-" + std::to_string(getpid()) +
                 "-" + std::to_string(id));
        std::filesystem::create_directories(path_);
    }

    ~MetadataTempDirectory() { std::filesystem::remove_all(path_); }

    const std::filesystem::path& path() const { return path_; }

   private:
    inline static std::atomic<uint64_t> next_id_{0};
    std::filesystem::path path_;
};

CheckpointState Checkpoint() {
    CheckpointState checkpoint;
    checkpoint.checkpoint_sequence = 7;
    checkpoint.next_sequence = 8;
    checkpoint.next_segment_id = 3;
    checkpoint.applied_delete_watermark = 6;
    checkpoint.index.push_back(IndexSnapshotEntry{
        .identity = RecordIdentity{.tenant_id = "tenant",
                                   .object_key = "key",
                                   .incarnation = {.high = 1, .low = 2}},
        .version = VersionEntry{.physical = {.segment_id = 1,
                                             .record_offset = 0,
                                             .value_offset = 80,
                                             .value_length = 5,
                                             .total_length = 112},
                                .state = VersionState::kCommitted,
                                .sequence = 7,
                                .mutation_epoch = 2}});
    checkpoint.segments.push_back(
        SegmentMetadata{.segment_id = 1,
                        .level = 0,
                        .state = SegmentLifecycle::kActive,
                        .valid_bytes = 112,
                        .live_bytes = 112,
                        .record_count = 1,
                        .mutation_epoch = 1});
    return checkpoint;
}

TEST(LogStructuredMetadataTest, PublishesAndLoadsCheckpointManifest) {
    MetadataTempDirectory temp;
    auto checkpoint_file =
        WriteCheckpoint(temp.path().string(), 1, Checkpoint());
    ASSERT_TRUE(checkpoint_file.has_value());

    ManifestState manifest{
        .format_version = 1,
        .generation = 1,
        .checkpoint_sequence = 7,
        .next_sequence = 8,
        .next_segment_id = 3,
        .active_segment_id = 1,
        .checkpoint_file = *checkpoint_file,
        .wal_file = "WAL-000002",
        .segments = Checkpoint().segments,
    };
    ASSERT_TRUE(PublishManifest(temp.path().string(), manifest).has_value());

    auto loaded_manifest = LoadCurrentManifest(temp.path().string());
    ASSERT_TRUE(loaded_manifest.has_value());
    EXPECT_EQ(loaded_manifest->generation, uint64_t{1});
    EXPECT_EQ(loaded_manifest->segments, manifest.segments);
    auto loaded_checkpoint =
        LoadCheckpoint(temp.path().string(), loaded_manifest->checkpoint_file);
    ASSERT_TRUE(loaded_checkpoint.has_value());
    EXPECT_EQ(loaded_checkpoint->checkpoint_sequence, uint64_t{7});
    ASSERT_EQ(loaded_checkpoint->index.size(), size_t{1});
    EXPECT_EQ(loaded_checkpoint->index[0].identity.object_key, "key");
    EXPECT_EQ(loaded_checkpoint->index[0].version.state,
              VersionState::kCommitted);
}

TEST(LogStructuredMetadataTest, CurrentIgnoresUnpublishedManifest) {
    MetadataTempDirectory temp;
    auto checkpoint_file =
        WriteCheckpoint(temp.path().string(), 1, Checkpoint());
    ASSERT_TRUE(checkpoint_file.has_value());
    ManifestState manifest{
        .format_version = 1,
        .generation = 1,
        .checkpoint_sequence = 7,
        .next_sequence = 8,
        .next_segment_id = 3,
        .active_segment_id = 1,
        .checkpoint_file = *checkpoint_file,
        .wal_file = "WAL-000002",
        .segments = Checkpoint().segments,
    };
    ASSERT_TRUE(PublishManifest(temp.path().string(), manifest).has_value());

    std::ofstream uncommitted(temp.path() / "MANIFEST-00000000000000000002",
                              std::ios::binary);
    uncommitted << "not-published";
    uncommitted.close();

    auto loaded = LoadCurrentManifest(temp.path().string());
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->generation, uint64_t{1});
}

TEST(LogStructuredMetadataTest, RejectsCorruptCheckpoint) {
    MetadataTempDirectory temp;
    auto checkpoint_file =
        WriteCheckpoint(temp.path().string(), 1, Checkpoint());
    ASSERT_TRUE(checkpoint_file.has_value());
    const auto path = temp.path() / *checkpoint_file;
    const auto size = std::filesystem::file_size(path);
    std::filesystem::resize_file(path, size - 1);

    auto loaded = LoadCheckpoint(temp.path().string(), *checkpoint_file);
    ASSERT_FALSE(loaded.has_value());
    EXPECT_EQ(loaded.error(), MetadataError::kCorruptData);
}

}  // namespace
}  // namespace mooncake::logstructured
