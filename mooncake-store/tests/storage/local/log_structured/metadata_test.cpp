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

TEST(LogStructuredMetadataTest, RejectsZeroCheckpointGeneration) {
    MetadataTempDirectory temp;
    auto checkpoint = WriteCheckpoint(temp.path().string(), 0, Checkpoint());
    ASSERT_FALSE(checkpoint.has_value());
    EXPECT_EQ(checkpoint.error(), MetadataError::kInvalidArgument);
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

TEST(LogStructuredMetadataTest, ReportsUncertainCurrentPublication) {
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

    auto published = PublishManifest(temp.path().string(), manifest, {},
                                     [] { return true; });
    ASSERT_FALSE(published.has_value());
    EXPECT_EQ(published.error(), MetadataError::kPublicationUncertain);
    auto loaded = LoadCurrentManifest(temp.path().string());
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->generation, uint64_t{1});
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

TEST(LogStructuredMetadataTest, RejectsMismatchedManifestGeneration) {
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
        .wal_file = "WAL-00000000000000000001",
        .segments = Checkpoint().segments,
    };
    ASSERT_TRUE(PublishManifest(temp.path().string(), manifest).has_value());

    std::filesystem::copy_file(temp.path() / "MANIFEST-00000000000000000001",
                               temp.path() / "MANIFEST-00000000000000000002");
    std::ofstream(temp.path() / "CURRENT", std::ios::binary | std::ios::trunc)
        << "MANIFEST-00000000000000000002\n";

    auto loaded = LoadCurrentManifest(temp.path().string());
    ASSERT_FALSE(loaded.has_value());
    EXPECT_EQ(loaded.error(), MetadataError::kCorruptData);
}

TEST(LogStructuredMetadataTest, RejectsMismatchedManifestReferences) {
    MetadataTempDirectory temp;
    ManifestState manifest{
        .format_version = 1,
        .generation = 2,
        .checkpoint_sequence = 7,
        .next_sequence = 8,
        .next_segment_id = 3,
        .active_segment_id = 1,
        .checkpoint_file = "CHECKPOINT-00000000000000000001",
        .wal_file = "WAL-00000000000000000002",
        .segments = Checkpoint().segments,
    };

    auto published = PublishManifest(temp.path().string(), manifest);
    ASSERT_FALSE(published.has_value());
    EXPECT_EQ(published.error(), MetadataError::kInvalidArgument);
}

TEST(LogStructuredMetadataTest, CleansUnreferencedMetadataArtifacts) {
    MetadataTempDirectory temp;
    auto first_checkpoint =
        WriteCheckpoint(temp.path().string(), 1, Checkpoint());
    ASSERT_TRUE(first_checkpoint.has_value());
    ManifestState first_manifest{
        .format_version = 1,
        .generation = 1,
        .checkpoint_sequence = 7,
        .next_sequence = 8,
        .next_segment_id = 3,
        .active_segment_id = 1,
        .checkpoint_file = *first_checkpoint,
        .wal_file = "WAL-00000000000000000001",
        .segments = Checkpoint().segments,
    };
    ASSERT_TRUE(
        PublishManifest(temp.path().string(), first_manifest).has_value());

    auto second_checkpoint =
        WriteCheckpoint(temp.path().string(), 2, Checkpoint());
    ASSERT_TRUE(second_checkpoint.has_value());
    ManifestState second_manifest = first_manifest;
    second_manifest.generation = 2;
    second_manifest.checkpoint_file = *second_checkpoint;
    second_manifest.wal_file = "WAL-00000000000000000002";
    ASSERT_TRUE(
        PublishManifest(temp.path().string(), second_manifest).has_value());

    std::ofstream(temp.path() / first_manifest.wal_file) << "old";
    std::ofstream(temp.path() / second_manifest.wal_file) << "current";
    std::ofstream(temp.path() / "MANIFEST-00000000000000000003.tmp")
        << "partial";
    std::ofstream(temp.path() / "user.tmp") << "unmanaged";

    ASSERT_TRUE(CleanupMetadataArtifacts(temp.path().string(), 2,
                                         second_manifest.wal_file)
                    .has_value());
    EXPECT_FALSE(std::filesystem::exists(temp.path() / *first_checkpoint));
    EXPECT_FALSE(
        std::filesystem::exists(temp.path() / "MANIFEST-00000000000000000001"));
    EXPECT_FALSE(
        std::filesystem::exists(temp.path() / first_manifest.wal_file));
    EXPECT_FALSE(std::filesystem::exists(temp.path() /
                                         "MANIFEST-00000000000000000003.tmp"));
    EXPECT_TRUE(std::filesystem::exists(temp.path() / *second_checkpoint));
    EXPECT_TRUE(
        std::filesystem::exists(temp.path() / "MANIFEST-00000000000000000002"));
    EXPECT_TRUE(
        std::filesystem::exists(temp.path() / second_manifest.wal_file));
    EXPECT_TRUE(std::filesystem::exists(temp.path() / "user.tmp"));

    auto loaded = LoadCurrentManifest(temp.path().string());
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->generation, uint64_t{2});
}

TEST(LogStructuredMetadataTest, CleansArtifactsBeforeFirstPublication) {
    MetadataTempDirectory temp;
    std::ofstream(temp.path() / "WAL-00000000000000000000") << "current";
    std::ofstream(temp.path() / "WAL-00000000000000000001") << "orphan";
    std::ofstream(temp.path() / "CHECKPOINT-00000000000000000001") << "orphan";
    std::ofstream(temp.path() / "MANIFEST-00000000000000000001") << "orphan";

    ASSERT_TRUE(CleanupMetadataArtifacts(temp.path().string(), 0,
                                         "WAL-00000000000000000000")
                    .has_value());
    EXPECT_TRUE(
        std::filesystem::exists(temp.path() / "WAL-00000000000000000000"));
    EXPECT_FALSE(
        std::filesystem::exists(temp.path() / "WAL-00000000000000000001"));
    EXPECT_FALSE(std::filesystem::exists(temp.path() /
                                         "CHECKPOINT-00000000000000000001"));
    EXPECT_FALSE(
        std::filesystem::exists(temp.path() / "MANIFEST-00000000000000000001"));
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
