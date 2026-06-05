#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/catalog_backed_snapshot_provider.h"
#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"
#include "ha/snapshot/snapshot_test_utils.h"

namespace mooncake::test {

DEFINE_string(redis_endpoint, "",
              "Redis endpoint for catalog-backed snapshot provider tests");

namespace {

namespace fs = std::filesystem;

class CatalogBackedSnapshotProviderTest
    : public ::testing::TestWithParam<CatalogBackendParam> {
   protected:
    void SetUp() override {
        if (GetParam().requires_redis && FLAGS_redis_endpoint.empty()) {
            GTEST_SKIP() << "Redis endpoint is not configured";
        }

        cluster_id_ = "snapshot-provider-test-" + UuidToString(generate_uuid());
        temp_dir_ =
            MakeSnapshotTestTempDir("catalog_backed_snapshot_provider_");
        local_path_env_ =
            std::make_unique<ScopedEnvVar>(kSnapshotLocalPathEnv, temp_dir_);

        object_store_ =
            std::make_unique<LocalFileSnapshotObjectStore>(temp_dir_);
        catalog_store_ = CreateCatalogStoreForTest(
            GetParam(), object_store_.get(), cluster_id_, FLAGS_redis_endpoint);
        ASSERT_NE(catalog_store_, nullptr);

        descriptor_ = MakeTestSnapshotDescriptor();
    }

    void TearDown() override {
        if (snapshot_published_ && catalog_store_ != nullptr) {
            (void)catalog_store_->Delete(descriptor_.snapshot_id);
        }
        catalog_store_.reset();
        object_store_.reset();
        local_path_env_.reset();
        if (!temp_dir_.empty() && fs::exists(temp_dir_)) {
            fs::remove_all(temp_dir_);
        }
    }

    void PublishSnapshotPayload(
        SnapshotMetadataFormat format = SnapshotMetadataFormat::kLegacy) {
        auto result = mooncake::test::PublishSnapshotPayload(
            *object_store_, *catalog_store_, descriptor_, UUID{1, 2},
            kDefaultTestObjectKey, kDefaultTestDiskFilePath,
            kDefaultTestObjectSize, format);
        ASSERT_TRUE(result.has_value()) << result.error();
        snapshot_published_ = true;
    }

    // Loads the published snapshot and asserts the single default object
    // round-trips intact, regardless of the metadata on-wire format.
    void ExpectLoadsDefaultObject() {
        auto provider = CreateProvider();
        ASSERT_TRUE(provider.has_value()) << toString(provider.error());

        auto snapshot = provider.value()->LoadLatestSnapshot(cluster_id_);
        ASSERT_TRUE(snapshot.has_value()) << toString(snapshot.error());
        ASSERT_TRUE(snapshot->has_value());
        ASSERT_EQ(snapshot->value().metadata.size(), 1u);

        const auto& [tenant_id, key, metadata] =
            snapshot->value().metadata.front();
        EXPECT_EQ(tenant_id, "default");
        EXPECT_EQ(key, kDefaultTestObjectKey);
        EXPECT_EQ(metadata.client_id, (UUID{1, 2}));
        EXPECT_EQ(metadata.size, kDefaultTestObjectSize);
        ASSERT_EQ(metadata.replicas.size(), 1u);

        const auto& replica = metadata.replicas.front();
        EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
        ASSERT_TRUE(replica.is_disk_replica());
        EXPECT_EQ(replica.get_disk_descriptor().file_path,
                  kDefaultTestDiskFilePath);
        EXPECT_EQ(replica.get_disk_descriptor().object_size,
                  kDefaultTestObjectSize);
    }

    tl::expected<std::unique_ptr<SnapshotProvider>, ErrorCode> CreateProvider()
        const {
        return CreateCatalogBackedSnapshotProvider(MakeSnapshotProviderConfig(
            GetParam(), cluster_id_, FLAGS_redis_endpoint));
    }

    std::string cluster_id_;
    std::string temp_dir_;
    bool snapshot_published_{false};
    ha::SnapshotDescriptor descriptor_;
    std::unique_ptr<ScopedEnvVar> local_path_env_;
    std::unique_ptr<LocalFileSnapshotObjectStore> object_store_;
    std::unique_ptr<ha::SnapshotCatalogStore> catalog_store_;
};

TEST_P(CatalogBackedSnapshotProviderTest,
       LoadLatestSnapshotReturnsEmptyWhenCatalogMissing) {
    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    auto snapshot = provider.value()->LoadLatestSnapshot(cluster_id_);
    ASSERT_TRUE(snapshot.has_value()) << toString(snapshot.error());
    EXPECT_FALSE(snapshot->has_value());
}

TEST_P(CatalogBackedSnapshotProviderTest, LoadLatestSnapshotRoundTrip) {
    PublishSnapshotPayload();

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    auto snapshot = provider.value()->LoadLatestSnapshot(cluster_id_);
    ASSERT_TRUE(snapshot.has_value()) << toString(snapshot.error());
    ASSERT_TRUE(snapshot->has_value());
    EXPECT_EQ(snapshot->value().snapshot_id, descriptor_.snapshot_id);
    EXPECT_EQ(snapshot->value().snapshot_sequence_id,
              descriptor_.last_included_seq);
    ASSERT_EQ(snapshot->value().metadata.size(), 1u);

    // The test snapshot's segment payload is built from an empty
    // SegmentManager (BuildSegmentsPayload), so the loaded snapshot
    // must report no StandbySegmentInfo entries. This pins the contract
    // documented at the extraction site in
    // catalog_backed_snapshot_provider.cpp: only memory segments
    // (mounted_segments_) populate snapshot.segments; local-disk and
    // NoF segments arrive via SEGMENT_MOUNT OpLog replay instead.
    EXPECT_TRUE(snapshot->value().segments.empty())
        << "Empty segment manager must produce zero StandbySegmentInfo "
           "entries";

    const auto& entry = snapshot->value().metadata.front();
    EXPECT_EQ(entry.key, kDefaultTestObjectKey);
    EXPECT_EQ(entry.metadata.client_id, (UUID{1, 2}));
    EXPECT_EQ(entry.metadata.size, kDefaultTestObjectSize);
    EXPECT_EQ(entry.metadata.last_sequence_id, descriptor_.last_included_seq);
    ASSERT_EQ(entry.metadata.replicas.size(), 1u);

    const auto& replica = entry.metadata.replicas.front();
    EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
    ASSERT_TRUE(replica.is_disk_replica());
    EXPECT_EQ(replica.get_disk_descriptor().file_path,
              kDefaultTestDiskFilePath);
    EXPECT_EQ(replica.get_disk_descriptor().object_size,
              kDefaultTestObjectSize);
}

// The master snapshot writer evolved its per-object metadata layout over time
// (data_type field, trailing hard_pinned flag, trailing group_id). The standby
// restore reader must accept every shape; otherwise it rejects the snapshot and
// falls back to OpLog-only bootstrap. These tests pin each on-wire format.

TEST_P(CatalogBackedSnapshotProviderTest, LoadLatestSnapshotWithDataTypeField) {
    // 8 + replica_count: data_type packed right after replica_count.
    PublishSnapshotPayload(SnapshotMetadataFormat::kDataTypeOnly);
    ExpectLoadsDefaultObject();
}

TEST_P(CatalogBackedSnapshotProviderTest,
       LoadLatestSnapshotWithHardPinnedField) {
    // 8 + replica_count: trailing hard_pinned flag, no data_type. Exercises the
    // type-based disambiguation (first replica is not a positive integer).
    PublishSnapshotPayload(SnapshotMetadataFormat::kHardPinnedOnly);
    ExpectLoadsDefaultObject();
}

TEST_P(CatalogBackedSnapshotProviderTest,
       LoadLatestSnapshotWithDataTypeAndHardPinned) {
    // 9 + replica_count: data_type + trailing hard_pinned.
    PublishSnapshotPayload(SnapshotMetadataFormat::kDataTypeAndHardPinned);
    ExpectLoadsDefaultObject();
}

TEST_P(CatalogBackedSnapshotProviderTest, LoadLatestSnapshotWithGroupId) {
    // 10 + replica_count: current writer format (data_type + hard_pinned +
    // trailing group_id). Regression test for the live snapshot restore
    // failure against the latest metadata layout.
    PublishSnapshotPayload(SnapshotMetadataFormat::kWithGroupId);
    ExpectLoadsDefaultObject();
}

TEST_P(CatalogBackedSnapshotProviderTest, RejectsOverflowingReplicaCount) {
    // A near-UINT32_MAX replica_count must not wrap the format-detection
    // arithmetic into a valid-looking total and slip an out-of-bounds index
    // past the size check. The entry packs zero replicas (array size 7) but
    // declares UINT32_MAX, so every format must be rejected, not parsed.
    auto published = mooncake::test::PublishSnapshotPayloadBytes(
        *object_store_, *catalog_store_, descriptor_,
        BuildMetadataPayloadWithDeclaredReplicaCount(
            std::numeric_limits<uint32_t>::max()));
    ASSERT_TRUE(published.has_value()) << published.error();
    snapshot_published_ = true;

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    auto snapshot = provider.value()->LoadLatestSnapshot(cluster_id_);
    ASSERT_FALSE(snapshot.has_value());
    EXPECT_EQ(snapshot.error(), ErrorCode::DESERIALIZE_FAIL);
}

TEST_P(CatalogBackedSnapshotProviderTest, RejectsClusterMismatch) {
    PublishSnapshotPayload();

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    auto snapshot =
        provider.value()->LoadLatestSnapshot(cluster_id_ + "-other");
    ASSERT_FALSE(snapshot.has_value());
    EXPECT_EQ(snapshot.error(), ErrorCode::INVALID_PARAMS);
}

INSTANTIATE_TEST_SUITE_P(
    SnapshotCatalogBackends, CatalogBackedSnapshotProviderTest,
    ::testing::ValuesIn(BuildCatalogBackendParams()),
    [](const ::testing::TestParamInfo<CatalogBackendParam>& info) {
        return info.param.name;
    });

}  // namespace
}  // namespace mooncake::test

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    const int result = RUN_ALL_TESTS();
    google::ShutdownGoogleLogging();
    return result;
}
