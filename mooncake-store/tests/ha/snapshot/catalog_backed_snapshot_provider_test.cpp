#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <chrono>
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

    void PublishSnapshotPayload() {
        auto result = mooncake::test::PublishSnapshotPayload(
            *object_store_, *catalog_store_, descriptor_);
        ASSERT_TRUE(result.has_value()) << result.error();
        snapshot_published_ = true;
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

    auto snapshot = provider.value()->LoadLatestSnapshot(
        cluster_id_, SnapshotRestoreMode::kColdRestore);
    ASSERT_TRUE(snapshot.has_value()) << toString(snapshot.error());
    EXPECT_FALSE(snapshot->has_value());
}

TEST_P(CatalogBackedSnapshotProviderTest, LoadLatestSnapshotRoundTrip) {
    PublishSnapshotPayload();

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    auto snapshot = provider.value()->LoadLatestSnapshot(
        cluster_id_, SnapshotRestoreMode::kColdRestore);
    ASSERT_TRUE(snapshot.has_value()) << toString(snapshot.error());
    ASSERT_TRUE(snapshot->has_value());
    EXPECT_EQ(snapshot->value().snapshot_id, descriptor_.snapshot_id);
    EXPECT_EQ(snapshot->value().snapshot_sequence_id,
              descriptor_.last_included_seq);
    ASSERT_EQ(snapshot->value().metadata.size(), 1u);

    const auto& [key, metadata] = snapshot->value().metadata.front();
    EXPECT_EQ(key, kDefaultTestObjectKey);
    EXPECT_EQ(metadata.client_id, (UUID{1, 2}));
    EXPECT_EQ(metadata.size, kDefaultTestObjectSize);
    EXPECT_EQ(metadata.last_sequence_id,
              snapshot->value().snapshot_sequence_id);
    ASSERT_EQ(metadata.replicas.size(), 1u);

    const auto& replica = metadata.replicas.front();
    EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
    ASSERT_TRUE(replica.is_disk_replica());
    EXPECT_EQ(replica.get_disk_descriptor().file_path,
              kDefaultTestDiskFilePath);
    EXPECT_EQ(replica.get_disk_descriptor().object_size,
              kDefaultTestObjectSize);
}

TEST_P(CatalogBackedSnapshotProviderTest,
       LoadLatestSnapshotColdRestoreDropsLeaseExpiredObject) {
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    auto publish_result = mooncake::test::PublishSnapshotPayload(
        *object_store_, *catalog_store_, descriptor_, UUID{1, 2},
        kDefaultTestObjectKey, kDefaultTestDiskFilePath, kDefaultTestObjectSize,
        /*lease_timeout_ms=*/now_ms - 1000,
        /*has_soft_pin_timeout=*/true,
        /*soft_pin_timeout_ms=*/now_ms + 60000);
    ASSERT_TRUE(publish_result.has_value()) << publish_result.error();
    snapshot_published_ = true;

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    auto snapshot = provider.value()->LoadLatestSnapshot(
        cluster_id_, SnapshotRestoreMode::kColdRestore);
    ASSERT_TRUE(snapshot.has_value()) << toString(snapshot.error());
    ASSERT_TRUE(snapshot->has_value());
    EXPECT_EQ(snapshot->value().snapshot_id, descriptor_.snapshot_id);
    EXPECT_EQ(snapshot->value().snapshot_sequence_id,
              descriptor_.last_included_seq);
    EXPECT_TRUE(snapshot->value().metadata.empty());
}

TEST_P(CatalogBackedSnapshotProviderTest,
       LoadLatestSnapshotOplogCatchupKeepsLeaseExpiredObject) {
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    auto publish_result = mooncake::test::PublishSnapshotPayload(
        *object_store_, *catalog_store_, descriptor_, UUID{1, 2},
        kDefaultTestObjectKey, kDefaultTestDiskFilePath, kDefaultTestObjectSize,
        /*lease_timeout_ms=*/now_ms - 1000,
        /*has_soft_pin_timeout=*/true,
        /*soft_pin_timeout_ms=*/now_ms + 60000);
    ASSERT_TRUE(publish_result.has_value()) << publish_result.error();
    snapshot_published_ = true;

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    auto snapshot = provider.value()->LoadLatestSnapshot(
        cluster_id_, SnapshotRestoreMode::kStandbyCatchupWithOplog);
    ASSERT_TRUE(snapshot.has_value()) << toString(snapshot.error());
    ASSERT_TRUE(snapshot->has_value());
    ASSERT_EQ(1u, snapshot->value().metadata.size());
    EXPECT_EQ(kDefaultTestObjectKey, snapshot->value().metadata.front().first);
    EXPECT_EQ(kDefaultTestObjectSize,
              snapshot->value().metadata.front().second.size);
    EXPECT_EQ(descriptor_.last_included_seq,
              snapshot->value().metadata.front().second.last_sequence_id);
}

TEST_P(CatalogBackedSnapshotProviderTest, RejectsClusterMismatch) {
    PublishSnapshotPayload();

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    auto snapshot = provider.value()->LoadLatestSnapshot(
        cluster_id_ + "-other", SnapshotRestoreMode::kColdRestore);
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
