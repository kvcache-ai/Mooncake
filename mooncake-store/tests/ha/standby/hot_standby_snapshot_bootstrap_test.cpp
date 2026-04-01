#include "hot_standby_service.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/catalog_backed_snapshot_provider.h"
#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"
#include "ha/snapshot/snapshot_test_utils.h"

namespace mooncake::test {

DEFINE_string(redis_endpoint, "",
              "Redis endpoint for HotStandby snapshot bootstrap tests");

namespace {

namespace fs = std::filesystem;

bool WaitUntil(const std::function<bool()>& predicate,
               std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return predicate();
}

class HotStandbySnapshotBootstrapTest
    : public ::testing::TestWithParam<CatalogBackendParam> {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("HotStandbySnapshotBootstrapTest");
        FLAGS_logtostderr = 1;

        if (GetParam().requires_redis && FLAGS_redis_endpoint.empty()) {
            GTEST_SKIP() << "Redis endpoint is not configured";
        }

        cluster_id_ =
            "hot-standby-bootstrap-test-" + UuidToString(generate_uuid());
        temp_dir_ = MakeSnapshotTestTempDir("hot_standby_snapshot_bootstrap_");
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
        if (catalog_store_ != nullptr) {
            for (const auto& snapshot_id : published_snapshot_ids_) {
                (void)catalog_store_->Delete(snapshot_id);
            }
        }
        catalog_store_.reset();
        object_store_.reset();
        local_path_env_.reset();
        if (!temp_dir_.empty() && fs::exists(temp_dir_)) {
            fs::remove_all(temp_dir_);
        }
        google::ShutdownGoogleLogging();
    }

    HotStandbyConfig MakeSnapshotOnlyConfig() const {
        HotStandbyConfig config;
        config.enable_verification = false;
        config.enable_snapshot_bootstrap = true;
        config.enable_oplog_following = false;
        config.snapshot_refresh_interval_ms = 20;
        return config;
    }

    tl::expected<std::unique_ptr<SnapshotProvider>, ErrorCode> CreateProvider()
        const {
        return CreateCatalogBackedSnapshotProvider(MakeSnapshotProviderConfig(
            GetParam(), cluster_id_, FLAGS_redis_endpoint));
    }

    void PublishSnapshot(const ha::SnapshotDescriptor& descriptor,
                         std::string_view object_key = kDefaultTestObjectKey,
                         uint64_t object_size = kDefaultTestObjectSize,
                         uint64_t lease_timeout_ms = kDefaultTestLeaseTimeoutMs,
                         bool has_soft_pin_timeout = false,
                         uint64_t soft_pin_timeout_ms = 0) {
        auto result = PublishSnapshotPayload(
            *object_store_, *catalog_store_, descriptor, UUID{1, 2}, object_key,
            kDefaultTestDiskFilePath, object_size, lease_timeout_ms,
            has_soft_pin_timeout, soft_pin_timeout_ms);
        ASSERT_TRUE(result.has_value()) << result.error();
        published_snapshot_ids_.push_back(descriptor.snapshot_id);
    }

    void PublishSnapshotWithMissingMetadata(
        const ha::SnapshotDescriptor& descriptor) {
        auto manifest = object_store_->UploadString(
            descriptor.manifest_key, "messagepack|1.0.0|standby-test");
        ASSERT_TRUE(manifest.has_value()) << manifest.error();

        auto segments = object_store_->UploadBuffer(
            descriptor.object_prefix + "segments", BuildSegmentsPayload());
        ASSERT_TRUE(segments.has_value()) << segments.error();

        ASSERT_EQ(catalog_store_->Publish(descriptor), ErrorCode::OK);
        published_snapshot_ids_.push_back(descriptor.snapshot_id);
    }

    std::string cluster_id_;
    std::string temp_dir_;
    ha::SnapshotDescriptor descriptor_;
    std::vector<std::string> published_snapshot_ids_;
    std::unique_ptr<ScopedEnvVar> local_path_env_;
    std::unique_ptr<LocalFileSnapshotObjectStore> object_store_;
    std::unique_ptr<ha::SnapshotCatalogStore> catalog_store_;
};

TEST_P(HotStandbySnapshotBootstrapTest,
       SnapshotOnlyStartBootstrapsFromConfiguredBackend) {
    PublishSnapshot(descriptor_);

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    HotStandbyService service(MakeSnapshotOnlyConfig());
    service.SetSnapshotProvider(std::move(provider.value()));

    ASSERT_EQ(ErrorCode::OK, service.Start("", "", cluster_id_));
    EXPECT_EQ(StandbyState::WATCHING, service.GetState());
    EXPECT_EQ(1u, service.GetMetadataCount());
    EXPECT_EQ(descriptor_.last_included_seq,
              service.GetLatestAppliedSequenceId());

    auto status = service.GetSyncStatus();
    EXPECT_EQ(StandbyState::WATCHING, status.state);
    EXPECT_EQ(descriptor_.last_included_seq, status.applied_seq_id);
    EXPECT_EQ(descriptor_.last_included_seq, status.primary_seq_id);

    std::vector<std::pair<std::string, StandbyObjectMetadata>> exported;
    ASSERT_TRUE(service.ExportMetadataSnapshot(exported));
    ASSERT_EQ(1u, exported.size());
    EXPECT_EQ(kDefaultTestObjectKey, exported.front().first);
    EXPECT_EQ(kDefaultTestObjectSize, exported.front().second.size);
    EXPECT_EQ(descriptor_.last_included_seq,
              exported.front().second.last_sequence_id);
}

TEST_P(HotStandbySnapshotBootstrapTest,
       SnapshotOnlyStartUsesEmptyBaselineWhenSnapshotMissing) {
    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    HotStandbyService service(MakeSnapshotOnlyConfig());
    service.SetSnapshotProvider(std::move(provider.value()));

    ASSERT_EQ(ErrorCode::OK, service.Start("", "", cluster_id_));
    EXPECT_EQ(StandbyState::WATCHING, service.GetState());
    EXPECT_EQ(0u, service.GetMetadataCount());
    EXPECT_EQ(0u, service.GetLatestAppliedSequenceId());

    auto status = service.GetSyncStatus();
    EXPECT_EQ(StandbyState::WATCHING, status.state);
    EXPECT_EQ(0u, status.applied_seq_id);
    EXPECT_EQ(0u, status.primary_seq_id);
}

TEST_P(HotStandbySnapshotBootstrapTest,
       SnapshotOnlyStartDropsLeaseExpiredObjectEvenWhenSoftPinned) {
    const auto now_ms = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
    PublishSnapshot(descriptor_, kDefaultTestObjectKey, kDefaultTestObjectSize,
                    now_ms - 1000, true, now_ms + 60000);

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    HotStandbyService service(MakeSnapshotOnlyConfig());
    service.SetSnapshotProvider(std::move(provider.value()));

    ASSERT_EQ(ErrorCode::OK, service.Start("", "", cluster_id_));
    EXPECT_EQ(StandbyState::WATCHING, service.GetState());
    EXPECT_EQ(0u, service.GetMetadataCount());
    EXPECT_EQ(descriptor_.last_included_seq,
              service.GetLatestAppliedSequenceId());

    std::vector<std::pair<std::string, StandbyObjectMetadata>> exported;
    ASSERT_TRUE(service.ExportMetadataSnapshot(exported));
    EXPECT_TRUE(exported.empty());
}

TEST_P(HotStandbySnapshotBootstrapTest,
       SnapshotOnlyStandbyRefreshesNewPublishedSnapshot) {
    PublishSnapshot(descriptor_);

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    HotStandbyService service(MakeSnapshotOnlyConfig());
    service.SetSnapshotProvider(std::move(provider.value()));

    ASSERT_EQ(ErrorCode::OK, service.Start("", "", cluster_id_));
    ASSERT_TRUE(WaitUntil(
        [&]() {
            return service.GetLatestAppliedSequenceId() ==
                   descriptor_.last_included_seq;
        },
        std::chrono::milliseconds(500)));

    const auto next_descriptor = MakeTestSnapshotDescriptor(
        "20240330_120500_002", 84, descriptor_.producer_view_version + 1,
        descriptor_.created_at_ms + 1000);
    PublishSnapshot(next_descriptor, "key-2", 8192);

    ASSERT_TRUE(
        WaitUntil([&]() { return service.GetLatestAppliedSequenceId() == 84; },
                  std::chrono::milliseconds(1500)));

    std::vector<std::pair<std::string, StandbyObjectMetadata>> exported;
    ASSERT_TRUE(service.ExportMetadataSnapshot(exported));
    ASSERT_EQ(1u, exported.size());
    EXPECT_EQ("key-2", exported.front().first);
    EXPECT_EQ(8192u, exported.front().second.size);
    EXPECT_EQ(84u, exported.front().second.last_sequence_id);
}

TEST_P(HotStandbySnapshotBootstrapTest,
       SnapshotOnlyStandbyKeepsPreviousBaselineWhenLatestSnapshotIsUnreadable) {
    PublishSnapshot(descriptor_);

    auto provider = CreateProvider();
    ASSERT_TRUE(provider.has_value()) << toString(provider.error());

    HotStandbyService service(MakeSnapshotOnlyConfig());
    service.SetSnapshotProvider(std::move(provider.value()));

    ASSERT_EQ(ErrorCode::OK, service.Start("", "", cluster_id_));
    ASSERT_TRUE(WaitUntil(
        [&]() {
            return service.GetLatestAppliedSequenceId() ==
                   descriptor_.last_included_seq;
        },
        std::chrono::milliseconds(500)));

    const auto broken_descriptor = MakeTestSnapshotDescriptor(
        "20240330_120500_002", 84, descriptor_.producer_view_version + 1,
        descriptor_.created_at_ms + 1000);
    PublishSnapshotWithMissingMetadata(broken_descriptor);

    ASSERT_TRUE(WaitUntil(
        [&]() { return service.GetSyncStatus().primary_seq_id == 84; },
        std::chrono::milliseconds(1500)));

    EXPECT_EQ(StandbyState::WATCHING, service.GetState());
    EXPECT_EQ(descriptor_.last_included_seq,
              service.GetLatestAppliedSequenceId());

    std::vector<std::pair<std::string, StandbyObjectMetadata>> exported;
    ASSERT_TRUE(service.ExportMetadataSnapshot(exported));
    ASSERT_EQ(1u, exported.size());
    EXPECT_EQ(kDefaultTestObjectKey, exported.front().first);
    EXPECT_EQ(kDefaultTestObjectSize, exported.front().second.size);
    EXPECT_EQ(descriptor_.last_included_seq,
              exported.front().second.last_sequence_id);
}

INSTANTIATE_TEST_SUITE_P(
    SnapshotCatalogBackends, HotStandbySnapshotBootstrapTest,
    ::testing::ValuesIn(BuildCatalogBackendParams()),
    [](const ::testing::TestParamInfo<CatalogBackendParam>& info) {
        return info.param.name;
    });

}  // namespace
}  // namespace mooncake::test

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
