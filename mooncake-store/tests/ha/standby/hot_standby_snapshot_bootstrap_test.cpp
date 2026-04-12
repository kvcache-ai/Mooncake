#include "hot_standby_service.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include "ha/standby_controller.h"
#include "ha/snapshot/catalog/snapshot_catalog_store.h"
#include "ha/snapshot/catalog_backed_snapshot_provider.h"
#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"
#include "ha/snapshot/snapshot_test_utils.h"

namespace mooncake::test {

DEFINE_string(redis_endpoint, "",
              "Redis endpoint for HotStandby snapshot bootstrap tests");

namespace {

namespace fs = std::filesystem;

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
        if (snapshot_published_ && catalog_store_ != nullptr) {
            (void)catalog_store_->Delete(descriptor_.snapshot_id);
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
        return config;
    }

    tl::expected<std::unique_ptr<SnapshotProvider>, ErrorCode> CreateProvider()
        const {
        return CreateCatalogBackedSnapshotProvider(MakeSnapshotProviderConfig(
            GetParam(), cluster_id_, FLAGS_redis_endpoint));
    }

    void PublishSnapshot() {
        auto result = PublishSnapshotPayload(*object_store_, *catalog_store_,
                                             descriptor_);
        ASSERT_TRUE(result.has_value()) << result.error();
        snapshot_published_ = true;
    }

    std::string cluster_id_;
    std::string temp_dir_;
    bool snapshot_published_{false};
    ha::SnapshotDescriptor descriptor_;
    std::unique_ptr<ScopedEnvVar> local_path_env_;
    std::unique_ptr<LocalFileSnapshotObjectStore> object_store_;
    std::unique_ptr<ha::SnapshotCatalogStore> catalog_store_;
};

TEST_P(HotStandbySnapshotBootstrapTest,
       SnapshotOnlyStartBootstrapsFromConfiguredBackend) {
    PublishSnapshot();

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

TEST(StandbyControllerTest, PromoteStandbyReturnsStartFailure) {
    ha::HABackendSpec spec{
        .type = ha::HABackendType::ETCD,
        .connstring = "http://localhost:2379",
        .cluster_namespace = "replication-controller-test",
    };
    MasterServiceSupervisorConfig config;
    config.cluster_id = "replication-controller-test";
    config.local_hostname = "127.0.0.1:50051";
    config.ha_backend_connstring = spec.connstring;
    config.etcd_endpoints = spec.connstring;
    config.enable_snapshot_restore = true;
    config.snapshot_object_store_type = "local";
    config.snapshot_catalog_store_type = "invalid";

    auto controller = ha::CreateStandbyController(spec, config);
    ASSERT_NE(controller, nullptr);
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              controller->StartStandby(std::nullopt));
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, controller->PromoteStandby());
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
