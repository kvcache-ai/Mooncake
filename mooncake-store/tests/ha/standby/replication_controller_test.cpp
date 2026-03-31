#include "ha/replication_controller.h"

#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "ha/snapshot/object/backends/local/local_file_snapshot_object_store.h"
#include "ha/snapshot/snapshot_test_utils.h"

namespace mooncake::test {
namespace {

DEFINE_string(redis_endpoint, "",
              "Redis endpoint for replication controller snapshot tests");

class ScopedUnsetEnvVar {
   public:
    explicit ScopedUnsetEnvVar(std::string name) : name_(std::move(name)) {
        const char* previous = std::getenv(name_.c_str());
        if (previous != nullptr) {
            had_previous_value_ = true;
            previous_value_ = previous;
        }
        ::unsetenv(name_.c_str());
    }

    ~ScopedUnsetEnvVar() {
        if (had_previous_value_) {
            ::setenv(name_.c_str(), previous_value_.c_str(), 1);
        } else {
            ::unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    bool had_previous_value_{false};
    std::string previous_value_;
};

MasterServiceSupervisorConfig MakeFailingSnapshotOnlyControllerConfig() {
    MasterServiceSupervisorConfig config;
    config.local_hostname = "replication-controller-test-standby";
    config.cluster_id = "replication-controller-test-cluster";
    config.enable_snapshot_restore = true;
    config.snapshot_object_store_type = "local";
    config.snapshot_catalog_store_type = "embedded";
    return config;
}

class ReplicationControllerSnapshotCatalogTest
    : public ::testing::TestWithParam<CatalogBackendParam> {
   protected:
    void SetUp() override {
        if (GetParam().requires_redis && FLAGS_redis_endpoint.empty()) {
            GTEST_SKIP() << "Redis endpoint is not configured";
        }
    }
};

TEST(ReplicationControllerTest, PromoteFailsClosedWhenStandbyStartFails) {
    ScopedUnsetEnvVar unset_snapshot_path(kSnapshotLocalPathEnv);

    auto controller = ha::CreateReplicationController(
        ha::HABackendSpec{
            .type = ha::HABackendType::REDIS,
            .connstring = "",
            .cluster_namespace = "replication-controller-test",
        },
        MakeFailingSnapshotOnlyControllerConfig());
    ASSERT_NE(controller, nullptr);

    auto start_err = controller->StartStandby(std::nullopt);
    ASSERT_NE(start_err, ErrorCode::OK);

    auto promote_err = controller->PromoteStandby();
    EXPECT_EQ(promote_err, start_err);
}

TEST_P(ReplicationControllerSnapshotCatalogTest,
       PromoteExportsPreloadedStateForSnapshotOnlyStandby) {
    namespace fs = std::filesystem;

    const auto& backend_param = GetParam();
    const auto cluster_id =
        "replication-controller-hot-state-" + UuidToString(generate_uuid());
    const auto temp_dir =
        MakeSnapshotTestTempDir("replication_controller_hot_state_");
    auto local_path_env =
        std::make_unique<ScopedEnvVar>(kSnapshotLocalPathEnv, temp_dir);
    auto object_store =
        std::make_unique<LocalFileSnapshotObjectStore>(temp_dir);
    auto catalog_store = CreateCatalogStoreForTest(
        backend_param, object_store.get(), cluster_id, FLAGS_redis_endpoint);
    ASSERT_NE(catalog_store, nullptr);

    const auto descriptor = MakeTestSnapshotDescriptor(
        "20240330_130500_001", 84, 9, kDefaultTestCreatedAtMs + 1000);
    auto publish_result =
        PublishSnapshotPayload(*object_store, *catalog_store, descriptor);
    ASSERT_TRUE(publish_result.has_value()) << publish_result.error();

    MasterServiceSupervisorConfig config;
    config.local_hostname = "replication-controller-test-standby";
    config.cluster_id = cluster_id;
    config.enable_snapshot_restore = true;
    config.snapshot_object_store_type = "local";
    config.snapshot_catalog_store_type = backend_param.catalog_store_type;
    if (backend_param.requires_redis) {
        config.snapshot_catalog_store_connstring = FLAGS_redis_endpoint;
        config.ha_backend_connstring = FLAGS_redis_endpoint;
    }

    auto controller = ha::CreateReplicationController(
        ha::HABackendSpec{
            .type = ha::HABackendType::REDIS,
            .connstring = FLAGS_redis_endpoint,
            .cluster_namespace = cluster_id,
        },
        config);
    ASSERT_NE(controller, nullptr);

    ASSERT_EQ(ErrorCode::OK, controller->StartStandby(std::nullopt));
    ASSERT_EQ(ErrorCode::OK, controller->PromoteStandby());

    auto promoted_state = controller->TakePromotedStandbyState();
    ASSERT_TRUE(promoted_state.has_value());
    ASSERT_TRUE(promoted_state->has_value());
    EXPECT_EQ(84u, promoted_state->value().applied_seq_id);
    ASSERT_EQ(1u, promoted_state->value().metadata_snapshot.size());
    EXPECT_EQ(kDefaultTestObjectKey,
              promoted_state->value().metadata_snapshot.front().first);
    EXPECT_EQ(kDefaultTestObjectSize,
              promoted_state->value().metadata_snapshot.front().second.size);
    EXPECT_EQ(84u, promoted_state->value()
                       .metadata_snapshot.front()
                       .second.last_sequence_id);

    auto consumed_state = controller->TakePromotedStandbyState();
    ASSERT_TRUE(consumed_state.has_value());
    EXPECT_FALSE(consumed_state->has_value());

    fs::remove_all(temp_dir);
}

INSTANTIATE_TEST_SUITE_P(
    SnapshotCatalogBackends, ReplicationControllerSnapshotCatalogTest,
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
