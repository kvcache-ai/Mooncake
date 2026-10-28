#include "ha/standby_controller.h"

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <optional>
#include <stdexcept>
#include <string>

namespace mooncake::test {

namespace {

constexpr char kSnapshotLocalPathEnv[] = "MOONCAKE_SNAPSHOT_LOCAL_PATH";

std::string MakeTempDir(const std::string& prefix) {
    std::string templ =
        (std::filesystem::temp_directory_path() / (prefix + "XXXXXX")).string();
    if (::mkdtemp(templ.data()) == nullptr) {
        throw std::runtime_error("failed to create temp directory");
    }
    return templ;
}

// Points MOONCAKE_SNAPSHOT_LOCAL_PATH at a fresh directory for one test and
// restores the environment afterwards.
class ScopedSnapshotLocalPath {
   public:
    explicit ScopedSnapshotLocalPath(const std::string& prefix)
        : path_(MakeTempDir(prefix)) {
        if (const char* previous = std::getenv(kSnapshotLocalPathEnv)) {
            previous_ = previous;
        }
        ::setenv(kSnapshotLocalPathEnv, path_.c_str(), 1);
    }

    ~ScopedSnapshotLocalPath() {
        if (previous_) {
            ::setenv(kSnapshotLocalPathEnv, previous_->c_str(), 1);
        } else {
            ::unsetenv(kSnapshotLocalPathEnv);
        }
        std::error_code ec;
        std::filesystem::remove_all(path_, ec);
    }

   private:
    std::string path_;
    std::optional<std::string> previous_;
};

}  // namespace

TEST(StandbyControllerTest, OplogEnablementControlsReaderController) {
    ha::HABackendSpec spec{
        .type = ha::HABackendType::ETCD,
        .connstring = "http://localhost:2379",
        .cluster_namespace = "oplog-reader-gate-test",
    };
    MasterServiceSupervisorConfig config;

    auto disabled = ha::CreateStandbyController(spec, config);
    ASSERT_NE(disabled, nullptr);
    EXPECT_EQ(ErrorCode::OK, disabled->PromoteStandby());

    config.enable_oplog = true;
    auto enabled = ha::CreateStandbyController(spec, config);
    ASSERT_NE(enabled, nullptr);
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS,
              enabled->PromoteStandby());

    spec.type = ha::HABackendType::REDIS;
    auto redis = ha::CreateStandbyController(spec, config);
    ASSERT_NE(redis, nullptr);
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS,
              redis->PromoteStandby());

    spec.type = ha::HABackendType::K8S;
    auto unsupported = ha::CreateStandbyController(spec, config);
    ASSERT_NE(unsupported, nullptr);
    EXPECT_EQ(ErrorCode::OK, unsupported->PromoteStandby());
}

TEST(StandbyControllerTest, PromotionReportsStandbyStartFailure) {
    // Inject a synchronous startup failure so this test does not depend on
    // asynchronous HA backend connection behavior.
    ha::HABackendSpec spec{};
    spec.type = ha::HABackendType::ETCD;
    MasterServiceSupervisorConfig config;
    config.cluster_id = "promotion-start-failure-test";
    config.enable_oplog = true;

    auto controller = ha::CreateStandbyController(
        spec, config, [] { return ErrorCode::INVALID_PARAMS; });
    ASSERT_NE(controller, nullptr);

    auto not_started = controller->PromoteStandbyAndExport();
    ASSERT_FALSE(not_started.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, not_started.error());

    const ErrorCode start_error = controller->StartStandby(std::nullopt);
    ASSERT_NE(ErrorCode::OK, start_error);
    EXPECT_EQ(start_error, controller->PromoteStandby());
    auto failed = controller->PromoteStandbyAndExport();
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(start_error, failed.error());
}

TEST(StandbyControllerTest, BatchSnapshotDependencyFailureThrowsBeforeStart) {
    ha::HABackendSpec spec{};
    spec.type = ha::HABackendType::ETCD;
    MasterServiceSupervisorConfig config;
    config.enable_oplog = true;
    config.enable_oplog_snapshot = true;
    config.snapshot_object_store_type = "invalid";
    EXPECT_THROW(ha::CreateStandbyController(spec, config), std::runtime_error);

    // The same unused snapshot setting must not prevent pure OpLog startup.
    config.enable_oplog_snapshot = false;
    EXPECT_NO_THROW(ha::CreateStandbyController(spec, config));
}

TEST(StandbyControllerTest, BatchSnapshotRepeatedStartStopAndPromotion) {
    const char* endpoints = std::getenv("MOONCAKE_TEST_ETCD_ENDPOINTS");
    if (!endpoints) GTEST_SKIP() << "Run with the real-etcd snapshot smoke";
    ScopedSnapshotLocalPath local_path("batch-lifecycle-");
    MasterServiceSupervisorConfig config;
    config.cluster_id = "batch-lifecycle-" + UuidToString(generate_uuid());
    config.enable_oplog = true;
    config.enable_oplog_snapshot = true;
    config.snapshot_object_store_type = "local";
    config.ha_backend_connstring = endpoints;
    config.snapshot_interval_seconds = 1;
    ha::HABackendSpec spec{};
    spec.type = ha::HABackendType::ETCD;
    auto controller = ha::CreateStandbyController(spec, config);
    for (int cycle = 0; cycle < 2; ++cycle) {
        ASSERT_EQ(ErrorCode::OK, controller->StartStandby(std::nullopt));
        ASSERT_EQ(ErrorCode::OK, controller->StartStandby(std::nullopt));
        controller->StopStandby();
        controller->StopStandby();
    }
    ASSERT_EQ(ErrorCode::OK, controller->StartStandby(std::nullopt));
    auto promoted = controller->PromoteStandbyAndExport();
    ASSERT_TRUE(promoted.has_value());
    EXPECT_NE(nullptr, promoted->metadata_store);
    ASSERT_EQ(ErrorCode::OK, controller->StartStandby(std::nullopt));
    controller.reset();  // Destruction while both workers are running.
}

TEST(StandbyControllerTest, HaWithoutOplogPromotesWithEmptyContext) {
    ha::HABackendSpec spec{
        .type = ha::HABackendType::ETCD,
        .connstring = "http://localhost:2379",
        .cluster_namespace = "ha-without-oplog-test",
    };
    MasterServiceSupervisorConfig config;
    config.enable_oplog = false;

    auto controller = ha::CreateStandbyController(spec, config);
    ASSERT_EQ(ErrorCode::OK, controller->StartStandby(std::nullopt));

    auto result = controller->PromoteStandbyAndExport();
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(0u, result->applied_seq_id);
    EXPECT_TRUE(result->objects.empty());
    EXPECT_TRUE(result->segments.empty());
}

}  // namespace mooncake::test
