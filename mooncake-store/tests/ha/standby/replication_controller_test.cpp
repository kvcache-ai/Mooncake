#include "ha/replication_controller.h"

#include <cstdlib>
#include <string>

#include <gtest/gtest.h>

#include "ha/snapshot/snapshot_test_utils.h"

namespace mooncake::test {
namespace {

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

}  // namespace
}  // namespace mooncake::test
