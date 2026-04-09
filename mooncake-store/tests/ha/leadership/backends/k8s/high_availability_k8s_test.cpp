#include <gflags/gflags.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <future>
#include <memory>
#include <string>
#include <thread>

#ifdef STORE_USE_K8S_LEASE
#include "k8s_lease_helper.h"
#endif
#include "ha/leadership/leader_coordinator_factory.h"
#include "ha/leadership/high_availability_test_fixture.h"
#include "types.h"

namespace mooncake {
namespace testing {

DEFINE_string(k8s_namespace, "default",
              "K8s namespace for HA integration tests");
DEFINE_string(k8s_lease_name, "mooncake-ha-test",
              "K8s Lease name for HA integration tests");

namespace {

std::optional<std::string> GetK8sSkipReason() {
#ifdef STORE_USE_K8S_LEASE
    // Probe: try to init K8s client and read a lease.
    auto err = K8sLeaseHelper::Init();
    if (err != ErrorCode::OK) {
        return "K8s API not reachable (Init failed)";
    }
    std::string holder;
    int64_t transitions = 0;
    err = K8sLeaseHelper::GetHolder(FLAGS_k8s_namespace, FLAGS_k8s_lease_name,
                                    holder, transitions);
    if (err != ErrorCode::OK &&
        err != ErrorCode::K8S_LEASE_NOT_FOUND) {
        return "K8s API not reachable (GetHolder probe failed)";
    }
    return std::nullopt;
#else
    return "K8s Lease HA backend is not enabled in this build";
#endif
}

ha::HABackendSpec MakeK8sBackendSpec(const std::string& ns,
                                     const std::string& lease_name) {
    return ha::HABackendSpec{
        .type = ha::HABackendType::K8S,
        .connstring = ns + "/" + lease_name,
        .cluster_namespace = "",
    };
}

std::unique_ptr<ha::LeaderCoordinator> CreateK8sCoordinatorOrNull(
    const std::string& ns, const std::string& lease_name) {
    auto coordinator =
        ha::CreateLeaderCoordinator(MakeK8sBackendSpec(ns, lease_name));
    if (!coordinator) {
        return nullptr;
    }
    return std::move(coordinator.value());
}

std::string MakeK8sTestLeaseName(const std::string& suffix) {
    return FLAGS_k8s_lease_name + "-" + suffix;
}

}  // namespace

TEST_F(HighAvailabilityTest, K8sBasicMasterViewOperations) {
    if (auto skip_reason = GetK8sSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    const auto lease_name = MakeK8sTestLeaseName("basic");
    auto coordinator =
        CreateK8sCoordinatorOrNull(FLAGS_k8s_namespace, lease_name);
    ASSERT_NE(coordinator, nullptr);

    // Initially, the master view should be empty (no leader)
    auto initial_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(initial_view.has_value());
    // Note: may or may not have value depending on prior test state

    // Acquire leadership
    auto acquire = coordinator->TryAcquireLeadership("127.0.0.1:8899");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());

    // Read current view — should show our address
    auto current_view = coordinator->ReadCurrentView();
    ASSERT_TRUE(current_view.has_value());
    ASSERT_TRUE(current_view->has_value());
    EXPECT_EQ("127.0.0.1:8899", current_view->value().leader_address);

    // Renew
    auto renewed = coordinator->RenewLeadership(*acquire->session);
    ASSERT_TRUE(renewed.has_value());
    EXPECT_TRUE(renewed.value());

    // WaitForViewChange — should time out since nothing changed
    auto no_change = coordinator->WaitForViewChange(
        acquire->session->view.view_version, std::chrono::milliseconds(200));
    ASSERT_TRUE(no_change.has_value());
    ASSERT_FALSE(no_change->changed);
    ASSERT_TRUE(no_change->timed_out);

    // Release
    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(*acquire->session));

    // After release, view should eventually show no leader or a different
    // transitions count
    auto released = coordinator->WaitForViewChange(
        acquire->session->view.view_version, std::chrono::seconds(10));
    ASSERT_TRUE(released.has_value());
    ASSERT_TRUE(released->changed);
}

TEST_F(HighAvailabilityTest, K8sLeadershipMonitorIgnoresExplicitRelease) {
    if (auto skip_reason = GetK8sSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    const auto lease_name = MakeK8sTestLeaseName("monitor-release");
    auto coordinator =
        CreateK8sCoordinatorOrNull(FLAGS_k8s_namespace, lease_name);
    ASSERT_NE(coordinator, nullptr);

    auto acquire = coordinator->TryAcquireLeadership("127.0.0.1:9922");
    ASSERT_TRUE(acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, acquire->status);
    ASSERT_TRUE(acquire->session.has_value());
    const auto session = *acquire->session;

    auto renew = coordinator->RenewLeadership(session);
    ASSERT_TRUE(renew.has_value());
    ASSERT_TRUE(renew.value());

    auto callback_fired = std::make_shared<std::atomic<bool>>(false);
    auto monitor = coordinator->StartLeadershipMonitor(
        session, [callback_fired](ha::LeadershipLossReason) {
            callback_fired->store(true);
        });
    ASSERT_TRUE(monitor.has_value());

    // Explicit release should NOT fire the monitor callback
    ASSERT_EQ(ErrorCode::OK, coordinator->ReleaseLeadership(session));
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_FALSE(callback_fired->load());
}

TEST_F(HighAvailabilityTest, K8sCanReacquireAfterRelease) {
    if (auto skip_reason = GetK8sSkipReason(); skip_reason.has_value()) {
        GTEST_SKIP() << *skip_reason;
    }

    const auto lease_name = MakeK8sTestLeaseName("reacquire");
    auto coordinator =
        CreateK8sCoordinatorOrNull(FLAGS_k8s_namespace, lease_name);
    ASSERT_NE(coordinator, nullptr);

    // First acquisition
    auto first_acquire = coordinator->TryAcquireLeadership("127.0.0.1:9933");
    ASSERT_TRUE(first_acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, first_acquire->status);
    ASSERT_TRUE(first_acquire->session.has_value());

    auto first_renew = coordinator->RenewLeadership(*first_acquire->session);
    ASSERT_TRUE(first_renew.has_value());
    ASSERT_TRUE(first_renew.value());

    ASSERT_EQ(ErrorCode::OK,
              coordinator->ReleaseLeadership(*first_acquire->session));

    // Wait for lease to expire
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Second acquisition
    auto second_acquire = coordinator->TryAcquireLeadership("127.0.0.1:9944");
    ASSERT_TRUE(second_acquire.has_value());
    ASSERT_EQ(ha::AcquireLeadershipStatus::ACQUIRED, second_acquire->status);
    ASSERT_TRUE(second_acquire->session.has_value());

    auto second_renew = coordinator->RenewLeadership(*second_acquire->session);
    ASSERT_TRUE(second_renew.has_value());
    ASSERT_TRUE(second_renew.value());

    ASSERT_EQ(ErrorCode::OK,
              coordinator->ReleaseLeadership(*second_acquire->session));
}

}  // namespace testing
}  // namespace mooncake
