#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>

#include "k8s_lease_helper.h"
#include "ha/leadership/leader_coordinator.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace k8s {

class K8sLeaderCoordinator final : public LeaderCoordinator {
   public:
    explicit K8sLeaderCoordinator(const HABackendSpec& spec);
    ~K8sLeaderCoordinator() override;

    ErrorCode Connect();

    tl::expected<std::optional<MasterView>, ErrorCode> ReadCurrentView()
        override;

    tl::expected<AcquireLeadershipResult, ErrorCode> TryAcquireLeadership(
        const std::string& leader_address) override;

    tl::expected<bool, ErrorCode> RenewLeadership(
        const LeadershipSession& session) override;

    tl::expected<ViewChangeResult, ErrorCode> WaitForViewChange(
        std::optional<ViewVersionId> known_version,
        std::chrono::milliseconds timeout) override;

    tl::expected<std::unique_ptr<LeadershipMonitorHandle>, ErrorCode>
    StartLeadershipMonitor(const LeadershipSession& session,
                           LeadershipLostCallback on_leadership_lost) override;

    ErrorCode ReleaseLeadership(const LeadershipSession& session) override;

   private:
    ErrorCode EnsureConnected();
    ErrorCode ShutdownElection();
    void ClearLeadershipMonitorStateLocked();
    bool IsSameViewVersion(const std::optional<MasterView>& current_view,
                           std::optional<ViewVersionId> known_version) const;
    static std::pair<std::string, std::string> ParseConnstring(
        const std::string& connstring);

    HABackendSpec spec_;
    std::string namespace_;
    std::string lease_name_;
    bool connected_ = false;

    std::mutex election_mutex_;
    std::thread election_monitor_thread_;
    std::string election_identity_;
    bool election_active_ = false;
    bool election_shutdown_requested_ = false;
    LeadershipLostCallback leadership_monitor_callback_;
    std::shared_ptr<std::atomic<bool>> leadership_monitor_armed_;
    OwnerToken leadership_monitor_owner_token_;
};

}  // namespace k8s
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
