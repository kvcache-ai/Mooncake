#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>

#include "etcd_helper.h"
#include "ha/leadership/leader_coordinator.h"

namespace mooncake {
namespace ha {
namespace backends {
namespace etcd {

class EtcdLeaderCoordinator final : public LeaderCoordinator {
   public:
    explicit EtcdLeaderCoordinator(const HABackendSpec& spec);
    ~EtcdLeaderCoordinator() override;

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
    static ClusterNamespace ResolveClusterNamespace(
        const ClusterNamespace& cluster_namespace);
    static std::string BuildMasterViewKey(
        const ClusterNamespace& cluster_namespace);
    static tl::expected<EtcdLeaseId, ErrorCode> ParseLeaseId(
        const OwnerToken& owner_token);
    static OwnerToken MakeOwnerToken(EtcdLeaseId lease_id);
    static LeadershipLossReason ClassifyLeadershipLossReason(ErrorCode err);

    ErrorCode EnsureConnected();
    ErrorCode ShutdownKeepAliveThread();
    void ClearLeadershipMonitorStateLocked();
    bool IsSameViewVersion(const std::optional<MasterView>& current_view,
                           std::optional<ViewVersionId> known_version) const;

    HABackendSpec spec_;
    std::string master_view_key_;
    bool connected_ = false;

    std::mutex keepalive_mutex_;
    std::thread keepalive_thread_;
    OwnerToken keepalive_owner_token_;
    EtcdLeaseId keepalive_lease_id_ = 0;
    bool keepalive_stopped_ = false;
    bool keepalive_shutdown_requested_ = false;
    ErrorCode keepalive_result_ = ErrorCode::OK;
    LeadershipLostCallback leadership_monitor_callback_;
    std::shared_ptr<std::atomic<bool>> leadership_monitor_armed_;
    OwnerToken leadership_monitor_owner_token_;
};

}  // namespace etcd
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
