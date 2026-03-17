#pragma once

#include <mutex>
#include <thread>

#include "etcd_helper.h"
#include "ha/leader_coordinator.h"

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

    ErrorCode ReleaseLeadership(const LeadershipSession& session) override;

   private:
    static ClusterNamespace ResolveClusterNamespace(
        const ClusterNamespace& cluster_namespace);
    static std::string BuildMasterViewKey(
        const ClusterNamespace& cluster_namespace);
    static tl::expected<EtcdLeaseId, ErrorCode> ParseLeaseId(
        const OwnerToken& owner_token);
    static OwnerToken MakeOwnerToken(EtcdLeaseId lease_id);

    ErrorCode EnsureConnected();
    ErrorCode ShutdownKeepAliveThread();
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
};

}  // namespace etcd
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
