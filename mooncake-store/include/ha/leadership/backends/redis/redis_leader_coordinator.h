#pragma once

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include "ha/leadership/leader_coordinator.h"

struct redisContext;

namespace mooncake {
namespace ha {
namespace backends {
namespace redis {

class RedisLeaderCoordinator final : public LeaderCoordinator {
   public:
    explicit RedisLeaderCoordinator(const HABackendSpec& spec);
    ~RedisLeaderCoordinator() override;

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
    static std::string BuildViewVersionKey(
        const ClusterNamespace& cluster_namespace);
    static OwnerToken MakeOwnerToken();
    static LeadershipLossReason ClassifyLeadershipLossReason(ErrorCode err);

    ErrorCode ConnectLocked();
    void DisconnectLocked();
    ErrorCode RenewLeadershipOnceLocked(const LeadershipSession& session,
                                        bool& renewed);
    ErrorCode ReleaseLeadershipOnceLocked(const OwnerToken& owner_token,
                                          bool& leadership_missing,
                                          bool& owner_mismatch);
    ErrorCode ShutdownRenewThread();
    void ClearLeadershipMonitorStateLocked();
    std::chrono::milliseconds ResolveRenewInterval(
        std::chrono::milliseconds lease_ttl) const;
    bool IsSameViewVersion(const std::optional<MasterView>& current_view,
                           std::optional<ViewVersionId> known_version) const;

    HABackendSpec spec_;
    std::string master_view_key_;
    std::string view_version_key_;

    std::mutex state_mutex_;
    std::condition_variable renew_cv_;
    redisContext* context_ = nullptr;
    bool connected_ = false;

    std::thread renew_thread_;
    LeadershipSession renew_session_;
    OwnerToken renew_owner_token_;
    bool renew_stopped_ = false;
    bool renew_stop_requested_ = false;
    bool shutdown_requested_ = false;
    ErrorCode renew_result_ = ErrorCode::OK;

    LeadershipLostCallback leadership_monitor_callback_;
    std::shared_ptr<std::atomic<bool>> leadership_monitor_armed_;
    OwnerToken leadership_monitor_owner_token_;
};

}  // namespace redis
}  // namespace backends
}  // namespace ha
}  // namespace mooncake
