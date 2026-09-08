#pragma once

#include <atomic>
#include <csignal>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <boost/functional/hash.hpp>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/tl/expected.hpp>

#include "p2p/common/p2p_master_config.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/master/p2p_master_metric_manager.h"
#include "p2p/master/p2p_master_service.h"
#include "types.h"

namespace mooncake {

inline constexpr uint64_t kP2PMetricReportIntervalSeconds = 10;

/**
 * @brief Standalone P2P master RPC service.
 *
 * P2P handlers use their native coro_rpc method IDs. Rolling-upgrade
 * compatibility is intentionally out of scope because this code is still
 * under development and has not been deployed.
 */
class P2PMasterRpcService final {
   public:
    explicit P2PMasterRpcService(const P2PMasterConfig& config,
                                 ViewVersionId view_version = 0);
    ~P2PMasterRpcService();

    P2PMasterRpcService(const P2PMasterRpcService&) = delete;
    P2PMasterRpcService& operator=(const P2PMasterRpcService&) = delete;

    void init();
    uint16_t GetHttpPort() const { return http_server_.port(); }
    P2PMasterService& GetMasterService() { return master_service_; }
    const P2PMasterService& GetMasterService() const { return master_service_; }

    tl::expected<bool, ErrorCode> ExistKey(std::string_view key);

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string_view>& keys);

    tl::expected<
        std::unordered_map<std::string, std::vector<P2PRouteDescriptor>>,
        ErrorCode>
    GetReadRouteByRegex(std::string_view regex);

    tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode> GetReadRoute(
        const P2PGetReadRouteRequest& req);
    P2PBatchGetReadRouteResponse BatchGetReadRoute(
        const P2PBatchGetReadRouteRequest& req);

    tl::expected<void, ErrorCode> UnmountSegment(
        const P2PUnmountSegmentRequest& req);
    tl::expected<void, ErrorCode> MountSegment(
        const P2PMountSegmentRequest& req);

    tl::expected<P2PHeartbeatResponse, ErrorCode> Heartbeat(
        const P2PHeartbeatRequest& req);
    tl::expected<P2PClientStatus, ErrorCode> QueryClientStatus(
        const UUID& client_id);
    tl::expected<ViewVersionId, ErrorCode> RegisterClient(
        const P2PRegisterClientRequest& req);
    tl::expected<ViewVersionId, ErrorCode> UnregisterClient(
        const UUID& client_id);

    tl::expected<std::string, ErrorCode> ServiceReady();
    tl::expected<uint32_t, ErrorCode> HeartbeatServiceReady();

    tl::expected<std::vector<P2PWriteCandidate>, ErrorCode> GetWriteRoute(
        const P2PGetWriteRouteRequest& req);
    P2PBatchGetWriteRouteResponse BatchGetWriteRoute(
        const P2PBatchGetWriteRouteRequest& req);

    tl::expected<void, ErrorCode> PublishRoute(
        const P2PPublishRouteRequest& req);
    tl::expected<void, ErrorCode> WithdrawRoute(
        const P2PWithdrawRouteRequest& req);
    std::vector<tl::expected<void, ErrorCode>> BatchWithdrawRoute(
        const P2PBatchWithdrawRouteRequest& req);
    P2PBatchSyncRoutesResponse BatchSyncRoutes(
        const P2PBatchSyncRoutesRequest& req);
    tl::expected<void, ErrorCode> CompleteRouteSync(
        const UUID& client_id);

   private:
    void init_http_server();

    P2PMasterService master_service_;
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
    uint32_t heartbeat_rpc_port_ = 0;
};

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::P2PMasterRpcService& wrapped_master_service,
    bool include_heartbeat = true);

void RegisterP2PHeartbeatRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::P2PMasterRpcService& wrapped_master_service);

}  // namespace mooncake
