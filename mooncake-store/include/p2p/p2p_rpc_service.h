#pragma once

#include <atomic>
#include <csignal>
#include <cstdint>
#include <thread>
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_config.h"
#include "master_admin_service.h"
#include "p2p/p2p_master_service.h"
#include "p2p/p2p_rpc_types.h"
#include "rpc_types.h"
#include "types.h"

namespace mooncake {

class WrappedP2PMasterService {
   public:
    explicit WrappedP2PMasterService(
        const WrappedMasterServiceConfig& config);
    ~WrappedP2PMasterService();

    void init_http_server();

    uint16_t GetHttpPort() const { return http_server_.port(); }

    P2PMasterService& GetMasterService() { return master_service_; }

    tl::expected<bool, ErrorCode> ExistKey(std::string_view key);

    tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
    CalcCacheStats();

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string_view>& keys);

    tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>
    BatchQueryIp(const std::vector<UUID>& client_ids);

    tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode>
    GetReplicaListByRegex(const std::string& str);

    tl::expected<GetReplicaListResponse, ErrorCode> GetReplicaList(
        std::string_view key, const GetReplicaListRequestConfig& config =
                                  GetReplicaListRequestConfig());

    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string_view>& keys,
                        const GetReplicaListRequestConfig& config =
                            GetReplicaListRequestConfig());

    tl::expected<void, ErrorCode> Remove(std::string_view key,
                                         bool force = false);

    tl::expected<long, ErrorCode> RemoveByRegex(std::string_view str,
                                                bool force = false);

    long RemoveAll(bool force = false);

    tl::expected<void, ErrorCode> UnmountSegment(const UUID& segment_id,
                                                 const UUID& client_id);

    tl::expected<void, ErrorCode> MountSegment(const Segment& segment,
                                               const UUID& client_id);

    tl::expected<HeartbeatResponse, ErrorCode> Heartbeat(
        const HeartbeatRequest& req);

    tl::expected<QueryClientStatusResponse, ErrorCode> QueryClientStatus(
        const QueryClientStatusRequest& req);

    tl::expected<RegisterClientResponse, ErrorCode> RegisterClient(
        const RegisterClientRequest& req);

    tl::expected<UnregisterClientResponse, ErrorCode> UnregisterClient(
        const UnregisterClientRequest& req);

    tl::expected<std::string, ErrorCode> ServiceReady();

    tl::expected<WriteRouteResponse, ErrorCode> GetWriteRoute(
        const WriteRouteRequest& req);

    BatchGetWriteRouteResponse BatchGetWriteRoute(
        const BatchGetWriteRouteRequest& req);

    tl::expected<void, ErrorCode> AddReplica(const AddReplicaRequest& req);

    tl::expected<void, ErrorCode> RemoveReplica(
        const RemoveReplicaRequest& req);

    std::vector<tl::expected<void, ErrorCode>> BatchRemoveReplica(
        const BatchRemoveReplicaRequest& req);

    BatchSyncReplicaResponse BatchSyncReplica(
        const BatchSyncReplicaRequest& req);

    tl::expected<void, ErrorCode> SetSyncCompleted(UUID client_id);

   private:
    P2PMasterService master_service_;
    std::thread metric_report_thread_;
    coro_http::coro_http_server http_server_;
    std::atomic<bool> metric_report_running_;
};

void RegisterP2PRpcService(
    coro_rpc::coro_rpc_server& server,
    mooncake::WrappedP2PMasterService& wrapped_master_service);

}  // namespace mooncake