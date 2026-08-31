#include "master_client.h"

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SyncAwait.h>
#include <csignal>
#include <string>
#include <string_view>
#include <vector>
#include <ylt/coro_rpc/impl/coro_rpc_client.hpp>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "rpc_service.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"
#include "request_context.h"

namespace mooncake {

template <>
struct RpcNameTraits<&WrappedMasterService::ExistKey> {
    static constexpr const char* value = "ExistKey";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchExistKeyRpc> {
    static constexpr const char* value = "BatchExistKey";
};

template <>
struct RpcNameTraits<&WrappedMasterService::CalcCacheStats> {
    static constexpr const char* value = "CalcCacheStats";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchQueryIp> {
    static constexpr const char* value = "BatchQueryIp";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetReplicaListByRegex> {
    static constexpr const char* value = "GetReplicaListByRegex";
};

template <>
struct RpcNameTraits<&WrappedMasterService::GetReplicaListRpc> {
    static constexpr const char* value = "GetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedMasterService::BatchGetReplicaListRpc> {
    static constexpr const char* value = "BatchGetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedMasterService::Remove> {
    static constexpr const char* value = "Remove";
};

template <>
struct RpcNameTraits<&WrappedMasterService::RemoveByRegex> {
    static constexpr const char* value = "RemoveByRegex";
};

template <>
struct RpcNameTraits<&WrappedMasterService::RemoveAll> {
    static constexpr const char* value = "RemoveAll";
};

template <>
struct RpcNameTraits<&WrappedMasterService::MountSegment> {
    static constexpr const char* value = "MountSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::UnmountSegment> {
    static constexpr const char* value = "UnmountSegment";
};

template <>
struct RpcNameTraits<&WrappedMasterService::Heartbeat> {
    static constexpr const char* value = "Heartbeat";
};

template <>
struct RpcNameTraits<&WrappedMasterService::RegisterClient> {
    static constexpr const char* value = "RegisterClient";
};

template <>
struct RpcNameTraits<&WrappedMasterService::UnregisterClient> {
    static constexpr const char* value = "UnregisterClient";
};

template <>
struct RpcNameTraits<&WrappedMasterService::QueryClientStatus> {
    static constexpr const char* value = "QueryClientStatus";
};

template <>
struct RpcNameTraits<&WrappedMasterService::ServiceReady> {
    static constexpr const char* value = "ServiceReady";
};

template <>
struct RpcNameTraits<&WrappedMasterService::HeartbeatServiceReady> {
    static constexpr const char* value = "HeartbeatServiceReady";
};

ErrorCode MasterClient::Connect(const std::string& master_addr) {
    ScopedVLogTimer timer(1, "MasterClient::Connect");
    timer.LogRequest("master_addr=", master_addr);

    MutexLocker lock(&connect_mutex_);
    bool is_same_addr = (client_addr_param_ == master_addr);
    if (!is_same_addr) {
        // WARNING: The existing client pool cannot be erased. So if there are a
        // lot of different addresses, there will be resource leak problems.
        auto client_pool = client_pools_->at(master_addr);
        client_accessor_.SetClientPool(client_pool);
        client_addr_param_ = master_addr;
        // Route heartbeats to the dedicated heartbeat server when configured.
        // The heartbeat endpoint is the same host as the master with the
        // dedicated heartbeat port, so it follows the leader automatically on
        // HA failover. When no dedicated port is set, heartbeats use the main
        // pool (legacy behavior).
        if (heartbeat_rpc_port_ > 0) {
            auto colon = master_addr.rfind(':');
            std::string host = (colon == std::string::npos)
                                   ? master_addr
                                   : master_addr.substr(0, colon);
            std::string heartbeat_addr =
                host + ":" + std::to_string(heartbeat_rpc_port_);
            heartbeat_accessor_.SetClientPool(
                client_pools_->at(heartbeat_addr));
        } else {
            heartbeat_accessor_.SetClientPool(client_pool);
        }
    }
    // The client pool does not have native connection check method, so we need
    // to use custom ServiceReady API.
    auto result =
        invoke_rpc<&WrappedMasterService::ServiceReady, std::string>();
    if (!result.has_value() && is_same_addr) {
        timer.LogResponse("error_code=", result.error());
        // Stale connection pool might still exist.
        // Retrying once will force the pool to re-establish a new connection.
        result = invoke_rpc<&WrappedMasterService::ServiceReady, std::string>();
    }

    if (!result.has_value()) {
        timer.LogResponse("error_code=", result.error());
        client_addr_param_.clear();
        return result.error();
    }
    // Check if server version matches client version
    std::string server_version = result.value();
    std::string client_version = GetMooncakeStoreVersion();
    if (server_version != client_version) {
        LOG(ERROR) << "Version mismatch: server=" << server_version
                   << " client=" << client_version;
        timer.LogResponse("error_code=", ErrorCode::INVALID_VERSION);
        return ErrorCode::INVALID_VERSION;
    }
    // Ask the master how it routes heartbeats, then verify it matches the
    // client's expectation. Catches both mismatch directions at startup:
    //   - client expects a dedicated heartbeat server the master never opened
    //   - client is legacy but the master dropped Heartbeat from the main
    //     server in favor of a dedicated port
    // Either direction would otherwise silently starve heartbeats until the
    // client gets reaped (client_live_ttl expiry, segment reclaim).
    auto hb_ready = invoke_rpc<&WrappedMasterService::HeartbeatServiceReady,
                               HeartbeatServiceReadyResponse>();
    if (!hb_ready.has_value()) {
        LOG(ERROR) << "HeartbeatServiceReady probe failed: error_code="
                   << hb_ready.error()
                   << " (master may predate this RPC; upgrade master first)";
        timer.LogResponse("error_code=", hb_ready.error());
        client_addr_param_.clear();
        return hb_ready.error();
    }
    const bool client_dedicated = heartbeat_rpc_port_ > 0;
    const bool master_dedicated = hb_ready->heartbeat_rpc_port > 0;
    if (client_dedicated != master_dedicated) {
        LOG(ERROR) << "Heartbeat routing mismatch: client_hb_port="
                   << heartbeat_rpc_port_
                   << " master_hb_port=" << hb_ready->heartbeat_rpc_port
                   << " (one side is dedicated, the other is legacy)";
        timer.LogResponse("error_code=", ErrorCode::HEARTBEAT_ROUTING_MISMATCH);
        client_addr_param_.clear();
        return ErrorCode::HEARTBEAT_ROUTING_MISMATCH;
    }
    // Both sides dedicated: confirm the dedicated heartbeat server is actually
    // reachable (catches a configured-but-dead dedicated server). Mirrors the
    // main-pool stale-connection retry above when reconnecting to the same
    // address.
    if (client_dedicated) {
        auto hb_result =
            invoke_rpc_via<&WrappedMasterService::ServiceReady, std::string>(
                heartbeat_accessor_);
        if (!hb_result.has_value() && is_same_addr) {
            hb_result = invoke_rpc_via<&WrappedMasterService::ServiceReady,
                                       std::string>(heartbeat_accessor_);
        }
        if (!hb_result.has_value()) {
            LOG(ERROR) << "Dedicated heartbeat RPC server unreachable at"
                       << " heartbeat_rpc_port=" << heartbeat_rpc_port_
                       << ": error_code=" << hb_result.error();
            timer.LogResponse("error_code=",
                              ErrorCode::HEARTBEAT_RPC_UNREACHABLE);
            client_addr_param_.clear();
            return ErrorCode::HEARTBEAT_RPC_UNREACHABLE;
        }
    }
    timer.LogResponse("error_code=", ErrorCode::OK);
    return ErrorCode::OK;
}

tl::expected<bool, ErrorCode> MasterClient::ExistKey(
    std::string_view object_key) {
    ScopedVLogTimer timer(1, "MasterClient::ExistKey");
    timer.LogRequest("object_key=", object_key);

    auto result = invoke_rpc<&WrappedMasterService::ExistKey, bool>(object_key);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> MasterClient::BatchExistKey(
    const std::vector<std::string_view>& object_keys) {
    ScopedVLogTimer timer(1, "MasterClient::BatchExistKey");
    timer.LogRequest("keys_count=", object_keys.size());

    auto result = invoke_batch_rpc<&WrappedMasterService::BatchExistKeyRpc, bool>(
        object_keys.size(), object_keys);
    timer.LogResponse("result=", result.size(), " keys");
    return result;
}

tl::expected<GetReplicaListResponse, ErrorCode> MasterClient::GetReplicaList(
    std::string_view key, const GetReplicaListRequestConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaList");
    timer.LogRequest("object_key=", key);

    auto result = invoke_rpc<&WrappedMasterService::GetReplicaListRpc,
                             GetReplicaListResponse>(key, config);
    timer.LogResponseExpected(result);
    return result;
}

async_simple::coro::Lazy<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterClient::AsyncGetReplicaList(std::string_view key,
                                  const GetReplicaListRequestConfig& config) {
    auto result =
        co_await invoke_rpc_async<&WrappedMasterService::GetReplicaListRpc,
                                  GetReplicaListResponse>(key, config);
    co_return result;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
MasterClient::BatchGetReplicaList(const std::vector<std::string_view>& keys,
                                  const GetReplicaListRequestConfig& config) {
    ScopedVLogTimer timer(1, "MasterClient::BatchGetReplicaList");
    timer.LogRequest("requests_count=", keys.size());

    if (keys.empty()) {
        return {};
    }

    auto result = invoke_rpc<
        &WrappedMasterService::BatchGetReplicaListRpc,
        std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>>(keys, config);
    if (result.has_value()) {
        timer.LogResponse("result=", result.value().size(), " requests");
    }
    return result.value();
}

tl::expected<MasterMetricManager::CacheHitStatDict, ErrorCode>
MasterClient::CalcCacheStats() {
    return invoke_rpc<&WrappedMasterService::CalcCacheStats,
                      MasterMetricManager::CacheHitStatDict>();
}

tl::expected<
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
    ErrorCode>
MasterClient::BatchQueryIp(const std::vector<UUID>& client_ids) {
    ScopedVLogTimer timer(1, "MasterClient::BatchQueryIp");
    timer.LogRequest("client_ids_count=", client_ids.size());

    auto result = invoke_rpc<
        &WrappedMasterService::BatchQueryIp,
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>>(
        client_ids);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
             ErrorCode>
MasterClient::GetReplicaListByRegex(const std::string& str) {
    ScopedVLogTimer timer(1, "MasterClient::GetReplicaListByRegex");
    timer.LogRequest("Regex=", str);

    auto result = invoke_rpc<
        &WrappedMasterService::GetReplicaListByRegex,
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>>(str);

    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::Remove(std::string_view key,
                                                   bool force) {
    ScopedVLogTimer timer(1, "MasterClient::Remove");
    timer.LogRequest("key=", key, ", force=", force);

    auto result = invoke_rpc<&WrappedMasterService::Remove, void>(key, force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> MasterClient::RemoveByRegex(std::string_view str,
                                                          bool force) {
    ScopedVLogTimer timer(1, "MasterClient::RemoveByRegex");
    timer.LogRequest("key=", str, ", force=", force);

    auto result =
        invoke_rpc<&WrappedMasterService::RemoveByRegex, long>(str, force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> MasterClient::RemoveAll(bool force) {
    ScopedVLogTimer timer(1, "MasterClient::RemoveAll");
    timer.LogRequest("action=remove_all_objects, force=", force);

    auto result = invoke_rpc<&WrappedMasterService::RemoveAll, long>(force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::UnmountSegment(
    const UUID& segment_id) {
    ScopedVLogTimer timer(1, "MasterClient::UnmountSegment");
    timer.LogRequest("segment_id=", segment_id, ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::UnmountSegment, void>(
        segment_id, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<HeartbeatResponse, ErrorCode> MasterClient::Heartbeat(
    const HeartbeatRequest& req) {
    ScopedVLogTimer timer(1, "MasterClient::Heartbeat");
    timer.LogRequest("client_id=", client_id_);

    // Send via the dedicated heartbeat accessor (separate pool that targets the
    // master's heartbeat server when configured, else the main pool).
    auto result =
        invoke_rpc_via<&WrappedMasterService::Heartbeat, HeartbeatResponse>(
            heartbeat_accessor_, req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<QueryClientStatusResponse, ErrorCode>
MasterClient::QueryClientStatus(const UUID& client_id) {
    ScopedVLogTimer timer(1, "MasterClient::QueryClientStatus");
    timer.LogRequest("client_id=", client_id);

    QueryClientStatusRequest req;
    req.client_id = client_id;

    auto result = invoke_rpc<&WrappedMasterService::QueryClientStatus,
                             QueryClientStatusResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> MasterClient::MountSegment(
    const Segment& segment) {
    ScopedVLogTimer timer(1, "MasterClient::MountSegment");
    timer.LogRequest("segment_name=", segment.name, ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedMasterService::MountSegment, void>(
        segment, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<RegisterClientResponse, ErrorCode> MasterClient::RegisterClient(
    const RegisterClientRequest& req) {
    ScopedVLogTimer timer(1, "MasterClient::RegisterClient");
    timer.LogRequest("client_id=", client_id_,
                     ", segments_count=", req.segments.size(),
                     ", deployment_mode=", req.deployment_mode);

    auto result = invoke_rpc<&WrappedMasterService::RegisterClient,
                             RegisterClientResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<UnregisterClientResponse, ErrorCode>
MasterClient::UnregisterClient(const UnregisterClientRequest& req) {
    ScopedVLogTimer timer(1, "MasterClient::UnregisterClient");
    timer.LogRequest("client_id=", client_id_,
                     ", deployment_mode=", req.deployment_mode);

    auto result = invoke_rpc<&WrappedMasterService::UnregisterClient,
                             UnregisterClientResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

}  // namespace mooncake
