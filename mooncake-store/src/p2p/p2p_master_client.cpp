#include "p2p/p2p_master_client.h"

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
#include "p2p/p2p_rpc_service.h"
#include "types.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

namespace mooncake {

template <>
struct RpcNameTraits<&WrappedP2PMasterService::ExistKey> {
    static constexpr const char* value = "ExistKey";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::BatchExistKey> {
    static constexpr const char* value = "BatchExistKey";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::CalcCacheStats> {
    static constexpr const char* value = "CalcCacheStats";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::BatchQueryIp> {
    static constexpr const char* value = "BatchQueryIp";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::GetReplicaListByRegex> {
    static constexpr const char* value = "GetReplicaListByRegex";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::GetReplicaList> {
    static constexpr const char* value = "GetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::BatchGetReplicaList> {
    static constexpr const char* value = "BatchGetReplicaList";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::Remove> {
    static constexpr const char* value = "Remove";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::RemoveByRegex> {
    static constexpr const char* value = "RemoveByRegex";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::RemoveAll> {
    static constexpr const char* value = "RemoveAll";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::MountSegment> {
    static constexpr const char* value = "MountSegment";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::UnmountSegment> {
    static constexpr const char* value = "UnmountSegment";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::Heartbeat> {
    static constexpr const char* value = "Heartbeat";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::RegisterClient> {
    static constexpr const char* value = "RegisterClient";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::UnregisterClient> {
    static constexpr const char* value = "UnregisterClient";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::QueryClientStatus> {
    static constexpr const char* value = "QueryClientStatus";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::ServiceReady> {
    static constexpr const char* value = "ServiceReady";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::GetWriteRoute> {
    static constexpr const char* value = "GetWriteRoute";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::BatchGetWriteRoute> {
    static constexpr const char* value = "BatchGetWriteRoute";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::AddReplica> {
    static constexpr const char* value = "AddReplica";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::RemoveReplica> {
    static constexpr const char* value = "RemoveReplica";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::BatchRemoveReplica> {
    static constexpr const char* value = "BatchRemoveReplica";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::BatchSyncReplica> {
    static constexpr const char* value = "BatchSyncReplica";
};

template <>
struct RpcNameTraits<&WrappedP2PMasterService::SetSyncCompleted> {
    static constexpr const char* value = "SetSyncCompleted";
};

tl::expected<void, ErrorCode> P2PMasterClient::Connect(const std::string& master_addr) {
    ScopedVLogTimer timer(1, "P2PMasterClient::Connect");
    timer.LogRequest("master_addr=", master_addr);

    MutexLocker lock(&connect_mutex_);
    bool is_same_addr = (client_addr_param_ == master_addr);
    if (!is_same_addr) {
        auto client_pool = client_pools_->at(master_addr);
        client_accessor_.SetClientPool(client_pool);
        client_addr_param_ = master_addr;
    }
    auto result = invoke_rpc<&WrappedP2PMasterService::ServiceReady,
                             std::string>();
    if (!result.has_value() && is_same_addr) {
        timer.LogResponse("error_code=", result.error());
        result = invoke_rpc<&WrappedP2PMasterService::ServiceReady,
                            std::string>();
    }

    if (!result.has_value()) {
        timer.LogResponse("error_code=", result.error());
        client_addr_param_.clear();
        return tl::unexpected(result.error());
    }
    std::string server_version = result.value();
    std::string client_version = GetMooncakeStoreVersion();
    if (server_version != client_version) {
        LOG(ERROR) << "Version mismatch: server=" << server_version
                   << " client=" << client_version;
        timer.LogResponse("error_code=", ErrorCode::INVALID_VERSION);
        return tl::unexpected(ErrorCode::INVALID_VERSION);
    }
    timer.LogResponse("error_code=", ErrorCode::OK);
    return {};
}

tl::expected<bool, ErrorCode> P2PMasterClient::ExistKey(
    std::string_view object_key) {
    ScopedVLogTimer timer(1, "P2PMasterClient::ExistKey");
    timer.LogRequest("object_key=", object_key);

    auto result = invoke_rpc<&WrappedP2PMasterService::ExistKey, bool>(
        object_key);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> P2PMasterClient::BatchExistKey(
    const std::vector<std::string_view>& object_keys) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchExistKey");
    timer.LogRequest("keys_count=", object_keys.size());

    auto result =
        invoke_batch_rpc<&WrappedP2PMasterService::BatchExistKey, bool>(
            object_keys.size(), object_keys);
    timer.LogResponse("result=", result.size(), " keys");
    return result;
}

tl::expected<GetReplicaListResponse, ErrorCode>
P2PMasterClient::GetReplicaList(std::string_view key,
                                const GetReplicaListRequestConfig& config) {
    ScopedVLogTimer timer(1, "P2PMasterClient::GetReplicaList");
    timer.LogRequest("object_key=", key);

    auto result = invoke_rpc<&WrappedP2PMasterService::GetReplicaList,
                             GetReplicaListResponse>(key, config);
    timer.LogResponseExpected(result);
    return result;
}

async_simple::coro::Lazy<tl::expected<GetReplicaListResponse, ErrorCode>>
P2PMasterClient::AsyncGetReplicaList(
    std::string_view key, const GetReplicaListRequestConfig& config) {
    auto result =
        co_await invoke_rpc_async<&WrappedP2PMasterService::GetReplicaList,
                                  GetReplicaListResponse>(key, config);
    co_return result;
}

std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
P2PMasterClient::BatchGetReplicaList(
    const std::vector<std::string_view>& keys,
    const GetReplicaListRequestConfig& config) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchGetReplicaList");
    timer.LogRequest("requests_count=", keys.size());

    if (keys.empty()) {
        return {};
    }

    auto result = invoke_rpc<
        &WrappedP2PMasterService::BatchGetReplicaList,
        std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>>(keys,
                                                                      config);
    if (result.has_value()) {
        timer.LogResponse("result=", result.value().size(), " requests");
    }
    return result.value();
}

CacheStats P2PMasterClient::CalcCacheStats() {
    return CacheStats{};
}

tl::expected<std::vector<std::string>, ErrorCode>
P2PMasterClient::BatchQueryIp(const std::vector<UUID>& client_ids) {
    return std::vector<std::string>{};
}

tl::expected<std::vector<Replica>, ErrorCode>
P2PMasterClient::GetReplicaListByRegex(const std::string& str) {
    return std::vector<Replica>{};
}

tl::expected<void, ErrorCode> P2PMasterClient::Remove(std::string_view key,
                                                      bool force) {
    ScopedVLogTimer timer(1, "P2PMasterClient::Remove");
    timer.LogRequest("key=", key, ", force=", force);

    auto result =
        invoke_rpc<&WrappedP2PMasterService::Remove, void>(key, force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> P2PMasterClient::RemoveByRegex(
    std::string_view str, bool force) {
    ScopedVLogTimer timer(1, "P2PMasterClient::RemoveByRegex");
    timer.LogRequest("key=", str, ", force=", force);

    auto result =
        invoke_rpc<&WrappedP2PMasterService::RemoveByRegex, long>(str, force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<long, ErrorCode> P2PMasterClient::RemoveAll(bool force) {
    ScopedVLogTimer timer(1, "P2PMasterClient::RemoveAll");
    timer.LogRequest("action=remove_all_objects, force=", force);

    auto result =
        invoke_rpc<&WrappedP2PMasterService::RemoveAll, long>(force);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::UnmountSegment(
    const UUID& segment_id) {
    ScopedVLogTimer timer(1, "P2PMasterClient::UnmountSegment");
    timer.LogRequest("segment_id=", segment_id, ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedP2PMasterService::UnmountSegment, void>(
        segment_id, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<HeartbeatResponse, ErrorCode> P2PMasterClient::Heartbeat(
    const HeartbeatRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::Heartbeat");
    timer.LogRequest("client_id=", client_id_);

    auto result = invoke_rpc<&WrappedP2PMasterService::Heartbeat,
                             HeartbeatResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<QueryClientStatusResponse, ErrorCode>
P2PMasterClient::QueryClientStatus(const UUID& client_id) {
    ScopedVLogTimer timer(1, "P2PMasterClient::QueryClientStatus");
    timer.LogRequest("client_id=", client_id);

    QueryClientStatusRequest req;
    req.client_id = client_id;

    auto result = invoke_rpc<&WrappedP2PMasterService::QueryClientStatus,
                             QueryClientStatusResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::MountSegment(
    const Segment& segment) {
    ScopedVLogTimer timer(1, "P2PMasterClient::MountSegment");
    timer.LogRequest("segment_name=", segment.name, ", client_id=", client_id_);

    auto result = invoke_rpc<&WrappedP2PMasterService::MountSegment, void>(
        segment, client_id_);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<RegisterClientResponse, ErrorCode>
P2PMasterClient::RegisterClient(const RegisterClientRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::RegisterClient");
    timer.LogRequest("client_id=", client_id_,
                     ", segments_count=", req.segments.size(),
                     ", deployment_mode=", req.deployment_mode);

    auto result = invoke_rpc<&WrappedP2PMasterService::RegisterClient,
                             RegisterClientResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<UnregisterClientResponse, ErrorCode>
P2PMasterClient::UnregisterClient(const UnregisterClientRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::UnregisterClient");
    timer.LogRequest("client_id=", client_id_,
                     ", deployment_mode=", req.deployment_mode);

    auto result = invoke_rpc<&WrappedP2PMasterService::UnregisterClient,
                             UnregisterClientResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<WriteRouteResponse, ErrorCode> P2PMasterClient::GetWriteRoute(
    const WriteRouteRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::GetWriteRoute");
    timer.LogRequest("key=", req.key);

    auto result =
        invoke_rpc<&WrappedP2PMasterService::GetWriteRoute, WriteRouteResponse>(
            req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<BatchGetWriteRouteResponse, ErrorCode>
P2PMasterClient::BatchGetWriteRoute(const BatchGetWriteRouteRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchGetWriteRoute");
    timer.LogRequest("key_count=", req.keys.size());

    auto result = invoke_rpc<&WrappedP2PMasterService::BatchGetWriteRoute,
                             BatchGetWriteRouteResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::AddReplica(
    const AddReplicaRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::AddReplica");
    timer.LogRequest("key=", req.key);

    auto result =
        invoke_rpc<&WrappedP2PMasterService::AddReplica, void>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::RemoveReplica(
    const RemoveReplicaRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::RemoveReplica");
    timer.LogRequest("key=", req.key);

    auto result =
        invoke_rpc<&WrappedP2PMasterService::RemoveReplica, void>(req);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>>
P2PMasterClient::BatchRemoveReplica(const BatchRemoveReplicaRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchRemoveReplica");
    timer.LogRequest("key=", req.key,
                     "segment_count=", req.segment_ids.size());

    auto result = invoke_rpc<&WrappedP2PMasterService::BatchRemoveReplica,
                             std::vector<tl::expected<void, ErrorCode>>>(req);

    if (!result) {
        LOG(ERROR) << "BatchRemoveReplica RPC failed: "
                   << toString(result.error());
        std::vector<tl::expected<void, ErrorCode>> fallback;
        for (size_t i = 0; i < req.segment_ids.size(); i++) {
            fallback.push_back(tl::make_unexpected(result.error()));
        }
        return fallback;
    }
    return *result;
}

tl::expected<BatchSyncReplicaResponse, ErrorCode>
P2PMasterClient::BatchSyncReplica(const BatchSyncReplicaRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchSyncReplica");
    timer.LogRequest("adds=", req.add_keys.size(),
                     ", removes=", req.remove_keys.size());

    auto result = invoke_rpc<&WrappedP2PMasterService::BatchSyncReplica,
                             BatchSyncReplicaResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::SetSyncCompleted(
    UUID client_id) {
    ScopedVLogTimer timer(1, "P2PMasterClient::SetSyncCompleted");
    timer.LogRequest("client_id=", client_id);

    auto result =
        invoke_rpc<&WrappedP2PMasterService::SetSyncCompleted, void>(
            client_id);
    timer.LogResponseExpected(result);
    return result;
}

}  // namespace mooncake