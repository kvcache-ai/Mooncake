#include "p2p_master_client.h"

#include "p2p_rpc_service.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

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

    auto result = invoke_rpc<&WrappedP2PMasterService::AddReplica, void>(req);
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

std::vector<tl::expected<void, ErrorCode>> P2PMasterClient::BatchRemoveReplica(
    const BatchRemoveReplicaRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchRemoveReplica");
    timer.LogRequest("key=", req.key, "segment_count=", req.segment_ids.size());

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
        invoke_rpc<&WrappedP2PMasterService::SetSyncCompleted, void>(client_id);
    timer.LogResponseExpected(result);
    return result;
}

}  // namespace mooncake
