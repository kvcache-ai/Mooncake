#include "p2p_master_client.h"

#include "p2p_rpc_service.h"
#include "utils/scoped_vlog_timer.h"

namespace mooncake {

template <>
struct RpcNameTraits<&WrappedP2PMasterService::GetWriteRoute> {
    static constexpr const char* value = "GetWriteRoute";
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
struct RpcNameTraits<&WrappedP2PMasterService::BatchMutateReplica> {
    static constexpr const char* value = "BatchMutateReplica";
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

std::vector<tl::expected<void, ErrorCode>> P2PMasterClient::BatchMutateReplica(
    const BatchReplicaMutationRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchMutateReplica");
    timer.LogRequest("mutation_count=", req.mutations.size());

    auto result = invoke_rpc<&WrappedP2PMasterService::BatchMutateReplica,
                             std::vector<tl::expected<void, ErrorCode>>>(req);
    if (!result) {
        LOG(ERROR) << "BatchMutateReplica RPC failed: "
                   << toString(result.error());
        std::vector<tl::expected<void, ErrorCode>> fallback;
        fallback.reserve(req.mutations.size());
        for (size_t i = 0; i < req.mutations.size(); ++i) {
            fallback.push_back(tl::make_unexpected(result.error()));
        }
        return fallback;
    }
    return *result;
}

}  // namespace mooncake
