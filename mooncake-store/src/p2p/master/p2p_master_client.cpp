#include "p2p/master/p2p_master_client.h"

#include "p2p/master/p2p_rpc_service.h"
#include "utils/scoped_vlog_timer.h"
#include "version.h"

namespace mooncake {

template <>
struct RpcNameTraits<&P2PMasterRpcService::ExistKey> {
    static constexpr const char* value = "ExistKey";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::BatchExistKey> {
    static constexpr const char* value = "BatchExistKey";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::GetReadRouteByRegex> {
    static constexpr const char* value = "GetReadRouteByRegex";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::GetReadRoute> {
    static constexpr const char* value = "GetReadRoute";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::BatchGetReadRoute> {
    static constexpr const char* value = "BatchGetReadRoute";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::MountSegment> {
    static constexpr const char* value = "MountSegment";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::UnmountSegment> {
    static constexpr const char* value = "UnmountSegment";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::Heartbeat> {
    static constexpr const char* value = "Heartbeat";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::RegisterClient> {
    static constexpr const char* value = "RegisterClient";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::UnregisterClient> {
    static constexpr const char* value = "UnregisterClient";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::QueryClientStatus> {
    static constexpr const char* value = "QueryClientStatus";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::ServiceReady> {
    static constexpr const char* value = "ServiceReady";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::HeartbeatServiceReady> {
    static constexpr const char* value = "HeartbeatServiceReady";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::GetWriteRoute> {
    static constexpr const char* value = "GetWriteRoute";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::BatchGetWriteRoute> {
    static constexpr const char* value = "BatchGetWriteRoute";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::PublishRoute> {
    static constexpr const char* value = "PublishRoute";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::WithdrawRoute> {
    static constexpr const char* value = "WithdrawRoute";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::BatchWithdrawRoute> {
    static constexpr const char* value = "BatchWithdrawRoute";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::BatchSyncRoutes> {
    static constexpr const char* value = "BatchSyncRoutes";
};

template <>
struct RpcNameTraits<&P2PMasterRpcService::CompleteRouteSync> {
    static constexpr const char* value = "CompleteRouteSync";
};

ErrorCode P2PMasterClient::Connect(const std::string& master_addr) {
    ScopedVLogTimer timer(1, "P2PMasterClient::Connect");
    timer.LogRequest("master_addr=", master_addr);

    MutexLocker lock(&connect_mutex_);
    bool is_same_addr = (client_addr_param_ == master_addr);
    if (!is_same_addr) {
        auto client_pool = client_pools_->at(master_addr);
        client_accessor_.SetClientPool(client_pool);
        client_addr_param_ = master_addr;
        if (heartbeat_rpc_port_ > 0) {
            auto colon = master_addr.rfind(':');
            std::string host = (colon == std::string::npos)
                                   ? master_addr
                                   : master_addr.substr(0, colon);
            heartbeat_accessor_.SetClientPool(client_pools_->at(
                host + ":" + std::to_string(heartbeat_rpc_port_)));
        } else {
            heartbeat_accessor_.SetClientPool(client_pool);
        }
    }

    auto result =
        invoke_rpc<&P2PMasterRpcService::ServiceReady, std::string>();
    if (!result.has_value() && is_same_addr) {
        timer.LogResponse("error_code=", result.error());
        result =
            invoke_rpc<&P2PMasterRpcService::ServiceReady, std::string>();
    }
    if (!result.has_value()) {
        timer.LogResponse("error_code=", result.error());
        client_addr_param_.clear();
        return result.error();
    }

    std::string client_version = GetMooncakeStoreVersion();
    if (result.value() != client_version) {
        LOG(ERROR) << "Version mismatch: server=" << result.value()
                   << " client=" << client_version;
        timer.LogResponse("error_code=", ErrorCode::INVALID_VERSION);
        return ErrorCode::INVALID_VERSION;
    }

    auto hb_ready = invoke_rpc<&P2PMasterRpcService::HeartbeatServiceReady,
                               uint32_t>();
    if (!hb_ready.has_value()) {
        LOG(ERROR) << "HeartbeatServiceReady probe failed: error_code="
                   << hb_ready.error()
                   << " (master may predate this RPC; upgrade master first)";
        timer.LogResponse("error_code=", hb_ready.error());
        client_addr_param_.clear();
        return hb_ready.error();
    }
    const bool client_dedicated = heartbeat_rpc_port_ > 0;
    const bool master_dedicated = *hb_ready > 0;
    if (client_dedicated != master_dedicated) {
        LOG(ERROR) << "Heartbeat routing mismatch: client_hb_port="
                   << heartbeat_rpc_port_
                   << " master_hb_port=" << *hb_ready
                   << " (one side is dedicated, the other is legacy)";
        timer.LogResponse("error_code=",
                          ErrorCode::HEARTBEAT_ROUTING_MISMATCH);
        client_addr_param_.clear();
        return ErrorCode::HEARTBEAT_ROUTING_MISMATCH;
    }

    if (client_dedicated) {
        auto hb_result = invoke_rpc_via<&P2PMasterRpcService::ServiceReady,
                                        std::string>(heartbeat_accessor_);
        if (!hb_result.has_value() && is_same_addr) {
            hb_result =
                invoke_rpc_via<&P2PMasterRpcService::ServiceReady,
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

tl::expected<bool, ErrorCode> P2PMasterClient::ExistKey(
    std::string_view object_key) {
    ScopedVLogTimer timer(1, "P2PMasterClient::ExistKey");
    timer.LogRequest("object_key=", object_key);
    auto result =
        invoke_rpc<&P2PMasterRpcService::ExistKey, bool>(object_key);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<bool, ErrorCode>> P2PMasterClient::BatchExistKey(
    const std::vector<std::string_view>& object_keys) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchExistKey");
    timer.LogRequest("keys_count=", object_keys.size());
    auto result = invoke_batch_rpc<&P2PMasterRpcService::BatchExistKey, bool>(
        object_keys.size(), object_keys);
    timer.LogResponse("result=", result.size(), " keys");
    return result;
}

tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode>
P2PMasterClient::GetReadRoute(
    std::string_view key, const P2PReadRouteConfig& config) {
    ScopedVLogTimer timer(1, "P2PMasterClient::GetReadRoute");
    timer.LogRequest("object_key=", key);
    auto result = invoke_rpc<&P2PMasterRpcService::GetReadRoute,
                             std::vector<P2PRouteDescriptor>>(
        P2PGetReadRouteRequest{.key = key, .config = config});
    timer.LogResponseExpected(result);
    return result;
}

async_simple::coro::Lazy<
    tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode>>
P2PMasterClient::AsyncGetReadRoute(
    std::string_view key, const P2PReadRouteConfig& config) {
    co_return co_await invoke_rpc_async<
        &P2PMasterRpcService::GetReadRoute, std::vector<P2PRouteDescriptor>>(
        P2PGetReadRouteRequest{.key = key, .config = config});
}

std::vector<tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode>>
P2PMasterClient::BatchGetReadRoute(
    const std::vector<std::string_view>& keys,
    const P2PReadRouteConfig& config) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchGetReadRoute");
    timer.LogRequest("requests_count=", keys.size());
    if (keys.empty()) return {};

    P2PBatchGetReadRouteRequest req;
    req.config = config;
    req.keys.reserve(keys.size());
    for (std::string_view key : keys) {
        req.keys.emplace_back(key);
    }
    auto result = invoke_rpc<&P2PMasterRpcService::BatchGetReadRoute,
                             P2PBatchGetReadRouteResponse>(req);
    if (!result.has_value()) {
        LOG(ERROR) << "BatchGetReadRoute RPC failed: "
                   << toString(result.error());
        return std::vector<
            tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode>>(
            keys.size(), tl::make_unexpected(result.error()));
    }
    if (result->responses.size() != keys.size() ||
        result->error_codes.size() != keys.size()) {
        LOG(ERROR) << "BatchGetReadRoute RPC returned inconsistent result";
        return std::vector<
            tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode>>(
            keys.size(), tl::make_unexpected(ErrorCode::RPC_FAIL));
    }
    std::vector<tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode>>
        response;
    response.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
        if (result->error_codes[i] == ErrorCode::OK) {
            response.push_back(std::move(result->responses[i]));
        } else {
            response.emplace_back(
                tl::make_unexpected(result->error_codes[i]));
        }
    }
    timer.LogResponse("result=", response.size(), " requests");
    return response;
}

tl::expected<
    std::unordered_map<std::string, std::vector<P2PRouteDescriptor>>,
    ErrorCode>
P2PMasterClient::GetReadRouteByRegex(std::string_view regex) {
    ScopedVLogTimer timer(1, "P2PMasterClient::GetReadRouteByRegex");
    timer.LogRequest("Regex=", regex);
    auto result = invoke_rpc<
        &P2PMasterRpcService::GetReadRouteByRegex,
        std::unordered_map<std::string, std::vector<P2PRouteDescriptor>>>(
        regex);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::UnmountSegment(
    const UUID& segment_id) {
    ScopedVLogTimer timer(1, "P2PMasterClient::UnmountSegment");
    timer.LogRequest("segment_id=", segment_id, ", client_id=", client_id_);
    auto result = invoke_rpc<&P2PMasterRpcService::UnmountSegment, void>(
        P2PUnmountSegmentRequest{.client_id = client_id_,
                                 .segment_id = segment_id});
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<P2PHeartbeatResponse, ErrorCode> P2PMasterClient::Heartbeat(
    const P2PHeartbeatRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::Heartbeat");
    timer.LogRequest("client_id=", client_id_);
    auto result =
        invoke_rpc_via<&P2PMasterRpcService::Heartbeat, P2PHeartbeatResponse>(
            heartbeat_accessor_, req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<P2PClientStatus, ErrorCode>
P2PMasterClient::QueryClientStatus(const UUID& client_id) {
    ScopedVLogTimer timer(1, "P2PMasterClient::QueryClientStatus");
    timer.LogRequest("client_id=", client_id);
    auto result = invoke_rpc<&P2PMasterRpcService::QueryClientStatus,
                             P2PClientStatus>(client_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::MountSegment(
    const P2PSegment& segment) {
    ScopedVLogTimer timer(1, "P2PMasterClient::MountSegment");
    timer.LogRequest("segment_name=", segment.name,
                     ", client_id=", client_id_);
    auto result = invoke_rpc<&P2PMasterRpcService::MountSegment, void>(
        P2PMountSegmentRequest{.client_id = client_id_, .segment = segment});
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<ViewVersionId, ErrorCode>
P2PMasterClient::RegisterClient(const P2PRegisterClientRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::RegisterClient");
    timer.LogRequest("client_id=", client_id_,
                     ", segments_count=", req.segments.size());
    auto result = invoke_rpc<&P2PMasterRpcService::RegisterClient,
                             ViewVersionId>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<ViewVersionId, ErrorCode>
P2PMasterClient::UnregisterClient(const UUID& client_id) {
    ScopedVLogTimer timer(1, "P2PMasterClient::UnregisterClient");
    timer.LogRequest("client_id=", client_id_);
    auto result = invoke_rpc<&P2PMasterRpcService::UnregisterClient,
                             ViewVersionId>(client_id);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<std::vector<P2PWriteCandidate>, ErrorCode>
P2PMasterClient::GetWriteRoute(const P2PGetWriteRouteRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::GetWriteRoute");
    timer.LogRequest("key=", req.key);

    auto result = invoke_rpc<&P2PMasterRpcService::GetWriteRoute,
                             std::vector<P2PWriteCandidate>>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<P2PBatchGetWriteRouteResponse, ErrorCode>
P2PMasterClient::BatchGetWriteRoute(const P2PBatchGetWriteRouteRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchGetWriteRoute");
    timer.LogRequest("key_count=", req.keys.size());

    auto result = invoke_rpc<&P2PMasterRpcService::BatchGetWriteRoute,
                             P2PBatchGetWriteRouteResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::PublishRoute(
    const P2PPublishRouteRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::PublishRoute");
    timer.LogRequest("key=", req.key);

    auto result = invoke_rpc<&P2PMasterRpcService::PublishRoute, void>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::WithdrawRoute(
    const P2PWithdrawRouteRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::WithdrawRoute");
    timer.LogRequest("key=", req.key);

    auto result =
        invoke_rpc<&P2PMasterRpcService::WithdrawRoute, void>(req);
    timer.LogResponseExpected(result);
    return result;
}

std::vector<tl::expected<void, ErrorCode>> P2PMasterClient::BatchWithdrawRoute(
    const P2PBatchWithdrawRouteRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchWithdrawRoute");
    timer.LogRequest("key=", req.key, "segment_count=", req.segment_ids.size());

    auto result = invoke_batch_rpc<&P2PMasterRpcService::BatchWithdrawRoute,
                                   void>(req.segment_ids.size(), req);
    timer.LogResponse("result=", result.size(), " routes");
    return result;
}

tl::expected<P2PBatchSyncRoutesResponse, ErrorCode>
P2PMasterClient::BatchSyncRoutes(const P2PBatchSyncRoutesRequest& req) {
    ScopedVLogTimer timer(1, "P2PMasterClient::BatchSyncRoutes");
    timer.LogRequest("adds=", req.publish_operations.size(),
                     ", removes=", req.withdraw_operations.size());

    auto result = invoke_rpc<&P2PMasterRpcService::BatchSyncRoutes,
                             P2PBatchSyncRoutesResponse>(req);
    timer.LogResponseExpected(result);
    return result;
}

tl::expected<void, ErrorCode> P2PMasterClient::CompleteRouteSync(
    const UUID& client_id) {
    ScopedVLogTimer timer(1, "P2PMasterClient::CompleteRouteSync");
    timer.LogRequest("client_id=", client_id);

    auto result = invoke_rpc<&P2PMasterRpcService::CompleteRouteSync, void>(
        client_id);
    timer.LogResponseExpected(result);
    return result;
}

}  // namespace mooncake
