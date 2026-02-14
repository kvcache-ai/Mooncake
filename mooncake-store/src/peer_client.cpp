#include "peer_client.h"

#include <cstdlib>
#include <glog/logging.h>

#include "client_rpc_service.h"
#include "types.h"

namespace mooncake {

tl::expected<void, ErrorCode> PeerClient::Connect(const std::string& endpoint) {
    endpoint_ = endpoint;

    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config pool_conf{};
    const char* value = std::getenv("MC_RPC_PROTOCOL");
    if (value && std::string_view(value) == "rdma") {
        pool_conf.client_config.socket_config =
            coro_io::ib_socket_t::config_t{};
    }

    client_pools_ =
        std::make_shared<coro_io::client_pools<coro_rpc::coro_rpc_client>>(
            pool_conf);
    client_pool_ = client_pools_->at(endpoint);

    return {};
}

async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
PeerClient::AsyncReadRemoteData(const RemoteReadRequest& request) {
    if (!client_pool_) {
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto ret = co_await client_pool_->send_request(
        [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
            return client.send_request<&ClientRpcService::ReadRemoteData>(
                request);
        });
    if (!ret.has_value()) {
        LOG(ERROR) << "AsyncReadRemoteData: client not available";
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = co_await std::move(ret.value());
    if (!result) {
        LOG(ERROR) << "AsyncReadRemoteData: RPC call failed: "
                   << result.error().msg;
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    co_return result->result();
}

async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
PeerClient::AsyncWriteRemoteData(const RemoteWriteRequest& request) {
    if (!client_pool_) {
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto ret = co_await client_pool_->send_request(
        [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
            return client.send_request<&ClientRpcService::WriteRemoteData>(
                request);
        });
    if (!ret.has_value()) {
        LOG(ERROR) << "AsyncWriteRemoteData: client not available";
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = co_await std::move(ret.value());
    if (!result) {
        LOG(ERROR) << "AsyncWriteRemoteData: RPC call failed: "
                   << result.error().msg;
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    co_return result->result();
}

tl::expected<void, ErrorCode> PeerClient::ReadRemoteData(
    const RemoteReadRequest& request) {
    return async_simple::coro::syncAwait(AsyncReadRemoteData(request));
}

tl::expected<void, ErrorCode> PeerClient::WriteRemoteData(
    const RemoteWriteRequest& request) {
    return async_simple::coro::syncAwait(AsyncWriteRemoteData(request));
}

}  // namespace mooncake
