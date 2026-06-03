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

async_simple::coro::Lazy<tl::expected<UUID, ErrorCode>>
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

async_simple::coro::Lazy<tl::expected<PreWriteResponse, ErrorCode>>
PeerClient::AsyncPreWrite(const PreWriteRequest& request) {
    if (!client_pool_) {
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto ret = co_await client_pool_->send_request(
        [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
            return client.send_request<&ClientRpcService::PreWrite>(request);
        });
    if (!ret.has_value()) {
        LOG(ERROR) << "AsyncPreWrite: client not available";
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = co_await std::move(ret.value());
    if (!result) {
        LOG(ERROR) << "AsyncPreWrite: RPC call failed: " << result.error().msg;
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    co_return result->result();
}

async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
PeerClient::AsyncWriteCommit(const WriteCommitRequest& request) {
    if (!client_pool_) {
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto ret = co_await client_pool_->send_request(
        [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
            return client.send_request<&ClientRpcService::WriteCommit>(request);
        });
    if (!ret.has_value()) {
        LOG(ERROR) << "AsyncWriteCommit: client not available";
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = co_await std::move(ret.value());
    if (!result) {
        LOG(ERROR) << "AsyncWriteCommit: RPC call failed: "
                   << result.error().msg;
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    co_return result->result();
}

async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
PeerClient::AsyncWriteRevoke(const WriteRevokeRequest& request) {
    if (!client_pool_) {
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto ret = co_await client_pool_->send_request(
        [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
            return client.send_request<&ClientRpcService::WriteRevoke>(request);
        });
    if (!ret.has_value()) {
        LOG(ERROR) << "AsyncWriteRevoke: client not available";
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = co_await std::move(ret.value());
    if (!result) {
        LOG(ERROR) << "AsyncWriteRevoke: RPC call failed: "
                   << result.error().msg;
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    co_return result->result();
}

async_simple::coro::Lazy<tl::expected<PinKeyResponse, ErrorCode>>
PeerClient::AsyncPinKey(const PinKeyRequest& request) {
    if (!client_pool_) {
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto ret = co_await client_pool_->send_request(
        [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
            return client.send_request<&ClientRpcService::PinKey>(request);
        });
    if (!ret.has_value()) {
        LOG(ERROR) << "AsyncPinKey: client not available";
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = co_await std::move(ret.value());
    if (!result) {
        LOG(ERROR) << "AsyncPinKey: RPC call failed: " << result.error().msg;
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    co_return result->result();
}

async_simple::coro::Lazy<tl::expected<void, ErrorCode>>
PeerClient::AsyncUnPinKey(const UnPinKeyRequest& request) {
    if (!client_pool_) {
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto ret = co_await client_pool_->send_request(
        [&](coro_io::client_reuse_hint, coro_rpc::coro_rpc_client& client) {
            return client.send_request<&ClientRpcService::UnPinKey>(request);
        });
    if (!ret.has_value()) {
        LOG(ERROR) << "AsyncUnPinKey: client not available";
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    auto result = co_await std::move(ret.value());
    if (!result) {
        LOG(ERROR) << "AsyncUnPinKey: RPC call failed: " << result.error().msg;
        co_return tl::make_unexpected(ErrorCode::RPC_FAIL);
    }

    co_return result->result();
}

tl::expected<void, ErrorCode> PeerClient::ReadRemoteData(
    const RemoteReadRequest& request) {
    return async_simple::coro::syncAwait(AsyncReadRemoteData(request));
}

tl::expected<UUID, ErrorCode> PeerClient::WriteRemoteData(
    const RemoteWriteRequest& request) {
    return async_simple::coro::syncAwait(AsyncWriteRemoteData(request));
}

tl::expected<PreWriteResponse, ErrorCode> PeerClient::PreWrite(
    const PreWriteRequest& request) {
    return async_simple::coro::syncAwait(AsyncPreWrite(request));
}

tl::expected<void, ErrorCode> PeerClient::WriteCommit(
    const WriteCommitRequest& request) {
    return async_simple::coro::syncAwait(AsyncWriteCommit(request));
}

tl::expected<void, ErrorCode> PeerClient::WriteRevoke(
    const WriteRevokeRequest& request) {
    return async_simple::coro::syncAwait(AsyncWriteRevoke(request));
}

tl::expected<PinKeyResponse, ErrorCode> PeerClient::PinKey(
    const PinKeyRequest& request) {
    return async_simple::coro::syncAwait(AsyncPinKey(request));
}

tl::expected<void, ErrorCode> PeerClient::UnPinKey(
    const UnPinKeyRequest& request) {
    return async_simple::coro::syncAwait(AsyncUnPinKey(request));
}

}  // namespace mooncake
