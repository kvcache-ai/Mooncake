// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "engine.h"

#include <netdb.h>

#include <chrono>
#include <string>
#include <string_view>
#include <utility>

#include <glog/logging.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "error.h"

namespace mooncake {
namespace {

int MapTransportError(const coro_rpc::rpc_error &error) {
    const auto ec = error.code.ec;
    if (ec == coro_rpc::errc::message_too_large) return ERR_INVALID_ARGUMENT;
    if (ec == coro_rpc::errc::protocol_error ||
        ec == coro_rpc::errc::function_not_registered ||
        ec == coro_rpc::errc::invalid_rpc_arguments ||
        ec == coro_rpc::errc::invalid_rpc_result ||
        ec == coro_rpc::errc::rpc_throw_exception) {
        return ERR_MALFORMED_JSON;
    }
    return ERR_SOCKET;
}

int MapStatus(int32_t status) {
    switch (static_cast<HandshakeStatus>(status)) {
        case HandshakeStatus::kOk:
            return 0;
        case HandshakeStatus::kBusy:
            return ERR_TOO_MANY_REQUESTS;
        case HandshakeStatus::kBadJson:
            return ERR_MALFORMED_JSON;
        case HandshakeStatus::kCallbackMissing:
        case HandshakeStatus::kHandlerFailed:
            return ERR_METADATA;
        case HandshakeStatus::kResponseTooLarge:
            return ERR_INVALID_ARGUMENT;
        default:
            return ERR_MALFORMED_JSON;
    }
}

bool MayRetry(int func_id) {
    return func_id == kHandshakeMetadata || func_id == kHandshakeProbe;
}

bool TransportFailed(coro_rpc::errc ec) {
    return ec == coro_rpc::errc::timed_out || ec == coro_rpc::errc::io_error ||
           ec == coro_rpc::errc::not_connected;
}

// coro_rpc reports resolve failure and connect failure as not_connected.
// Match SocketHandShakePlugin: only a name that getaddrinfo cannot resolve
// is ERR_DNS. A refused connect stays ERR_SOCKET.
bool HostResolves(const std::string &host, uint16_t port, bool ipv6) {
    addrinfo hints{};
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_family = ipv6 ? AF_INET6 : AF_INET;
    addrinfo *result = nullptr;
    const std::string service = std::to_string(port);
    const int rc = getaddrinfo(host.c_str(), service.c_str(), &hints, &result);
    if (rc != 0) {
        LOG(ERROR) << "handshake DNS failed for " << host << ":" << port << ": "
                   << gai_strerror(rc);
        return false;
    }
    const bool resolved = result != nullptr;
    freeaddrinfo(result);
    return resolved;
}

int ClassifyConnectError(coro_rpc::errc ec, const std::string &host,
                         uint16_t port, bool ipv6) {
    if (ec == coro_rpc::errc::not_connected ||
        ec == coro_rpc::errc::bad_address) {
        if (!HostResolves(host, port, ipv6)) return ERR_DNS;
    }
    return ERR_SOCKET;
}

}  // namespace

int HandshakeRpc::Call(const std::string &host, uint16_t port, int func_id,
                       const std::string &request, std::string &response) {
    if (request.size() > limits_.max_frame) {
        LOG(ERROR) << "handshake request exceeds MC_HANDSHAKE_MAX_LENGTH";
        return ERR_INVALID_ARGUMENT;
    }
    auto result =
        async_simple::coro::syncAwait(CallOne(host, port, func_id, request));
    if (result.first == 0) response = std::move(result.second);
    return result.first;
}

async_simple::coro::Lazy<std::pair<int, std::string>> HandshakeRpc::CallOne(
    std::string host, uint16_t port, int func_id, std::string request) {
    for (int attempt = 0; attempt < 2; ++attempt) {
        HandshakeClientPool::Lease lease = pool_->Acquire(host, port);
        if (!lease.pool) {
            co_return std::make_pair(ERR_TOO_MANY_REQUESTS, std::string{});
        }
        if (!lease.client) {
            lease.client = std::make_unique<coro_rpc::coro_rpc_client>(
                HandshakeClientExecutor(), pool_->ClientConfig());
            auto connected = co_await lease.client->connect(
                host, std::to_string(port),
                std::chrono::seconds(limits_.connect_timeout_seconds));
            if (connected) {
                lease.broken = true;
                co_return std::make_pair(
                    ClassifyConnectError(connected.ec, host, port,
                                         limits_.ipv6),
                    std::string{});
            }
        }
        if (lease.client->has_closed()) {
            lease.broken = true;
            if (attempt == 0) continue;
            co_return std::make_pair(ERR_SOCKET, std::string{});
        }
        lease.client->set_req_attachment(request);
        auto result =
            co_await lease.client->call<&HandshakeRpc::process>(func_id);
        if (!result.has_value()) {
            const auto ec = result.error().code.ec;
            lease.broken = true;
            if (attempt == 0 && lease.from_pool && MayRetry(func_id) &&
                TransportFailed(ec)) {
                pool_->ClearGeneration(lease);
                continue;
            }
            co_return std::make_pair(MapTransportError(result.error()),
                                     std::string{});
        }
        const int status = MapStatus(result.value());
        if (status != 0) co_return std::make_pair(status, std::string{});
        const std::string_view view = lease.client->get_resp_attachment();
        if (view.size() > limits_.max_frame) {
            lease.broken = true;
            co_return std::make_pair(ERR_INVALID_ARGUMENT, std::string{});
        }
        co_return std::make_pair(0, std::string(view));
    }
    co_return std::make_pair(ERR_SOCKET, std::string{});
}

}  // namespace mooncake
