// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp>

namespace mooncake::tent {

// Real loopback peer: acknowledge one warmup, then consume an entire second
// request before dropping/truncating its response. Keep accepting afterward
// so an illicit reconnect/replay is observable in the counters.
class RpcFaultPeer {
   public:
    enum class Fault { Eof, Reset, ShortHeader, ShortBody, Timeout };
    explicit RpcFaultPeer(Fault fault) : fault_(fault) {
        listener_ = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
        if (listener_ < 0) return;
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        if (::bind(listener_, reinterpret_cast<sockaddr*>(&address),
                   sizeof(address)) ||
            ::listen(listener_, 8))
            return;
        socklen_t length = sizeof(address);
        if (::getsockname(listener_, reinterpret_cast<sockaddr*>(&address),
                          &length))
            return;
        port_ = ntohs(address.sin_port);
        worker_ = std::thread([this] { serve(); });
    }
    ~RpcFaultPeer() {
        stop_ = true;
        if (worker_.joinable()) worker_.join();
        if (listener_ >= 0) ::close(listener_);
    }
    bool available() const { return port_ != 0; }
    std::string address() const { return "127.0.0.1:" + std::to_string(port_); }
    std::atomic<int> requests{0};
    std::atomic<int> connections{0};

   private:
    using Protocol = coro_rpc::protocol::coro_rpc_protocol;

    bool readExact(int socket, void* buffer, size_t size) {
        auto* data = static_cast<char*>(buffer);
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(3);
        while (size && !stop_ && std::chrono::steady_clock::now() < deadline) {
            pollfd event{socket, POLLIN, 0};
            if (::poll(&event, 1, 10) <= 0) continue;
            const auto count = ::recv(socket, data, size, MSG_DONTWAIT);
            if (count <= 0) return false;
            data += count;
            size -= count;
        }
        return size == 0;
    }

    void serve() {
        while (!stop_) {
            pollfd event{listener_, POLLIN, 0};
            if (::poll(&event, 1, 10) <= 0) continue;
            const int socket = ::accept(listener_, nullptr, nullptr);
            if (socket < 0) continue;
            ++connections;
            while (!stop_) {
                std::array<char, Protocol::REQ_HEAD_LEN> wire{};
                if (!readExact(socket, wire.data(), wire.size())) break;
                Protocol::req_header request{};
                auto ec = struct_pack::deserialize_to<
                    struct_pack::sp_config::DISABLE_ALL_META_INFO>(
                    request, std::string_view(wire.data(), wire.size()));
                const uint64_t length =
                    uint64_t{request.length} + request.attach_length;
                if (ec || length > 65536) break;
                std::vector<char> payload(length);
                if (!readExact(socket, payload.data(), payload.size())) break;
                const bool fail = ++requests == 2;
                if (fail && fault_ == Fault::Reset) {
                    linger reset{1, 0};
                    ::setsockopt(socket, SOL_SOCKET, SO_LINGER, &reset,
                                 sizeof(reset));
                } else if (fail && fault_ == Fault::Timeout) {
                    // Hold the reply until the caller's timer closes the
                    // connection. A complete request was already received.
                    char byte;
                    (void)readExact(socket, &byte, 1);
                } else if (!fail || fault_ == Fault::ShortHeader ||
                           fault_ == Fault::ShortBody) {
                    Protocol::resp_header reply{};
                    reply.magic = Protocol::magic_number;
                    reply.seq_num = request.seq_num;
                    auto body = struct_pack::serialize(std::monostate{});
                    reply.length = body.size();
                    auto bytes = struct_pack::serialize<
                        struct_pack::sp_config::DISABLE_ALL_META_INFO>(reply);
                    const auto count =
                        fail && fault_ == Fault::ShortHeader ? 3 : bytes.size();
                    if (::send(socket, bytes.data(), count, MSG_NOSIGNAL) !=
                        static_cast<ssize_t>(count))
                        break;
                    if (!fail || fault_ == Fault::ShortBody) {
                        const auto body_size = fail ? 1 : body.size();
                        if (::send(socket, body.data(), body_size,
                                   MSG_NOSIGNAL) !=
                            static_cast<ssize_t>(body_size))
                            break;
                    }
                }
                if (fail) break;
            }
            ::close(socket);
        }
    }

    Fault fault_;
    int listener_ = -1;
    uint16_t port_ = 0;
    std::atomic<bool> stop_{false};
    std::thread worker_;
};

}  // namespace mooncake::tent
