// Copyright 2025 KVCache.AI
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

#include "tent/transport/tcp_hp/conn_manager.h"

#include <glog/logging.h>
#include <poll.h>
#include <netinet/tcp.h>
#include <sys/socket.h>

#include <chrono>
#include <thread>

#include <asio.hpp>

#include "tent/transport/tcp_hp/protocol.h"

namespace mooncake {
namespace tent {
namespace tcp_hp {

ConnManager::ConnManager(IoContextPool& pool, ConnOptions opts)
    : pool_(pool), opts_(std::move(opts)) {}

ConnManager::~ConnManager() { shutdown(); }

std::string ConnManager::makeKey(const std::string& host, uint16_t port) {
    return host + ":" + std::to_string(port);
}

void ConnManager::configureSocket(asio::ip::tcp::socket& socket) {
    asio::error_code ec;

    // TCP_NODELAY: disable Nagle for low latency
    socket.set_option(asio::ip::tcp::no_delay(true), ec);
    if (ec) LOG(WARNING) << "set TCP_NODELAY failed: " << ec.message();

    // SO_KEEPALIVE
    socket.set_option(asio::socket_base::keep_alive(true), ec);
    if (ec) LOG(WARNING) << "set SO_KEEPALIVE failed: " << ec.message();

    int fd = socket.native_handle();

    // TCP keepalive parameters (Linux-specific)
#ifdef TCP_KEEPIDLE
    if (opts_.keepalive_idle_s > 0) {
        setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &opts_.keepalive_idle_s,
                   sizeof(opts_.keepalive_idle_s));
    }
#endif
#ifdef TCP_KEEPINTVL
    if (opts_.keepalive_interval_s > 0) {
        setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &opts_.keepalive_interval_s,
                   sizeof(opts_.keepalive_interval_s));
    }
#endif
#ifdef TCP_KEEPCNT
    if (opts_.keepalive_count > 0) {
        setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &opts_.keepalive_count,
                   sizeof(opts_.keepalive_count));
    }
#endif

    // Socket buffer sizes
    if (opts_.sndbuf > 0) {
        socket.set_option(asio::socket_base::send_buffer_size(opts_.sndbuf), ec);
        if (ec) LOG(WARNING) << "set SO_SNDBUF failed: " << ec.message();
    }
    if (opts_.rcvbuf > 0) {
        socket.set_option(
            asio::socket_base::receive_buffer_size(opts_.rcvbuf), ec);
        if (ec) LOG(WARNING) << "set SO_RCVBUF failed: " << ec.message();
    }
}

bool ConnManager::isSocketAlive(asio::ip::tcp::socket& socket) {
    if (!socket.is_open()) return false;
    struct pollfd pfd;
    pfd.fd = socket.native_handle();
    pfd.events = POLLIN;
    pfd.revents = 0;
    int ret = ::poll(&pfd, 1, 0);
    if (ret > 0 && (pfd.revents & (POLLERR | POLLHUP | POLLNVAL))) {
        return false;
    }
    // If POLLIN is set, the peer might have sent data or closed.
    // A recv with MSG_PEEK can check for EOF.
    if (ret > 0 && (pfd.revents & POLLIN)) {
        char buf;
        ssize_t n = ::recv(pfd.fd, &buf, 1, MSG_PEEK | MSG_DONTWAIT);
        if (n == 0) return false;   // peer closed
        if (n < 0 && errno != EAGAIN && errno != EWOULDBLOCK) return false;
    }
    return true;
}

Status ConnManager::clientHandshake(asio::ip::tcp::socket& socket) {
    HandshakeReq req;
    req.magic = kMagic;
    req.version = kProtocolVersion;
    req.flags = 0;

    asio::error_code ec;
    asio::write(socket, asio::buffer(&req, sizeof(req)), ec);
    if (ec) {
        return Status::InternalError("TCP_HP handshake send failed: " +
                                     ec.message());
    }

    HandshakeAck ack;
    asio::read(socket, asio::buffer(&ack, sizeof(ack)), ec);
    if (ec) {
        return Status::InternalError("TCP_HP handshake recv failed: " +
                                     ec.message());
    }

    if (ack.status != kHandshakeOK) {
        return Status::InternalError(
            "TCP_HP handshake rejected, status=" + std::to_string(ack.status));
    }
    return Status::OK();
}

Status ConnManager::serverHandshake(asio::ip::tcp::socket& socket) {
    HandshakeReq req;
    asio::error_code ec;
    asio::read(socket, asio::buffer(&req, sizeof(req)), ec);
    if (ec) {
        return Status::InternalError("TCP_HP server handshake read failed: " +
                                     ec.message());
    }

    HandshakeAck ack;
    ack.flags = 0;

    if (req.magic != kMagic) {
        ack.status = kHandshakeBadMagic;
        asio::write(socket, asio::buffer(&ack, sizeof(ack)), ec);
        return Status::InternalError("TCP_HP bad magic number");
    }
    if (req.version != kProtocolVersion) {
        ack.status = kHandshakeBadVersion;
        asio::write(socket, asio::buffer(&ack, sizeof(ack)), ec);
        return Status::InternalError(
            "TCP_HP version mismatch: got " + std::to_string(req.version) +
            ", expected " + std::to_string(kProtocolVersion));
    }

    ack.status = kHandshakeOK;
    asio::write(socket, asio::buffer(&ack, sizeof(ack)), ec);
    if (ec) {
        return Status::InternalError("TCP_HP server handshake write failed: " +
                                     ec.message());
    }
    return Status::OK();
}

Status ConnManager::connectWithRetry(const std::string& host, uint16_t port,
                                     asio::ip::tcp::socket& socket) {
    auto& ctx = pool_.getNextContext();
    asio::ip::tcp::resolver resolver(ctx);
    asio::error_code ec;
    auto endpoints = resolver.resolve(host, std::to_string(port), ec);
    if (ec) {
        return Status::InternalError("TCP_HP resolve failed for " + host +
                                     ":" + std::to_string(port) + ": " +
                                     ec.message());
    }

    int backoff_ms = 100;
    for (int attempt = 0; attempt <= opts_.connect_retries; ++attempt) {
        socket = asio::ip::tcp::socket(ctx);
        asio::connect(socket, endpoints, ec);
        if (!ec) {
            return Status::OK();
        }
        LOG(WARNING) << "TCP_HP connect attempt " << (attempt + 1) << "/"
                     << (opts_.connect_retries + 1) << " to " << host << ":"
                     << port << " failed: " << ec.message();
        if (attempt < opts_.connect_retries) {
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(backoff_ms * 2, 5000);
        }
    }
    return Status::InternalError("TCP_HP connect failed after " +
                                 std::to_string(opts_.connect_retries + 1) +
                                 " attempts to " + host + ":" +
                                 std::to_string(port));
}

Status ConnManager::acquire(const std::string& host, uint16_t port,
                            asio::ip::tcp::socket& out_socket) {
    std::string key = makeKey(host, port);
    {
        std::lock_guard<std::mutex> lock(pool_mutex_);
        auto it = pool_map_.find(key);
        if (it != pool_map_.end()) {
            while (!it->second.empty()) {
                auto socket = std::move(it->second.back());
                it->second.pop_back();
                if (isSocketAlive(socket)) {
                    out_socket = std::move(socket);
                    return Status::OK();
                }
                // Stale connection, discard and try next
            }
        }
    }
    // No pooled connection — create new, configure, and handshake.
    asio::ip::tcp::socket socket(pool_.getNextContext());
    auto status = connectWithRetry(host, port, socket);
    if (!status.ok()) return status;

    configureSocket(socket);

    status = clientHandshake(socket);
    if (!status.ok()) {
        asio::error_code ec;
        socket.close(ec);
        return status;
    }

    out_socket = std::move(socket);
    return Status::OK();
}

void ConnManager::release(const std::string& host, uint16_t port,
                          asio::ip::tcp::socket socket) {
    std::string key = makeKey(host, port);
    std::lock_guard<std::mutex> lock(pool_mutex_);
    auto& vec = pool_map_[key];
    if (vec.size() < opts_.max_pool_size) {
        vec.push_back(std::move(socket));
    } else {
        asio::error_code ec;
        socket.shutdown(asio::ip::tcp::socket::shutdown_both, ec);
        socket.close(ec);
    }
}

// Helper that holds state for an async server handshake.
struct AsyncHandshakeState
    : public std::enable_shared_from_this<AsyncHandshakeState> {
    asio::ip::tcp::socket socket;
    ConnManager::HandshakeHandler handler;
    HandshakeReq req{};
    HandshakeAck ack{};

    AsyncHandshakeState(asio::ip::tcp::socket s,
                        ConnManager::HandshakeHandler h)
        : socket(std::move(s)), handler(std::move(h)) {}

    void start() {
        auto self = shared_from_this();
        asio::async_read(
            socket, asio::buffer(&req, sizeof(req)),
            [this, self](const asio::error_code& ec, std::size_t) {
                if (ec) {
                    handler(Status::InternalError(
                                "TCP_HP async handshake read failed: " +
                                ec.message()),
                            std::move(socket));
                    return;
                }
                ack.flags = 0;
                if (req.magic != kMagic) {
                    ack.status = kHandshakeBadMagic;
                    sendAckAndFail("TCP_HP bad magic number");
                    return;
                }
                if (req.version != kProtocolVersion) {
                    ack.status = kHandshakeBadVersion;
                    sendAckAndFail("TCP_HP version mismatch: got " +
                                  std::to_string(req.version) + ", expected " +
                                  std::to_string(kProtocolVersion));
                    return;
                }
                ack.status = kHandshakeOK;
                sendAckAndSucceed();
            });
    }

   private:
    void sendAckAndFail(const std::string& reason) {
        auto self = shared_from_this();
        asio::async_write(
            socket, asio::buffer(&ack, sizeof(ack)),
            [this, self, reason](const asio::error_code&, std::size_t) {
                handler(Status::InternalError(reason), std::move(socket));
            });
    }

    void sendAckAndSucceed() {
        auto self = shared_from_this();
        asio::async_write(
            socket, asio::buffer(&ack, sizeof(ack)),
            [this, self](const asio::error_code& ec, std::size_t) {
                if (ec) {
                    handler(Status::InternalError(
                                "TCP_HP async handshake write failed: " +
                                ec.message()),
                            std::move(socket));
                    return;
                }
                handler(Status::OK(), std::move(socket));
            });
    }
};

void ConnManager::asyncServerHandshake(asio::ip::tcp::socket socket,
                                       HandshakeHandler handler) {
    auto state = std::make_shared<AsyncHandshakeState>(std::move(socket),
                                                       std::move(handler));
    state->start();
}

void ConnManager::shutdown() {
    std::lock_guard<std::mutex> lock(pool_mutex_);
    pool_map_.clear();
}

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake
