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

#ifndef TCP_HP_CONN_MANAGER_H_
#define TCP_HP_CONN_MANAGER_H_

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <asio/ip/tcp.hpp>

#include "tent/common/status.h"
#include "tent/transport/tcp_hp/io_context_pool.h"

namespace mooncake {
namespace tent {
namespace tcp_hp {

struct ConnOptions {
    size_t max_pool_size = 64;
    int connect_timeout_ms = 3000;
    int connect_retries = 5;
    int keepalive_idle_s = 10;
    int keepalive_interval_s = 2;
    int keepalive_count = 5;
    int sndbuf = 0;   // 0 = OS default
    int rcvbuf = 0;   // 0 = OS default
};

class ConnManager {
   public:
    ConnManager(IoContextPool& pool, ConnOptions opts);
    ~ConnManager();

    // Acquire a handshaked connection. Creates a new one if pool is empty.
    Status acquire(const std::string& host, uint16_t port,
                   asio::ip::tcp::socket& out_socket);

    // Return a connection to the pool. Closes if pool is full.
    void release(const std::string& host, uint16_t port,
                 asio::ip::tcp::socket socket);

    // Server-side: validate incoming handshake (synchronous). Returns OK if valid.
    Status serverHandshake(asio::ip::tcp::socket& socket);

    // Server-side: validate incoming handshake (async, non-blocking).
    // Calls handler(Status, socket) when done; socket is moved on success.
    using HandshakeHandler =
        std::function<void(Status, asio::ip::tcp::socket)>;
    void asyncServerHandshake(asio::ip::tcp::socket socket,
                              HandshakeHandler handler);

    // Configure socket options (TCP_NODELAY, keepalive, buffer sizes).
    void configureSocket(asio::ip::tcp::socket& socket);

    // Close all pooled connections.
    void shutdown();

   private:
    static std::string makeKey(const std::string& host, uint16_t port);
    Status connectWithRetry(const std::string& host, uint16_t port,
                            asio::ip::tcp::socket& socket);
    Status clientHandshake(asio::ip::tcp::socket& socket);
    bool isSocketAlive(asio::ip::tcp::socket& socket);

    // Per-endpoint connection pool to avoid global lock contention.
    struct EndpointPool {
        std::mutex mutex;
        std::vector<asio::ip::tcp::socket> sockets;
    };

    // Get or create per-endpoint pool (thread-safe).
    std::shared_ptr<EndpointPool> getEndpointPool(const std::string& key);

    IoContextPool& pool_;
    ConnOptions opts_;
    std::mutex map_mutex_;  // protects pool_map_ structural changes only
    std::unordered_map<std::string, std::shared_ptr<EndpointPool>> pool_map_;
};

}  // namespace tcp_hp
}  // namespace tent
}  // namespace mooncake

#endif  // TCP_HP_CONN_MANAGER_H_
