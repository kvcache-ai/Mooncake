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

#ifndef MOONCAKE_CORO_HANDSHAKE_CLIENT_POOL_H
#define MOONCAKE_CORO_HANDSHAKE_CLIENT_POOL_H

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include <ylt/coro_io/io_context_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "rpc_config.h"

namespace mooncake {

// Per-peer coro_rpc clients. One in-flight RPC holds one connection.
// Lookup, creation, and the active-count increment happen under one lock.
class HandshakeClientPool {
    struct ClientPool;

   public:
    struct Lease {
        std::unique_ptr<coro_rpc::coro_rpc_client> client;
        std::shared_ptr<ClientPool> pool;
        uint64_t generation = 0;
        bool from_pool = false;
        bool broken = false;
        int max_idle = 0;

        Lease() = default;
        Lease(const Lease &) = delete;
        Lease &operator=(const Lease &) = delete;
        Lease(Lease &&other) noexcept
            : client(std::move(other.client)),
              pool(std::move(other.pool)),
              generation(other.generation),
              from_pool(other.from_pool),
              broken(other.broken),
              max_idle(other.max_idle) {}
        Lease &operator=(Lease &&other) noexcept {
            if (this == &other) return *this;
            client = std::move(other.client);
            pool = std::move(other.pool);
            generation = other.generation;
            from_pool = other.from_pool;
            broken = other.broken;
            max_idle = other.max_idle;
            return *this;
        }
        ~Lease() {
            if (!pool) return;
            std::lock_guard<std::mutex> lock(pool->mu);
            if (pool->active > 0) --pool->active;
            if (broken || pool->closing || !client) return;
            if (static_cast<int>(pool->idle.size()) >= max_idle) return;
            pool->idle.push_back(IdleClient{std::move(client),
                                            std::chrono::steady_clock::now()});
            pool->last_used = std::chrono::steady_clock::now();
        }
    };

    explicit HandshakeClientPool(const HandshakeLimits &limits);
    ~HandshakeClientPool();

    HandshakeClientPool(const HandshakeClientPool &) = delete;
    HandshakeClientPool &operator=(const HandshakeClientPool &) = delete;

    void UpdateLimits(const HandshakeLimits &limits);
    Lease Acquire(const std::string &host, uint16_t port);
    void ClearGeneration(const Lease &lease);
    coro_rpc::coro_rpc_client::config ClientConfig();
    void StopSweeper();
    void Clear();

   private:
    struct IdleClient {
        std::unique_ptr<coro_rpc::coro_rpc_client> client;
        std::chrono::steady_clock::time_point idle_since;
    };
    struct ClientPool {
        std::mutex mu;
        std::string key;
        uint64_t generation = 0;
        int active = 0;
        bool closing = false;
        std::chrono::steady_clock::time_point last_used =
            std::chrono::steady_clock::now();
        std::vector<IdleClient> idle;
    };

    static std::string MakePeerKey(const std::string &host, uint16_t port);
    void ReapExpiredIdle(std::chrono::steady_clock::time_point now);
    void ExpireIdle(ClientPool &pool,
                    std::chrono::steady_clock::time_point now) const;
    void SweepLoop();

    int max_pools_ = 1024;
    int max_idle_per_peer_ = 128;
    int max_active_per_peer_ = 128;
    int timeout_seconds_ = 60;
    int connect_timeout_seconds_ = 5;
    std::atomic<int> idle_ttl_seconds_{60};
    std::thread sweeper_;
    std::mutex sweep_mu_;
    std::condition_variable sweep_cv_;
    bool sweeper_stop_ = false;
    std::mutex pools_mu_;
    std::unordered_map<std::string, std::shared_ptr<ClientPool>> pools_;
};

coro_io::ExecutorWrapper<> *HandshakeClientExecutor();

}  // namespace mooncake

#endif  // MOONCAKE_CORO_HANDSHAKE_CLIENT_POOL_H
