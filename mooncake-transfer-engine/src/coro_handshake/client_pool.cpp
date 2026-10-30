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

#include "client_pool.h"

namespace mooncake {
namespace {
constexpr int kIdleSweepSeconds = 1;
constexpr int kClientIoThreads = 2;
}  // namespace

coro_io::ExecutorWrapper<> *HandshakeClientExecutor() {
    static auto pool = coro_io::create_io_context_pool(kClientIoThreads);
    return pool->get_executor();
}

HandshakeClientPool::HandshakeClientPool(const HandshakeLimits &limits) {
    UpdateLimits(limits);
    sweeper_ = std::thread([this] { SweepLoop(); });
}

HandshakeClientPool::~HandshakeClientPool() {
    StopSweeper();
    Clear();
}

void HandshakeClientPool::UpdateLimits(const HandshakeLimits &limits) {
    std::lock_guard<std::mutex> lock(pools_mu_);
    max_pools_ = limits.max_pools;
    max_idle_per_peer_ = limits.max_idle_per_peer;
    max_active_per_peer_ = limits.max_active_per_peer;
    timeout_seconds_ = limits.timeout_seconds;
    connect_timeout_seconds_ = limits.connect_timeout_seconds;
    idle_ttl_seconds_.store(limits.idle_ttl_seconds);
}

std::string HandshakeClientPool::MakePeerKey(const std::string &host,
                                             uint16_t port) {
    const std::string suffix = ":" + std::to_string(port);
    if (!host.empty() && host.front() != '[' &&
        host.find(':') != std::string::npos) {
        return "[" + host + "]" + suffix;
    }
    return host + suffix;
}

HandshakeClientPool::Lease HandshakeClientPool::Acquire(const std::string &host,
                                                        uint16_t port) {
    const std::string key = MakePeerKey(host, port);
    std::lock_guard<std::mutex> map_lock(pools_mu_);
    const auto now = std::chrono::steady_clock::now();
    ReapExpiredIdle(now);
    std::shared_ptr<ClientPool> pool;
    auto found = pools_.find(key);
    if (found != pools_.end()) {
        pool = found->second;
    } else {
        while (static_cast<int>(pools_.size()) >= max_pools_) {
            std::shared_ptr<ClientPool> victim;
            auto oldest = std::chrono::steady_clock::time_point::max();
            for (auto &entry : pools_) {
                std::lock_guard<std::mutex> pool_lock(entry.second->mu);
                if (entry.second->active != 0 || entry.second->closing)
                    continue;
                if (entry.second->last_used < oldest) {
                    oldest = entry.second->last_used;
                    victim = entry.second;
                }
            }
            if (!victim) return {};
            {
                std::lock_guard<std::mutex> pool_lock(victim->mu);
                victim->closing = true;
                victim->idle.clear();
            }
            pools_.erase(victim->key);
        }
        pool = std::make_shared<ClientPool>();
        pool->key = key;
        pool->last_used = now;
        pools_.emplace(key, pool);
    }
    std::lock_guard<std::mutex> pool_lock(pool->mu);
    if (pool->closing || pool->active >= max_active_per_peer_) return {};
    Lease lease;
    lease.pool = pool;
    lease.max_idle = max_idle_per_peer_;
    ++pool->active;
    pool->last_used = now;
    ExpireIdle(*pool, now);
    lease.generation = pool->generation;
    if (!pool->idle.empty()) {
        lease.client = std::move(pool->idle.back().client);
        pool->idle.pop_back();
        lease.from_pool = true;
    }
    return lease;
}

void HandshakeClientPool::ClearGeneration(const Lease &lease) {
    if (!lease.pool) return;
    std::lock_guard<std::mutex> lock(lease.pool->mu);
    if (lease.pool->generation != lease.generation) return;
    ++lease.pool->generation;
    lease.pool->idle.clear();
}

coro_rpc::coro_rpc_client::config HandshakeClientPool::ClientConfig() {
    std::lock_guard<std::mutex> lock(pools_mu_);
    coro_rpc::coro_rpc_client::config config;
    config.connect_timeout_duration =
        std::chrono::seconds(connect_timeout_seconds_);
    config.request_timeout_duration = std::chrono::seconds(timeout_seconds_);
    return config;
}

void HandshakeClientPool::StopSweeper() {
    {
        std::lock_guard<std::mutex> lock(sweep_mu_);
        sweeper_stop_ = true;
    }
    sweep_cv_.notify_all();
    if (sweeper_.joinable() &&
        sweeper_.get_id() != std::this_thread::get_id()) {
        sweeper_.join();
    }
}

void HandshakeClientPool::Clear() {
    std::lock_guard<std::mutex> lock(pools_mu_);
    for (auto &entry : pools_) {
        std::lock_guard<std::mutex> pool_lock(entry.second->mu);
        entry.second->closing = true;
        entry.second->idle.clear();
    }
    pools_.clear();
}

void HandshakeClientPool::ReapExpiredIdle(
    std::chrono::steady_clock::time_point now) {
    for (auto it = pools_.begin(); it != pools_.end();) {
        bool drop = false;
        {
            std::lock_guard<std::mutex> pool_lock(it->second->mu);
            ExpireIdle(*it->second, now);
            if (it->second->active == 0 && it->second->idle.empty()) {
                it->second->closing = true;
                drop = true;
            }
        }
        if (drop)
            it = pools_.erase(it);
        else
            ++it;
    }
}

void HandshakeClientPool::ExpireIdle(
    ClientPool &pool, std::chrono::steady_clock::time_point now) const {
    const auto ttl = std::chrono::seconds(idle_ttl_seconds_.load());
    std::vector<IdleClient> kept;
    kept.reserve(pool.idle.size());
    for (auto &entry : pool.idle) {
        if (now - entry.idle_since < ttl) kept.push_back(std::move(entry));
    }
    pool.idle.swap(kept);
}

void HandshakeClientPool::SweepLoop() {
    std::unique_lock<std::mutex> lock(sweep_mu_);
    while (!sweeper_stop_) {
        sweep_cv_.wait_for(lock, std::chrono::seconds(kIdleSweepSeconds));
        if (sweeper_stop_) break;
        lock.unlock();
        {
            std::lock_guard<std::mutex> pools_lock(pools_mu_);
            ReapExpiredIdle(std::chrono::steady_clock::now());
        }
        lock.lock();
    }
}

}  // namespace mooncake
