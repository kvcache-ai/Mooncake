#pragma once

#include <cstdint>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <csignal>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>

#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_io/io_context_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

namespace mooncake {

std::shared_ptr<coro_io::io_context_pool> CreateRpcClientIoContextPool(
    uint32_t thread_count);

/**
 * Teardown guard for RPC entry points that serve out of a shared pool.
 *
 * The ylt client pool documents only send_request as thread-safe; closing or
 * destroying the pool while a request coroutine is suspended leaves the
 * resumed coroutine touching freed connection state (the teardown segfault in
 * #3909). Entry points take a ScopedCall; a destructor calls drain_for() so
 * the pool is only released once nothing is in flight.
 */
class RpcDrainGuard {
   public:
    class ScopedCall {
       public:
        explicit ScopedCall(RpcDrainGuard& guard)
            : guard_(guard), ok_(guard.try_enter()) {}
        ~ScopedCall() {
            if (ok_) guard_.leave();
        }
        ScopedCall(const ScopedCall&) = delete;
        ScopedCall& operator=(const ScopedCall&) = delete;
        bool ok() const { return ok_; }

       private:
        RpcDrainGuard& guard_;
        bool ok_;
    };

    // Bounded wait for in-flight calls; stops admitting new ones first.
    // Returns false on timeout, in which case the caller must not free shared
    // pool state without accepting the pre-existing UAF risk.
    bool drain_for(std::chrono::milliseconds timeout) {
        stopping_.store(true, std::memory_order_release);
        std::unique_lock lk(mutex_);
        return cv_.wait_for(lk, timeout, [&] {
            return inflight_.load(std::memory_order_acquire) == 0;
        });
    }

   private:
    bool try_enter() {
        if (stopping_.load(std::memory_order_acquire)) return false;
        inflight_.fetch_add(1, std::memory_order_acq_rel);
        // a drain that started between the two reads still sees this call
        if (stopping_.load(std::memory_order_acquire)) {
            leave();
            return false;
        }
        return true;
    }

    void leave() {
        if (inflight_.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            std::lock_guard lk(mutex_);
            cv_.notify_all();
        }
    }

    std::atomic<bool> stopping_{false};
    std::atomic<int> inflight_{0};
    std::mutex mutex_;
    std::condition_variable cv_;
};

template <typename PoolTag>
coro_io::io_context_pool& GetRpcClientIoContextPool(uint32_t thread_count) {
    static const auto io_pool = CreateRpcClientIoContextPool(thread_count);
    return *io_pool;
}

namespace detail {

// Process-wide client-pool registry, keyed by address. A ylt client pool owns
// background reconnect coroutines that hold references into pool storage
// (client_pool.hpp's reconnect loop), so freeing a pool while they are
// suspended is a use-after-free regardless of whether any user request is in
// flight (#3909). Pools are few in practice (one per distinct master address
// per process), so they are deliberately kept alive for the process lifetime.
inline std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
SharedPoolRegistryImpl(
    std::string_view address,
    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config config,
    coro_io::io_context_pool& io_context_pool) {
    using Pool = coro_io::client_pool<coro_rpc::coro_rpc_client>;
    static std::mutex registry_mutex;
    static std::unordered_map<std::string, std::shared_ptr<Pool>> registry;
    std::lock_guard<std::mutex> lock(registry_mutex);
    auto& slot = registry[std::string(address)];
    if (!slot) {
        slot = Pool::create(std::string(address), std::move(config),
                            io_context_pool);
    }
    return slot;
}

}  // namespace detail

/**
 * A replaceable client pool for callers that communicate with one target at a
 * time. Pools live in the process-wide registry above; an address switch only
 * re-points this holder, it never frees a pool.
 */
class RpcClientPool {
   public:
    using ClientPool = coro_io::client_pool<coro_rpc::coro_rpc_client>;
    using PoolConfig = ClientPool::pool_config;

    explicit RpcClientPool(coro_io::io_context_pool& io_context_pool,
                           PoolConfig config = {})
        : io_context_pool_(io_context_pool), config_(std::move(config)) {
        // Address replacement supersedes background recovery of the old host.
        config_.host_alive_detect_duration = std::chrono::seconds(0);
    }

    std::shared_ptr<ClientPool> GetOrCreateClientPool(
        std::string_view address) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        if (!client_pool_ || address_ != address) {
            client_pool_ = SharedPoolRegistry(address);
            address_ = address;
        }
        return client_pool_;
    }

    std::shared_ptr<ClientPool> GetClientPool() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return client_pool_;
    }

   private:
    std::shared_ptr<ClientPool> SharedPoolRegistry(std::string_view address) {
        return detail::SharedPoolRegistryImpl(address, config_,
                                              io_context_pool_);
    }

    mutable std::shared_mutex mutex_;
    coro_io::io_context_pool& io_context_pool_;
    PoolConfig config_;
    std::string address_;
    std::shared_ptr<ClientPool> client_pool_;
};

}  // namespace mooncake
