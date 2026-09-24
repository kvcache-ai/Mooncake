#pragma once

#include <cstdint>
#include <cstdio>
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
 *
 * The counters live in a shared State, not in the guard object itself. When a
 * drain times out the owner tears down anyway, and a ScopedCall still in
 * flight then outlives the guard; holding the state by shared_ptr keeps its
 * leave() from touching freed memory (#3943 review).
 */
class RpcDrainGuard {
   private:
    struct State {
        std::atomic<bool> stopping{false};
        std::atomic<int> inflight{0};
        std::mutex mutex;
        std::condition_variable cv;
    };

   public:
    class ScopedCall {
       public:
        explicit ScopedCall(const RpcDrainGuard& guard)
            : state_(guard.state_), ok_(try_enter(*state_)) {}
        ~ScopedCall() {
            if (ok_) leave(*state_);
        }
        ScopedCall(const ScopedCall&) = delete;
        ScopedCall& operator=(const ScopedCall&) = delete;
        bool ok() const { return ok_; }

       private:
        static bool try_enter(State& state) {
            if (state.stopping.load(std::memory_order_acquire)) return false;
            state.inflight.fetch_add(1, std::memory_order_acq_rel);
            // a drain that started between the two reads still sees this call
            if (state.stopping.load(std::memory_order_acquire)) {
                leave(state);
                return false;
            }
            return true;
        }

        static void leave(State& state) {
            if (state.inflight.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                std::lock_guard lk(state.mutex);
                state.cv.notify_all();
            }
        }

        std::shared_ptr<State> state_;
        bool ok_;
    };

    // Bounded wait for in-flight calls; stops admitting new ones first.
    // Returns false on timeout: the caller must keep the shared pool alive
    // regardless (the registry already does), and the state itself stays
    // valid until the last ScopedCall lets go of it.
    bool drain_for(std::chrono::milliseconds timeout) {
        state_->stopping.store(true, std::memory_order_release);
        std::unique_lock lk(state_->mutex);
        return state_->cv.wait_for(lk, timeout, [&] {
            return state_->inflight.load(std::memory_order_acquire) == 0;
        });
    }

   private:
    std::shared_ptr<State> state_ = std::make_shared<State>();
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
//
// The registry key is the address plus the pool's behavioral knobs. Two
// holders for one address with identical configuration share a pool, which is
// the case the keep-alive exists for. The same address can also legitimately
// host different policies at once: the foreground master pool is resilient
// while an HA probe on that address must fast-fail, and keying by address
// alone would hand the probe the foreground retry budget.
inline std::shared_ptr<coro_io::client_pool<coro_rpc::coro_rpc_client>>
SharedPoolRegistryImpl(
    std::string_view address,
    coro_io::client_pool<coro_rpc::coro_rpc_client>::pool_config config,
    coro_io::io_context_pool& io_context_pool) {
    using Pool = coro_io::client_pool<coro_rpc::coro_rpc_client>;
    static std::mutex registry_mutex;
    static std::unordered_map<std::string, std::shared_ptr<Pool>> registry;
    std::lock_guard<std::mutex> lock(registry_mutex);
    std::string key;
    key.reserve(address.size() + 64);
    key.append(address);
    auto append_knob = [&key](auto value) {
        key.push_back('|');
        key.append(std::to_string(value));
    };
    append_knob(config.max_connection);
    append_knob(config.connect_retry_count);
    append_knob(config.reconnect_wait_time.count());
    append_knob(config.client_config.connect_timeout_duration.count());
    append_knob(config.client_config.request_timeout_duration.count());
    append_knob(
        static_cast<size_t>(config.client_config.socket_config.index()));
    auto& pool = registry[key];
    if (!pool) {
        pool = Pool::create(std::string(address), std::move(config),
                            io_context_pool);
    }
    return pool;
}

// ClientRequester's offload pool collection has the same lifetime hazard: its
// pools host background reconnect coroutines, so they must outlive the owning
// RealClient. Collections are few in practice (one per RealClient), so they
// are parked here for the process lifetime instead of being freed at teardown.
inline void KeepClientPoolsAlive(
    std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>> pools) {
    static std::mutex keep_mutex;
    static std::vector<
        std::shared_ptr<coro_io::client_pools<coro_rpc::coro_rpc_client>>>
        keep;
    std::lock_guard<std::mutex> lock(keep_mutex);
    keep.push_back(std::move(pools));
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
