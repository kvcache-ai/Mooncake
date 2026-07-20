#pragma once

#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>

#include <ylt/coro_io/client_pool.hpp>
#include <ylt/coro_io/io_context_pool.hpp>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

namespace mooncake {

/**
 * Return Mooncake's process-wide YLT RPC client I/O context pool.
 *
 * MC_RPC_CLIENT_IO_THREADS is read when this function is first called.
 * Subsequent environment changes have no effect.
 */
coro_io::io_context_pool& GetRpcClientIoContextPool();

/**
 * A replaceable client pool for callers that communicate with one target at a
 * time. Requests retain a shared_ptr to the old pool while they are in flight;
 * after an address switch the old pool is destroyed when those requests end.
 */
class RpcClientPool {
   public:
    using ClientPool = coro_io::client_pool<coro_rpc::coro_rpc_client>;
    using PoolConfig = ClientPool::pool_config;

    RpcClientPool() : RpcClientPool(PoolConfig{}) {}

    explicit RpcClientPool(PoolConfig config) : config_(std::move(config)) {
        // Address replacement supersedes background recovery of the old host.
        config_.host_alive_detect_duration = std::chrono::seconds(0);
    }

    std::shared_ptr<ClientPool> GetOrCreateClientPool(
        std::string_view address) {
        std::lock_guard<std::shared_mutex> lock(mutex_);
        if (!client_pool_ || address_ != address) {
            client_pool_ = ClientPool::create(address, config_,
                                              GetRpcClientIoContextPool());
            address_ = address;
        }
        return client_pool_;
    }

    std::shared_ptr<ClientPool> GetClientPool() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return client_pool_;
    }

   private:
    mutable std::shared_mutex mutex_;
    PoolConfig config_;
    std::string address_;
    std::shared_ptr<ClientPool> client_pool_;
};

}  // namespace mooncake
