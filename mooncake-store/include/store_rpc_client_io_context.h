#pragma once

#include <thread>

#include "environ.h"
#include "rpc_client_io_context.h"

namespace mooncake {

inline constexpr char kStoreRpcClientIoThreadsEnv[] =
    "MC_STORE_RPC_CLIENT_IO_THREADS";

inline unsigned GetStoreRpcClientIoThreads(
    unsigned hardware_threads = std::thread::hardware_concurrency()) {
    return Environ::GetComponentRpcClientIoThreads(kStoreRpcClientIoThreadsEnv,
                                                   hardware_threads);
}

namespace detail {
struct StoreRpcClientIoContextPoolTag {};
}  // namespace detail

inline coro_io::io_context_pool& GetStoreRpcClientIoContextPool() {
    static auto& io_pool =
        GetRpcClientIoContextPool<detail::StoreRpcClientIoContextPoolTag>(
            "Store", GetStoreRpcClientIoThreads());
    return io_pool;
}

}  // namespace mooncake
