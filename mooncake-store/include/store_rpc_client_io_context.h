#pragma once

#include "rpc_client_io_context.h"

namespace mooncake {

inline uint32_t GetStoreRpcClientIoThreads() {
    return RpcClientIoThreadsConfig::Process().store;
}

namespace detail {
struct StoreRpcClientIoContextPoolTag {};
}  // namespace detail

inline coro_io::io_context_pool& GetStoreRpcClientIoContextPool() {
    static auto& io_pool =
        GetRpcClientIoContextPool<detail::StoreRpcClientIoContextPoolTag>(
            GetStoreRpcClientIoThreads());
    return io_pool;
}

}  // namespace mooncake
