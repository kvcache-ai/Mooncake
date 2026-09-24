#pragma once

#include "environ.h"
#include "rpc_client_io_context.h"

namespace mooncake {

inline uint32_t GetStoreRpcClientIoThreads() {
    return Environ::Get().GetStoreRpcClientIoThreads();
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
