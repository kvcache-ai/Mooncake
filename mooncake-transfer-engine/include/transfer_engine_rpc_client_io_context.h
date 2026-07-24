#pragma once

#include "environ.h"
#include "rpc_client_io_context.h"

namespace mooncake {

inline uint32_t GetTransferEngineRpcClientIoThreads() {
    return Environ::Get().GetTransferEngineRpcClientIoThreads();
}

namespace detail {
struct TransferEngineRpcClientIoContextPoolTag {};
}  // namespace detail

inline coro_io::io_context_pool& GetTransferEngineRpcClientIoContextPool() {
    static auto& io_pool = GetRpcClientIoContextPool<
        detail::TransferEngineRpcClientIoContextPoolTag>(
        GetTransferEngineRpcClientIoThreads());
    return io_pool;
}

}  // namespace mooncake
