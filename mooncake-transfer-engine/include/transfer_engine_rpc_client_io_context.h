#pragma once

#include <thread>

#include "environ.h"
#include "rpc_client_io_context.h"

namespace mooncake {

inline constexpr char kTransferEngineRpcClientIoThreadsEnv[] =
    "MC_TE_RPC_CLIENT_IO_THREADS";

inline unsigned GetTransferEngineRpcClientIoThreads(
    unsigned hardware_threads = std::thread::hardware_concurrency()) {
    return Environ::GetComponentRpcClientIoThreads(
        kTransferEngineRpcClientIoThreadsEnv, hardware_threads);
}

namespace detail {
struct TransferEngineRpcClientIoContextPoolTag {};
}  // namespace detail

inline coro_io::io_context_pool& GetTransferEngineRpcClientIoContextPool() {
    static auto& io_pool = GetRpcClientIoContextPool<
        detail::TransferEngineRpcClientIoContextPoolTag>(
        "Transfer Engine", GetTransferEngineRpcClientIoThreads());
    return io_pool;
}

}  // namespace mooncake
