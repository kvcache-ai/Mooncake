#include "rpc_client_io_context.h"

#include <iostream>

#include "environ.h"

namespace mooncake {

namespace {

std::shared_ptr<coro_io::io_context_pool> CreateRpcClientIoContextPool(
    const char* component, unsigned thread_count) {
    std::clog << "Mooncake " << component
              << " RPC client I/O pool initialized with " << thread_count
              << " thread(s)" << std::endl;
    return coro_io::create_io_context_pool(thread_count);
}

}  // namespace

coro_io::io_context_pool& GetStoreRpcClientIoContextPool() {
    static const auto io_pool = [] {
        const auto value = Environ::GetStoreRpcClientIoThreads();
        return CreateRpcClientIoContextPool("Store", value);
    }();
    return *io_pool;
}

coro_io::io_context_pool& GetTransferEngineRpcClientIoContextPool() {
    static const auto io_pool = [] {
        const auto value = Environ::GetTransferEngineRpcClientIoThreads();
        return CreateRpcClientIoContextPool("Transfer Engine", value);
    }();
    return *io_pool;
}

}  // namespace mooncake
