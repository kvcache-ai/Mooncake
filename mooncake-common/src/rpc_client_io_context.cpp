#include "rpc_client_io_context.h"

#include <iostream>

namespace mooncake {

std::shared_ptr<coro_io::io_context_pool> CreateRpcClientIoContextPool(
    const char* component, unsigned thread_count) {
    std::clog << "Mooncake " << component
              << " RPC client I/O pool initialized with " << thread_count
              << " thread(s)" << std::endl;
    return coro_io::create_io_context_pool(thread_count);
}

}  // namespace mooncake
