#include "rpc_client_io_context.h"

#include <iostream>

#include "environ.h"

namespace mooncake {
coro_io::io_context_pool& GetRpcClientIoContextPool() {
    static const auto io_pool = [] {
        const auto value = Environ::GetRpcClientIoThreads();
        std::clog << "Mooncake RPC client I/O pool initialized with " << value
                  << " thread(s)" << std::endl;
        return coro_io::create_io_context_pool(value);
    }();
    return *io_pool;
}

}  // namespace mooncake
