#include "rpc_client_io_context.h"

namespace mooncake {

std::shared_ptr<coro_io::io_context_pool> CreateRpcClientIoContextPool(
    uint32_t thread_count) {
    return coro_io::create_io_context_pool(thread_count);
}

}  // namespace mooncake
