#include "rpc_client_io_context.h"

#include <algorithm>
#include <thread>

#include "environ.h"
#include "environment_variables.h"

namespace mooncake {

namespace {

constexpr uint32_t kDefaultRpcClientIoThreads = 16;

uint32_t ResolveRpcClientIoThreads(const Environ& env,
                                   const EnvironmentVariable<int>& variable,
                                   uint32_t fallback) {
    const int configured = env.GetTypedOr(variable, static_cast<int>(fallback));
    return configured > 0 ? static_cast<uint32_t>(configured) : fallback;
}

}  // namespace

RpcClientIoThreadsConfig RpcClientIoThreadsConfig::FromEnvironment(
    const Environ& env) {
    using Variables = RpcClientIoEnvironmentVariables;
    const uint32_t hardware_threads =
        static_cast<uint32_t>(std::thread::hardware_concurrency());
    const uint32_t default_threads = std::min(
        kDefaultRpcClientIoThreads, std::max(uint32_t{1}, hardware_threads));

    RpcClientIoThreadsConfig config;
    config.common = ResolveRpcClientIoThreads(
        env, Variables::MC_RPC_CLIENT_IO_THREADS, default_threads);
    config.store = ResolveRpcClientIoThreads(
        env, Variables::MC_STORE_RPC_CLIENT_IO_THREADS, config.common);
    config.transfer_engine = ResolveRpcClientIoThreads(
        env, Variables::MC_TE_RPC_CLIENT_IO_THREADS, config.common);
    return config;
}

const RpcClientIoThreadsConfig& RpcClientIoThreadsConfig::Process() {
    static const RpcClientIoThreadsConfig config =
        FromEnvironment(Environ::Process());
    return config;
}

std::shared_ptr<coro_io::io_context_pool> CreateRpcClientIoContextPool(
    uint32_t thread_count) {
    return coro_io::create_io_context_pool(thread_count);
}

}  // namespace mooncake
