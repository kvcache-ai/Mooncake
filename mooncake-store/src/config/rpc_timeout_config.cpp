#include "config/rpc_timeout_config.h"

#include <cstdlib>

#include "environ.h"
#include "client_environment_variables.h"

namespace mooncake {

RpcTimeoutConfig RpcTimeoutConfig::FromEnvironment(const Environ& env) {
    RpcTimeoutConfig config;
    using Variables = ClientEnvironmentVariables::RpcTimeout;
    if (const auto value = env.GetTyped(Variables::MC_RPC_TIMEOUT_MS)) {
        config.request_timeout =
            std::chrono::milliseconds{std::atoll(value->c_str())};
    }
    if (const auto value = env.GetTyped(Variables::MC_RPC_CONNECT_TIMEOUT_MS)) {
        config.connect_timeout =
            std::chrono::milliseconds{std::atoll(value->c_str())};
    }
    return config;
}

}  // namespace mooncake
