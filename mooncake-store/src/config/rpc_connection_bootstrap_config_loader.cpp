#include "rpc_connection_bootstrap_config_loader.h"

#include "default_config.h"

namespace mooncake {

RpcConnectionBootstrapConfig ResolveRpcConnectionBootstrapConfig(
    const DefaultConfig* file_config,
    const RpcConnectionCommandLineOverrides& command_line) {
    RpcConnectionBootstrapConfig result;
    int32_t timeout_seconds = 0;

    if (file_config != nullptr) {
        if (file_config->Contains("rpc_conn_timeout_seconds")) {
            file_config->GetInt32("rpc_conn_timeout_seconds", &timeout_seconds);
        }
        if (file_config->Contains("rpc_enable_tcp_no_delay")) {
            file_config->GetBool("rpc_enable_tcp_no_delay",
                                 &result.tcp_no_delay);
        }
    }

    if (command_line.timeout_seconds.has_value()) {
        timeout_seconds = *command_line.timeout_seconds;
    }
    if (command_line.tcp_no_delay.has_value()) {
        result.tcp_no_delay = *command_line.tcp_no_delay;
    }
    result.timeout = std::chrono::seconds(timeout_seconds);
    return result;
}

}  // namespace mooncake
