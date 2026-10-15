#pragma once

#include <cstdint>
#include <optional>

#include "config/rpc_connection_bootstrap_config.h"

namespace mooncake {

class DefaultConfig;

struct RpcConnectionCommandLineOverrides {
    std::optional<int32_t> timeout_seconds;
    std::optional<bool> tcp_no_delay;
};

RpcConnectionBootstrapConfig ResolveRpcConnectionBootstrapConfig(
    const DefaultConfig* file_config,
    const RpcConnectionCommandLineOverrides& command_line);

}  // namespace mooncake
