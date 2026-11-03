#pragma once

#include <chrono>
#include <optional>

namespace mooncake {

struct RpcTimeoutConfig {
    // An absent override leaves the caller's connection policy unchanged.
    std::optional<std::chrono::milliseconds> request_timeout;
    std::optional<std::chrono::milliseconds> connect_timeout;

    static RpcTimeoutConfig FromEnvironment();
};

}  // namespace mooncake
