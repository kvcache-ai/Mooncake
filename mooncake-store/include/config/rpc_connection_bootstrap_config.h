#pragma once

#include <chrono>

namespace mooncake {

struct RpcConnectionBootstrapConfig {
    std::chrono::seconds timeout{0};
    bool tcp_no_delay = true;
};

}  // namespace mooncake
