#pragma once

#include <chrono>
#include <cstdint>

namespace mooncake {

struct RpcConnectionBootstrapConfig {
    // The flag definitions in master.cpp reuse these defaults, so the file
    // default, the config default, and the flag default cannot drift apart.
    static constexpr int32_t kDefaultTimeoutSeconds = 0;
    static constexpr bool kDefaultTcpNoDelay = true;

    std::chrono::seconds timeout{kDefaultTimeoutSeconds};
    bool tcp_no_delay{kDefaultTcpNoDelay};
};

}  // namespace mooncake
