#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace mooncake {

struct MetricsBootstrapConfig {
    static constexpr bool kDefaultEnabled = true;
    static constexpr uint32_t kDefaultPort = 9003;
    static constexpr std::string_view kDefaultHost = "0.0.0.0";

    bool enabled = kDefaultEnabled;
    uint32_t port = kDefaultPort;
    std::string host{kDefaultHost};
};

}  // namespace mooncake
