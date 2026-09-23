#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace mooncake {

struct AdminHttpBootstrapConfig {
    static constexpr bool kDefaultEnabled = false;
    static constexpr uint16_t kDefaultPort = 8080;
    static constexpr std::string_view kDefaultHost = "0.0.0.0";

    bool enabled = kDefaultEnabled;
    uint16_t port = kDefaultPort;
    std::string host{kDefaultHost};
};

}  // namespace mooncake
