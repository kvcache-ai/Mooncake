#pragma once

#include <optional>

namespace mooncake {

struct ClientNumaConfig {
    std::optional<int> socket_id;

    static ClientNumaConfig FromEnvironment();
};

}  // namespace mooncake
