#pragma once

#include <optional>

namespace mooncake {

class Environ;

struct ClientNumaConfig {
    std::optional<int> socket_id;

    static ClientNumaConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake
