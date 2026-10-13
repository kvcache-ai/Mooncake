#pragma once

#include <string>

namespace mooncake {

class Environ;

struct ClientHostIdentityConfig {
    std::string host_id;

    static ClientHostIdentityConfig FromEnvironment(
        const Environ& env, const std::string& local_hostname);
};

}  // namespace mooncake
