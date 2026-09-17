#pragma once

#include <string>

namespace mooncake {

struct ClientHostIdentityConfig {
    std::string host_id;

    static ClientHostIdentityConfig FromEnvironment(
        const std::string& local_hostname);
};

}  // namespace mooncake
