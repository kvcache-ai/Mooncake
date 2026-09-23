#pragma once

#include <string>

namespace mooncake {

struct RuntimeIdentityConfig {
    std::string pod_name;
    std::string pod_namespace;
};

}  // namespace mooncake
