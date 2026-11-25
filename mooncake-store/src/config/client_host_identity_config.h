#pragma once

#include <string>
#include <optional>

#include "types.h"

namespace mooncake {

struct ClientHostIdentityConfig {
    std::string host_id;

    static ClientHostIdentityConfig FromEnvironment(
        const std::string& local_hostname);

    // MC_PHYSICAL_GPU is physical, not CUDA's CVD-renumbered logical index.
    static std::optional<PhysicalPlacement> PhysicalFromEnvironment(
        const std::string& local_hostname);
};

}  // namespace mooncake
