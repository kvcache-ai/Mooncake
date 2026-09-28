#pragma once

#include <cstddef>
#include <string>

#include "types.h"

namespace mooncake {

struct CxlBootstrapConfig {
    // The flag definition in master.cpp reuses this default, so the config
    // default and the flag default cannot drift apart.
    static constexpr bool kDefaultEnabled = false;

    bool enabled{kDefaultEnabled};
    std::string path{DEFAULT_CXL_PATH};
    size_t size{DEFAULT_CXL_SIZE};
};

}  // namespace mooncake
