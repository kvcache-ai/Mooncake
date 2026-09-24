#pragma once

#include <cstddef>
#include <string>

#include "types.h"

namespace mooncake {

struct CxlBootstrapConfig {
    bool enabled = false;
    std::string path = DEFAULT_CXL_PATH;
    size_t size = DEFAULT_CXL_SIZE;
};

}  // namespace mooncake
