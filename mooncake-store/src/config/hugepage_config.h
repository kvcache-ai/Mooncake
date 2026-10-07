#pragma once

#include <cstddef>

namespace mooncake {

struct HugepageConfig {
    bool enabled = false;
    size_t page_size = 0;

    static bool IsEnabledFromEnvironment();
    static HugepageConfig FromEnvironment();
};

}  // namespace mooncake
