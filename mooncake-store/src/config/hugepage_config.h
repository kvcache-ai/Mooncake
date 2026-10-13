#pragma once

#include <cstddef>

namespace mooncake {

class Environ;

struct HugepageConfig {
    bool enabled = false;
    size_t page_size = 0;

    static bool IsEnabledFromEnvironment(const Environ& env);
    static HugepageConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake
