#pragma once

#include <cstdint>

namespace mooncake {

struct RegisteredPinnedMemoryConfig {
    uint64_t max_bytes = 0;

    static RegisteredPinnedMemoryConfig FromEnvironment();
};

}  // namespace mooncake
