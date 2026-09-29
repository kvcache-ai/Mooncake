#pragma once

#include <cstdint>

namespace mooncake {

class Environ;

struct RegisteredPinnedMemoryConfig {
    uint64_t max_bytes = 0;

    static RegisteredPinnedMemoryConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake
