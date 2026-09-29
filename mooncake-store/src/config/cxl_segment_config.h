#pragma once

#include <cstddef>
#include <optional>

namespace mooncake {

class Environ;

struct CxlSegmentConfig {
    std::optional<size_t> device_size;

    static CxlSegmentConfig FromEnvironment(const Environ& env);
};

}  // namespace mooncake
