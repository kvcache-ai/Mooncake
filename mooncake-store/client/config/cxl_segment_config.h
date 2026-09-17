#pragma once

#include <cstddef>
#include <optional>

namespace mooncake {

struct CxlSegmentConfig {
    std::optional<size_t> device_size;

    static CxlSegmentConfig FromEnvironment();
};

}  // namespace mooncake
