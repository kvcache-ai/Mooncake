#pragma once

#include <cstdint>

namespace mooncake {

struct LiveAllocation {
    uint64_t offset_bytes{0};
    uint64_t requested_bytes{0};
};

}  // namespace mooncake
