#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "types.h"

namespace mooncake {

enum class RegionKind {
    HOST_MEMORY = 0,
    CXL,
};

struct RegionResourceSpec {
    UUID id{0, 0};
    std::string name;
    uintptr_t base{0};
    size_t size{0};
    std::string transport_endpoint;
};

struct LiveAllocation {
    uint64_t offset_bytes{0};
    uint64_t requested_bytes{0};
};

struct RegionInitialState {
    std::vector<LiveAllocation> allocations;
};

}  // namespace mooncake
