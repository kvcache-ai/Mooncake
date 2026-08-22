#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "allocator_import.h"
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

struct RegionInitialState {
    std::vector<LiveAllocation> allocations;
};

}  // namespace mooncake
