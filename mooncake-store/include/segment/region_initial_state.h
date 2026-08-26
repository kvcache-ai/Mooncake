#pragma once

#include <span>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "allocator.h"
#include "allocator_import.h"
#include "segment/region.h"

namespace mooncake {

struct RegionInitialState {
    std::vector<LiveAllocation> allocations;
};

tl::expected<RegionInitialState, ErrorCode> BuildRegionInitialState(
    const RegionResourceSpec& spec,
    std::span<const AllocatedBuffer::Descriptor> descriptors);

}  // namespace mooncake
