#pragma once

#include <span>

#include <ylt/util/tl/expected.hpp>

#include "allocator.h"
#include "segment/region.h"

namespace mooncake {

tl::expected<RegionInitialState, ErrorCode> BuildRegionInitialState(
    const RegionResourceSpec& spec,
    std::span<const AllocatedBuffer::Descriptor> descriptors);

}  // namespace mooncake
