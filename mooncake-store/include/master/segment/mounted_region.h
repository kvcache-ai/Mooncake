#pragma once

#include <cstdint>

#include "master/segment/region.h"
#include "master/segment/status.h"

namespace mooncake {

struct MountedRegion {
    Segment segment;
    UUID client_id{0, 0};
    SegmentStatus status{SegmentStatus::UNDEFINED};
    RegionKind kind{RegionKind::HOST_MEMORY};
    // Assigned by Catalog on registration and each lifecycle transition.
    uint64_t generation{0};
};

}  // namespace mooncake
