#pragma once

#include "segment/region.h"
#include "segment/status.h"

namespace mooncake {

struct MountedRegion {
    Segment segment;
    UUID client_id{0, 0};
    SegmentStatus status{SegmentStatus::UNDEFINED};
    RegionKind kind{RegionKind::HOST_MEMORY};
};

}  // namespace mooncake
