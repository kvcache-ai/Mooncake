#pragma once

#include <cstdint>

#include "client_liveness.h"
#include "segment/region.h"
#include "segment/status.h"

namespace mooncake {

struct MountedRegion {
    Segment segment;
    UUID client_id{0, 0};
    SegmentStatus status{SegmentStatus::UNDEFINED};
    RegionKind kind{RegionKind::HOST_MEMORY};
    // Assigned by Catalog on registration and each lifecycle transition.
    uint64_t generation{0};
    ClientSessionSharedPtr owner_session{nullptr};
};

}  // namespace mooncake
