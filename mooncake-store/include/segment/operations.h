#pragma once

#include <vector>

#include "types.h"

namespace mooncake {

// Process-local identity, distinct from the reusable region UUID. Restoring or
// clearing a Pool invalidates outstanding operations, never retargets them.
struct SegmentUnmountOperation {
    UUID id;
    Segment segment;
};

struct PendingSegmentUnmount {
    Segment segment;
    ErrorCode error;
};

struct ClientUnmountBatch {
    std::vector<SegmentUnmountOperation> prepared;
    std::vector<PendingSegmentUnmount> pending;
};

}  // namespace mooncake
