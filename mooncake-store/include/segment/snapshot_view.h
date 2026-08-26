#pragma once

#include "placement/index.h"
#include "segment/catalog.h"
#include "segment/resource_view.h"

namespace mooncake {

class SegmentPool;

// Lock-free, read-only view for a forked snapshot child. The child has a
// private copy-on-write address space, but may inherit pool_mutex_ while it is
// locked by a thread that no longer exists. Do not use this view in a live,
// concurrently mutating parent process.
class SegmentPoolSnapshotView final {
   public:
    const RegionCatalog& Catalog() const;
    RegionResourceReadView Resources() const;
    PlacementReadView Placement() const;

   private:
    explicit SegmentPoolSnapshotView(const SegmentPool* segment_pool)
        : segment_pool_(segment_pool) {}

    const SegmentPool* segment_pool_;

    friend class SegmentPool;
};

}  // namespace mooncake
