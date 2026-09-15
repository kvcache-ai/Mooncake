#pragma once

#include <string>
#include <vector>

#include "allocator.h"
#include "segment/mounted_region.h"

namespace mooncake {

// Detached legacy buffer record. Codecs encode fields; the Pool validates
// ownership, allocator layout and lifetime binding when restoring it.
struct BufferSnapshot {
    UUID region_id;
    uint64_t size;
    uint64_t address;
    std::optional<offset_allocator::OffsetAllocationSnapshot> allocation;
};

struct RegionSnapshot {
    MountedRegion mounted;
    OffsetBufferAllocatorSnapshot allocator;
};

// Detached, owning snapshot of the resources in the legacy host-memory format.
// All metadata, including allocator bins/nodes, is copied. Encoding and
// destroying this value never accesses a SegmentPool or changes runtime
// capacity/usage.
struct SegmentPoolSnapshot {
    BufferAllocatorType memory_allocator_type = BufferAllocatorType::OFFSET;
    std::vector<std::string> active_names;
    std::vector<RegionSnapshot> regions;
};

}  // namespace mooncake
