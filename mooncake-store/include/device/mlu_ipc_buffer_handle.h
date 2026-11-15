#pragma once

#include <cstdint>
#include <string>

#include "types.h"

namespace mooncake {

// CNDRV IPC handles are opaque integer tokens, not addresses to dereference.
// Device UUIDs remain stable when the peers reorder their visible devices.
struct MluIpcBufferHandle {
    uint64_t handle = 0;
    uint64_t offset = 0;
    uint64_t size = 0;
    std::string device_uuid;
    std::string lease;
    uint64_t lease_device = 0;
    uint64_t lease_inode = 0;
};

struct MluIpcWriteRequest {
    std::string key;
    // Small Tensor headers travel with the RPC. The receiver owns these bytes
    // until the write finishes, without a shared-memory allocation round trip.
    std::string metadata;
    MluIpcBufferHandle payload;
};

struct MluIpcReadRequest {
    std::string key;
    MluIpcBufferHandle destination;
    uint64_t source_offset = 0;
    uint64_t size = 0;
    bool size_is_buffer_capacity = false;
};

}  // namespace mooncake

YLT_REFL(mooncake::MluIpcBufferHandle, handle, offset, size, device_uuid, lease,
         lease_device, lease_inode);
YLT_REFL(mooncake::MluIpcWriteRequest, key, metadata, payload);
YLT_REFL(mooncake::MluIpcReadRequest, key, destination, source_offset, size,
         size_is_buffer_capacity);
