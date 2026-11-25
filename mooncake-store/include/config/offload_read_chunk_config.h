#pragma once

#include <cstddef>
#include <cstdint>

namespace mooncake {

// Reader side of LOCAL_DISK (SSD offload) batch reads: how one owner's keys
// are split into owner RPCs (see offload_read_scheduler.h). Off by default.
struct OffloadReadChunkConfig {
    static constexpr uint64_t kDefaultChunkBytes = 0;
    static constexpr size_t kDefaultChunkConcurrency = 4;
    static constexpr size_t kMaxChunkConcurrency = 64;

    // MOONCAKE_OFFLOAD_READ_CHUNK_BYTES: byte cap per owner RPC; 0 sends each
    // owner's keys as one RPC.
    uint64_t chunk_bytes = kDefaultChunkBytes;
    // MOONCAKE_OFFLOAD_READ_CHUNK_CONCURRENCY: chunks of one owner's read in
    // flight, 1 to kMaxChunkConcurrency.
    size_t chunk_concurrency = kDefaultChunkConcurrency;

    static OffloadReadChunkConfig FromEnvironment();
};

}  // namespace mooncake
