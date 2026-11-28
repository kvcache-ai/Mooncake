// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef SHARED_SEGMENT_INTERNAL_H
#define SHARED_SEGMENT_INTERNAL_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "shared_segment/shared_segment.h"

namespace mooncake {

// One physical memory shard contributed by `owner_rank` and mapped at
// `[base_addr + offset, base_addr + offset + size)` on every rank.
struct OwnerChunk {
    uint32_t owner_rank = 0;
    uint64_t offset = 0;
    uint64_t size = 0;
};

// Platform operations behind a shared segment. One instance owns the resources
// of exactly one segment in one process and releases them on destruction.
class SharedSegmentBackend {
   public:
    virtual ~SharedSegmentBackend() = default;

    // Allocation size of each shard must be a multiple of this, and the
    // reserved base address is aligned to it.
    virtual uint64_t Granularity(const SharedSegmentOptions& options) const = 0;

    // Phase one: reserve the full contiguous VA window of `total_size` bytes.
    // If `local_chunk != nullptr`, this rank is an owner and also allocates
    // physical memory for `*local_chunk`, maps it at
    // `base_addr + local_chunk->offset`, and exports an opaque shareable handle
    // that fits in kMaxHandleBytes.
    virtual Status Prepare(uint64_t total_size, const OwnerChunk* local_chunk,
                           const SharedSegmentOptions& options,
                           uintptr_t& base_addr,
                           std::vector<uint8_t>& handle) = 0;

    // Phase two: import and map every remote owner's shard into the reserved
    // window, then run any whole-segment post-map step (such as HostRegister
    // for mmap). `handles` is aligned 1:1 with `chunks`.
    virtual Status CompleteMapping(
        const std::vector<OwnerChunk>& chunks,
        const std::vector<std::vector<uint8_t>>& handles,
        const SharedSegmentOptions& options) = 0;

    // Identifies the backend in the exchanged blob, so a mismatched peer build
    // is rejected instead of importing garbage.
    virtual uint16_t BackendId() const = 0;

    // Accelerator address of the same pages when device-accessible
    // (HostRegister device VA for mmap, or the host SVM VA itself after
    // MemSetAccess for Ascend VMM). Zero when host-only.
    virtual uintptr_t DeviceAddr() const { return 0; }
};

// Creates the requested backend implementation. Supported implementation names
// are "mmap" and "ascend". Returns nullptr when the implementation is
// unknown, unavailable in this build, or unsupported by the running system.
std::unique_ptr<SharedSegmentBackend> CreateSharedSegmentBackend(
    const std::string& impl);

// Largest platform handle carried in a blob: aclrtMemFabricHandle is 128 bytes.
constexpr size_t kMaxHandleBytes = 128;

// Every rank contributes a blob of the same length, so the exchange is a plain
// fixed-size all-gather with no serialization framework involved. Owner ranks
// fill in their shard handle; non-owner ranks leave the handle empty.
struct SegmentBlobHeader {
    uint32_t magic;
    uint16_t version;
    uint16_t backend_id;
    uint32_t rank_id;
    uint32_t handle_bytes;
    uint64_t fingerprint;
    uint64_t alloc_size;
};

// The header is memcpy'd raw into the blob, so its layout is part of the wire
// format.
static_assert(sizeof(SegmentBlobHeader) == 32,
              "SegmentBlobHeader layout is part of the exchanged blob");

constexpr size_t kSegmentBlobBytes =
    sizeof(SegmentBlobHeader) + kMaxHandleBytes;

Status EncodeSegmentBlob(const SegmentBlobHeader& header,
                         const std::vector<uint8_t>& handle, std::string& blob);

Status DecodeSegmentBlob(const std::string& blob, SegmentBlobHeader& header,
                         std::vector<uint8_t>& handle);

uint64_t AlignUp(uint64_t value, uint64_t alignment);

// Resolves `options.owner_ranks`, falling back to `{options.owner_rank}` when
// `options.owner_ranks` is empty.
std::vector<uint32_t> ResolveOwnerRanks(const SharedSegmentOptions& options);

// Partitions `size` bytes across `owner_ranks` in `granularity`-sized units so
// every owner contributes at least one granule and `total_alloc_size >= size`.
std::vector<OwnerChunk> ComputeOwnerChunks(
    uint64_t size, uint64_t granularity,
    const std::vector<uint32_t>& owner_ranks, uint64_t& total_alloc_size);

// Digest of everything the ranks must agree on. Ranks compare digests instead
// of exchanging the full spec, which keeps the blob fixed-length. Callers that
// care about a richer layout should fold that declaration into `name`.
uint64_t ComputeSegmentFingerprint(const std::string& name, uint64_t size,
                                   const SharedSegmentOptions& options);

}  // namespace mooncake

#endif  // SHARED_SEGMENT_INTERNAL_H
