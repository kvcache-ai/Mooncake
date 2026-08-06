// Copyright 2026 Huawei Technologies Co., Ltd
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

// Platform VMM operations behind a shared segment. One instance owns the
// resources of exactly one segment in one process and releases them on
// destruction.
class SharedSegmentBackend {
   public:
    virtual ~SharedSegmentBackend() = default;

    // Allocation size must be a multiple of this, and the reserved base address
    // is aligned to it. It depends on the options because the host NUMA node is
    // derived from device_id.
    virtual uint64_t Granularity(const SharedSegmentOptions& options) const = 0;

    // Owner path: allocate physical memory, map it at a fresh local address,
    // and export a handle that peers can import. The handle is opaque bytes and
    // must fit in kMaxHandleBytes.
    virtual Status CreateOwner(uint64_t size,
                               const SharedSegmentOptions& options,
                               uintptr_t& base_addr,
                               std::vector<uint8_t>& handle) = 0;

    // Non-owner path, phase one: reserve an address window. Done before the
    // handle exchange so the segment base is known as early as on the owner.
    virtual Status ReserveLocal(uint64_t size,
                                const SharedSegmentOptions& options,
                                uintptr_t& base_addr) = 0;

    // Non-owner path, phase two: map the owner's physical pages into the window
    // this instance reserved.
    virtual Status ImportAndMap(uint64_t size,
                                const SharedSegmentOptions& options,
                                const std::vector<uint8_t>& handle) = 0;

    // Identifies the backend in the exchanged blob, so a mismatched peer build
    // is rejected instead of importing garbage.
    virtual uint16_t BackendId() const = 0;

    // Accelerator address of the same pages when the backend had to map them
    // separately for the device. Zero when the base address is already
    // device-accessible, which is the case for every VMM backend.
    virtual uintptr_t DeviceAddr() const { return 0; }
};

// Returns nullptr when the requested backend is unavailable on this build or
// the running system cannot share memory.
std::unique_ptr<SharedSegmentBackend> CreateSharedSegmentBackend(
    const SharedSegmentOptions& options);

// Always compiled; returns nullptr when neither CUDA nor Ascend is available
// for HostRegister.
std::unique_ptr<SharedSegmentBackend> CreateMmapSharedSegmentBackend();

#ifdef USE_ASCEND_DIRECT
std::unique_ptr<SharedSegmentBackend> CreateAscendSharedSegmentBackend();
#endif

#ifdef USE_CUDA
std::unique_ptr<SharedSegmentBackend> CreateCudaSharedSegmentBackend();
#endif

// Largest platform handle carried in a blob: aclrtMemFabricHandle is 128 bytes,
// CUmemFabricHandle is 64.
constexpr size_t kMaxHandleBytes = 128;

// Every rank contributes a blob of the same length, so the exchange is a plain
// fixed-size all-gather with no serialization framework involved. Only the
// owner fills in a handle.
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

// Digest of everything the ranks must agree on. Ranks compare digests instead
// of exchanging the full spec, which keeps the blob fixed-length. Callers that
// care about a richer layout should fold that declaration into `name`.
uint64_t ComputeSegmentFingerprint(const std::string& name, uint64_t size,
                                   const SharedSegmentOptions& options);

}  // namespace mooncake

#endif  // SHARED_SEGMENT_INTERNAL_H
