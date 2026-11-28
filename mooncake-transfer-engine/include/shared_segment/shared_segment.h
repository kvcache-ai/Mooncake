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

#ifndef SHARED_SEGMENT_H
#define SHARED_SEGMENT_H

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/base/status.h"

namespace mooncake {

// A shared segment is one contiguous span of host memory backed by physical
// pages contributed by one or more `owner_ranks`, mapped into every rank of
// the group. It targets the write-once/read-many KV offload pattern: sizes are
// known before the first allocation, nothing is ever freed, and addresses must
// stay fixed so they can be captured in a graph.
//
// Virtual addresses are local to each process (each rank reserves one
// contiguous window and maps every owner's shard at the same byte offset).
// Layout (which offset holds which tensor) is the caller's responsibility;
// this class only shares the raw span.
//
// Creation is two-phase because the only communicator in reach belongs to the
// framework (a torch process group in practice), and driving it from C++ would
// mean calling back into Python:
//
//   1. Create   - reserve the full contiguous VA window; each rank in
//                 `owner_ranks` also allocates, maps, and exports its physical
//                 shard. Returns a fixed-length blob.
//   2. Complete - after the caller all-gathers the blobs, verify every rank
//                 asked for the same layout, then import and map every remote
//                 owner's shard into the reserved window.
//
// Peers are trusted: the checks catch a group that was configured
// inconsistently, not a peer that lies. Owners' segments must outlive
// non-owners' mappings, because freeing physical pages invalidates peer
// mappings.

struct SharedSegmentOptions {
    uint32_t world_size = 1;
    uint32_t rank_id = 0;
    uint32_t owner_rank = 0;
    // Ranks that allocate and export physical memory shards for the segment.
    // When empty, defaults to `{owner_rank}`.
    std::vector<uint32_t> owner_ranks;
    // Accelerator that is granted access to the host pages (D2H/H2D).
    int32_t device_id = 0;
    // true: unnamed memfd + mmap. Prefer THP (MADV_HUGEPAGE) so the kernel
    // can back the span with 2MiB pages; 4KiB pages if THP cannot allocate.
    // Does not use the HugeTLB pool. Ranks share the pages through the memfd.
    // false: platform VMM with a fabric shareable handle.
    bool mmap = true;
    // When mmap is true: HostRegister the pages for device_id (Ascend).
    // Needed for TE location=npu ROCE D2rH; off by default.
    bool host_register = false;
};

class SharedSegmentBackend;

class SharedSegment {
   public:
    ~SharedSegment();

    SharedSegment(const SharedSegment&) = delete;
    SharedSegment& operator=(const SharedSegment&) = delete;

    // Phase one. On success `segment` owns the reservation (and, on the owner,
    // the allocation) and `out_blob` is ready for the caller's all-gather.
    static Status Create(const std::string& name, uint64_t size,
                         const SharedSegmentOptions& options,
                         std::shared_ptr<SharedSegment>& segment,
                         std::string& out_blob);

    // Whether this build can share memory across processes. mmap uses an
    // anonymous memfd (always available). host_register additionally needs
    // Ascend. VMM (mmap=false) needs a platform fabric backend.
    static bool Supported(bool mmap = true, bool host_register = false);

    // Phase two. `blobs` is indexed by rank. After success, `ready()` is true
    // and `base_addr()` is valid.
    Status Complete(const std::vector<std::string>& blobs);

    bool ready() const { return ready_; }
    uintptr_t base_addr() const { return base_addr_; }
    uint64_t size() const { return size_; }
    const std::vector<uint32_t>& owner_ranks() const { return owner_ranks_; }

    // Address the accelerator sees, when the backend had to map the pages for
    // it separately (mmap plus HostRegister on Ascend), or the host SVM VA
    // itself once mapped (Ascend VMM). Zero when host-only.
    uintptr_t device_addr() const;

   private:
    SharedSegment(std::string name, uint64_t size,
                  SharedSegmentOptions options);

    std::string name_;
    uint64_t size_ = 0;
    SharedSegmentOptions options_;
    std::vector<uint32_t> owner_ranks_;
    uint64_t fingerprint_ = 0;
    uint64_t granularity_ = 0;
    uint64_t alloc_size_ = 0;
    uintptr_t base_addr_ = 0;
    bool ready_ = false;
    std::unique_ptr<SharedSegmentBackend> backend_;
};

}  // namespace mooncake

#endif  // SHARED_SEGMENT_H
