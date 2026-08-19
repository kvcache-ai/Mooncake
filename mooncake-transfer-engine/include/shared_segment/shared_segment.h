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

// A shared segment is one contiguous span of host memory whose physical pages
// exist exactly once, in the process of `owner_rank`, while every rank of the
// group maps them at a rank-local address. It targets the write-once/read-many
// KV offload pattern: sizes are known before the first allocation, nothing is
// ever freed, and addresses must stay fixed so they can be captured in a graph.
//
// Virtual addresses are deliberately *not* required to match across ranks.
// Peers agree on the byte layout instead, so a rank locates data as
// `local_base + offset`. That keeps the reserved address space equal to the
// segment size rather than size x world_size. Layout (which offset holds which
// tensor) is the caller's responsibility; this class only shares the raw span.
//
// Creation is two-phase because the only communicator in reach belongs to the
// framework (a torch process group in practice), and driving it from C++ would
// mean calling back into Python:
//
//   1. Create  - reserve the local address window; the owner also allocates and
//                exports the physical pages. Returns a fixed-length blob.
//   2. Complete - after the caller all-gathers the blobs, verify every rank
//                asked for the same thing and, on non-owners, map the owner's
//                pages into the reserved window.
//
// Peers are trusted: the checks catch a group that was configured
// inconsistently, not a peer that lies. The owner's segment must outlive every
// other rank's, because freeing its pages invalidates their mappings.

struct SharedSegmentOptions {
    uint32_t world_size = 1;
    uint32_t rank_id = 0;
    uint32_t owner_rank = 0;
    // Accelerator that is granted access to the host pages (D2H/H2D).
    int32_t device_id = 0;
    // true: POSIX shm_open + mmap on the same host.
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

    // Whether this build can share memory across processes. mmap uses POSIX
    // shm (always available). host_register additionally needs Ascend.
    // VMM (mmap=false) needs a platform fabric backend.
    static bool Supported(bool mmap = true, bool host_register = false);

    // Phase two. `blobs` is indexed by rank. After success, `ready()` is true
    // and `base_addr()` is valid.
    Status Complete(const std::vector<std::string>& blobs);

    bool ready() const { return ready_; }
    uintptr_t base_addr() const { return base_addr_; }
    uint64_t size() const { return size_; }

    // Address the accelerator sees, when the backend had to map the pages for
    // it separately (POSIX shm plus HostRegister on Ascend). Zero when
    // `base_addr()` is already device-accessible.
    uintptr_t device_addr() const;

   private:
    SharedSegment(std::string name, uint64_t size,
                  SharedSegmentOptions options);

    std::string name_;
    uint64_t size_ = 0;
    SharedSegmentOptions options_;
    uint64_t fingerprint_ = 0;
    uint64_t alloc_size_ = 0;
    uintptr_t base_addr_ = 0;
    bool ready_ = false;
    std::unique_ptr<SharedSegmentBackend> backend_;
};

}  // namespace mooncake

#endif  // SHARED_SEGMENT_H
