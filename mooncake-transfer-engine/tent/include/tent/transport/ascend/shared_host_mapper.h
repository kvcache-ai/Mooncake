// Copyright 2025 Huawei Technologies Co., Ltd
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

#ifndef TENT_SHARED_HOST_MAPPER_H
#define TENT_SHARED_HOST_MAPPER_H

#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "tent/runtime/segment.h"

namespace mooncake {
namespace tent {

// Maps host segments owned by other processes on this machine into the local
// address space, so a transfer to them becomes a memcpy instead of a HIXL
// round trip.
//
// Only works when the owner allocated the segment from a shared memfd
// (ASCEND_USE_SHMEM), which is what puts a handle in BufferDesc::shm_path. A
// peer on another machine, or in another PID namespace, cannot open the handle
// and simply keeps using HIXL.
//
// Virtual addresses are not shared: the owner's base and this process's base
// differ, so Translate() rebases every address onto the local mapping.
class SharedHostMapper {
   public:
    SharedHostMapper() = default;
    ~SharedHostMapper();

    SharedHostMapper(const SharedHostMapper&) = delete;
    SharedHostMapper& operator=(const SharedHostMapper&) = delete;

    // Rebase [dest_addr, dest_addr+length) onto this process's mapping of the
    // owning buffer. Returns false, leaving `local_addr` untouched, when the
    // segment advertises no handle, when the handle cannot be imported, or
    // when the range escapes the buffer it started in.
    bool Translate(const SegmentDesc& segment, uint64_t dest_addr,
                   uint64_t length, void** local_addr);

   private:
    struct ImportedRegion {
        void* local_base = nullptr;
        uint64_t length = 0;
        // Owned mappings are unmapped on destruction. A handle exported by this
        // same process is used in place, so there is nothing to release.
        bool owns_mapping = false;
    };

    // Returns the local base for `handle`, importing it on first use. Negative
    // results are cached too: a handle we cannot open will not become openable,
    // and retrying an open() per transfer would be costly.
    const ImportedRegion* ResolveHandle(const std::string& handle,
                                        uint64_t remote_base, uint64_t length);

    // Keyed by the handle string, which already encodes the owner pid and fd.
    // A remounted segment therefore gets a new key rather than reusing a stale
    // mapping. Entries are never evicted: a translated pointer may be in use by
    // another thread, and unmapping underneath it would fault.
    std::unordered_map<std::string, ImportedRegion> regions_;
    std::mutex mutex_;
};

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_SHARED_HOST_MAPPER_H
