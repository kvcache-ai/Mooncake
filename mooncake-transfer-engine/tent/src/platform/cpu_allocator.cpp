// Copyright 2025 KVCache.AI
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

#include "tent/platform/cpu.h"
#include "tent/runtime/topology.h"

#include <numa.h>
#include <glog/logging.h>

namespace mooncake {
namespace tent {
Status CpuPlatform::allocate(void** pptr, size_t size, MemoryOptions& options) {
    LocationParser location(options.location);
    if (location.type() == "cuda") {
        return Status::NotImplemented(
            "CUDA not supported in cpu platform" LOC_MARK);
    }
    int socket_id = 0;
    if (location.type() == "cpu") socket_id = location.index();
    *pptr = numa_alloc_onnode(size, socket_id);
    if (!(*pptr))
        return Status::InternalError("Unable to allocate DRAM memory");
    return Status::OK();
}

Status CpuPlatform::free(void* ptr, size_t size) {
    numa_free(ptr, size);
    return Status::OK();
}

Status CpuPlatform::copy(void* dst, void* src, size_t length) {
    memcpy(dst, src, length);
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
