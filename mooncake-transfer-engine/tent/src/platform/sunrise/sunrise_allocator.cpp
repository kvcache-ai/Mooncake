// Copyright 2026 KVCache.AI
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

#include "tent/platform/sunrise.h"

#include <numa.h>

#include "tent/runtime/topology.h"
#include <tang_runtime_api.h>

namespace mooncake {
namespace tent {

Status SunrisePlatform::allocate(void** pptr, size_t size,
                                 MemoryOptions& options) {
    LocationParser location(options.location);
    if (location.type() == "cuda") {
        int saved_dev = -1;
        const tangError_t get_dev_ret = tangGetDevice(&saved_dev);
        if (tangSetDevice(location.index()) != tangSuccess) {
            return Status::InternalError("Unable to switch sunrise device");
        }
        tangError_t ret = tangMalloc(pptr, size);
        if (get_dev_ret == tangSuccess && saved_dev >= 0) {
            tangSetDevice(saved_dev);
        }
        if (ret != tangSuccess)
            return Status::InternalError(
                "Unable to allocate sunrise device memory");
        return Status::OK();
    }
    int socket_id = 0;
    if (location.type() == "cpu") socket_id = location.index();
    *pptr = numa_alloc_onnode(size, socket_id);
    if (!(*pptr))
        return Status::InternalError("Unable to allocate DRAM memory");
    return Status::OK();
}

Status SunrisePlatform::free(void* ptr, size_t size) {
    tangPointerAttributes attributes{};
    tangError_t ret = tangPointerGetAttributes(&attributes, ptr);
    if (ret == tangSuccess && attributes.type == tangMemoryTypeDevice) {
        if (attributes.device >= 0) {
            int saved_dev = 0;
            tangGetDevice(&saved_dev);
            tangSetDevice(attributes.device);
            tangFree(ptr);
            tangSetDevice(saved_dev);
        } else {
            tangFree(ptr);
        }
    } else {
        numa_free(ptr, size);
    }
    return Status::OK();
}

Status SunrisePlatform::copy(void* dst, void* src, size_t length) {
    tangPointerAttributes sa{};
    tangPointerAttributes da{};
    (void)tangPointerGetAttributes(&sa, src);
    (void)tangPointerGetAttributes(&da, dst);
    tangMemcpyKind kind = tangMemcpyHostToHost;
    if (sa.type == tangMemoryTypeDevice && da.type == tangMemoryTypeDevice) {
        kind = tangMemcpyDeviceToDevice;
    } else if (sa.type == tangMemoryTypeDevice &&
               da.type != tangMemoryTypeDevice) {
        kind = tangMemcpyDeviceToHost;
    } else if (sa.type != tangMemoryTypeDevice &&
               da.type == tangMemoryTypeDevice) {
        kind = tangMemcpyHostToDevice;
    }
    tangError_t ret = tangMemcpy(dst, src, length, kind);
    if (ret != tangSuccess)
        return Status::InternalError("Sunrise memcpy failed");
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
