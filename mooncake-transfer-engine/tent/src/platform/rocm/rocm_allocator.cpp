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

#include "tent/platform/rocm.h"
#include "tent/common/status.h"

#include <hip/hip_runtime.h>
#include <numa.h>
#include <glog/logging.h>

namespace mooncake {
namespace tent {

Status RocmPlatform::allocate(void** pptr, size_t size,
                              MemoryOptions& options) {
    LocationParser location(options.location);
    if (location.type() == "rocm") {
        int hip_dev = 0;
        CHECK_HIP(hipGetDevice(&hip_dev));
        CHECK_HIP(hipSetDevice(location.index()));
        CHECK_HIP(hipMalloc(pptr, size));
        hipSetDevice(hip_dev);
        return Status::OK();
    }
    int socket_id = 0;
    if (location.type() == "cpu") socket_id = location.index();
    *pptr = numa_alloc_onnode(size, socket_id);
    if (!(*pptr))
        return Status::InternalError("Unable to allocate DRAM memory");
    return Status::OK();
}

Status RocmPlatform::free(void* ptr, size_t size) {
    hipPointerAttribute_t attributes;
    CHECK_HIP(hipPointerGetAttributes(&attributes, ptr));
    if (attributes.type == hipMemoryTypeDevice) {
        CHECK_HIP(hipFree(ptr));
    } else if (attributes.type == hipMemoryTypeHost ||
               attributes.type == hipMemoryTypeUnregistered) {
        numa_free(ptr, size);
    } else {
        LOG(ERROR) << "Unknown memory type, " << ptr << " " << attributes.type;
    }
    return Status::OK();
}

Status RocmPlatform::copy(void* dst, void* src, size_t length) {
    HIPStreamHandle stream;
    CHECK_STATUS(getStreamFromPool(stream));
    CHECK_HIP(hipMemcpyAsync(dst, src, length, hipMemcpyDefault, stream.get()));
    CHECK_HIP(hipStreamSynchronize(stream.get()));
    return Status::OK();
}

}  // namespace tent
}  // namespace mooncake
