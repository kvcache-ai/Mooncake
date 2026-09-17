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

#ifndef TENT_PLATFORM_CUDA_UTILS_H
#define TENT_PLATFORM_CUDA_UTILS_H

#include <cuda.h>

#include <mutex>

namespace mooncake {
namespace tent {

// cuInit() is idempotent and creates no context; the driver API does not
// initialize itself lazily, so it must run before any driver query.
inline bool ensureCudaDriverInit() {
    static std::once_flag flag;
    static CUresult result = CUDA_ERROR_NOT_INITIALIZED;
    std::call_once(flag, []() { result = cuInit(0); });
    return result == CUDA_SUCCESS;
}

// Device ordinal owning `ptr`, or -1 for host memory. Unlike the runtime
// cudaPointerGetAttributes(), this never implicitly initializes the calling
// thread's current device and its primary context (default GPU 0).
inline int getCudaDeviceForPtr(const void* ptr) {
    if (!ensureCudaDriverInit()) return -1;
    unsigned int mem_type = 0;
    CUresult res = cuPointerGetAttribute(
        &mem_type, CU_POINTER_ATTRIBUTE_MEMORY_TYPE, (CUdeviceptr)ptr);
    if (res != CUDA_SUCCESS || mem_type != CU_MEMORYTYPE_DEVICE) return -1;
    unsigned int dev_ordinal = 0;
    res = cuPointerGetAttribute(
        &dev_ordinal, CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL, (CUdeviceptr)ptr);
    if (res != CUDA_SUCCESS) return -1;
    return static_cast<int>(dev_ordinal);
}

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_PLATFORM_CUDA_UTILS_H
