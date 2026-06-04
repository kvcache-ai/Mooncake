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

#ifndef TENT_PLATFORM_CUDA_UTILS_H
#define TENT_PLATFORM_CUDA_UTILS_H

#include <cuda.h>

namespace mooncake {
namespace tent {

// Use the CUDA driver API to find the device ordinal for a pointer without
// requiring an initialized runtime context (avoids GPU 0 context creation).
// Returns the device index if ptr is device memory, or -1 for host memory.
inline int getCudaDeviceForPtr(void* ptr) {
    unsigned int memType = 0;
    CUresult res = cuPointerGetAttribute(
        &memType, CU_POINTER_ATTRIBUTE_MEMORY_TYPE, (CUdeviceptr)ptr);
    if (res != CUDA_SUCCESS || memType != CU_MEMORYTYPE_DEVICE) return -1;
    unsigned int devOrdinal = 0;
    res = cuPointerGetAttribute(
        &devOrdinal, CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL, (CUdeviceptr)ptr);
    if (res != CUDA_SUCCESS) return -1;
    return static_cast<int>(devOrdinal);
}

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_PLATFORM_CUDA_UTILS_H
