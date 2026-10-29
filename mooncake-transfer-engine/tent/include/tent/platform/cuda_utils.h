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
inline CUresult cudaDriverInitResult() {
    static std::once_flag flag;
    static CUresult result = CUDA_ERROR_NOT_INITIALIZED;
    std::call_once(flag, []() { result = cuInit(0); });
    return result;
}

inline bool ensureCudaDriverInit() {
    return cudaDriverInitResult() == CUDA_SUCCESS;
}

enum class CudaMemoryKind { Unknown, Host, Device, Managed };

struct CudaPointerInfo {
    CudaMemoryKind kind = CudaMemoryKind::Unknown;
    int device = -1;
    bool registered_host = false;

    // CUDA copy/free eligibility, not GPUDirect transport eligibility.
    bool isCuda() const {
        return kind == CudaMemoryKind::Device ||
               kind == CudaMemoryKind::Managed;
    }
};

// No runtime API calls: classification must not create or switch contexts.
// Only a successful multi-attribute query can authorize host handling. Unlike
// cuPointerGetAttribute(), cuPointerGetAttributes() returns success with
// default attributes for ordinary, unregistered host pointers. This is
// classification, not validation of a host address or allocation ownership.
inline CUresult getCudaPointerInfo(const void* ptr, CudaPointerInfo& info) {
    info = {};
    if (!ptr) return CUDA_ERROR_INVALID_VALUE;
    CUresult res = cudaDriverInitResult();
    if (res != CUDA_SUCCESS) return res;
    unsigned int mem_type = 0;
    unsigned int managed = 0;
    int device = -1;
    CUpointer_attribute attributes[] = {CU_POINTER_ATTRIBUTE_MEMORY_TYPE,
                                        CU_POINTER_ATTRIBUTE_IS_MANAGED,
                                        CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL};
    void* values[] = {&mem_type, &managed, &device};
    res = cuPointerGetAttributes(3, attributes, values, (CUdeviceptr)ptr);
    if (res != CUDA_SUCCESS) return res;
    if (managed > 1 ||
        (mem_type != 0 && mem_type != CU_MEMORYTYPE_HOST &&
         mem_type != CU_MEMORYTYPE_DEVICE &&
         mem_type != CU_MEMORYTYPE_UNIFIED) ||
        (managed && mem_type == 0))
        return CUDA_ERROR_INVALID_VALUE;
    CudaPointerInfo result;
    if (managed || mem_type == CU_MEMORYTYPE_UNIFIED) {
        result.kind = CudaMemoryKind::Managed;
    } else if (mem_type == CU_MEMORYTYPE_DEVICE) {
        result.kind = CudaMemoryKind::Device;
    } else if (mem_type == 0 || mem_type == CU_MEMORYTYPE_HOST) {
        result.kind = CudaMemoryKind::Host;
        result.registered_host = mem_type == CU_MEMORYTYPE_HOST;
    } else {
        return CUDA_ERROR_INVALID_VALUE;
    }
    if (result.isCuda()) {
        if (device < 0) return CUDA_ERROR_INVALID_DEVICE;
        CUdevice cu_device;
        res = cuDeviceGet(&cu_device, device);
        if (res != CUDA_SUCCESS) return res;
        result.device = device;
    }
    info = result;
    return CUDA_SUCCESS;
}

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_PLATFORM_CUDA_UTILS_H
