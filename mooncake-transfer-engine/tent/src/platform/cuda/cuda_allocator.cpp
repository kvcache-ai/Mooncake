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

#include "tent/platform/cuda.h"
#include "tent/common/status.h"

#include <bits/stduint-uintn.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <numa.h>
#include <glog/logging.h>

namespace mooncake {
namespace tent {
Status CudaPlatform::allocate(void** pptr, size_t size,
                              MemoryOptions& options) {
    LocationParser location(options.location);
    if (location.type() == "cuda") {
        int cuda_dev = 0;
        CHECK_CUDA(cudaGetDevice(&cuda_dev));
        CHECK_CUDA(cudaSetDevice(location.index()));
        CHECK_CUDA(cudaMalloc(pptr, size));
        cudaSetDevice(cuda_dev);
        return Status::OK();
    }
    int socket_id = 0;
    if (location.type() == "cpu") socket_id = location.index();
    *pptr = numa_alloc_onnode(size, socket_id);
    if (!(*pptr))
        return Status::InternalError("Unable to allocate DRAM memory");
    return Status::OK();
}

Status CudaPlatform::free(void* ptr, size_t size) {
    cudaPointerAttributes attributes;
    CHECK_CUDA(cudaPointerGetAttributes(&attributes, ptr));
    if (attributes.type == cudaMemoryTypeDevice) {
        CHECK_CUDA(cudaFree(ptr));
    } else if (attributes.type == cudaMemoryTypeHost ||
               attributes.type == cudaMemoryTypeUnregistered) {
        numa_free(ptr, size);
    } else {
        LOG(ERROR) << "Unknown memory type, " << ptr << " " << attributes.type;
    }
    return Status::OK();
}

// Define USE_CU_MEMCPY to use direction-aware CUDA Driver API functions
// (supports all directions: H2D, D2H, D2D, H2H, avoids stream deadlocks)
// If not defined, uses the original cudaMemcpyAsync + stream approach
#define USE_CU_MEMCPY

Status CudaPlatform::copy(void* dst, void* src, size_t length) {
#ifdef USE_CU_MEMCPY
    // Use direction-aware CUDA Driver API functions to support all transfer
    // directions (H2D, D2H, D2D, H2H) while avoiding deadlock issues.
    // These synchronous functions avoid both:
    // - cudaMemcpy() deadlock (legacy default stream dependency)
    // - cudaMemcpyAsync + cudaStreamSynchronize deadlock
    //
    // Direction-aware functions:
    // - cuMemcpyHtoD: Host to Device (synchronous, no stream dependency)
    // - cuMemcpyDtoH: Device to Host (synchronous, no stream dependency)
    // - cuMemcpyDtoD: Device to Device (synchronous, no stream dependency)
    // - memcpy: Host to Host (standard CPU copy)

    // Ensure CUDA Driver API is initialized (idempotent)
    static CUresult init_result = cuInit(0);
    if (init_result != CUDA_SUCCESS) {
        const char* error_str = nullptr;
        cuGetErrorString(init_result, &error_str);
        return Status::InternalError(
            std::string("CUDA Driver API init failed: ") + error_str);
    }

    // Detect pointer types to determine transfer direction
    cudaPointerAttributes src_attr, dst_attr;
    CUresult result;

    // Get source pointer attributes
    cudaError_t cuda_err = cudaPointerGetAttributes(&src_attr, src);
    bool src_is_device =
        (cuda_err == cudaSuccess) && (src_attr.type == cudaMemoryTypeDevice);

    // Get destination pointer attributes
    cuda_err = cudaPointerGetAttributes(&dst_attr, dst);
    bool dst_is_device =
        (cuda_err == cudaSuccess) && (dst_attr.type == cudaMemoryTypeDevice);

    // Select appropriate transfer function based on direction
    if (src_is_device && dst_is_device) {
        // Device to Device
        result = cuMemcpyDtoD((CUdeviceptr)dst, (CUdeviceptr)src, length);
    } else if (src_is_device && !dst_is_device) {
        // Device to Host
        result = cuMemcpyDtoH(dst, (CUdeviceptr)src, length);
    } else if (!src_is_device && dst_is_device) {
        // Host to Device
        result = cuMemcpyHtoD((CUdeviceptr)dst, src, length);
    } else {
        // Host to Host - use standard CPU memcpy
        memcpy(dst, src, length);
        return Status::OK();
    }

    if (result != CUDA_SUCCESS) {
        const char* error_str = nullptr;
        cuGetErrorString(result, &error_str);
        return Status::InternalError(std::string("CUDA memcpy failed: ") +
                                     error_str);
    }

    return Status::OK();

#else  // Original implementation using cudaMemcpyAsync
    // Use cudaMemcpyAsync with a non-blocking stream instead of cudaMemcpy(),
    // as the latter relies on the legacy default stream and can introduce
    // unintended synchronization or even deadlocks in downstream
    // components (e.g. mooncake-pg).

    CUDAStreamHandle stream;
    CHECK_STATUS(getStreamFromPool(stream));
    CHECK_CUDA(
        cudaMemcpyAsync(dst, src, length, cudaMemcpyDefault, stream.get()));
    CHECK_CUDA(cudaStreamSynchronize(stream.get()));
    return Status::OK();
#endif
}
}  // namespace tent
}  // namespace mooncake
