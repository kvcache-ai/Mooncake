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

Status CudaPlatform::copy(void* dst, void* src, size_t length) {
    // Use cuMemcpy from CUDA Driver API instead of Runtime API.
    // This avoids synchronization issues with the legacy default stream
    // that can cause deadlocks in downstream components (e.g. mooncake-pg).
    // cuMemcpy is synchronous but doesn't rely on the default stream,
    // providing better performance than cudaMemcpy and avoiding the
    // cudaStreamSynchronize deadlocks of cudaMemcpyAsync.

    // Ensure CUDA Driver API is initialized (idempotent)
    static CUresult init_result = cuInit(0);
    if (init_result != CUDA_SUCCESS) {
        const char* error_str = nullptr;
        cuGetErrorString(init_result, &error_str);
        return Status::InternalError(
            std::string("CUDA Driver API init failed: ") + error_str);
    }

    CUresult result = cuMemcpy((CUdeviceptr)dst, (CUdeviceptr)src, length);
    if (result != CUDA_SUCCESS) {
        const char* error_str = nullptr;
        cuGetErrorString(result, &error_str);
        return Status::InternalError(
            std::string("cuMemcpy failed: ") + error_str);
    }
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
