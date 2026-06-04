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

#include <bits/stdint-uintn.h>
#include <numa.h>
#include <glog/logging.h>
#include <cstring>

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
    int dev = getCudaDeviceForPtr(ptr);
    if (dev >= 0) {
        CHECK_CUDA(cudaFree(ptr));
    } else {
        numa_free(ptr, size);
    }
    return Status::OK();
}

Status CudaPlatform::copy(void* dst, void* src, size_t length) {
    // Determine which GPU (if any) owns src or dst before touching any CUDA
    // runtime API, to avoid implicitly creating a primary context on GPU 0.
    int dev = getCudaDeviceForPtr(src);
    if (dev < 0) dev = getCudaDeviceForPtr(dst);
    if (dev < 0) {
        // Both pointers are host memory; plain memcpy is sufficient.
        ::memcpy(dst, src, length);
        return Status::OK();
    }
    // Set the device before any CUDA runtime call so the calling thread
    // doesn't implicitly initialize GPU 0 as its current device.
    CHECK_CUDA(cudaSetDevice(dev));
    // Use cudaMemcpyAsync with a non-blocking stream instead of cudaMemcpy(),
    // as the latter relies on the legacy default stream and can introduce
    // unintended synchronization or even deadlocks in downstream
    // components (e.g. mooncake-pg).
    CUDAStreamHandle stream;
    CHECK_STATUS(getStreamFromPool(stream, dev));
    CHECK_CUDA(
        cudaMemcpyAsync(dst, src, length, cudaMemcpyDefault, stream.get()));
    CHECK_CUDA(cudaStreamSynchronize(stream.get()));
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
