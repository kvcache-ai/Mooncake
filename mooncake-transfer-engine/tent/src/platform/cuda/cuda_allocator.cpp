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
#include <cuda_runtime.h>
#include <numa.h>
#include <glog/logging.h>

namespace mooncake {
namespace tent {
namespace {

// Return the owning CUDA device for a device pointer. Host and unregistered
// pointers deliberately return -1 so host-only copies avoid CUDA entirely.
int getCudaDeviceId(const void* ptr) {
    cudaPointerAttributes attributes{};
    auto status = cudaPointerGetAttributes(&attributes, ptr);
    if (status != cudaSuccess) {
        // cudaPointerGetAttributes may report an unregistered host pointer as
        // an error on older runtimes. Clear the sticky error before falling
        // back to a CPU copy.
        cudaGetLastError();
        return -1;
    }
    return attributes.type == cudaMemoryTypeDevice ? attributes.device : -1;
}

}  // namespace

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
    if (length == 0) return Status::OK();

    const int dst_device = getCudaDeviceId(dst);
    const int src_device = getCudaDeviceId(src);
    const int copy_device = dst_device >= 0 ? dst_device : src_device;
    if (copy_device >= 0) {
        // Transport worker threads start on CUDA's default device (GPU 0).
        // Select the device that owns the transfer buffer before acquiring a
        // stream or issuing the copy, otherwise the runtime implicitly creates
        // a GPU 0 primary context even when the payload belongs elsewhere.
        CHECK_CUDA(cudaSetDevice(copy_device));
        // Keep the selected device current on this worker. Restoring its
        // implicit default would initialize the GPU 0 context we are avoiding.
    }

    // Use cudaMemcpyAsync with a non-blocking stream instead of cudaMemcpy(),
    // as the latter relies on the legacy default stream and can introduce
    // unintended synchronization or even deadlocks in downstream
    // components (e.g. mooncake-pg).
    CUDAStreamHandle stream;
    if (copy_device >= 0) {
        CHECK_STATUS(getStreamFromPool(stream, copy_device));
    } else {
        CHECK_STATUS(getStreamFromPool(stream));
    }
    CHECK_CUDA(
        cudaMemcpyAsync(dst, src, length, cudaMemcpyDefault, stream.get()));
    CHECK_CUDA(cudaStreamSynchronize(stream.get()));
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
