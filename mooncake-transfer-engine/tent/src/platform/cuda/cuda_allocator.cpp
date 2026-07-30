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
    // Use cudaMemcpyAsync with a non-blocking stream instead of cudaMemcpy(),
    // as the latter relies on the legacy default stream and can introduce
    // unintended synchronization or even deadlocks in downstream
    // components (e.g. mooncake-pg).
    //
    // The stream must live on the CUDA device that owns the device-side buffer.
    // cudaMemcpyAsync routes the copy through its stream's device context, so a
    // stream on the wrong device fails without peer access. Control-plane RPC
    // worker threads default to cuda:0, but a registered buffer may live on
    // cuda:R (R != 0); acquiring the stream on the buffer's device makes the
    // copy route correctly without mutating the calling thread's current
    // device. Fall back to the current device for host-to-host copies.
    int device_id = CUDAStreamPool::kCurrentDevice;
    cudaPointerAttributes attr{};
    if (cudaPointerGetAttributes(&attr, dst) == cudaSuccess &&
        attr.type == cudaMemoryTypeDevice) {
        device_id = attr.device;
    } else if (cudaPointerGetAttributes(&attr, src) == cudaSuccess &&
               attr.type == cudaMemoryTypeDevice) {
        device_id = attr.device;
    }

    CUDAStreamHandle stream;
    CHECK_STATUS(getStreamFromPool(stream, device_id));
    CHECK_CUDA(
        cudaMemcpyAsync(dst, src, length, cudaMemcpyDefault, stream.get()));
    CHECK_CUDA(cudaStreamSynchronize(stream.get()));
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
