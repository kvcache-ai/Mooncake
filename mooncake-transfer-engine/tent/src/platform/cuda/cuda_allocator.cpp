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
#include <cuda.h>
#include <cuda_runtime.h>
#include <glog/logging.h>
#include <mutex>
#include <numa.h>
#include <vector>

namespace mooncake {
namespace tent {
namespace {

// cuInit is process-global and idempotent; still call it once so the driver
// probe is not re-entered on every topology device.
bool ensureCudaDriverInit() {
    static std::once_flag flag;
    static CUresult result = CUDA_ERROR_NOT_INITIALIZED;
    std::call_once(flag, []() { result = cuInit(0); });
    return result == CUDA_SUCCESS;
}

// Driver-API probe: true iff this process already has a primary context on
// `device`. Does not create one. Topology lists every visible GPU for NIC
// affinity; cudaSetDevice on those names would allocate idle-card contexts
// when the process can see more GPUs than this rank uses.
bool cudaPrimaryContextIsActive(int device) {
    if (!ensureCudaDriverInit()) return false;
    CUdevice cu_dev = 0;
    if (cuDeviceGet(&cu_dev, device) != CUDA_SUCCESS) return false;
    unsigned int flags = 0;
    int active = 0;
    if (cuDevicePrimaryCtxGetState(cu_dev, &flags, &active) != CUDA_SUCCESS) {
        return false;
    }
    return active != 0;
}

// Bare cudaGetDevice() can implicitly create GPU 0 when this thread has no
// current context. Only save/restore the caller's device when one exists.
bool cudaHasCurrentContext() {
    if (!ensureCudaDriverInit()) return false;
    CUcontext ctx = nullptr;
    return cuCtxGetCurrent(&ctx) == CUDA_SUCCESS && ctx != nullptr;
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
    // Use cudaMemcpyAsync with a non-blocking stream instead of cudaMemcpy(),
    // as the latter relies on the legacy default stream and can introduce
    // unintended synchronization or even deadlocks in downstream
    // components (e.g. mooncake-pg).
    //
    // cudaMemcpyAsync routes the copy through its stream's device context, so
    // the stream must live on the device owning the device-side buffer.
    // Control-plane RPC worker threads sit on cuda:0 while a registered buffer
    // may live on cuda:R; taking the stream from the buffer's device routes the
    // copy correctly without mutating the calling thread's current device.
    // Host-only copies keep the current device.
    int device_id = getPointerDeviceId(dst);
    if (device_id == CUDAStreamPool::kCurrentDevice) {
        device_id = getPointerDeviceId(src);
    }

    CUDAStreamHandle stream;
    CHECK_STATUS(getStreamFromPool(stream, device_id));
    CHECK_CUDA(
        cudaMemcpyAsync(dst, src, length, cudaMemcpyDefault, stream.get()));
    CHECK_CUDA(cudaStreamSynchronize(stream.get()));
    return Status::OK();
}

Status CudaPlatform::synchronizeDevices(const Topology* topology) {
    const std::vector<int> devices =
        topologyDeviceIndices(topology, Topology::MEM_CUDA);
    if (devices.empty()) return Status::OK();

    int device_count = 0;
    cudaError_t err = cudaGetDeviceCount(&device_count);
    if (err != cudaSuccess || device_count <= 0) {
        if (err != cudaSuccess) {
            LOG(WARNING) << "CudaPlatform::synchronizeDevices "
                            "cudaGetDeviceCount failed: "
                         << cudaGetErrorString(err);
            (void)cudaGetLastError();
        }
        return Status::OK();
    }

    int saved = 0;
    const bool have_saved =
        cudaHasCurrentContext() && cudaGetDevice(&saved) == cudaSuccess;
    if (!have_saved) (void)cudaGetLastError();

    for (int device : devices) {
        if (device >= device_count) continue;
        if (!cudaPrimaryContextIsActive(device)) continue;
        err = cudaSetDevice(device);
        if (err != cudaSuccess) {
            LOG(WARNING) << "CudaPlatform::synchronizeDevices cudaSetDevice("
                         << device << ") failed: " << cudaGetErrorString(err);
            (void)cudaGetLastError();
            continue;
        }
        err = cudaDeviceSynchronize();
        if (err != cudaSuccess) {
            LOG(WARNING)
                << "CudaPlatform::synchronizeDevices cudaDeviceSynchronize "
                   "device "
                << device << " failed: " << cudaGetErrorString(err);
            (void)cudaGetLastError();
        }
    }
    if (have_saved) {
        err = cudaSetDevice(saved);
        if (err != cudaSuccess) {
            LOG(WARNING)
                << "CudaPlatform::synchronizeDevices restore cudaSetDevice("
                << saved << ") failed: " << cudaGetErrorString(err);
            (void)cudaGetLastError();
        }
    }
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
