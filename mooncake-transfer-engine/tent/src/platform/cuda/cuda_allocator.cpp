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
#include "tent/platform/cuda_utils.h"
#include "tent/common/status.h"

#include <bits/stdint-uintn.h>
#include <cuda.h>
#include <cuda_runtime.h>
#include <glog/logging.h>
#include <limits>
#include <mutex>
#include <numa.h>
#include <vector>
#include <cstring>
#include <unistd.h>

namespace mooncake {
namespace tent {
namespace {

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
    CudaPointerInfo info;
    const CUresult result = getCudaPointerInfo(ptr, info);
    // Only an initialization result of CUDA_ERROR_NO_DEVICE permits fallback.
    if (!(result == CUDA_ERROR_NO_DEVICE &&
          cudaDriverInitResult() == CUDA_ERROR_NO_DEVICE))
        CHECK_CU(result);
    if (info.isCuda()) {
        CHECK_CUDA(cudaFree(ptr));
    } else {
        // CUDA host registration does not establish NUMA allocator ownership.
        if (info.registered_host)
            return Status::InvalidArgument(
                "Cannot NUMA-free registered CUDA host memory");
        numa_free(ptr, size);
    }
    return Status::OK();
}

Status CudaPlatform::allocatePinnedHost(void** pptr, size_t size) {
    // Portable, so copies against any device's memory run as DMA. Like any
    // CUDA runtime call this initializes the calling thread's current device
    // if it has no context yet.
    CHECK_CUDA(cudaHostAlloc(pptr, size, cudaHostAllocPortable));
    return Status::OK();
}

Status CudaPlatform::freePinnedHost(void* ptr, size_t size) {
    (void)size;
    CHECK_CUDA(cudaFreeHost(ptr));
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
    int dst_device, src_device;
    CHECK_STATUS(getPointerDeviceId(dst, dst_device));
    CHECK_STATUS(getPointerDeviceId(src, src_device));
    int device_id =
        dst_device == CUDAStreamPool::kCurrentDevice ? src_device : dst_device;
    if (device_id == CUDAStreamPool::kCurrentDevice) {
        // Host-to-host: plain memcpy. The stream pool would create a primary
        // context on the thread's default device (GPU 0).
        ::memcpy(dst, src, length);
        return Status::OK();
    }

    // cudaMemcpyAsync() validates the stream against the current device's
    // context, so the copy has to run with the buffer's device pinned. A
    // device ordinal alone cannot restore the caller afterwards: it cannot
    // tell a custom non-primary CUcontext apart from the primary one, and a
    // thread that had no current context must not be left holding the one
    // this copy creates. Save the exact identity through the driver API.
    CUcontext caller_ctx = nullptr;
    if (ensureCudaDriverInit() &&
        cuCtxGetCurrent(&caller_ctx) != CUDA_SUCCESS) {
        return Status::InternalError(std::string("cuCtxGetCurrent failed") +
                                     LOC_MARK);
    }
    // Bare cudaGetDevice() can implicitly create GPU 0 when this thread has
    // no current context; only query the runtime binding when one exists.
    int caller_device = -1;
    if (caller_ctx != nullptr && cudaGetDevice(&caller_device) != cudaSuccess) {
        caller_device = -1;
        (void)cudaGetLastError();
    }

    Status status;
    cudaError_t err = cudaSetDevice(device_id);
    if (err != cudaSuccess) {
        status = Status::InternalError(std::string("cudaSetDevice: ") +
                                       cudaGetErrorString(err) + LOC_MARK);
    } else {
        // The handle returns the stream to the pool at the end of this scope,
        // before the caller's state is restored below.
        CUDAStreamHandle stream;
        status = getStreamFromPool(stream, device_id);
        if (status.ok()) {
            err = cudaMemcpyAsync(dst, src, length, cudaMemcpyDefault,
                                  stream.get());
            if (err != cudaSuccess) {
                status =
                    Status::InternalError(std::string("cudaMemcpyAsync: ") +
                                          cudaGetErrorString(err) + LOC_MARK);
            }
        }
        if (status.ok()) {
            err = cudaStreamSynchronize(stream.get());
            if (err != cudaSuccess) {
                status = Status::InternalError(
                    std::string("cudaStreamSynchronize: ") +
                    cudaGetErrorString(err) + LOC_MARK);
            }
        }
    }

    // Restore on every exit path, success or failure. cudaSetDevice() first
    // brings the runtime binding back (and may swap in that device's primary
    // context), then cuCtxSetCurrent() restores the exact context identity: a
    // custom CUcontext, or none at all for a caller that started bare.
    if (caller_device != -1 && caller_device != device_id) {
        err = cudaSetDevice(caller_device);
        if (status.ok() && err != cudaSuccess) {
            status =
                Status::InternalError(std::string("cudaSetDevice(restore): ") +
                                      cudaGetErrorString(err) + LOC_MARK);
        }
    }
    const CUresult restore_rc = cuCtxSetCurrent(caller_ctx);
    if (status.ok() && restore_rc != CUDA_SUCCESS) {
        status = Status::InternalError(
            std::string("cuCtxSetCurrent failed to restore the caller "
                        "context") +
            LOC_MARK);
    }
    if (!status.ok()) {
        LOG(ERROR) << "CudaPlatform::copy failed: " << status.message();
    }
    return status;
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

Status CudaPlatform::exportDmabuf(void* addr, size_t length,
                                  DmabufExport& out) {
    out = DmabufExport{};
    CudaPointerInfo info;
    const CUresult ptr_rc = getCudaPointerInfo(addr, info);
    if (ptr_rc != CUDA_SUCCESS || info.kind != CudaMemoryKind::Device) {
        return Status::InvalidArgument("Not CUDA device memory" LOC_MARK);
    }
    if (!length) {
        return Status::InvalidArgument("Empty CUDA DMA-BUF range" LOC_MARK);
    }
    if (useCudaPeerMemoryModule()) {
        return Status::NotImplemented(
            "CUDA peer-memory module active; using ibv_reg_mr" LOC_MARK);
    }

    CUcontext caller_ctx = nullptr;
    if (!ensureCudaDriverInit() ||
        cuCtxGetCurrent(&caller_ctx) != CUDA_SUCCESS) {
        return Status::InternalError("cuCtxGetCurrent failed" LOC_MARK);
    }
    CUdevice cu_dev = 0;
    CHECK_CU(cuDeviceGet(&cu_dev, info.device));
    int dma_buf_supported = 0;
    if (cuDeviceGetAttribute(&dma_buf_supported,
                             CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED,
                             cu_dev) != CUDA_SUCCESS ||
        !dma_buf_supported) {
        return Status::NotImplemented(
            "CUDA device does not support DMA-BUF export" LOC_MARK);
    }
    CUcontext primary_ctx = nullptr;
    CHECK_CU(cuDevicePrimaryCtxRetain(&primary_ctx, cu_dev));

    auto restore_ctx = [&]() -> CUresult {
        const CUresult set_rc = cuCtxSetCurrent(caller_ctx);
        (void)cuDevicePrimaryCtxRelease(cu_dev);
        return set_rc;
    };
    if (cuCtxSetCurrent(primary_ctx) != CUDA_SUCCESS) {
        (void)restore_ctx();
        return Status::InternalError("cuCtxSetCurrent failed" LOC_MARK);
    }

    CUdeviceptr alloc_base = 0;
    size_t alloc_size = 0;
    CUresult rc =
        cuMemGetAddressRange(&alloc_base, &alloc_size, (CUdeviceptr)addr);
    if (rc != CUDA_SUCCESS) {
        const char* err_str = nullptr;
        cuGetErrorString(rc, &err_str);
        (void)restore_ctx();
        return Status::InternalError(
            std::string("cuMemGetAddressRange failed: ") +
            (err_str ? err_str : "unknown") + LOC_MARK);
    }

    // cuMemGetHandleForAddressRange() requires page-aligned base and size.
    // When registering a sub-range (or a VMM growth chunk smaller than
    // `length`), export only [aligned, addr + length) rather than the whole
    // allocation so concurrent imports do not over-map BAR1.
    const long page_size = sysconf(_SC_PAGESIZE);
    if (page_size <= 0) {
        (void)restore_ctx();
        return Status::InternalError("Unable to query page size" LOC_MARK);
    }
    const size_t page = static_cast<size_t>(page_size);
    CUdeviceptr export_base = alloc_base;
    uint64_t export_offset = (uintptr_t)addr - (uintptr_t)alloc_base;
    size_t raw_size = alloc_size;
    if (export_offset != 0 || length != alloc_size) {
        uintptr_t aligned = (uintptr_t)addr & ~(page - 1);
        if (aligned < (uintptr_t)alloc_base) {
            aligned = (uintptr_t)alloc_base;
        }
        export_base = (CUdeviceptr)aligned;
        export_offset = (uintptr_t)addr - aligned;
        if (export_offset > std::numeric_limits<size_t>::max() - length) {
            (void)restore_ctx();
            return Status::InvalidArgument(
                "CUDA DMA-BUF size overflow" LOC_MARK);
        }
        raw_size = static_cast<size_t>(export_offset) + length;
    }
    if (raw_size > std::numeric_limits<size_t>::max() - (page - 1)) {
        (void)restore_ctx();
        return Status::InvalidArgument("CUDA DMA-BUF size overflow" LOC_MARK);
    }
    const size_t export_size = (raw_size + page - 1) & ~(page - 1);

    int fd = -1;
    rc = cuMemGetHandleForAddressRange(&fd, export_base, export_size,
                                       CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD, 0);
    const CUresult restore_rc = restore_ctx();
    if (rc != CUDA_SUCCESS) {
        if (fd >= 0) {
            ::close(fd);
        }
        const char* err_str = nullptr;
        cuGetErrorString(rc, &err_str);
        return Status::InternalError(
            std::string("cuMemGetHandleForAddressRange failed: ") +
            (err_str ? err_str : "unknown") + LOC_MARK);
    }
    if (restore_rc != CUDA_SUCCESS) {
        if (fd >= 0) {
            ::close(fd);
        }
        return Status::InternalError(
            "cuCtxSetCurrent failed to restore the caller context" LOC_MARK);
    }

    out.fd = fd;
    out.offset = export_offset;
    out.close_fd = true;
    return Status::OK();
}
}  // namespace tent
}  // namespace mooncake
