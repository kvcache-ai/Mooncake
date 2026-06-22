#pragma once

#include "cuda_alike.h"

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include <acl/acl_rt.h>
#endif

#include <cstddef>
#include <cstring>
#include <glog/logging.h>

namespace mooncake {
namespace gpu_staging {

// Detect whether ptr resides in accelerator device memory.
// If so, writes the device ID to *out_device_id for subsequent SetDevice.
inline bool IsDevicePointer(const void* ptr, int* out_device_id) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
    cudaPointerAttributes attr{};
    if (cudaPointerGetAttributes(&attr, ptr) == cudaSuccess &&
        attr.type == cudaMemoryTypeDevice) {
        if (out_device_id) *out_device_id = attr.device;
        return true;
    }
#elif defined(USE_HIP)
    hipPointerAttribute_t attr{};
    if (hipPointerGetAttributes(&attr, ptr) == hipSuccess &&
        attr.type == hipMemoryTypeDevice) {
        if (out_device_id) *out_device_id = attr.device;
        return true;
    }
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    aclrtPtrAttributes attr{};
    if (aclrtPointerGetAttributes(const_cast<void*>(ptr), &attr) ==
            ACL_SUCCESS &&
        attr.location.type == ACL_MEM_LOCATION_TYPE_DEVICE) {
        if (out_device_id) *out_device_id = static_cast<int>(attr.location.id);
        return true;
    }
#endif
    (void)ptr;
    (void)out_device_id;
    return false;
}

// Copy device memory to host. Caller must have called SetDevice first.
inline bool CopyDeviceToHost(void* dst, const void* src, size_t size) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
    return cudaMemcpy(dst, src, size, cudaMemcpyDeviceToHost) == cudaSuccess;
#elif defined(USE_HIP)
    return hipMemcpy(dst, src, size, hipMemcpyDeviceToHost) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    return aclrtMemcpy(dst, size, src, size, ACL_MEMCPY_DEVICE_TO_HOST) ==
           ACL_SUCCESS;
#else
    (void)dst;
    (void)src;
    (void)size;
    return false;
#endif
}

// Auto-direction copy: runtime determines the transfer direction from pointer
// attributes (cudaMemcpyDefault). Works for H2H, H2D, D2H, and D2D.
// Caller must have called SetDevice first when device memory is involved.
inline bool CopyAuto(void* dst, const void* src, size_t size) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
    return cudaMemcpy(dst, src, size, cudaMemcpyDefault) == cudaSuccess;
#elif defined(USE_HIP)
    return hipMemcpy(dst, src, size, hipMemcpyDefault) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    aclrtPtrAttributes src_attr{}, dst_attr{};
    bool src_dev = aclrtPointerGetAttributes(const_cast<void*>(src),
                                             &src_attr) == ACL_SUCCESS &&
                   src_attr.location.type == ACL_MEM_LOCATION_TYPE_DEVICE;
    bool dst_dev = aclrtPointerGetAttributes(dst, &dst_attr) == ACL_SUCCESS &&
                   dst_attr.location.type == ACL_MEM_LOCATION_TYPE_DEVICE;
    aclrtMemcpyKind kind = ACL_MEMCPY_HOST_TO_HOST;
    if (src_dev && dst_dev)
        kind = ACL_MEMCPY_DEVICE_TO_DEVICE;
    else if (src_dev)
        kind = ACL_MEMCPY_DEVICE_TO_HOST;
    else if (dst_dev)
        kind = ACL_MEMCPY_HOST_TO_DEVICE;
    return aclrtMemcpy(dst, size, src, size, kind) == ACL_SUCCESS;
#else
    (void)dst;
    (void)src;
    (void)size;
    return false;
#endif
}

// Bind the calling thread to the given device context.
inline void SetDevice(int device_id) {
    if (device_id < 0) return;
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
    cudaSetDevice(device_id);
#elif defined(USE_HIP)
    hipSetDevice(device_id);
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    aclrtSetDevice(device_id);
#endif
}

// Copy host memory to device. Caller must have called SetDevice first.
inline bool CopyHostToDevice(void* dst, const void* src, size_t size) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
    return cudaMemcpy(dst, src, size, cudaMemcpyHostToDevice) == cudaSuccess;
#elif defined(USE_HIP)
    return hipMemcpy(dst, src, size, hipMemcpyHostToDevice) == hipSuccess;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    return aclrtMemcpy(dst, size, src, size, ACL_MEMCPY_HOST_TO_DEVICE) ==
           ACL_SUCCESS;
#else
    (void)dst;
    (void)src;
    (void)size;
    return false;
#endif
}

// Detect whether ptr resides in host (CPU) memory.
// Used together with IsDevicePointer for safe pointer-type dispatching:
//   if IsDevicePointer  -> CopyHostToDevice / CopyDeviceToHost
//   else if IsHostPointer -> memcpy
//   else                  -> reject (unknown type, e.g. non-standard allocator)
//
// Pageable host memory (not tracked by CUDA runtime) is treated as host.
inline bool IsHostPointer(const void* ptr) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
    cudaPointerAttributes attr{};
    if (cudaPointerGetAttributes(&attr, ptr) != cudaSuccess) {
        // Query failed: pageable host memory not tracked by the runtime.
        cudaGetLastError();  // clear sticky error
        return true;
    }
    return attr.type != cudaMemoryTypeDevice;
#elif defined(USE_HIP)
    hipPointerAttribute_t attr{};
    if (hipPointerGetAttributes(&attr, ptr) != hipSuccess) {
        hipGetLastError();  // clear sticky error
        return true;
    }
    return attr.type != hipMemoryTypeDevice;
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
    aclrtPtrAttributes attr{};
    if (aclrtPointerGetAttributes(const_cast<void*>(ptr), &attr) !=
        ACL_SUCCESS) {
        // Query failed: likely pageable host memory not tracked by the runtime.
        return true;
    }
    return attr.location.type != ACL_MEM_LOCATION_TYPE_DEVICE;
#else
    (void)ptr;
    return true;  // CPU-only build: all pointers are host
#endif
}
/// GPU-safe memcpy: auto-detects pointer types and dispatches to CopyAuto
/// for GPU pointers, std::memcpy for host pointers.
inline bool MemcpySafe(void* dst, const void* src, size_t size) {
    int src_dev = -1, dst_dev = -1;
    bool src_gpu = IsDevicePointer(src, &src_dev);
    bool dst_gpu = IsDevicePointer(dst, &dst_dev);
    if (!src_gpu && !dst_gpu) {
        std::memcpy(dst, src, size);
        return true;
    }
    int dev = src_gpu ? src_dev : dst_dev;
    SetDevice(dev);
    return CopyAuto(dst, src, size);
}

// ── Async copy support ─────────────────────────────────────────────────
// Used by ensureRegisteredForRDMA to batch multiple GPU→host copies on a
// single stream, synchronise once, amortising per-call overhead.
//
// Design choices (borrowed from community PRs):
//   - CUDA/HYGON/COREX: use Driver API (cuMemcpyAsync) instead of Runtime
//     API (cudaMemcpyAsync) to avoid deadlocking with PyTorch's global
//     CUDA Runtime mutex.  See: github.com/kvcache-ai/Mooncake/pull/2094
//   - CUDA 12.8+: cudaMemcpyBatchAsync batches all copies into one driver
//     call for lower per-transfer overhead.
//     See: github.com/kvcache-ai/Mooncake/pull/1890
//   - MUSA/MACA: use Runtime API (no known deadlock issue with these runtimes).

#if defined(USE_CUDA) || defined(USE_HYGON) || defined(USE_COREX)
// ── CUDA Driver API path ───────────────────────────────────────────────

/// One-shot Driver API initialization.
inline bool InitDriverAPI() {
    static CUresult init_result = cuInit(0);
    return init_result == CUDA_SUCCESS;
}

/// Thread-local stream type for CUDA Driver API.
using AsyncCopyStream = CUstream;

/// Get (or lazily create) a thread-local CUDA stream via Driver API.
inline AsyncCopyStream GetCopyStream() {
    thread_local CUstream stream = nullptr;
    if (!stream) {
        if (!InitDriverAPI()) return nullptr;
        auto err = cuStreamCreate(&stream, CU_STREAM_NON_BLOCKING);
        if (err != CUDA_SUCCESS) {
            const char* err_str = nullptr;
            cuGetErrorString(err, &err_str);
            LOG(WARNING) << "cuStreamCreate failed: "
                         << (err_str ? err_str : "unknown")
                         << "; falling back to sync";
            stream = nullptr;
        }
    }
    return stream;
}

/// Enqueue a single async copy via Driver API (auto-detects direction via UVA).
inline bool CopyAutoAsync(void* dst, const void* src, size_t size,
                           AsyncCopyStream stream) {
    return cuMemcpyAsync(reinterpret_cast<CUdeviceptr>(dst),
                         reinterpret_cast<CUdeviceptr>(src),
                         size, stream) == CUDA_SUCCESS;
}

/// Batch-enqueue multiple async copies.  Uses cudaMemcpyBatchAsync on
/// CUDA 12.8+ for lower per-call overhead; falls back to looped
/// cuMemcpyAsync on older toolkits.
/// Returns false on any copy failure.
inline bool CopyBatchAsync(void* const* dsts, const void* const* srcs,
                            const size_t* sizes, size_t count,
                            AsyncCopyStream stream) {
#if CUDART_VERSION >= 13000
    // CUDA 13+: const void** signature, no fail_idx.
    cudaMemcpyAttributes attr{};
    attr.srcAccessOrder = cudaMemcpySrcAccessOrderStream;
    size_t attrs_idx = 0;
    return cudaMemcpyBatchAsync(
               const_cast<const void**>(reinterpret_cast<void**>(
                   const_cast<void**>(dsts))),
               const_cast<const void**>(reinterpret_cast<void**>(
                   const_cast<void* const*>(srcs))),
               sizes, count, &attr, &attrs_idx, 1,
               reinterpret_cast<cudaStream_t>(stream)) == cudaSuccess;
#elif CUDART_VERSION >= 12080
    // CUDA 12.8+: void** signature with fail_idx.
    cudaMemcpyAttributes attr{};
    attr.srcAccessOrder = cudaMemcpySrcAccessOrderStream;
    size_t attrs_idx = 0;
    size_t fail_idx = count;
    auto err = cudaMemcpyBatchAsync(
        const_cast<void**>(reinterpret_cast<void* const*>(dsts)),
        const_cast<void**>(reinterpret_cast<void* const*>(srcs)),
        sizes, count, &attr, &attrs_idx, 1, &fail_idx,
        reinterpret_cast<cudaStream_t>(stream));
    if (err != cudaSuccess) {
        LOG(ERROR) << "cudaMemcpyBatchAsync failed at index " << fail_idx
                   << ": " << cudaGetErrorString(err);
    }
    return err == cudaSuccess;
#else
    // Pre-12.8: loop cuMemcpyAsync (Driver API, deadlock-safe).
    for (size_t i = 0; i < count; ++i) {
        auto err = cuMemcpyAsync(reinterpret_cast<CUdeviceptr>(dsts[i]),
                                 reinterpret_cast<CUdeviceptr>(srcs[i]),
                                 sizes[i], stream);
        if (err != CUDA_SUCCESS) {
            const char* err_str = nullptr;
            cuGetErrorString(err, &err_str);
            LOG(ERROR) << "cuMemcpyAsync failed at index " << i << ": "
                       << (err_str ? err_str : "unknown");
            return false;
        }
    }
    return true;
#endif
}

/// Synchronize the copy stream (Driver API).
inline bool SyncCopyStream(AsyncCopyStream stream) {
    return cuStreamSynchronize(stream) == CUDA_SUCCESS;
}

#elif defined(USE_MUSA) || defined(USE_MACA)
// ── MUSA/MACA Runtime API path ─────────────────────────────────────────

using AsyncCopyStream = cudaStream_t;

inline AsyncCopyStream GetCopyStream() {
    thread_local cudaStream_t stream = nullptr;
    if (!stream) {
        auto err = cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking);
        if (err != cudaSuccess) {
            LOG(WARNING) << "cudaStreamCreate failed: "
                         << cudaGetErrorString(err);
            stream = nullptr;
        }
    }
    return stream;
}

inline bool CopyAutoAsync(void* dst, const void* src, size_t size,
                           AsyncCopyStream stream) {
    return cudaMemcpyAsync(dst, src, size, cudaMemcpyDefault, stream) ==
           cudaSuccess;
}

inline bool CopyBatchAsync(void* const* dsts, const void* const* srcs,
                            const size_t* sizes, size_t count,
                            AsyncCopyStream stream) {
    for (size_t i = 0; i < count; ++i) {
        if (cudaMemcpyAsync(const_cast<void*>(
                                static_cast<const void*>(dsts[i])),
                            srcs[i], sizes[i], cudaMemcpyDefault,
                            stream) != cudaSuccess) {
            return false;
        }
    }
    return true;
}

inline bool SyncCopyStream(AsyncCopyStream stream) {
    return cudaStreamSynchronize(stream) == cudaSuccess;
}

#endif  // platform selection

}  // namespace gpu_staging
}  // namespace mooncake
