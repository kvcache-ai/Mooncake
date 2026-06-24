#pragma once

#include "cuda_alike.h"

#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT) || defined(USE_UBSHMEM)
#include <acl/acl_rt.h>
#endif

#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <mutex>
#include <string>
#include <unordered_map>
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
    if (size == 0) return true;
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

inline bool PinMemoryEnabled() {
    const char* pin_env = std::getenv("MC_STORE_PIN_MEMORY");
    return !(pin_env &&
             (std::string(pin_env) == "0" || std::string(pin_env) == "false"));
}

inline size_t PinMemoryMaxBytes() {
    const char* max_env = std::getenv("MC_STORE_PIN_MEMORY_MAX_BYTES");
    if (!max_env || max_env[0] == '\0') return 0;
    char* end = nullptr;
    unsigned long long value = std::strtoull(max_env, &end, 10);
    if (end == max_env) return 0;
    if (value > std::numeric_limits<size_t>::max()) {
        return std::numeric_limits<size_t>::max();
    }
    return static_cast<size_t>(value);
}

inline std::mutex& PinnedHostMemoryMutex() {
    static std::mutex mutex;
    return mutex;
}

inline size_t& PinnedHostMemoryBytes() {
    static size_t bytes = 0;
    return bytes;
}

inline std::unordered_map<void*, size_t>& PinnedHostMemoryRegions() {
    static std::unordered_map<void*, size_t> regions;
    return regions;
}

inline bool TryPinHostMemory(void* ptr, size_t size, const char* name) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
    if (!PinMemoryEnabled() || ptr == nullptr || size == 0) return false;

    {
        std::lock_guard<std::mutex> lock(PinnedHostMemoryMutex());
        auto& regions = PinnedHostMemoryRegions();
        if (regions.find(ptr) != regions.end()) return true;

        size_t max_bytes = PinMemoryMaxBytes();
        size_t current = PinnedHostMemoryBytes();
        if (max_bytes > 0 &&
            (current > max_bytes || size > max_bytes - current)) {
            LOG(WARNING) << "Skip cudaHostRegister for " << name << " size=" << size
                         << " because MC_STORE_PIN_MEMORY_MAX_BYTES=" << max_bytes
                         << " current_pinned=" << current;
            return false;
        }
        PinnedHostMemoryBytes() += size;
    }

    auto cuda_ret = cudaHostRegister(ptr, size, cudaHostRegisterDefault);
    if (cuda_ret != cudaSuccess) {
        std::lock_guard<std::mutex> lock(PinnedHostMemoryMutex());
        PinnedHostMemoryBytes() -= size;
        LOG(WARNING) << "cudaHostRegister failed for " << name << " size=" << size
                     << ": " << cudaGetErrorString(cuda_ret)
                     << "; GPU copies will use pageable fallback";
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(PinnedHostMemoryMutex());
        PinnedHostMemoryRegions()[ptr] = size;
    }
    LOG(INFO) << "cudaHostRegister OK for " << name << ", size=" << size;
    return true;
#else
    (void)ptr;
    (void)size;
    (void)name;
    return false;
#endif
}

inline void UnpinHostMemory(void* ptr, const char* name) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA) || \
    defined(USE_HYGON) || defined(USE_COREX)
    if (ptr == nullptr) return;

    size_t size = 0;
    {
        std::lock_guard<std::mutex> lock(PinnedHostMemoryMutex());
        auto& regions = PinnedHostMemoryRegions();
        auto it = regions.find(ptr);
        if (it == regions.end()) return;
        size = it->second;
        regions.erase(it);
        PinnedHostMemoryBytes() -= size;
    }

    auto cuda_ret = cudaHostUnregister(ptr);
    if (cuda_ret != cudaSuccess) {
        LOG(WARNING) << "cudaHostUnregister failed for " << name << " size="
                     << size << ": " << cudaGetErrorString(cuda_ret);
    }
#else
    (void)ptr;
    (void)name;
#endif
}

}  // namespace gpu_staging
}  // namespace mooncake
