#include "cuda_alike.h"
#include <sys/types.h>

#include <iostream>

// CUDA Fabric Memory was introduced after the CUDA 12.0 driver headers.  The
// allocator is also built for ordinary CUDA builds, so keep Fabric-only
// symbols out of older-toolkit compilations and use cudaMalloc there.
#if defined(USE_CUDA)
#if defined(CUDA_VERSION) && CUDA_VERSION >= 12040
#define MOONCAKE_CUDA_FABRIC_MEM_SUPPORTED 1
#else
#define MOONCAKE_CUDA_FABRIC_MEM_SUPPORTED 0
#endif
#elif defined(USE_MUSA) || defined(USE_HIP) || defined(USE_MACA) || \
    defined(USE_SUPA)
#define MOONCAKE_CUDA_FABRIC_MEM_SUPPORTED 1
#else
#define MOONCAKE_CUDA_FABRIC_MEM_SUPPORTED 0
#endif

// ref: http://github.com/NVIDIA/nccl/blob/v2.28.9-1/src/allocator.cc#L53-L68
static CUresult cuMemCreateTryFabric(CUmemGenericAllocationHandle *handle,
                                     size_t size, CUmemAllocationProp *prop,
                                     unsigned long long flags) {
    CUresult err = cuMemCreate(handle, size, prop, flags);
#if MOONCAKE_CUDA_FABRIC_MEM_SUPPORTED
    if ((prop->requestedHandleTypes & CU_MEM_HANDLE_TYPE_FABRIC) &&
        (err == CUDA_ERROR_NOT_PERMITTED || err == CUDA_ERROR_NOT_SUPPORTED)) {
        prop->requestedHandleTypes = static_cast<CUmemAllocationHandleType>(
            prop->requestedHandleTypes & ~CU_MEM_HANDLE_TYPE_FABRIC);
        err = cuMemCreate(handle, size, prop, flags);
    }
#endif
    return err;
}

enum class MemoryBackendType { use_cudamalloc, use_cumemcreate, unknown };

namespace {

MemoryBackendType ProbeAllocatorBackend(int device_id) {
#if !MOONCAKE_CUDA_FABRIC_MEM_SUPPORTED
    (void)device_id;
    return MemoryBackendType::use_cudamalloc;
#else
    CUdevice dev;
    CUresult res = cuDeviceGet(&dev, device_id);
    if (res != CUDA_SUCCESS) {
        return MemoryBackendType::unknown;
    }

    int fabric_attr = 0;
    res = cuDeviceGetAttribute(
        &fabric_attr, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED, dev);
    if (res != CUDA_SUCCESS || !fabric_attr) {
        return MemoryBackendType::use_cudamalloc;
    }

    CUmemAllocationProp prop = {};
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = dev;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;

    CUmemGenericAllocationHandle handle;
    size_t size = 4096;

    res = cuMemCreate(&handle, size, &prop, 0);
    if (res == CUDA_SUCCESS) {
        cuMemRelease(handle);
        return MemoryBackendType::use_cumemcreate;
    }
    return MemoryBackendType::use_cudamalloc;
#endif
}

void *AllocateFabricMemory(ssize_t size, int device, cudaStream_t stream) {
#if !MOONCAKE_CUDA_FABRIC_MEM_SUPPORTED
    (void)stream;
    int previous_device = -1;
    auto result = cudaGetDevice(&previous_device);
    if (result != cudaSuccess) {
        std::cerr << "cudaGetDevice fallback failed: " << result << "\n";
        return nullptr;
    }
    const bool switched = device >= 0 && previous_device != device;
    if (switched) {
        result = cudaSetDevice(device);
        if (result != cudaSuccess) {
            std::cerr << "cudaSetDevice fallback failed: " << result << "\n";
            return nullptr;
        }
    }
    void *ptr = nullptr;
    result = cudaMalloc(&ptr, static_cast<size_t>(size));
    if (switched) {
        auto restore_result = cudaSetDevice(previous_device);
        if (restore_result != cudaSuccess) {
            std::cerr << "cudaSetDevice restore failed: " << restore_result
                      << "\n";
            if (result == cudaSuccess) cudaFree(ptr);
            return nullptr;
        }
    }
    if (result != cudaSuccess) {
        std::cerr << "cudaMalloc fallback failed: " << result << "\n";
        return nullptr;
    }
    return ptr;
#else
    (void)stream;
    size_t granularity = 0;
    CUdevice currentDev;
    CUmemAllocationProp prop = {};
    CUmemGenericAllocationHandle handle;
    void *ptr = nullptr;
    int flag = 0;
    CUresult result = cuDeviceGet(&currentDev, device);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuDeviceGet failed: " << result << "\n";
        return nullptr;
    }
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = currentDev;

    int fabric_supported = 0;
    result = cuDeviceGetAttribute(
        &fabric_supported, CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED,
        currentDev);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuDeviceGetAttribute (fabric) failed: " << result << "\n";
        return nullptr;
    }
    if (fabric_supported) {
        prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;
    }

    result = cuDeviceGetAttribute(
        &flag, CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED,
        currentDev);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuDeviceGetAttribute failed: " << result;
        return nullptr;
    }
    if (flag) prop.allocFlags.gpuDirectRDMACapable = 1;
    result = cuMemGetAllocationGranularity(&granularity, &prop,
                                           CU_MEM_ALLOC_GRANULARITY_MINIMUM);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuMemGetAllocationGranularity failed: " << result;
        return nullptr;
    }
    size = (size + granularity - 1) & ~(granularity - 1);
    if (size == 0) size = granularity;
    result = cuMemCreateTryFabric(&handle, size, &prop, 0);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuMemCreateTryFabric failed: " << result;
        return nullptr;
    }
    result = cuMemAddressReserve((CUdeviceptr *)&ptr, size, granularity, 0, 0);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuMemAddressReserve failed: " << result;
        cuMemRelease(handle);
        return nullptr;
    }
    result = cuMemMap((CUdeviceptr)ptr, size, 0, handle, 0);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuMemMap failed: " << result;
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return nullptr;
    }
    int device_count;
    cudaGetDeviceCount(&device_count);
    CUmemAccessDesc accessDesc[device_count];
    for (int idx = 0; idx < device_count; ++idx) {
        accessDesc[idx].location.type = CU_MEM_LOCATION_TYPE_DEVICE;
        accessDesc[idx].location.id = idx;
        accessDesc[idx].flags = CU_MEM_ACCESS_FLAGS_PROT_READWRITE;
    }
    result = cuMemSetAccess((CUdeviceptr)ptr, size, accessDesc, device_count);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuMemSetAccess failed: " << result;
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
        cuMemRelease(handle);
        return nullptr;
    }
    return ptr;
#endif
}

void FreeFabricMemory(void *ptr, ssize_t ssize, int device,
                      cudaStream_t stream) {
#if !MOONCAKE_CUDA_FABRIC_MEM_SUPPORTED
    (void)ssize;
    (void)stream;
    if (ptr == nullptr) return;
    int previous_device = -1;
    auto result = cudaGetDevice(&previous_device);
    if (result != cudaSuccess) {
        std::cerr << "cudaGetDevice free fallback failed: " << result << "\n";
        return;
    }
    const bool switched = device >= 0 && previous_device != device;
    if (switched) {
        result = cudaSetDevice(device);
        if (result != cudaSuccess) {
            std::cerr << "cudaSetDevice free fallback failed: " << result
                      << "\n";
            return;
        }
    }
    result = cudaFree(ptr);
    if (switched) {
        auto restore_result = cudaSetDevice(previous_device);
        if (restore_result != cudaSuccess) {
            std::cerr << "cudaSetDevice free restore failed: "
                      << restore_result << "\n";
        }
    }
    if (result != cudaSuccess) {
        std::cerr << "cudaFree fallback failed: " << result << "\n";
    }
    return;
#else
    (void)ssize;
    (void)device;
    (void)stream;
    CUmemGenericAllocationHandle handle;
    size_t size = 0;
    if (!ptr) return;
    auto result = cuMemRetainAllocationHandle(&handle, ptr);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuMemRetainAllocationHandle failed: " << result << "\n";
        return;
    }
    result = cuMemGetAddressRange(NULL, &size, (CUdeviceptr)ptr);
    if (result == CUDA_SUCCESS) {
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
    }
    cuMemRelease(handle);
#endif
}

}  // namespace

extern "C" {

MemoryBackendType mc_probe_fabric_support(int device_id) {
    return ProbeAllocatorBackend(device_id);
}

int mc_allocator_probe(int device_id) {
    return static_cast<int>(ProbeAllocatorBackend(device_id));
}

void *mc_allocator_malloc(ssize_t size, int device, cudaStream_t stream) {
    return AllocateFabricMemory(size, device, stream);
}

void *mc_nvlink_malloc(ssize_t size, int device, cudaStream_t stream) {
    return mc_allocator_malloc(size, device, stream);
}

void mc_allocator_free(void *ptr, ssize_t ssize, int device,
                       cudaStream_t stream) {
    FreeFabricMemory(ptr, ssize, device, stream);
}

void mc_nvlink_free(void *ptr, ssize_t ssize, int device, cudaStream_t stream) {
    mc_allocator_free(ptr, ssize, device, stream);
}
}
