#include "cuda_alike.h"
#include <sys/types.h>

#include <iostream>

// ref: http://github.com/NVIDIA/nccl/blob/v2.28.9-1/src/allocator.cc#L53-L68
static CUresult cuMemCreateTryFabric(CUmemGenericAllocationHandle *handle,
                                     size_t size, CUmemAllocationProp *prop,
                                     unsigned long long flags) {
    CUresult err = cuMemCreate(handle, size, prop, flags);
    if ((prop->requestedHandleTypes & CU_MEM_HANDLE_TYPE_FABRIC) &&
        (err == CUDA_ERROR_NOT_PERMITTED || err == CUDA_ERROR_NOT_SUPPORTED)) {
        prop->requestedHandleTypes = static_cast<CUmemAllocationHandleType>(
            prop->requestedHandleTypes & ~CU_MEM_HANDLE_TYPE_FABRIC);
        err = cuMemCreate(handle, size, prop, flags);
    }
    return err;
}

enum class MemoryBackendType { use_cudamalloc, use_cumemcreate, unknown };

namespace {

// The fabric attribute only says the GPU has the link. An accelerator that is
// not provisioned yet (UALink) still creates fabric allocations and fails at
// export, so a caller trusting the attribute ends up with memory no peer can
// map. Probe the whole export/import cycle instead.
bool FabricHandleRoundTrip(CUmemGenericAllocationHandle handle) {
#if defined(USE_HIP) && !defined(MC_HIP_HAS_FABRIC_HANDLE)
    // CU_MEM_HANDLE_TYPE_FABRIC is a POSIX fd here (see gpu_vendor/hip.h);
    // creating the allocation already proved it.
    (void)handle;
    return true;
#else
    CUmemFabricHandle fabric_handle = {};
    if (cuMemExportToShareableHandle(&fabric_handle, handle,
                                     CU_MEM_HANDLE_TYPE_FABRIC,
                                     0) != CUDA_SUCCESS) {
        return false;
    }
    CUmemGenericAllocationHandle imported;
    if (cuMemImportFromShareableHandle(&imported, &fabric_handle,
                                       CU_MEM_HANDLE_TYPE_FABRIC) !=
        CUDA_SUCCESS) {
        return false;
    }
    cuMemRelease(imported);
    return true;
#endif
}

MemoryBackendType ProbeAllocatorBackend(int device_id) {
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

    size_t granularity = 0;
    res = cuMemGetAllocationGranularity(&granularity, &prop,
                                        CU_MEM_ALLOC_GRANULARITY_MINIMUM);
    if (res != CUDA_SUCCESS || granularity == 0) {
        // Failures here are expected off-fabric; on HIP each one leaves a
        // sticky error that would surface at the caller's next runtime call.
        (void)cudaGetLastError();
        return MemoryBackendType::use_cudamalloc;
    }

    CUmemGenericAllocationHandle handle;
    res = cuMemCreate(&handle, granularity, &prop, 0);
    if (res != CUDA_SUCCESS) {
        (void)cudaGetLastError();
        return MemoryBackendType::use_cudamalloc;
    }
    const bool usable = FabricHandleRoundTrip(handle);
    cuMemRelease(handle);
    if (!usable) {
        (void)cudaGetLastError();
        std::cerr << "mc_allocator_probe: device " << device_id
                  << " creates fabric memory but cannot export or import it; "
                     "falling back to cudaMalloc\n";
        return MemoryBackendType::use_cudamalloc;
    }
    return MemoryBackendType::use_cumemcreate;
}

void *AllocateFabricMemory(ssize_t size, int device, cudaStream_t stream) {
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
    // The mapping keeps the allocation alive; FreeFabricMemory retains a
    // fresh handle to release it.
    cuMemRelease(handle);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuMemMap failed: " << result;
        cuMemAddressFree((CUdeviceptr)ptr, size);
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
        return nullptr;
    }
    return ptr;
}

void FreeFabricMemory(void *ptr, ssize_t ssize, int device,
                      cudaStream_t stream) {
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
    // Not all runtimes accept a NULL base output; always pass a real pointer.
    CUdeviceptr base = 0;
    result = cuMemGetAddressRange(&base, &size, (CUdeviceptr)ptr);
    if (result == CUDA_SUCCESS) {
        cuMemUnmap((CUdeviceptr)ptr, size);
        cuMemAddressFree((CUdeviceptr)ptr, size);
    }
    cuMemRelease(handle);
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
