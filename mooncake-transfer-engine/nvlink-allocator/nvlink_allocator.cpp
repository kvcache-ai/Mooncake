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

extern "C" {

MemoryBackendType mc_probe_fabric_support(int device_id) {
    CUdevice dev;
    CUresult res = cuDeviceGet(&dev, device_id);
    if (res != CUDA_SUCCESS) {
        return MemoryBackendType::unknown;
    }

    // Check device attribute first
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
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;  // require fabric

    CUmemGenericAllocationHandle handle;
    size_t size = 4096;

    res = cuMemCreate(&handle, size, &prop, 0);

    if (res == CUDA_SUCCESS) {
        cuMemRelease(handle);  // success → clean up
        return MemoryBackendType::use_cumemcreate;
    } else {
        return MemoryBackendType::use_cudamalloc;
    }
}

void *mc_nvlink_malloc(ssize_t size, int device, cudaStream_t stream) {
    size_t granularity = 0;
    CUdevice currentDev;
    CUmemAllocationProp prop = {};
    CUmemGenericAllocationHandle handle;
    void *ptr = nullptr;
    int cudaDev;
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
    // fix size
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
}

void mc_nvlink_free(void *ptr, ssize_t ssize, int device, cudaStream_t stream) {
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
}
}
