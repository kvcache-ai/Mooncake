#include "cuda_alike.h"
#include <sys/types.h>
#include <atomic>

#include <iostream>

extern "C" {
enum class MemoryBackend { FABRIC, IPC_POSIX_FD, UNKNOWN };
MemoryBackend detectMemoryBackend() {
    CUdevice dev;
    int cudaDev;
    cudaError_t err = cudaGetDevice(&cudaDev);
    if (err != cudaSuccess) {
        std::cerr << "cudaGetDevice failed: " << cudaGetErrorString(err);
        return MemoryBackend::IPC_POSIX_FD;
    }

    CUresult result = cuDeviceGet(&dev, cudaDev);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuDeviceGet failed: " << result;
        return MemoryBackend::IPC_POSIX_FD;
    }

    // Validation method: Create Fabric Handle
    CUmemAllocationProp prop = {};
    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    prop.location.id = dev;
    prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;

    CUmemGenericAllocationHandle handle;
    size_t alloc_size = 4096;
    result = cuMemCreate(&handle, alloc_size, &prop, 0);
    if (result == CUDA_SUCCESS) {
        // Support FABRIC
        cuMemRelease(handle);
        return MemoryBackend::FABRIC;
    } else {
        return MemoryBackend::IPC_POSIX_FD;
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
    static std::atomic<MemoryBackend> backend{MemoryBackend::UNKNOWN};

    if (backend.load() == MemoryBackend::UNKNOWN) {
        backend = detectMemoryBackend();
    }

    prop.type = CU_MEM_ALLOCATION_TYPE_PINNED;
    prop.location.type = CU_MEM_LOCATION_TYPE_DEVICE;
    switch (backend.load()) {
        case MemoryBackend::FABRIC:
            prop.requestedHandleTypes = CU_MEM_HANDLE_TYPE_FABRIC;
            break;

        case MemoryBackend::IPC_POSIX_FD:
            prop.requestedHandleTypes =
                CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR;
            break;

        default:
            return nullptr;
    }
    prop.location.id = currentDev;
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
    result = cuMemCreate(&handle, size, &prop, 0);
    if (result != CUDA_SUCCESS) {
        std::cerr << "cuMemCreate failed: " << result;
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
