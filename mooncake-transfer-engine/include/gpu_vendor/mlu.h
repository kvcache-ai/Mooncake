#pragma once

#include <cn_api.h>
#include <cnrt.h>

#include <string>

const static std::string GPU_PREFIX = "mlu:";

using cudaError_t = cnrtRet_t;
using cudaPointerAttributes = cnrtPointerAttributes_t;
using cudaStream_t = cnrtQueue_t;

using CUdevice = CNdev;
using CUdeviceptr = CNaddr;
using CUmemorytype = int;
using CUresult = CNresult;

#define CUDA_SUCCESS CN_SUCCESS
#define CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED \
    CN_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED
#define CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD CN_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD
#define CU_MEMORYTYPE_HOST CN_MEMORYTYPE_HOST
#define CU_MEMORYTYPE_DEVICE CN_MEMORYTYPE_DEVICE
#define CU_POINTER_ATTRIBUTE_MEMORY_TYPE CN_MEM_ATTRIBUTE_TYPE
#define CU_POINTER_ATTRIBUTE_RANGE_SIZE CN_MEM_ATTRIBUTE_RANGE_SIZE

#define cudaSuccess cnrtSuccess
#define cudaMemoryTypeUnregistered cnrtMemTypeUnregistered
#define cudaMemoryTypeHost cnrtMemTypeHost
#define cudaMemoryTypeDevice cnrtMemTypeDevice
#define cudaMemcpyHostToDevice cnrtMemcpyHostToDev
#define cudaMemcpyDeviceToHost cnrtMemcpyDevToHost
#define cudaMemcpyDeviceToDevice cnrtMemcpyDevToDev
#define cudaMemcpyHostToHost cnrtMemcpyHostToHost
#define cudaMemcpyDefault cnrtMemcpyNoDirection

static inline const char *cudaGetErrorString(cudaError_t error) {
    return cnrtGetErrorStr(error);
}

static inline cudaError_t cudaGetLastError() { return cnrtGetLastError(); }

static inline cudaError_t cudaGetDeviceCount(int *count) {
    if (!count) return cnrtErrorArgsInvalid;
    unsigned int device_count = 0;
    auto ret = cnrtGetDeviceCount(&device_count);
    if (ret == cnrtSuccess) {
        *count = static_cast<int>(device_count);
    }
    return ret;
}

static inline cudaError_t cudaGetDevice(int *device) {
    return cnrtGetDevice(device);
}

static inline cudaError_t cudaSetDevice(int device) {
    return cnrtSetDevice(device);
}

static inline cudaError_t cudaDeviceGetPCIBusId(char *pci_bus_id, int len,
                                                int device) {
    return cnrtDeviceGetPCIBusId(pci_bus_id, len, device);
}

static inline cudaError_t cudaPointerGetAttributes(cudaPointerAttributes *attr,
                                                   const void *ptr) {
    return cnrtPointerGetAttributes(attr, const_cast<void *>(ptr));
}

static inline cudaError_t cudaMalloc(void **ptr, size_t bytes) {
    return cnrtMalloc(ptr, bytes);
}

static inline cudaError_t cudaFree(void *ptr) { return cnrtFree(ptr); }

static inline cudaError_t cudaMallocHost(void **ptr, size_t bytes) {
    return cnrtHostMalloc(ptr, bytes);
}

static inline cudaError_t cudaFreeHost(void *ptr) { return cnrtFreeHost(ptr); }

static inline cudaError_t cudaMemcpy(void *dst, const void *src, size_t bytes,
                                     int kind) {
    return cnrtMemcpy(dst, const_cast<void *>(src), bytes,
                      static_cast<cnrtMemTransDir_t>(kind));
}

static inline cudaError_t cudaMemcpyAsync(void *dst, const void *src,
                                          size_t bytes, int kind,
                                          cudaStream_t stream) {
    return cnrtMemcpyAsync_V3(dst, const_cast<void *>(src), bytes, stream,
                              static_cast<cnrtMemTransDir_t>(kind));
}

static inline cudaError_t cudaMemset(void *ptr, int value, size_t bytes) {
    return cnrtMemset(ptr, value, bytes);
}

static inline cudaError_t cudaStreamCreate(cudaStream_t *stream) {
    return cnrtQueueCreate(stream);
}

static inline cudaError_t cudaStreamDestroy(cudaStream_t stream) {
    return cnrtQueueDestroy(stream);
}

static inline cudaError_t cudaStreamSynchronize(cudaStream_t stream) {
    return cnrtQueueSync(stream);
}

static inline cudaError_t cudaDeviceCanAccessPeer(int *can_access, int device,
                                                  int peer_device) {
    if (!can_access) return cnrtErrorArgsInvalid;
    unsigned int peer_accessible = 0;
    auto ret = cnrtGetPeerAccessibility(&peer_accessible, device, peer_device);
    if (ret == cnrtSuccess) {
        *can_access = static_cast<int>(peer_accessible);
    }
    return ret;
}

static inline CUresult cuDeviceGet(CUdevice *device, int ordinal) {
    return cnDeviceGet(device, ordinal);
}

static inline CUresult cuDeviceGetAttribute(int *value, int attribute,
                                            CUdevice device) {
    return cnDeviceGetAttribute(
        value, static_cast<CNdevice_attribute>(attribute), device);
}

static inline CUresult cuGetErrorString(CUresult error, const char **err_str) {
    return cnGetErrorString(error, err_str);
}

static inline CUresult cuPointerGetAttribute(void *data, int attribute,
                                             CUdeviceptr ptr) {
    if (!data) return CN_ERROR_INVALID_VALUE;

    cudaPointerAttributes attributes;
    const auto ret =
        cudaPointerGetAttributes(&attributes, reinterpret_cast<void *>(ptr));
    if (ret != cudaSuccess) {
        return CN_ERROR_INVALID_VALUE;
    }

    switch (attribute) {
        case CU_POINTER_ATTRIBUTE_MEMORY_TYPE: {
            auto *memory_type = static_cast<CUmemorytype *>(data);
            if (attributes.type == cudaMemoryTypeHost) {
                *memory_type = CU_MEMORYTYPE_HOST;
            } else if (attributes.type == cudaMemoryTypeDevice) {
                *memory_type = CU_MEMORYTYPE_DEVICE;
            } else {
                return CN_ERROR_INVALID_VALUE;
            }
            return CUDA_SUCCESS;
        }
        case CU_POINTER_ATTRIBUTE_RANGE_SIZE: {
            if (attributes.type != cudaMemoryTypeDevice) {
                return CN_ERROR_INVALID_VALUE;
            }
            *static_cast<size_t *>(data) = attributes.size;
            return CUDA_SUCCESS;
        }
        default:
            return CN_ERROR_INVALID_VALUE;
    }
}

static inline CUresult cuMemGetHandleForAddressRange(void *handle,
                                                     CUdeviceptr ptr,
                                                     size_t size,
                                                     int handle_type,
                                                     unsigned long long flags) {
    return cnMemGetHandleForAddressRange(
        handle, ptr, size, static_cast<CNmemRangeHandleType>(handle_type),
        static_cast<cn_uint64_t>(flags));
}
