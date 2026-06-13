#pragma once

#include <string>
#include <mcr/maca.h>
#include <mcr/mc_runtime.h>
#include <mcr/mc_runtime_api.h>

// First-stage MACA integration: provide a CUDA-like API surface.
const static std::string GPU_PREFIX = "maca:";

// torch-maca's cu-bridge already defines CUDA/CU compatibility macros after
// ATen/cuda headers are included. Keep this fallback mapping for translation
// units that include Mooncake's cuda_alike.h without cu-bridge first.
#ifndef __CUDA_TO_MACA_ADAPTOR_H__
#define CUdevice MCdevice
#define CUdeviceptr mcDeviceptr_t
#define CUmemorytype MCmemorytype
#define CUresult mcError_t
#define cuDeviceGet mcDeviceGet
#define cuDeviceGetAttribute mcDeviceGetAttribute
#define cuMemAddressFree mcMemAddressFree
#define cuMemAddressReserve mcMemAddressReserve
#define cuMemCreate mcMemCreate
#define cuMemExportToShareableHandle mcMemExportToShareableHandle
#define cuMemGetAddressRange mcMemGetAddressRange
#define cuMemGetAllocationGranularity mcMemGetAllocationGranularity
#define cuMemGetHandleForAddressRange mcMemGetHandleForAddressRange
#define cuMemImportFromShareableHandle mcMemImportFromShareableHandle
#define cuMemMap mcMemMap
#define cuMemRelease mcMemRelease
#define cuMemRetainAllocationHandle mcMemRetainAllocationHandle
#define cuMemSetAccess mcMemSetAccess
#define cuMemUnmap mcMemUnmap
#define cuPointerGetAttribute mcPointerGetAttribute

#define CUDA_SUCCESS mcSuccess
#define CUDA_ERROR_NOT_PERMITTED mcErrorNotPermitted
#define CUDA_ERROR_NOT_SUPPORTED mcErrorNotSupported

#define CUmemFabricHandle mcMemFabricHandle_t
#define CUmemGenericAllocationHandle mcMemGenericAllocationHandle
#define CUmemAllocationProp mcMemAllocationProp
#define CUmemAllocationHandleType mcMemAllocationHandleType
#define CUmemAccessDesc mcMemAccessDesc
#define CUmemAllocationType mcMemAllocationType
#define CU_MEM_ALLOCATION_TYPE_PINNED mcMemAllocationTypePinned
#define CU_MEM_LOCATION_TYPE_DEVICE mcMemLocationTypeDevice
#define CU_MEM_HANDLE_TYPE_FABRIC mcMemHandleTypeFabric
#define CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR \
    mcMemHandleTypePosixFileDescriptor
#define CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED \
    mcDeviceAttributeHandleTypeFabricSupported
#define CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED \
    mcDeviceAttributeHandleTypePosixFileDescriptorSupported
#define CU_MEM_ACCESS_FLAGS_PROT_READWRITE mcMemAccessFlagsProtReadWrite
#define CU_MEM_ALLOC_GRANULARITY_MINIMUM MC_MEM_ALLOC_GRANULARITY_MINIMUM

#define CU_MEMORYTYPE_HOST mcMemoryTypeHost
#define CU_MEMORYTYPE_DEVICE mcMemoryTypeDevice
#define CU_POINTER_ATTRIBUTE_MEMORY_TYPE mcPointerAttributeMemoryType
#define CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL mcPointerAttributeDevice
#define CU_POINTER_ATTRIBUTE_RANGE_START_ADDR mcPointerAttributeRangeStartAddr
#define CU_POINTER_ATTRIBUTE_RANGE_SIZE mcPointerAttributeRangeSize
#define CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD mcMemHandleTypePosixFileDescriptor
#define CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED \
    mcDeviceAttributeHandleTypePosixFileDescriptorSupported

static inline CUresult cuGetErrorString(CUresult error, const char **err_str) {
    if (err_str) {
        *err_str = mcGetErrorString(error);
    }
    return CUDA_SUCCESS;
}

#define cudaDeviceCanAccessPeer mcDeviceCanAccessPeer
#define cudaDeviceEnablePeerAccess mcDeviceEnablePeerAccess
#define cudaDeviceGetAttribute mcDeviceGetAttribute
#define cudaDeviceGetPCIBusId mcDeviceGetPCIBusId
#define cudaDeviceSynchronize mcDeviceSynchronize
#define cudaDevAttrMultiProcessorCount mcDeviceAttributeMultiProcessorCount
#define cudaErrorNotReady mcErrorNotReady
#define cudaErrorPeerAccessAlreadyEnabled mcErrorPeerAccessAlreadyEnabled
#define cudaError_t mcError_t
#define cudaEvent_t mcEvent_t
#define cudaEventCreate mcEventCreate
#define cudaEventCreateWithFlags mcEventCreateWithFlags
#define cudaEventDestroy mcEventDestroy
#define cudaEventDisableTiming mcEventDisableTiming
#define cudaEventElapsedTime mcEventElapsedTime
#define cudaEventQuery mcEventQuery
#define cudaEventRecord mcEventRecord
#define cudaEventSynchronize mcEventSynchronize
#define cudaFree mcFree
#define cudaFreeHost mcFreeHost
#define cudaFuncAttributes mcFuncAttributes
#define cudaGetDevice mcGetDevice
#define cudaGetDeviceCount mcGetDeviceCount
#define cudaGetErrorString mcGetErrorString
#define cudaGetLastError mcGetLastError
#define cudaHostAlloc mcMallocHost
#define cudaHostAllocDefault mcMallocHostDefault
#define cudaHostAllocMapped mcMallocHostMapped
#define cudaHostAllocPortable mcMallocHostPortable
#define cudaHostAllocWriteCombined mcMallocHostWriteCombined
#define cudaHostRegister mcHostRegister
#define cudaHostRegisterMapped mcHostRegisterMapped
#define cudaHostRegisterPortable mcHostRegisterPortable
#define cudaHostUnregister mcHostUnregister
#define cudaIpcCloseMemHandle mcIpcCloseMemHandle
#define cudaIpcGetMemHandle mcIpcGetMemHandle
#define cudaIpcMemHandle_t mcIpcMemHandle_t
#define cudaIpcMemLazyEnablePeerAccess mcIpcMemLazyEnablePeerAccess
#define cudaIpcOpenMemHandle mcIpcOpenMemHandle
#define cudaMalloc mcMalloc
#define cudaMallocHost mcMallocHost
#define cudaMemcpy mcMemcpy
#define cudaMemcpyAsync mcMemcpyAsync
#define cudaMemcpyDefault mcMemcpyDefault
#define cudaMemcpyDeviceToDevice mcMemcpyDeviceToDevice
#define cudaMemcpyDeviceToHost mcMemcpyDeviceToHost
#define cudaMemcpyHostToDevice mcMemcpyHostToDevice
#define cudaMemcpyKind mcMemcpyKind
#define cudaMemset mcMemset
#define cudaMemsetAsync mcMemsetAsync
#define cudaMemoryTypeDevice mcMemoryTypeDevice
#define cudaMemoryTypeHost mcMemoryTypeHost
#define cudaMemoryTypeUnregistered mcMemoryTypeUnregistered
#define cudaPointerAttributes mcPointerAttribute_t
#define cudaPointerGetAttributes mcPointerGetAttributes
#define cudaSetDevice mcSetDevice
#define cudaStreamCreate mcStreamCreate
#define cudaStreamCreateWithFlags mcStreamCreateWithFlags
#define cudaStreamDestroy mcStreamDestroy
#define cudaStreamNonBlocking mcStreamNonBlocking
#define cudaStreamPerThread mcStreamPerThread
#define cudaStreamSynchronize mcStreamSynchronize
#define cudaStream_t mcStream_t
#define cudaStreamWaitEvent mcStreamWaitEvent
#define cudaSuccess mcSuccess

static inline mcError_t cudaFuncGetAttributes(mcFuncAttributes *attr,
                                              const void *func) {
    return mcFuncGetAttributes(attr, func);
}

#ifdef __cplusplus
template <class T>
static inline mcError_t cudaFuncGetAttributes(mcFuncAttributes *attr, T func) {
    return mcFuncGetAttributes(attr, reinterpret_cast<const void *>(func));
}

template <class T>
static inline mcError_t cudaHostGetDevicePointer(T **dev_ptr, void *host_ptr,
                                                 unsigned int flags) {
    return mcHostGetDevicePointer(reinterpret_cast<void **>(dev_ptr), host_ptr,
                                  flags);
}
#else
static inline mcError_t cudaHostGetDevicePointer(void **dev_ptr, void *host_ptr,
                                                 unsigned int flags) {
    return mcHostGetDevicePointer(dev_ptr, host_ptr, flags);
}
#endif
#endif  // __CUDA_TO_MACA_ADAPTOR_H__
