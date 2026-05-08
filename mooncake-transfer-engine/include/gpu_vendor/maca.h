#pragma once

#include <string>
#include <mcr/maca.h>
#include <mcr/mc_runtime.h>
#include <mcr/mc_runtime_api.h>

// First-stage MACA integration: provide a CUDA-like API surface.
const static std::string GPU_PREFIX = "maca:";

#define CUdevice MCdevice
#define CUdeviceptr mcDeviceptr_t
#define CUmemorytype MCmemorytype
#define CUresult mcError_t
#define cuDeviceGet mcDeviceGet
#define cuDeviceGetAttribute mcDeviceGetAttribute
#define cuGetErrorString mcGetErrorString
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
#define CUmemAccessDesc mcMemAccessDesc
#define CUmemAllocationType mcMemAllocationType
#define CU_MEM_ALLOCATION_TYPE_PINNED mcMemAllocationTypePinned
#define CU_MEM_LOCATION_TYPE_DEVICE mcMemLocationTypeDevice
#define CU_MEM_HANDLE_TYPE_FABRIC mcMemHandleTypeFabric
#define CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED \
    mcDeviceAttributeHandleTypeFabricSupported
#define CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED \
    mcDeviceAttributeHandleTypePosixFileDescriptorSupported
#define CU_MEM_ACCESS_FLAGS_PROT_READWRITE mcMemAccessFlagsProtReadWrite
#define CU_MEM_ALLOC_GRANULARITY_MINIMUM MC_MEM_ALLOC_GRANULARITY_MINIMUM

#define CU_MEMORYTYPE_HOST mcMemoryTypeHost
#define CU_MEMORYTYPE_DEVICE mcMemoryTypeDevice
#define CU_POINTER_ATTRIBUTE_MEMORY_TYPE mcPointerAttributeMemoryType
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
#define cudaDeviceGetPCIBusId mcDeviceGetPCIBusId
#define cudaErrorPeerAccessAlreadyEnabled mcErrorPeerAccessAlreadyEnabled
#define cudaError_t mcError_t
#define cudaFree mcFree
#define cudaFreeHost mcFreeHost
#define cudaGetDevice mcGetDevice
#define cudaGetDeviceCount mcGetDeviceCount
#define cudaGetErrorString mcGetErrorString
#define cudaGetLastError mcGetLastError
#define cudaHostRegister mcHostRegister
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
#define cudaMemcpyDeviceToHost mcMemcpyDeviceToHost
#define cudaMemcpyHostToDevice mcMemcpyHostToDevice
#define cudaMemset mcMemset
#define cudaMemsetAsync mcMemsetAsync
#define cudaMemoryTypeDevice mcMemoryTypeDevice
#define cudaMemoryTypeHost mcMemoryTypeHost
#define cudaMemoryTypeUnregistered mcMemoryTypeUnregistered
#define cudaPointerAttributes mcPointerAttribute_t
#define cudaPointerGetAttributes mcPointerGetAttributes
#define cudaSetDevice mcSetDevice
#define cudaStreamCreate mcStreamCreate
#define cudaStreamDestroy mcStreamDestroy
#define cudaStreamSynchronize mcStreamSynchronize
#define cudaStream_t mcStream_t
#define cudaSuccess mcSuccess
