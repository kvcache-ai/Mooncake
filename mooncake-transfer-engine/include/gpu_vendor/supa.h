#pragma once

#include <string>
#include <supa_driver.h>
#include <supa_runtime.h>

const static std::string GPU_PREFIX = "supa:";

// ===================== Runtime API types =====================
#define cudaError_t supaError_t
#define cudaSuccess supaSuccess
#define cudaErrorNotReady supaErrorNotReady
#define cudaErrorPeerAccessAlreadyEnabled supaErrorPeerAccessAlreadyEnabled
#define cudaGetErrorString supaGetErrorString
#define cudaMemoryTypeHost supaMemoryTypeHost
#define cudaMemoryTypeDevice supaMemoryTypeDevice
#define cudaMemoryTypeUnregistered supaMemoryTypeUnregistered
#define cudaPointerAttributes supaPointerAttributes
#define cudaDeviceProp supaDeviceProp
#define cudaIpcMemHandle_t supaIpcMemHandle_t
#define cudaIpcMemLazyEnablePeerAccess supaIpcMemLazyEnablePeerAccess
#define cudaStream_t supaStream_t
#define cudaEvent_t supaEvent_t
#define cudaMemcpyDefault supaMemcpyDefault
#define cudaMemcpyHostToDevice supaMemcpyHostToDevice
#define cudaMemcpyDeviceToHost supaMemcpyDeviceToHost
#define cudaMemcpyDeviceToDevice supaMemcpyDeviceToDevice
#define cudaHostAllocDefault supaHostAllocDefault
#define cudaHostAllocPortable supaHostAllocPortable
#define cudaHostAllocMapped supaHostAllocMapped
#define cudaHostRegisterDefault supaHostRegisterDefault
#define cudaHostRegisterPortable supaHostRegisterPortable
#define cudaHostRegisterMapped supaHostRegisterMapped
#define cudaHostRegisterIoMemory supaHostRegisterIoMemory
#define cudaStreamNonBlocking supaStreamNonBlocking
#define cudaEventDefault supaEventDefault
#define cudaEventDisableTiming supaEventDisableTiming
#define cudaDevAttrClockRate supaDevAttrClockRate

// ===================== Runtime API functions =====================
#define cudaGetDeviceCount supaGetDeviceCount
#define cudaSetDevice supaSetDevice
#define cudaGetDevice supaGetDevice
#define cudaDeviceGetAttribute supaDeviceGetAttribute
#define cudaDeviceGetPCIBusId supaDeviceGetPCIBusId
#define cudaMalloc supaMalloc
#define cudaFree supaFree
#define cudaMallocHost supaMallocHost
#define cudaHostAlloc supaHostAlloc
#define cudaFreeHost supaFreeHost
#define cudaHostRegister supaHostRegister
#define cudaHostUnregister supaHostUnregister
#define cudaHostGetDevicePointer supaHostGetDevicePointer
#define cudaMemcpy supaMemcpy
#define cudaMemcpyAsync supaMemcpyAsync
#define cudaMemset supaMemset
#define cudaMemsetAsync supaMemsetAsync
#define cudaPointerGetAttributes supaPointerGetAttributes
#define cudaDeviceCanAccessPeer supaDeviceCanAccessPeer
#define cudaDeviceEnablePeerAccess supaDeviceEnablePeerAccess
#define cudaIpcGetMemHandle supaIpcGetMemHandle
#define cudaIpcOpenMemHandle supaIpcOpenMemHandle
#define cudaIpcCloseMemHandle supaIpcCloseMemHandle
#define cudaDeviceSynchronize supaDeviceSynchronize
#define cudaStreamCreate supaStreamCreate
#define cudaStreamCreateWithFlags supaStreamCreateWithFlags
#define cudaStreamDestroy supaStreamDestroy
#define cudaStreamQuery supaStreamQuery
#define cudaStreamSynchronize supaStreamSynchronize
#define cudaLaunchHostFunc supaLaunchHostFunc
#define cudaEventCreate supaEventCreate
#define cudaEventCreateWithFlags supaEventCreateWithFlags
#define cudaEventDestroy supaEventDestroy
#define cudaEventRecord supaEventRecord
#define cudaEventQuery supaEventQuery
#define cudaGetLastError supaGetLastError
#define cudaGetDeviceProperties supaGetDeviceProperties
#define cudaStreamPerThread supaStreamPerThread
#define cudaEventSynchronize supaEventSynchronize

// ===================== Driver API types =====================
#define CUresult SUresult
#define CUDA_SUCCESS SUPA_SUCCESS
#define CUDA_ERROR_NOT_PERMITTED SUPA_ERROR_NOT_PERMITTED
#define CUDA_ERROR_NOT_SUPPORTED SUPA_ERROR_NOT_SUPPORTED
#define CUdevice SUdevice
#define CUdeviceptr SUdeviceptr
#define CUcontext SUcontext
#define CUmemGenericAllocationHandle SUmemGenericAllocationHandle
#define CUmemAllocationProp SUmemAllocationProp
#define CUmemAccessDesc SUmemAccessDesc
#define CUmemFabricHandle SUmemFabricHandle
#define CUmemAllocationHandleType SUmemAllocationHandleType
#define CUmemorytype SUmemorytype
#define CUmemRangeHandleType SUmemRangeHandleType

// ===================== Driver API enums =====================
#define CU_MEM_ALLOCATION_TYPE_PINNED SU_MEM_ALLOCATION_TYPE_PINNED
#define CU_MEM_LOCATION_TYPE_DEVICE SU_MEM_LOCATION_TYPE_DEVICE
#define CU_MEM_HANDLE_TYPE_FABRIC SU_MEM_HANDLE_TYPE_FABRIC
#define CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD SU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD
#define CU_MEM_ACCESS_FLAGS_PROT_READWRITE SU_MEM_ACCESS_FLAGS_PROT_READWRITE
#define CU_MEM_ALLOC_GRANULARITY_MINIMUM SU_MEM_ALLOC_GRANULARITY_MINIMUM
#define CU_MEMORYTYPE_HOST SU_MEMORYTYPE_HOST
#define CU_MEMORYTYPE_DEVICE SU_MEMORYTYPE_DEVICE
#define CU_POINTER_ATTRIBUTE_MEMORY_TYPE SU_POINTER_ATTRIBUTE_MEMORY_TYPE
#define CU_POINTER_ATTRIBUTE_RANGE_SIZE SU_POINTER_ATTRIBUTE_RANGE_SIZE
#define CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED \
    SU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED
#define CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED \
    SU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED
#define CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED \
    SU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_SUPA_VMM_SUPPORTED

// ===================== Driver API functions =====================
#define cuInit suInit
#define cuDeviceGet suDeviceGet
#define cuDeviceGetAttribute suDeviceGetAttribute
#define cuPointerGetAttribute suPointerGetAttribute
#define cuGetErrorString suGetErrorString
#define cuMemCreate suMemCreate
#define cuMemRelease suMemRelease
#define cuMemAddressReserve suMemAddressReserve
#define cuMemAddressFree suMemAddressFree
#define cuMemMap suMemMap
#define cuMemUnmap suMemUnmap
#define cuMemSetAccess suMemSetAccess
#define cuMemGetAddressRange suMemGetAddressRange
#define cuMemGetHandleForAddressRange suMemGetHandleForAddressRange
#define cuMemRetainAllocationHandle suMemRetainAllocationHandle
#define cuMemExportToShareableHandle suMemExportToShareableHandle
#define cuMemImportFromShareableHandle suMemImportFromShareableHandle
#define cuMemGetAllocationGranularity suMemGetAllocationGranularity
#define cuDevicePrimaryCtxRetain suDevicePrimaryCtxRetain
#define cuDevicePrimaryCtxRelease suDevicePrimaryCtxRelease
#define cuCtxSetCurrent suCtxSetCurrent
#define CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL SU_POINTER_ATTRIBUTE_DEVICE_ORDINAL
