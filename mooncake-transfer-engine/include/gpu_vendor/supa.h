#pragma once

#include <string>
#include <supa_driver.h>
#include <supa_runtime.h>

const static std::string GPU_PREFIX = "supa:";

// CUDA -> SUPA compatibility shim. The SUPA SDK exposes the runtime API with
// the `su` prefix (suGetDeviceCount, suMallocDevice, ...) and the driver API
// with the `sudrv` prefix (sudrvMemCreate, sudrvPrimaryContextRetain, ...), so
// each `cuda*`/`cu*`/`CU*` identifier used by Mooncake is remapped here.

// ===================== Runtime API types =====================
#define cudaError_t suError_t
#define cudaSuccess suSuccess
#define cudaErrorNotReady suErrorNotReady
#define cudaErrorPeerAccessAlreadyEnabled suErrorPeerAccessAlreadyEnabled
#define cudaGetErrorString suGetErrorString
#define cudaMemoryTypeHost suMemoryTypeHost
#define cudaMemoryTypeDevice suMemoryTypeDevice
#define cudaMemoryTypeUnregistered suMemoryTypeUnregistered
#define cudaPointerAttributes suPointerAttributes
#define cudaDeviceProp suDeviceProp
#define cudaIpcMemHandle_t suIpcMemHandle_t
#define cudaIpcMemLazyEnablePeerAccess suIpcMemLazyEnablePeerAccess
#define cudaStream_t suStream_t
#define cudaEvent_t suEvent_t
#define cudaMemcpyDefault suMemcpyDefault
#define cudaMemcpyKind suMemcpyKind
#define cudaMemcpyHostToDevice suMemcpyHostToDevice
#define cudaMemcpyDeviceToHost suMemcpyDeviceToHost
#define cudaMemcpyDeviceToDevice suMemcpyDeviceToDevice
#define cudaHostAllocDefault suMallocHostDefault
#define cudaHostAllocPortable suMallocHostPortable
#define cudaHostAllocMapped suMallocHostMapped
#define cudaHostRegisterDefault suHostRegisterDefault
#define cudaHostRegisterPortable suHostRegisterPortable
#define cudaHostRegisterMapped suHostRegisterMapped
#define cudaHostRegisterIoMemory suHostRegisterIoMemory
#define cudaStreamNonBlocking suStreamNonBlocking
#define cudaEventDefault suEventDefault
#define cudaEventDisableTiming suEventDisableTiming
#define cudaDevAttrClockRate suDevAttrClockRate

// ===================== Runtime API functions =====================
#define cudaGetDeviceCount suGetDeviceCount
#define cudaSetDevice suSetDevice
#define cudaGetDevice suGetDevice
#define cudaDeviceGetAttribute suDeviceGetAttribute
#define cudaDeviceGetPCIBusId suDeviceGetPCIBusId
#define cudaMalloc suMallocDevice
#define cudaFree suFree
#define cudaMallocHost suMallocHost
#define cudaHostAlloc suMallocHost
#define cudaFreeHost suFreeHost
#define cudaHostRegister suRegisterHostMemory
#define cudaHostUnregister suUnregisterHostMemory
#define cudaHostGetDevicePointer suHostGetDevicePointer
#define cudaMemcpy suMemcpy
#define cudaMemset suMemset
#define cudaMemsetAsync suMemsetAsync
#define cudaPointerGetAttributes suPointerGetAttributes
#define cudaDeviceCanAccessPeer suDeviceCanAccessPeer
#define cudaDeviceEnablePeerAccess suDeviceEnablePeerAccess
#define cudaIpcGetMemHandle suIpcGetMemHandle
#define cudaIpcOpenMemHandle suIpcOpenMemHandle
#define cudaIpcCloseMemHandle suIpcCloseMemHandle
#define cudaDeviceSynchronize suDeviceSynchronize
#define cudaStreamCreate suStreamCreate
#define cudaStreamCreateWithFlags suStreamCreateWithFlags
#define cudaStreamDestroy suStreamDestroy
#define cudaStreamQuery suStreamQuery
#define cudaStreamSynchronize suStreamSynchronize
#define cudaLaunchHostFunc suLaunchHostFunc
#define cudaEventCreate suEventCreate
#define cudaEventCreateWithFlags suEventCreateWithFlags
#define cudaEventDestroy suEventDestroy
#define cudaEventRecord suEventRecord
#define cudaEventQuery suEventQuery
#define cudaGetLastError suGetLastError
#define cudaGetDeviceProperties suGetDeviceProperties
#define cudaStreamPerThread suStreamDefault
#define cudaEventSynchronize suEventSynchronize

// ===================== Driver API types =====================
#define CUresult suError_t
#define CUDA_SUCCESS suSuccess
#define CUDA_ERROR_NOT_PERMITTED suErrorNotPermitted
#define CUDA_ERROR_NOT_SUPPORTED suErrorNotSupported
#define CUdevice suDevice
#define CUdeviceptr suDeviceptr_t
#define CUcontext suContext
#define CUmemGenericAllocationHandle suMemGenericAllocationHandle_t
#define CUmemAllocationProp suMemAllocationProp
#define CUmemAccessDesc suMemAccessDesc
#define CUmemAllocationHandleType suMemAllocationHandleType
#define CUmemorytype suMemoryType
#define CUmemRangeHandleType suMemRangeHandleType

// ===================== Driver API enums =====================
#define CU_MEM_ALLOCATION_TYPE_PINNED suMemAllocationTypePinned
#define CU_MEM_LOCATION_TYPE_DEVICE suMemLocationTypeDevice
#define CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD suMemRangeHandleTypeDmaBufFd
#define CU_MEM_ACCESS_FLAGS_PROT_READWRITE suMemAccessFlagsProtReadWrite
#define CU_MEM_ALLOC_GRANULARITY_MINIMUM suMemAllocGranularityMinimum
#define CU_MEMORYTYPE_HOST suMemoryTypeHost
#define CU_MEMORYTYPE_DEVICE suMemoryTypeDevice
#define CU_POINTER_ATTRIBUTE_MEMORY_TYPE suPointerAttributeMemoryType
#define CU_POINTER_ATTRIBUTE_RANGE_SIZE suPointerAttributeRangeSize
#define CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED suDevAttrDmaBufSupported

// ===================== Driver API functions =====================
#define cuInit sudrvInit
#define cuDeviceGet sudrvDeviceGet
#define cuDeviceGetAttribute sudrvDeviceGetAttribute
#define cuPointerGetAttribute sudrvPointerGetAttribute
#define cuGetErrorString sudrvGetErrorString
#define cuMemCreate sudrvMemCreate
#define cuMemRelease sudrvMemRelease
#define cuMemAddressReserve sudrvMemAddressReserve
#define cuMemAddressFree sudrvMemAddressFree
#define cuMemMap sudrvMemMap
#define cuMemUnmap sudrvMemUnmap
#define cuMemSetAccess sudrvMemSetAccess
#define cuMemGetAddressRange sudrvMemGetAddressRange
#define cuMemGetHandleForAddressRange sudrvMemGetHandleForAddressRange
#define cuMemRetainAllocationHandle sudrvMemRetainAllocationHandle
#define cuMemExportToShareableHandle sudrvMemExportToShareableHandle
#define cuMemImportFromShareableHandle sudrvMemImportFromShareableHandle
#define cuMemGetAllocationGranularity sudrvMemGetAllocationGranularity
#define cuDevicePrimaryCtxRetain sudrvPrimaryContextRetain
#define cuDevicePrimaryCtxRelease sudrvPrimaryContextRelease
#define cuCtxSetCurrent sudrvContextSetCurrent
#define CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL suPointerAttributeDeviceOrdinal

// SUPA orders the stream argument before the copy kind, unlike CUDA. Wrap it so
// existing `cudaMemcpyAsync(dst, src, size, kind, stream)` call sites keep
// working unchanged.
static inline suError_t cudaMemcpyAsync(void *dst, const void *src, size_t size,
                                        suMemcpyKind kind,
                                        suStream_t stream = nullptr) {
    return suMemcpyAsync(dst, src, size, stream, kind);
}
