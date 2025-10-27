#include <string>
#include <bits/stdint-uintn.h>
#include <hip/hip_runtime.h>

const static std::string GPU_PREFIX = "hip:";

#define CU_MEM_ACCESS_FLAGS_PROT_READWRITE hipMemAccessFlagsProtReadWrite
#define CU_MEM_ALLOCATION_TYPE_PINNED hipMemAllocationTypePinned
#define CU_MEM_LOCATION_TYPE_DEVICE hipMemLocationTypeDevice
#define CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD hipMemRangeHandleTypeDmaBufFd
#define CU_MEMORYTYPE_DEVICE hipMemoryTypeDevice
#define CU_MEMORYTYPE_HOST hipMemoryTypeHost
#define CU_POINTER_ATTRIBUTE_MEMORY_TYPE HIP_POINTER_ATTRIBUTE_MEMORY_TYPE
#define CU_POINTER_ATTRIBUTE_RANGE_SIZE HIP_POINTER_ATTRIBUTE_RANGE_SIZE

#define CUdevice hipDevice_t
#define CUdeviceptr hipDeviceptr_t
#define CUmemAccessDesc hipMemAccessDesc
#define CUmemAllocationProp hipMemAllocationProp
#define CUmemGenericAllocationHandle hipMemGenericAllocationHandle_t
#define CUmemorytype hipMemoryType
#define CUresult hipError_t
#define cuDeviceGet hipDeviceGet
#define cuDeviceGetAttribute hipDeviceGetAttribute
#define cuGetErrorString hipDrvGetErrorString
#define cuMemAddressFree hipMemAddressFree
#define cuMemAddressReserve hipMemAddressReserve
#define cuMemCreate hipMemCreate
#define cuMemGetAllocationGranularity hipMemGetAllocationGranularity
#define cuMemGetHandleForAddressRange hipMemGetHandleForAddressRange
#define cuMemMap hipMemMap
#define cuMemRelease hipMemRelease
#define cuMemSetAccess hipMemSetAccess
#define cuMemUnmap hipMemUnmap
#define cuPointerGetAttribute hipPointerGetAttribute

#define CUDA_SUCCESS hipSuccess
#define cudaDeviceCanAccessPeer hipDeviceCanAccessPeer
#define cudaDeviceEnablePeerAccess hipDeviceEnablePeerAccess
#define cudaDeviceGetPCIBusId hipDeviceGetPCIBusId
#define cudaError_t hipError_t
#define cudaFree hipFree
#define cudaFreeHost hipHostFree
#define cudaGetDevice hipGetDevice
#define cudaGetDeviceCount hipGetDeviceCount
#define cudaGetErrorString hipGetErrorString
#define cudaGetLastError hipGetLastError
#define cudaHostRegister hipHostRegister
#define cudaHostRegisterPortable hipHostRegisterPortable
#define cudaHostUnregister hipHostUnregister
#define cudaMalloc hipMalloc
#define cudaMallocHost(ptr, size) hipHostMalloc(ptr, size, hipHostMallocDefault)
#define cudaMemcpy hipMemcpy
#define cudaMemcpyAsync hipMemcpyAsync
#define cudaMemcpyDefault hipMemcpyDefault
#define cudaMemcpyDeviceToHost hipMemcpyDeviceToHost
#define cudaMemcpyHostToDevice hipMemcpyHostToDevice
#define cudaMemset hipMemset
#define cudaMemsetAsync hipMemsetAsync
#define cudaMemoryTypeDevice hipMemoryTypeDevice
#define cudaMemoryTypeHost hipMemoryTypeHost
// cudaMemoryTypeUnregistered is currently not supported as hipMemoryType enum,
// due to HIP functionality backward compatibility.
#define cudaMemoryTypeUnregistered 99
#define cudaPointerAttributes hipPointerAttribute_t
#define cudaPointerGetAttributes hipPointerGetAttributes
#define cudaSetDevice hipSetDevice
#define cudaStreamCreate hipStreamCreate
#define cudaStreamDestroy hipStreamDestroy
#define cudaStreamSynchronize hipStreamSynchronize
#define cudaStream_t hipStream_t
#define cudaSuccess hipSuccess
