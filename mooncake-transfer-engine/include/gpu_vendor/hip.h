#pragma once

#include <hip/hip_runtime.h>
#include <string>

const static std::string GPU_PREFIX = "hip:";

// hipify-perl warning: unsupported HIP identifier: cudaMemoryTypeUnregistered
#define cudaMemoryTypeUnregistered hipMemoryTypeUnregistered

// hipify-perl warning: unsupported HIP identifier: CU_MEM_HANDLE_TYPE_FABRIC
#ifdef USE_HYLINK
// Hygon DTK extends the HIP shareable-handle API with a real fabric handle
// type for hylink cross-machine sharing. 0x8 is outside the enum's
// declared range, so spell it as a cast instead of an enum constant.
#define CU_MEM_HANDLE_TYPE_FABRIC static_cast<hipMemAllocationHandleType>(0x8)
#elif defined(MC_HIP_HAS_FABRIC_HANDLE)
// Runtimes with hipMemHandleTypeFabric (detected at configure time) share
// memory across hosts of one UALink vPOD; older ones fall back to POSIX fds,
// which only work between processes on the same machine.
#define CU_MEM_HANDLE_TYPE_FABRIC hipMemHandleTypeFabric
#else
#define CU_MEM_HANDLE_TYPE_FABRIC hipMemHandleTypePosixFileDescriptor
#endif

// hipify-perl warning: unsupported HIP identifier:
// CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED
#define CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED \
    hipDeviceAttributeVirtualMemoryManagementSupported

// hipify-perl warning: unsupported HIP identifier: CUmemFabricHandle
#ifdef MC_HIP_HAS_FABRIC_HANDLE
#define CUmemFabricHandle hipMemFabricHandle_t
#else
#define CUmemFabricHandle void*
#endif

// HIP mappings for nvlink_allocator compatibility
// Map CUDA symbols to closest HIP equivalents
#define CUDA_ERROR_NOT_PERMITTED hipErrorNotSupported
#ifdef MC_HIP_HAS_FABRIC_HANDLE
#define CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED \
    hipDeviceAttributeHandleTypeFabricSupported
#else
#define CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED \
    hipDeviceAttributeVirtualMemoryManagementSupported
#endif
