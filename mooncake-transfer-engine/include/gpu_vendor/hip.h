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
#else
// Note: HIP does not currently support multi-node (fabric-based) transfers.
// Use POSIX file descriptor handle type for now, which only supports
// intra-node (same-machine) memory sharing between processes.
// TODO: Change to appropriate handle type when HIP adds multi-node support.
#define CU_MEM_HANDLE_TYPE_FABRIC hipMemHandleTypePosixFileDescriptor
#endif

// hipify-perl warning: unsupported HIP identifier:
// CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED
#define CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED \
    hipDeviceAttributeVirtualMemoryManagementSupported

// hipify-perl warning: unsupported HIP identifier: CUmemFabricHandle
#define CUmemFabricHandle void*

// HIP mappings for nvlink_allocator compatibility
// Map CUDA symbols to closest HIP equivalents
#define CUDA_ERROR_NOT_PERMITTED hipErrorNotSupported
#define CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED \
    hipDeviceAttributeVirtualMemoryManagementSupported
