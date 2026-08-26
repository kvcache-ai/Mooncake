#pragma once

// CUDA introduced the Fabric Memory handle API in CUDA 12.3. Mooncake can
// still be built with older CUDA toolkits (for example, CUDA 12.0/12.2
// runtime headers), while the driver API entry points used below are generic
// and can safely be probed at runtime. Keep the ABI-visible values here so
// those toolkits compile; an old driver will report the Fabric capability as
// unsupported and the transport will use its normal IPC/fallback path.
//
// This header must be included after cuda.h. It is intentionally limited to
// NVIDIA CUDA-like builds: HIP/MUSA/MACA provide their own compatibility
// mappings in gpu_vendor/*.h.

#if (defined(USE_CUDA) || defined(USE_HYGON) || defined(USE_COREX)) && \
    defined(CUDA_VERSION) && CUDA_VERSION < 12030

// CU_IPC_HANDLE_SIZE is present in the CUDA 12.0 driver headers. Keep a
// fallback for vendor CUDA-like headers that omit it.
#ifndef CU_IPC_HANDLE_SIZE
#define CU_IPC_HANDLE_SIZE 64
#endif

// Opaque Fabric handles have the same 64-byte wire representation as CUDA IPC
// handles. This is the definition used by CUDA 12.3+.
typedef struct CUmemFabricHandle_st {
    unsigned char data[CU_IPC_HANDLE_SIZE];
} CUmemFabricHandle_v1;
typedef CUmemFabricHandle_v1 CUmemFabricHandle;

// Numeric values are part of the CUDA driver ABI and were assigned before the
// declarations appeared in cuda.h.
#ifndef CU_MEM_HANDLE_TYPE_FABRIC
#define CU_MEM_HANDLE_TYPE_FABRIC ((CUmemAllocationHandleType)0x8)
#endif

#ifndef CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED
#define CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED \
    ((CUdevice_attribute)128)
#endif

#endif  // CUDA-like build with pre-12.3 CUDA headers
