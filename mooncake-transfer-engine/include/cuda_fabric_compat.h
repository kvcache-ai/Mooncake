#pragma once

// CUDA introduced the Fabric memory handle declarations in CUDA 12.3.  The
// driver entry points are present in older headers, but the declarations below
// are still needed to compile code that probes the optional capability.  Keep
// the compatibility definitions limited to old CUDA headers; newer toolkits
// provide the canonical declarations themselves.
#if (defined(USE_CUDA) || defined(USE_HYGON) || defined(USE_COREX)) && \
    defined(CUDA_VERSION) && CUDA_VERSION < 12030

#ifndef CU_IPC_HANDLE_SIZE
#define CU_IPC_HANDLE_SIZE 64
#endif

#ifndef CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED
#define CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED ((CUdevice_attribute)128)
#endif

#ifndef CU_MEM_HANDLE_TYPE_FABRIC
#define CU_MEM_HANDLE_TYPE_FABRIC ((CUmemAllocationHandleType)0x8)
#endif

typedef struct CUmemFabricHandle_st {
    unsigned char data[CU_IPC_HANDLE_SIZE];
} CUmemFabricHandle_v1;
typedef CUmemFabricHandle_v1 CUmemFabricHandle;

#endif
