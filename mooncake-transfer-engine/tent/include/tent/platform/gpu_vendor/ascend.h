// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
 * @file ascend.h
 * @brief Ascend NPU vendor abstraction for CUDA-like API
 *
 * Maps CUDA API calls to Ascend ACL (Ascend Computing Language) APIs.
 */

#ifndef TENT_PLATFORM_GPU_VENDOR_ASCEND_H
#define TENT_PLATFORM_GPU_VENDOR_ASCEND_H

#include <acl/acl.h>
#include <acl/acl_rt.h>
#include <string>

const static std::string GPU_PREFIX = "ascend:";

// ============================================================
// Error code mappings
// ============================================================

#define CUDA_SUCCESS ACL_ERROR_NONE
#define cudaSuccess ACL_ERROR_NONE
#define cudaError_t int
#define cudaErrorPeerAccessAlreadyEnabled ACL_ERROR_NONE

// ============================================================
// Device management
// ============================================================

#define cudaGetDevice aclrtGetDevice
#define cudaSetDevice aclrtSetDevice
#define cudaGetDeviceCount aclrtGetDeviceCount
#define cudaGetErrorString aclGetRecentErrMsg
#define cudaGetLastError aclGetRecentErrMsg
#define cudaDeviceCanAccessPeer(...) (1)
#define cudaDeviceEnablePeerAccess(...) (ACL_ERROR_NONE)
#define cudaDeviceGetPCIBusId(...) (ACL_ERROR_NOT_SUPPORTED)

// ============================================================
// Memory management
// ============================================================

#define cudaMalloc aclrtMalloc
#define cudaFree aclrtFree
#define cudaMallocHost aclrtMallocHost
#define cudaFreeHost aclrtFreeHost
#define cudaMemcpy aclrtMemcpy
#define cudaMemcpyAsync aclrtMemcpyAsync
#define cudaMemcpyDefault ACL_MEMCPY_DEVICE_TO_DEVICE
#define cudaMemcpyDeviceToHost ACL_MEMCPY_DEVICE_TO_HOST
#define cudaMemcpyHostToDevice ACL_MEMCPY_HOST_TO_DEVICE
#define cudaMemset aclrtMemset
#define cudaMemsetAsync aclrtMemsetAsync

#define cudaMemoryTypeDevice 1
#define cudaMemoryTypeHost 0
#define cudaMemoryTypeUnregistered 2

// ============================================================
// Stream management
// ============================================================

#define cudaStream_t aclrtStream
#define cudaStreamCreate aclrtCreateStream
#define cudaStreamDestroy aclrtDestroyStream
#define cudaStreamSynchronize aclrtSynchronizeStream

// ============================================================
// Pointer attributes
// ============================================================

struct cudaPointerAttributes {
    int type;
    int device;
};

// Ascend doesn't have cudaPointerGetAttributes, use a stub
inline int cudaPointerGetAttributes_ascend(
    const struct cudaPointerAttributes* attr, const void* ptr) {
    (void)ptr;
    aclrtMemBarrier(0, 0, 0, nullptr);
    const_cast<struct cudaPointerAttributes*>(attr)->type = cudaMemoryTypeHost;
    return ACL_ERROR_NOT_SUPPORTED;
}
#define cudaPointerGetAttributes(attr, ptr) \
    cudaPointerGetAttributes_ascend(attr, ptr)

// ============================================================
// IPC memory
// ============================================================

#define cudaIpcMemHandle_t void*
#define cudaIpcGetMemHandle(...) (ACL_ERROR_NOT_SUPPORTED)
#define cudaIpcOpenMemHandle(...) (ACL_ERROR_NOT_SUPPORTED)
#define cudaIpcCloseMemHandle(...) (ACL_ERROR_NOT_SUPPORTED)
#define cudaIpcMemLazyEnablePeerAccess(...) (ACL_ERROR_NONE)

// ============================================================
// Host memory registration
// ============================================================

#define cudaHostRegister(...) (ACL_ERROR_NOT_SUPPORTED)
#define cudaHostRegisterPortable 0
#define cudaHostUnregister(...) (ACL_ERROR_NOT_SUPPORTED)

// ============================================================
// Driver API types
// ============================================================

#define CU_DEVICE int32_t
#define CU_DEVICE_PTR void*
#define CUresult int
#define CUdevice int
#define CUdeviceptr void*

#endif  // TENT_PLATFORM_GPU_VENDOR_ASCEND_H
