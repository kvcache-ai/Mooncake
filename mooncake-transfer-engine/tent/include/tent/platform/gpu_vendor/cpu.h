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
 * @file cpu.h
 * @brief CPU-only fallback for CUDA-like API
 *
 * Provides stub implementations of CUDA API calls for CPU-only mode.
 * All GPU operations are no-ops or return appropriate error codes.
 */

#ifndef TENT_PLATFORM_GPU_VENDOR_CPU_H
#define TENT_PLATFORM_GPU_VENDOR_CPU_H

#include <cstring>
#include <cstdlib>
#include <string>

#define CPU_ONLY_MODE
const static std::string GPU_PREFIX = "cpu:";

// ============================================================
// Error code mappings
// ============================================================

#define CUDA_SUCCESS 0
#define cudaSuccess 0
#define cudaError_t int
#define cudaErrorPeerAccessAlreadyEnabled 0

// ============================================================
// Device management (all no-ops for CPU)
// ============================================================

#define cudaGetDevice(ptr) (*(ptr) = 0, 0)
#define cudaSetDevice(dev) (0)
#define cudaGetDeviceCount(count) (*(count) = 0, 0)
#define cudaGetErrorString(msg) "CPU-only mode"
#define cudaGetLastError() (0)
#define cudaDeviceCanAccessPeer(...) (0)
#define cudaDeviceEnablePeerAccess(...) (-1)
#define cudaDeviceGetPCIBusId(...) (-1)

// ============================================================
// Memory management
// ============================================================

#define cudaMalloc(ptr, size) (-1)
#define cudaFree(ptr) (0)
#define cudaMallocHost(ptr, size) posix_memalign(ptr, 64, size)
#define cudaFreeHost(ptr) free(ptr)

#define cudaMemcpy(dst, src, len, kind) (memcpy(dst, src, len), 0)
#define cudaMemcpyAsync(dst, src, len, kind, stream) (memcpy(dst, src, len), 0)
#define cudaMemcpyDefault (0)
#define cudaMemcpyDeviceToHost (0)
#define cudaMemcpyHostToDevice (0)

#define cudaMemset(ptr, val, size) (memset(ptr, val, size), 0)
#define cudaMemsetAsync(ptr, val, size, stream) (memset(ptr, val, size), 0)

#define cudaMemoryTypeDevice (0)
#define cudaMemoryTypeHost (1)
#define cudaMemoryTypeUnregistered (2)

// ============================================================
// Stream management (all no-ops for CPU)
// ============================================================

#define cudaStream_t void*
#define cudaStreamCreate(ptr) (*(ptr) = nullptr, 0)
#define cudaStreamDestroy(stream) (0)
#define cudaStreamSynchronize(stream) (0)

// ============================================================
// Pointer attributes
// ============================================================

struct cudaPointerAttributes {
    int type;
    int device;
};

// CPU-only: just set the type to host and ignore the pointer
// Note: CUDA API is cudaPointerGetAttributes(const cudaPointerAttributes* attr,
// const void* ptr)
inline int cudaPointerGetAttributes_impl(
    const struct cudaPointerAttributes* attr, const void* ptr) {
    (void)ptr;
    const_cast<struct cudaPointerAttributes*>(attr)->type = cudaMemoryTypeHost;
    return 0;
}
#define cudaPointerGetAttributes(attr, ptr) \
    cudaPointerGetAttributes_impl(attr, ptr)

// ============================================================
// IPC memory (not supported in CPU-only mode)
// ============================================================

#define cudaIpcMemHandle_t void*
#define cudaIpcGetMemHandle(...) (-1)
#define cudaIpcOpenMemHandle(...) (-1)
#define cudaIpcCloseMemHandle(...) (0)
#define cudaIpcMemLazyEnablePeerAccess(...) (0)

// ============================================================
// Host memory registration (not supported in CPU-only mode)
// ============================================================

#define cudaHostRegister(...) (-1)
#define cudaHostRegisterPortable (0)
#define cudaHostUnregister(...) (-1)

#endif  // TENT_PLATFORM_GPU_VENDOR_CPU_H
