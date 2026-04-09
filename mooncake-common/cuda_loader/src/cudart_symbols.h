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

#ifndef CUDA_LOADER_CUDART_SYMBOLS_H
#define CUDA_LOADER_CUDART_SYMBOLS_H

#include <cuda_runtime_api.h>

namespace cuda_loader {

struct CudaRtSymbols {
    // Memory management
    cudaError_t (*p_cudaMalloc)(void**, size_t) = nullptr;
    cudaError_t (*p_cudaFree)(void*) = nullptr;
    cudaError_t (*p_cudaMemcpy)(void*, const void*, size_t,
                                enum cudaMemcpyKind) = nullptr;
    cudaError_t (*p_cudaMemcpyAsync)(void*, const void*, size_t,
                                     enum cudaMemcpyKind,
                                     cudaStream_t) = nullptr;
    cudaError_t (*p_cudaMemset)(void*, int, size_t) = nullptr;
    cudaError_t (*p_cudaMemsetAsync)(void*, int, size_t,
                                     cudaStream_t) = nullptr;
    cudaError_t (*p_cudaMallocHost)(void**, size_t) = nullptr;
    cudaError_t (*p_cudaHostAlloc)(void**, size_t, unsigned int) = nullptr;
    cudaError_t (*p_cudaFreeHost)(void*) = nullptr;
    cudaError_t (*p_cudaHostRegister)(void*, size_t, unsigned int) = nullptr;
    cudaError_t (*p_cudaHostUnregister)(void*) = nullptr;
    cudaError_t (*p_cudaHostGetDevicePointer)(void**, void*,
                                              unsigned int) = nullptr;

    // Device management
    cudaError_t (*p_cudaSetDevice)(int) = nullptr;
    cudaError_t (*p_cudaGetDevice)(int*) = nullptr;
    cudaError_t (*p_cudaGetDeviceCount)(int*) = nullptr;
    cudaError_t (*p_cudaDeviceGetAttribute)(int*, enum cudaDeviceAttr,
                                            int) = nullptr;
    cudaError_t (*p_cudaDeviceGetPCIBusId)(char*, int, int) = nullptr;
    cudaError_t (*p_cudaDeviceSynchronize)(void) = nullptr;
    cudaError_t (*p_cudaDeviceCanAccessPeer)(int*, int, int) = nullptr;
    cudaError_t (*p_cudaDeviceEnablePeerAccess)(int, unsigned int) = nullptr;

    // Pointer attributes
    cudaError_t (*p_cudaPointerGetAttributes)(struct cudaPointerAttributes*,
                                              const void*) = nullptr;

    // Stream management
    cudaError_t (*p_cudaStreamCreate)(cudaStream_t*) = nullptr;
    cudaError_t (*p_cudaStreamCreateWithFlags)(cudaStream_t*,
                                               unsigned int) = nullptr;
    cudaError_t (*p_cudaStreamDestroy)(cudaStream_t) = nullptr;
    cudaError_t (*p_cudaStreamSynchronize)(cudaStream_t) = nullptr;
    cudaError_t (*p_cudaStreamQuery)(cudaStream_t) = nullptr;
    cudaError_t (*p_cudaLaunchHostFunc)(cudaStream_t, cudaHostFn_t,
                                        void*) = nullptr;

    // Event management
    cudaError_t (*p_cudaEventCreateWithFlags)(cudaEvent_t*,
                                              unsigned int) = nullptr;
    cudaError_t (*p_cudaEventDestroy)(cudaEvent_t) = nullptr;
    cudaError_t (*p_cudaEventRecord)(cudaEvent_t, cudaStream_t) = nullptr;
    cudaError_t (*p_cudaEventQuery)(cudaEvent_t) = nullptr;

    // IPC
    cudaError_t (*p_cudaIpcGetMemHandle)(cudaIpcMemHandle_t*, void*) = nullptr;
    cudaError_t (*p_cudaIpcOpenMemHandle)(void**, cudaIpcMemHandle_t,
                                          unsigned int) = nullptr;
    cudaError_t (*p_cudaIpcCloseMemHandle)(void*) = nullptr;

    // Error handling
    cudaError_t (*p_cudaGetLastError)(void) = nullptr;
    const char* (*p_cudaGetErrorString)(cudaError_t) = nullptr;
};

}  // namespace cuda_loader

#endif  // CUDA_LOADER_CUDART_SYMBOLS_H
