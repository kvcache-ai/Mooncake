// Copyright 2026 KVCache.AI
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

#ifndef TENT_TRANSPORT_P2P_GPU_API_TRAITS_H_
#define TENT_TRANSPORT_P2P_GPU_API_TRAITS_H_

#include <cstddef>
#include <string>

#include <cuda_runtime.h>

#include "tent/common/status.h"

namespace mooncake {
namespace tent {

/// Traits that abstract the GPU runtime API (CUDA vs MUSA).
/// Each specialization provides:
///   - Error/status types and their queries
///   - Memory allocation/deallocation
///   - IPC handle operations
///   - Peer access management
///   - Device management
///   - A human-readable transport name for log messages

struct CudaApiTraits {
    static constexpr const char* kTransportName = "NVLink";

    // --- Types ---
    using ErrorType = cudaError_t;
    using IpcMemHandleType = cudaIpcMemHandle_t;

    // --- Success check ---
    static bool isError(cudaError_t err) { return err != cudaSuccess; }
    static const char* errorString(cudaError_t err) {
        return cudaGetErrorString(err);
    }

    // --- Memory ---
    static cudaError_t malloc(void** ptr, size_t size) {
        return cudaMalloc(ptr, size);
    }
    static cudaError_t free(void* ptr) { return cudaFree(ptr); }
    static cudaError_t mallocHost(void** ptr, size_t size) {
        return cudaMallocHost(ptr, size);
    }
    static cudaError_t freeHost(void* ptr) { return cudaFreeHost(ptr); }
    static cudaError_t memset(void* ptr, int value, size_t size) {
        return cudaMemset(ptr, value, size);
    }
    static cudaError_t memcpy(void* dst, const void* src, size_t size,
                              cudaMemcpyKind kind) {
        return cudaMemcpy(dst, src, size, kind);
    }
    static constexpr cudaMemcpyKind kMemcpyHostToDevice =
        cudaMemcpyHostToDevice;

    // --- IPC ---
    static cudaError_t ipcGetMemHandle(cudaIpcMemHandle_t* handle,
                                       void* ptr) {
        return cudaIpcGetMemHandle(handle, ptr);
    }
    static cudaError_t ipcOpenMemHandle(void** ptr,
                                        cudaIpcMemHandle_t handle) {
        return cudaIpcOpenMemHandle(ptr, handle,
                                    cudaIpcMemLazyEnablePeerAccess);
    }
    static cudaError_t ipcCloseMemHandle(void* ptr) {
        return cudaIpcCloseMemHandle(ptr);
    }
    static constexpr size_t kIpcHandleSize = sizeof(cudaIpcMemHandle_t);

    // --- Peer access ---
    static cudaError_t deviceCanAccessPeer(int* can_access, int device,
                                           int peer_device) {
        return cudaDeviceCanAccessPeer(can_access, device, peer_device);
    }
    static cudaError_t deviceEnablePeerAccess(int peer_device,
                                              unsigned int flags = 0) {
        return cudaDeviceEnablePeerAccess(peer_device, flags);
    }
    static constexpr cudaError_t kErrorPeerAccessAlreadyEnabled =
        cudaErrorPeerAccessAlreadyEnabled;
    static cudaError_t getLastError() { return cudaGetLastError(); }

    // --- Device management ---
    static cudaError_t getDevice(int* device) {
        return cudaGetDevice(device);
    }
    static cudaError_t setDevice(int device) {
        return cudaSetDevice(device);
    }
    static cudaError_t getDeviceCount(int* count) {
        return cudaGetDeviceCount(count);
    }

    // --- Status wrapper ---
    static Status checkStatus(cudaError_t err, const char* message) {
        if (err == cudaSuccess) return Status::OK();
        return Status::InternalError(std::string(message) + ": " +
                                     cudaGetErrorString(err));
    }
};

#ifdef MOONCAKE_EP_USE_MUSA

struct MusaApiTraits {
    static constexpr const char* kTransportName = "MTLink";

    // --- Types ---
    using ErrorType = musaError_t;
    using IpcMemHandleType = musaIpcMemHandle_t;

    // --- Success check ---
    static bool isError(musaError_t err) { return err != musaSuccess; }
    static const char* errorString(musaError_t err) {
        return musaGetErrorString(err);
    }

    // --- Memory ---
    static musaError_t malloc(void** ptr, size_t size) {
        return musaMalloc(ptr, size);
    }
    static musaError_t free(void* ptr) { return musaFree(ptr); }
    static musaError_t mallocHost(void** ptr, size_t size) {
        return musaMallocHost(ptr, size);
    }
    static musaError_t freeHost(void* ptr) { return musaFreeHost(ptr); }
    static musaError_t memset(void* ptr, int value, size_t size) {
        return musaMemset(ptr, value, size);
    }
    static musaError_t memcpy(void* dst, const void* src, size_t size,
                              musaMemcpyKind kind) {
        return musaMemcpy(dst, src, size, kind);
    }
    static constexpr musaMemcpyKind kMemcpyHostToDevice =
        musaMemcpyHostToDevice;

    // --- IPC ---
    static musaError_t ipcGetMemHandle(musaIpcMemHandle_t* handle,
                                       void* ptr) {
        return musaIpcGetMemHandle(handle, ptr);
    }
    static musaError_t ipcOpenMemHandle(void** ptr,
                                        musaIpcMemHandle_t handle) {
        return musaIpcOpenMemHandle(ptr, handle,
                                    musaIpcMemLazyEnablePeerAccess);
    }
    static musaError_t ipcCloseMemHandle(void* ptr) {
        return musaIpcCloseMemHandle(ptr);
    }
    static constexpr size_t kIpcHandleSize = sizeof(musaIpcMemHandle_t);

    // --- Peer access ---
    static musaError_t deviceCanAccessPeer(int* can_access, int device,
                                           int peer_device) {
        return musaDeviceCanAccessPeer(can_access, device, peer_device);
    }
    static musaError_t deviceEnablePeerAccess(int peer_device,
                                              unsigned int flags = 0) {
        return musaDeviceEnablePeerAccess(peer_device, flags);
    }
    static constexpr musaError_t kErrorPeerAccessAlreadyEnabled =
        musaErrorPeerAccessAlreadyEnabled;
    static musaError_t getLastError() { return musaGetLastError(); }

    // --- Device management ---
    static musaError_t getDevice(int* device) {
        return musaGetDevice(device);
    }
    static musaError_t setDevice(int device) {
        return musaSetDevice(device);
    }
    static musaError_t getDeviceCount(int* count) {
        return musaGetDeviceCount(count);
    }

    // --- Status wrapper ---
    static Status checkStatus(musaError_t err, const char* message) {
        if (err == musaSuccess) return Status::OK();
        return Status::InternalError(std::string(message) + ": " +
                                     musaGetErrorString(err));
    }
};

#endif  // MOONCAKE_EP_USE_MUSA

}  // namespace tent
}  // namespace mooncake

#endif  // TENT_TRANSPORT_P2P_GPU_API_TRAITS_H_
