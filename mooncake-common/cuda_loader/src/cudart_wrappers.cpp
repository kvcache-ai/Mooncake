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

#include <cuda_runtime_api.h>

#include "internal.h"

using cuda_loader::GetCudaRtSymbols;

extern "C" {

// ---------- Memory management ----------

cudaError_t cudaMalloc(void** devPtr, size_t size) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaMalloc) return s.p_cudaMalloc(devPtr, size);
    return cudaErrorNoDevice;
}

cudaError_t cudaFree(void* devPtr) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaFree) return s.p_cudaFree(devPtr);
    return cudaErrorNoDevice;
}

cudaError_t cudaMemcpy(void* dst, const void* src, size_t count,
                       enum cudaMemcpyKind kind) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaMemcpy) return s.p_cudaMemcpy(dst, src, count, kind);
    return cudaErrorNoDevice;
}

cudaError_t cudaMemcpyAsync(void* dst, const void* src, size_t count,
                            enum cudaMemcpyKind kind, cudaStream_t stream) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaMemcpyAsync)
        return s.p_cudaMemcpyAsync(dst, src, count, kind, stream);
    return cudaErrorNoDevice;
}

cudaError_t cudaMemset(void* devPtr, int value, size_t count) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaMemset) return s.p_cudaMemset(devPtr, value, count);
    return cudaErrorNoDevice;
}

cudaError_t cudaMemsetAsync(void* devPtr, int value, size_t count,
                            cudaStream_t stream) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaMemsetAsync)
        return s.p_cudaMemsetAsync(devPtr, value, count, stream);
    return cudaErrorNoDevice;
}

cudaError_t cudaMallocHost(void** ptr, size_t size) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaMallocHost) return s.p_cudaMallocHost(ptr, size);
    return cudaErrorNoDevice;
}

cudaError_t cudaHostAlloc(void** pHost, size_t size, unsigned int flags) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaHostAlloc) return s.p_cudaHostAlloc(pHost, size, flags);
    return cudaErrorNoDevice;
}

cudaError_t cudaFreeHost(void* ptr) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaFreeHost) return s.p_cudaFreeHost(ptr);
    return cudaErrorNoDevice;
}

cudaError_t cudaHostRegister(void* ptr, size_t size, unsigned int flags) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaHostRegister) return s.p_cudaHostRegister(ptr, size, flags);
    return cudaErrorNoDevice;
}

cudaError_t cudaHostUnregister(void* ptr) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaHostUnregister) return s.p_cudaHostUnregister(ptr);
    return cudaErrorNoDevice;
}

cudaError_t cudaHostGetDevicePointer(void** pDevice, void* pHost,
                                     unsigned int flags) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaHostGetDevicePointer)
        return s.p_cudaHostGetDevicePointer(pDevice, pHost, flags);
    return cudaErrorNoDevice;
}

// ---------- Device management ----------

cudaError_t cudaSetDevice(int device) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaSetDevice) return s.p_cudaSetDevice(device);
    return cudaErrorNoDevice;
}

cudaError_t cudaGetDevice(int* device) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaGetDevice) return s.p_cudaGetDevice(device);
    return cudaErrorNoDevice;
}

cudaError_t cudaGetDeviceCount(int* count) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaGetDeviceCount) return s.p_cudaGetDeviceCount(count);
    if (count) *count = 0;
    return cudaErrorNoDevice;
}

cudaError_t cudaDeviceGetAttribute(int* value, enum cudaDeviceAttr attr,
                                   int device) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaDeviceGetAttribute)
        return s.p_cudaDeviceGetAttribute(value, attr, device);
    return cudaErrorNoDevice;
}

cudaError_t cudaDeviceGetPCIBusId(char* pciBusId, int len, int device) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaDeviceGetPCIBusId)
        return s.p_cudaDeviceGetPCIBusId(pciBusId, len, device);
    return cudaErrorNoDevice;
}

cudaError_t cudaDeviceSynchronize(void) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaDeviceSynchronize) return s.p_cudaDeviceSynchronize();
    return cudaErrorNoDevice;
}

cudaError_t cudaDeviceCanAccessPeer(int* canAccessPeer, int device,
                                    int peerDevice) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaDeviceCanAccessPeer)
        return s.p_cudaDeviceCanAccessPeer(canAccessPeer, device, peerDevice);
    return cudaErrorNoDevice;
}

cudaError_t cudaDeviceEnablePeerAccess(int peerDevice, unsigned int flags) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaDeviceEnablePeerAccess)
        return s.p_cudaDeviceEnablePeerAccess(peerDevice, flags);
    return cudaErrorNoDevice;
}

// ---------- Pointer attributes ----------

cudaError_t cudaPointerGetAttributes(struct cudaPointerAttributes* attributes,
                                     const void* ptr) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaPointerGetAttributes)
        return s.p_cudaPointerGetAttributes(attributes, ptr);
    return cudaErrorNoDevice;
}

// ---------- Stream management ----------

cudaError_t cudaStreamCreate(cudaStream_t* pStream) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaStreamCreate) return s.p_cudaStreamCreate(pStream);
    return cudaErrorNoDevice;
}

cudaError_t cudaStreamCreateWithFlags(cudaStream_t* pStream,
                                      unsigned int flags) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaStreamCreateWithFlags)
        return s.p_cudaStreamCreateWithFlags(pStream, flags);
    return cudaErrorNoDevice;
}

cudaError_t cudaStreamDestroy(cudaStream_t stream) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaStreamDestroy) return s.p_cudaStreamDestroy(stream);
    return cudaErrorNoDevice;
}

cudaError_t cudaStreamSynchronize(cudaStream_t stream) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaStreamSynchronize) return s.p_cudaStreamSynchronize(stream);
    return cudaErrorNoDevice;
}

cudaError_t cudaStreamQuery(cudaStream_t stream) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaStreamQuery) return s.p_cudaStreamQuery(stream);
    return cudaErrorNoDevice;
}

cudaError_t cudaLaunchHostFunc(cudaStream_t stream, cudaHostFn_t fn,
                               void* userData) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaLaunchHostFunc)
        return s.p_cudaLaunchHostFunc(stream, fn, userData);
    return cudaErrorNoDevice;
}

// ---------- Event management ----------

cudaError_t cudaEventCreateWithFlags(cudaEvent_t* event, unsigned int flags) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaEventCreateWithFlags)
        return s.p_cudaEventCreateWithFlags(event, flags);
    return cudaErrorNoDevice;
}

cudaError_t cudaEventDestroy(cudaEvent_t event) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaEventDestroy) return s.p_cudaEventDestroy(event);
    return cudaErrorNoDevice;
}

cudaError_t cudaEventRecord(cudaEvent_t event, cudaStream_t stream) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaEventRecord) return s.p_cudaEventRecord(event, stream);
    return cudaErrorNoDevice;
}

cudaError_t cudaEventQuery(cudaEvent_t event) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaEventQuery) return s.p_cudaEventQuery(event);
    return cudaErrorNoDevice;
}

// ---------- IPC ----------

cudaError_t cudaIpcGetMemHandle(cudaIpcMemHandle_t* handle, void* devPtr) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaIpcGetMemHandle) return s.p_cudaIpcGetMemHandle(handle, devPtr);
    return cudaErrorNoDevice;
}

cudaError_t cudaIpcOpenMemHandle(void** devPtr, cudaIpcMemHandle_t handle,
                                 unsigned int flags) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaIpcOpenMemHandle)
        return s.p_cudaIpcOpenMemHandle(devPtr, handle, flags);
    return cudaErrorNoDevice;
}

cudaError_t cudaIpcCloseMemHandle(void* devPtr) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaIpcCloseMemHandle) return s.p_cudaIpcCloseMemHandle(devPtr);
    return cudaErrorNoDevice;
}

// ---------- Error handling ----------

cudaError_t cudaGetLastError(void) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaGetLastError) return s.p_cudaGetLastError();
    return cudaErrorNoDevice;
}

const char* cudaGetErrorString(cudaError_t error) {
    auto& s = GetCudaRtSymbols();
    if (s.p_cudaGetErrorString) return s.p_cudaGetErrorString(error);
    return "CUDA not available";
}

}  // extern "C"
