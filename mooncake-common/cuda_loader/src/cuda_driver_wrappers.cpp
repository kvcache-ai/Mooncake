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

#include <cuda.h>

#include "internal.h"

using cuda_loader::GetCudaDriverSymbols;

extern "C" {

// ---------- Device management ----------

CUresult cuDeviceGet(CUdevice* device, int ordinal) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuDeviceGet) return s.p_cuDeviceGet(device, ordinal);
    return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult cuDeviceGetAttribute(int* pi, CUdevice_attribute attrib,
                              CUdevice dev) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuDeviceGetAttribute)
        return s.p_cuDeviceGetAttribute(pi, attrib, dev);
    return CUDA_ERROR_NOT_INITIALIZED;
}

// ---------- Memory allocation (VMM) ----------

CUresult cuMemCreate(CUmemGenericAllocationHandle* handle, size_t size,
                     const CUmemAllocationProp* prop,
                     unsigned long long flags) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemCreate) return s.p_cuMemCreate(handle, size, prop, flags);
    return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult cuMemRelease(CUmemGenericAllocationHandle handle) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemRelease) return s.p_cuMemRelease(handle);
    return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult cuMemRetainAllocationHandle(CUmemGenericAllocationHandle* handle,
                                     void* addr) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemRetainAllocationHandle)
        return s.p_cuMemRetainAllocationHandle(handle, addr);
    return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult cuMemGetAllocationGranularity(
    size_t* granularity, const CUmemAllocationProp* prop,
    CUmemAllocationGranularity_flags option) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemGetAllocationGranularity)
        return s.p_cuMemGetAllocationGranularity(granularity, prop, option);
    return CUDA_ERROR_NOT_INITIALIZED;
}

// ---------- Virtual address management ----------

CUresult cuMemAddressReserve(CUdeviceptr* ptr, size_t size, size_t alignment,
                             CUdeviceptr addr, unsigned long long flags) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemAddressReserve)
        return s.p_cuMemAddressReserve(ptr, size, alignment, addr, flags);
    return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult cuMemAddressFree(CUdeviceptr ptr, size_t size) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemAddressFree) return s.p_cuMemAddressFree(ptr, size);
    return CUDA_ERROR_NOT_INITIALIZED;
}

// ---------- Mapping ----------

CUresult cuMemMap(CUdeviceptr ptr, size_t size, size_t offset,
                  CUmemGenericAllocationHandle handle,
                  unsigned long long flags) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemMap) return s.p_cuMemMap(ptr, size, offset, handle, flags);
    return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult cuMemUnmap(CUdeviceptr ptr, size_t size) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemUnmap) return s.p_cuMemUnmap(ptr, size);
    return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult cuMemSetAccess(CUdeviceptr ptr, size_t size,
                        const CUmemAccessDesc* desc, size_t count) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemSetAccess) return s.p_cuMemSetAccess(ptr, size, desc, count);
    return CUDA_ERROR_NOT_INITIALIZED;
}

// ---------- IPC / sharing ----------

CUresult cuMemExportToShareableHandle(void* shareableHandle,
                                      CUmemGenericAllocationHandle handle,
                                      CUmemAllocationHandleType handleType,
                                      unsigned long long flags) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemExportToShareableHandle)
        return s.p_cuMemExportToShareableHandle(shareableHandle, handle,
                                                handleType, flags);
    return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult cuMemImportFromShareableHandle(
    CUmemGenericAllocationHandle* handle, void* osHandle,
    CUmemAllocationHandleType shHandleType) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemImportFromShareableHandle)
        return s.p_cuMemImportFromShareableHandle(handle, osHandle,
                                                  shHandleType);
    return CUDA_ERROR_NOT_INITIALIZED;
}

// ---------- Address range queries ----------

CUresult cuMemGetAddressRange(CUdeviceptr* pbase, size_t* psize,
                              CUdeviceptr dptr) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemGetAddressRange)
        return s.p_cuMemGetAddressRange(pbase, psize, dptr);
    return CUDA_ERROR_NOT_INITIALIZED;
}

CUresult cuMemGetHandleForAddressRange(void* handle, CUdeviceptr dptr,
                                       size_t size,
                                       CUmemRangeHandleType handleType,
                                       unsigned long long flags) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuMemGetHandleForAddressRange)
        return s.p_cuMemGetHandleForAddressRange(handle, dptr, size, handleType,
                                                 flags);
    return CUDA_ERROR_NOT_INITIALIZED;
}

// ---------- Pointer attributes ----------

CUresult cuPointerGetAttribute(void* data, CUpointer_attribute attribute,
                               CUdeviceptr ptr) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuPointerGetAttribute)
        return s.p_cuPointerGetAttribute(data, attribute, ptr);
    return CUDA_ERROR_NOT_INITIALIZED;
}

// ---------- Error handling ----------

CUresult cuGetErrorString(CUresult error, const char** pStr) {
    auto& s = GetCudaDriverSymbols();
    if (s.p_cuGetErrorString) return s.p_cuGetErrorString(error, pStr);
    if (pStr) *pStr = "CUDA driver not available";
    return CUDA_ERROR_NOT_INITIALIZED;
}

}  // extern "C"
