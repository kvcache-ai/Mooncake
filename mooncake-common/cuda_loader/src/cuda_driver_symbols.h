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

#ifndef CUDA_LOADER_CUDA_DRIVER_SYMBOLS_H
#define CUDA_LOADER_CUDA_DRIVER_SYMBOLS_H

#include <cuda.h>

namespace cuda_loader {

struct CudaDriverSymbols {
    // Device management
    CUresult (*p_cuDeviceGet)(CUdevice*, int) = nullptr;
    CUresult (*p_cuDeviceGetAttribute)(int*, CUdevice_attribute,
                                       CUdevice) = nullptr;

    // Memory allocation (VMM)
    CUresult (*p_cuMemCreate)(CUmemGenericAllocationHandle*, size_t,
                              const CUmemAllocationProp*,
                              unsigned long long) = nullptr;
    CUresult (*p_cuMemRelease)(CUmemGenericAllocationHandle) = nullptr;
    CUresult (*p_cuMemRetainAllocationHandle)(CUmemGenericAllocationHandle*,
                                              void*) = nullptr;
    CUresult (*p_cuMemGetAllocationGranularity)(
        size_t*, const CUmemAllocationProp*,
        CUmemAllocationGranularity_flags) = nullptr;

    // Virtual address management
    CUresult (*p_cuMemAddressReserve)(CUdeviceptr*, size_t, size_t, CUdeviceptr,
                                      unsigned long long) = nullptr;
    CUresult (*p_cuMemAddressFree)(CUdeviceptr, size_t) = nullptr;

    // Mapping
    CUresult (*p_cuMemMap)(CUdeviceptr, size_t, size_t,
                           CUmemGenericAllocationHandle,
                           unsigned long long) = nullptr;
    CUresult (*p_cuMemUnmap)(CUdeviceptr, size_t) = nullptr;
    CUresult (*p_cuMemSetAccess)(CUdeviceptr, size_t, const CUmemAccessDesc*,
                                 size_t) = nullptr;

    // IPC / sharing
    CUresult (*p_cuMemExportToShareableHandle)(void*,
                                               CUmemGenericAllocationHandle,
                                               CUmemAllocationHandleType,
                                               unsigned long long) = nullptr;
    CUresult (*p_cuMemImportFromShareableHandle)(
        CUmemGenericAllocationHandle*, void*,
        CUmemAllocationHandleType) = nullptr;

    // Address range queries
    CUresult (*p_cuMemGetAddressRange)(CUdeviceptr*, size_t*,
                                       CUdeviceptr) = nullptr;
    CUresult (*p_cuMemGetHandleForAddressRange)(void*, CUdeviceptr, size_t,
                                                CUmemRangeHandleType,
                                                unsigned long long) = nullptr;

    // Pointer attributes
    CUresult (*p_cuPointerGetAttribute)(void*, CUpointer_attribute,
                                        CUdeviceptr) = nullptr;

    // Error handling
    CUresult (*p_cuGetErrorString)(CUresult, const char**) = nullptr;
};

}  // namespace cuda_loader

#endif  // CUDA_LOADER_CUDA_DRIVER_SYMBOLS_H
