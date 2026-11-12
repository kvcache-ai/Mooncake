// Copyright 2024 KVCache.AI
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

#pragma once

// Enflame GCU CUDA-like API surface.
//
// Maps the CUDA Runtime API (cuda*) to the TopsRider runtime (tops*) and the
// CUDA Driver API VMM subset (cuMem*, cuPointerGetAttribute) to the EF driver
// (efdrv*). The error type is unified on topsError_t: efdrv calls return
// EfError (same base values as cudaError_t), so they are wrapped to cast to
// CUresult.
//
// Required include paths: -I <tops>/include -I <tops>/include/efdrv

#include <string>

#include <driver_types.h>
#include <efdrv.h>
#include <tops_device_runtime_api.h>
#include <tops_runtime_api.h>

const static std::string GPU_PREFIX = "gcu:";

// ---- Core types ---------------------------------------------------------
typedef topsDevice_t CUdevice;
typedef EfDevicePtr CUdeviceptr;  // unsigned long long, matches CUDA
typedef topsError_t CUresult;
typedef topsError_t cudaError_t;
typedef topsStream_t cudaStream_t;
typedef topsEvent_t cudaEvent_t;
typedef topsIpcMemHandle_t cudaIpcMemHandle_t;
typedef topsPointerAttributes cudaPointerAttributes;
typedef topsDeviceProp cudaDeviceProp;
typedef topsFuncAttributes cudaFuncAttributes;
typedef topsMemoryType cudaMemoryType;
typedef topsMemoryType CUmemorytype;

// ---- VMM types (driver) ------------------------------------------------
typedef EfMemGenericAllocationHandle CUmemGenericAllocationHandle;
typedef EfMemFabricHandle CUmemFabricHandle;
typedef EfMemAllocationProp CUmemAllocationProp;
typedef EfMemAllocationHandleType CUmemAllocationHandleType;
typedef EfMemAccessDesc CUmemAccessDesc;
typedef EfMemAllocationType CUmemAllocationType;
typedef EfMemLocation CUmemLocation;
typedef EfMemLocationType CUmemLocationType;
typedef EfMemRangeHandleType CUmemRangeHandleType;
typedef EfMemAllocationGranularityFlags CUmemAllocationGranularity_flags;
typedef EfPointerAttribute CUpointer_attribute;

// ---- Driver API: device / context (tops*) ------------------------------
#define cuInit topsInit
#define cuDeviceGet topsDeviceGet
#define cuDeviceGetCount topsGetDeviceCount
#define cuDeviceGetAttribute topsDeviceGetAttribute
#define cuDeviceGetPCIBusId topsDeviceGetPCIBusId
#define cuDevicePrimaryCtxRetain topsDevicePrimaryCtxRetain
#define cuDevicePrimaryCtxRelease topsDevicePrimaryCtxRelease
#define cuCtxSetCurrent topsCtxSetCurrent

// ---- Driver API: VMM (efdrv*, wrapped for error type) ------------------
static inline CUresult cuMemAddressReserve(CUdeviceptr *ptr, size_t size,
                                           size_t alignment, CUdeviceptr addr,
                                           unsigned long long flags) {
    return (CUresult)efdrvMemAddressReserve(ptr, size, alignment, addr, flags);
}

static inline CUresult cuMemAddressFree(CUdeviceptr ptr, size_t size) {
    return (CUresult)efdrvMemAddressFree(ptr, size);
}

static inline CUresult cuMemCreate(CUmemGenericAllocationHandle *handle,
                                   size_t size, const CUmemAllocationProp *prop,
                                   unsigned long long flags) {
    return (CUresult)efdrvMemCreate(handle, size, prop, flags);
}

static inline CUresult cuMemRelease(CUmemGenericAllocationHandle handle) {
    return (CUresult)efdrvMemRelease(handle);
}

static inline CUresult cuMemMap(CUdeviceptr ptr, size_t size, size_t offset,
                                CUmemGenericAllocationHandle handle,
                                unsigned long long flags) {
    return (CUresult)efdrvMemMap(ptr, size, offset, handle, flags);
}

static inline CUresult cuMemUnmap(CUdeviceptr ptr, size_t size) {
    return (CUresult)efdrvMemUnmap(ptr, size);
}

static inline CUresult cuMemSetAccess(CUdeviceptr ptr, size_t size,
                                      const CUmemAccessDesc *desc,
                                      size_t count) {
    return (CUresult)efdrvMemSetAccess(ptr, size, desc, count);
}

static inline CUresult cuMemExportToShareableHandle(
    void *shareableHandle, CUmemGenericAllocationHandle handle,
    CUmemAllocationHandleType handleType, unsigned long long flags) {
    return (CUresult)efdrvMemExportToShareableHandle(shareableHandle, handle,
                                                     handleType, flags);
}

static inline CUresult cuMemImportFromShareableHandle(
    CUmemGenericAllocationHandle *handle, void *osHandle,
    CUmemAllocationHandleType shHandleType) {
    return (CUresult)efdrvMemImportFromShareableHandle(handle, osHandle,
                                                       shHandleType);
}

static inline CUresult cuMemGetAllocationGranularity(
    size_t *granularity, const CUmemAllocationProp *prop,
    CUmemAllocationGranularity_flags option) {
    return (CUresult)efdrvMemGetAllocationGranularity(granularity, prop,
                                                      option);
}

static inline CUresult cuMemRetainAllocationHandle(
    CUmemGenericAllocationHandle *handle, void *addr) {
    return (CUresult)efdrvMemRetainAllocationHandle(handle, addr);
}

static inline CUresult cuMemGetHandleForAddressRange(
    void *handle, CUdeviceptr dptr, size_t size,
    CUmemRangeHandleType handleType, unsigned long long flags) {
    return (CUresult)efdrvMemGetHandleForAddressRange(handle, dptr, size,
                                                      handleType, flags);
}

static inline CUresult cuMemGetAddressRange(CUdeviceptr *pbase, size_t *psize,
                                            CUdeviceptr dptr) {
    return (CUresult)efdrvMemGetAddressRange(pbase, psize, dptr);
}

static inline CUresult cuPointerGetAttribute(void *data,
                                             CUpointer_attribute attribute,
                                             CUdeviceptr ptr) {
    return (CUresult)efdrvPointerGetAttribute(
        data, (EfPointerAttribute)attribute, ptr);
}

// ---- Error string helpers ----------------------------------------------
static inline CUresult cuGetErrorString(CUresult error, const char **err_str) {
    if (err_str) *err_str = topsGetErrorString(error);
    return topsSuccess;
}

static inline CUresult cuGetErrorName(CUresult error, const char **err_name) {
    if (err_name) *err_name = topsGetErrorName(error);
    return topsSuccess;
}

// ---- VMM / device constants --------------------------------------------
#define CU_MEM_ALLOCATION_TYPE_PINNED EF_MEM_ALLOCATION_TYPE_PINNED
#define CU_MEM_LOCATION_TYPE_DEVICE EF_MEM_LOCATION_TYPE_DEVICE
#define CU_MEM_HANDLE_TYPE_FABRIC EF_MEM_HANDLE_TYPE_FABRIC
#define CU_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR \
    EF_MEM_HANDLE_TYPE_POSIX_FILE_DESCRIPTOR
#define CU_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED \
    EF_DEVICE_ATTRIBUTE_HANDLE_TYPE_FABRIC_SUPPORTED
#define CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_WITH_CUDA_VMM_SUPPORTED \
    EF_DEVICE_ATTRIBUTE_GCU_DIRECT_RDMA_WITH_EF_VMM_SUPPORTED
#define CU_MEM_ACCESS_FLAGS_PROT_READWRITE EF_MEM_ACCESS_FLAGS_PROT_READWRITE
#define CU_MEM_ALLOC_GRANULARITY_MINIMUM EF_MEM_ALLOC_GRANULARITY_MINIMUM
#define CU_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD EF_MEM_RANGE_HANDLE_TYPE_DMA_BUF_FD
#define CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED \
    EF_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED
#define CU_POINTER_ATTRIBUTE_MEMORY_TYPE EF_POINTER_ATTRIBUTE_MEMORY_TYPE
#define CU_POINTER_ATTRIBUTE_DEVICE_ORDINAL EF_POINTER_ATTRIBUTE_DEVICE_ORDINAL
#define CU_POINTER_ATTRIBUTE_RANGE_START_ADDR \
    EF_POINTER_ATTRIBUTE_RANGE_START_ADDR
#define CU_POINTER_ATTRIBUTE_RANGE_SIZE EF_POINTER_ATTRIBUTE_RANGE_SIZE
#define CU_MEMORYTYPE_HOST topsMemoryTypeHost
#define CU_MEMORYTYPE_DEVICE topsMemoryTypeDevice

// ---- Error constants ----------------------------------------------------
#define CUDA_SUCCESS topsSuccess
#define CUDA_ERROR_NOT_PERMITTED topsErrorNotPermitted
#define CUDA_ERROR_NOT_SUPPORTED topsErrorNotSupported
#define cudaSuccess topsSuccess
#define cudaErrorNotReady topsErrorNotReady
#define cudaErrorPeerAccessAlreadyEnabled topsErrorPeerAccessAlreadyEnabled

// ---- Runtime API (cuda* -> tops*) --------------------------------------
#define cudaDeviceCanAccessPeer topsDeviceCanAccessPeer
#define cudaDeviceEnablePeerAccess topsDeviceEnablePeerAccess
#define cudaDeviceGetAttribute topsDeviceGetAttribute
#define cudaDeviceGetPCIBusId topsDeviceGetPCIBusId
#define cudaDeviceSynchronize topsDeviceSynchronize
#define cudaDeviceGetStreamPriorityRange topsDeviceGetStreamPriorityRange
#define cudaStreamCreateWithPriority topsStreamCreateWithPriority
#define cudaStreamCaptureStatus topsStreamCaptureStatus
#define cudaStreamCaptureStatusNone topsStreamCaptureStatusNone
#define cudaStreamIsCapturing topsStreamIsCapturing
#define cudaDevAttrMultiProcessorCount topsDevAttrMultiProcessorCount
#define cudaDevAttrClockRate topsDevAttrClockRate
#define cudaEventCreate topsEventCreate
#define cudaEventCreateWithFlags topsEventCreateWithFlags
#define cudaEventDestroy topsEventDestroy
#define cudaEventDisableTiming topsEventDisableTiming
#define cudaEventElapsedTime topsEventElapsedTime
#define cudaEventQuery topsEventQuery
#define cudaEventRecord topsEventRecord
#define cudaEventSynchronize topsEventSynchronize
#define cudaFree topsFree
#define cudaFreeHost topsFreeHost
#define cudaGetDevice topsGetDevice
#define cudaGetDeviceCount topsGetDeviceCount
#define cudaGetErrorString topsGetErrorString
#define cudaGetLastError topsGetLastError
#define cudaGetDeviceProperties topsGetDeviceProperties
#define cudaHostAlloc topsHostAlloc
#define cudaHostAllocDefault topsHostAllocDefault
#define cudaHostAllocMapped topsHostAllocMapped
#define cudaHostAllocPortable topsHostAllocPortable
#define cudaHostAllocWriteCombined topsHostAllocWriteCombined
#define cudaHostRegister topsHostRegister
#define cudaHostRegisterIoMemory topsHostRegisterIoMemory
#define cudaHostRegisterMapped topsHostRegisterMapped
#define cudaHostRegisterPortable topsHostRegisterPortable
#define cudaHostUnregister topsHostUnregister
#define cudaHostGetDevicePointer topsHostGetDevicePointer
#define cudaIpcCloseMemHandle topsIpcCloseMemHandle
#define cudaIpcGetMemHandle topsIpcGetMemHandle
#define cudaIpcOpenMemHandle topsIpcOpenMemHandle
#define cudaMalloc topsMalloc
#define cudaMallocHost topsMallocHost
#define cudaMemcpy topsMemcpy
#define cudaMemcpyAsync topsMemcpyAsync
#define cudaMemcpyDefault topsMemcpyDefault
#define cudaMemcpyDeviceToDevice topsMemcpyDeviceToDevice
#define cudaMemcpyDeviceToHost topsMemcpyDeviceToHost
#define cudaMemcpyHostToDevice topsMemcpyHostToDevice
#define cudaMemcpyKind topsMemcpyKind
#define cudaMemset topsMemset
#define cudaMemsetAsync topsMemsetAsync
#define cudaMemoryTypeDevice topsMemoryTypeDevice
#define cudaMemoryTypeHost topsMemoryTypeHost
#define cudaMemoryTypeUnregistered topsMemoryTypeUnregistered
#define cudaPointerGetAttributes topsPointerGetAttributes
#define cudaSetDevice topsSetDevice
#define cudaStreamCreate topsStreamCreate
#define cudaStreamCreateWithFlags topsStreamCreateWithFlags
#define cudaStreamDestroy topsStreamDestroy
#define cudaStreamNonBlocking topsStreamNonBlocking
#define cudaStreamPerThread topsStreamPerThread
#define cudaStreamQuery topsStreamQuery
#define cudaStreamSynchronize topsStreamSynchronize
#define cudaStreamWaitEvent topsStreamWaitEvent
