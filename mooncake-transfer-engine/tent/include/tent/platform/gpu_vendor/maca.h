#pragma once

#include <string>
#include <mcr/maca.h>
#include <mcr/mc_runtime.h>
#include <mcr/mc_runtime_api.h>

// First-stage MACA integration: provide a CUDA-like API surface.
const static std::string GPU_PREFIX = "maca:";

#define CUdevice MCdevice
#define CUdeviceptr MCdeviceptr
#define CUmemorytype MCmemorytype
#define CUresult mcError_t
#define cuDeviceGet mcDeviceGet
#define cuDeviceGetAttribute mcDeviceGetAttribute
#define cuGetErrorString mcGetErrorString
#define cuMemAddressFree mcMemAddressFree
#define cuMemAddressReserve mcMemAddressReserve
#define cuMemCreate mcMemCreate
#define cuMemExportToShareableHandle mcMemExportToShareableHandle
#define cuMemGetAddressRange mcMemGetAddressRange
#define cuMemGetAllocationGranularity mcMemGetAllocationGranularity
#define cuMemGetHandleForAddressRange mcMemGetHandleForAddressRange
#define cuMemImportFromShareableHandle mcMemImportFromShareableHandle
#define cuMemMap mcMemMap
#define cuMemRelease mcMemRelease
#define cuMemRetainAllocationHandle mcMemRetainAllocationHandle
#define cuMemSetAccess mcMemSetAccess
#define cuMemUnmap mcMemUnmap
#define cuPointerGetAttribute mcPointerGetAttribute

#define CUDA_SUCCESS mcSuccess
#define CUDA_ERROR_NOT_PERMITTED mcErrorNotPermitted
#define CUDA_ERROR_NOT_SUPPORTED mcErrorNotSupported

#define cudaDeviceCanAccessPeer mcDeviceCanAccessPeer
#define cudaDeviceEnablePeerAccess mcDeviceEnablePeerAccess
#define cudaDeviceGetPCIBusId mcDeviceGetPCIBusId
#define cudaErrorPeerAccessAlreadyEnabled mcErrorPeerAccessAlreadyEnabled
#define cudaError_t mcError_t
#define cudaFree mcFree
#define cudaFreeHost mcFreeHost
#define cudaGetDevice mcGetDevice
#define cudaGetDeviceCount mcGetDeviceCount
#define cudaGetErrorString mcGetErrorString
#define cudaGetLastError mcGetLastError
#define cudaHostRegister mcHostRegister
#define cudaHostRegisterPortable mcHostRegisterPortable
#define cudaHostUnregister mcHostUnregister
#define cudaIpcCloseMemHandle mcIpcCloseMemHandle
#define cudaIpcGetMemHandle mcIpcGetMemHandle
#define cudaIpcMemHandle_t mcIpcMemHandle_t
#define cudaIpcMemLazyEnablePeerAccess mcIpcMemLazyEnablePeerAccess
#define cudaIpcOpenMemHandle mcIpcOpenMemHandle
#define cudaMalloc mcMalloc
#define cudaMallocHost mcMallocHost
#define cudaMemcpy mcMemcpy
#define cudaMemcpyAsync mcMemcpyAsync
#define cudaMemcpyDefault mcMemcpyDefault
#define cudaMemcpyDeviceToHost mcMemcpyDeviceToHost
#define cudaMemcpyHostToDevice mcMemcpyHostToDevice
#define cudaMemset mcMemset
#define cudaMemsetAsync mcMemsetAsync
#define cudaMemoryTypeDevice mcMemoryTypeDevice
#define cudaMemoryTypeHost mcMemoryTypeHost
#define cudaMemoryTypeUnregistered mcMemoryTypeUnregistered
#define cudaPointerAttributes mcPointerAttributes
#define cudaPointerGetAttributes mcPointerGetAttributes
#define cudaSetDevice mcSetDevice
#define cudaStreamCreate mcStreamCreate
#define cudaStreamDestroy mcStreamDestroy
#define cudaStreamSynchronize mcStreamSynchronize
#define cudaStream_t mcStream_t
#define cudaSuccess mcSuccess
