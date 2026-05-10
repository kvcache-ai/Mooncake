#pragma once

#include <string>

#include <tang_runtime_api.h>

// SunriseLink uses CUDA-style location naming in metadata.
const static std::string GPU_PREFIX = "cuda:";

// Minimal CUDA-like API shim for benchmark paths.
#define cudaError_t tangError_t
#define cudaSuccess tangSuccess
#define cudaGetErrorString tangGetErrorString

#define cudaSetDevice tangSetDevice
#define cudaGetDeviceCount tangGetDeviceCount

#define cudaMalloc tangMalloc
#define cudaFree tangFree
#define cudaMemset tangMemset

#define cudaPointerAttributes tangPointerAttributes
#define cudaPointerGetAttributes tangPointerGetAttributes
#define cudaMemoryTypeDevice tangMemoryTypeDevice
#define cudaMemoryTypeHost tangMemoryTypeHost
#define cudaMemoryTypeUnregistered tangMemoryTypeUnregistered

#define cudaStreamSynchronize tangStreamSynchronize
