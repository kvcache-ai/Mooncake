#pragma once

struct CUstream_st;
using cudaStream_t = CUstream_st*;

enum cudaError { cudaSuccess = 0 };
using cudaError_t = enum cudaError;

struct cudaDeviceProp {
    int major;
    int minor;
};

extern "C" {
cudaError_t cudaGetDevice(int* device);
cudaError_t cudaFree(void* pointer);
cudaError_t cudaGetDeviceProperties(cudaDeviceProp* prop, int device);
}
