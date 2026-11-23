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

#include <cuda.h>
#include <cuda_runtime.h>

struct CUmod_st {};
struct CUfunc_st {};
struct CUctx_st {};

namespace {
CUmod_st module_storage;
CUfunc_st function_storage;
CUctx_st context_storage;
}  // namespace

extern "C" {
cudaError_t cudaGetDevice(int* device) {
    *device = 0;
    return cudaSuccess;
}

cudaError_t cudaFree(void*) { return cudaSuccess; }

cudaError_t cudaGetDeviceProperties(cudaDeviceProp* prop, int) {
    prop->major = 9;
    prop->minor = 0;
    return cudaSuccess;
}

CUresult cuGetErrorName(CUresult, const char** name) {
    *name = "CUDA_SUCCESS";
    return CUDA_SUCCESS;
}

CUresult cuGetErrorString(CUresult, const char** message) {
    *message = "success";
    return CUDA_SUCCESS;
}

CUresult cuCtxGetCurrent(CUcontext* context) {
    *context = &context_storage;
    return CUDA_SUCCESS;
}

CUresult cuInit(unsigned int) { return CUDA_SUCCESS; }

CUresult cuModuleLoadDataEx(CUmodule* module, const void*, unsigned int,
                            CUjit_option*, void**) {
    *module = &module_storage;
    return CUDA_SUCCESS;
}

CUresult cuModuleGetFunction(CUfunction* function, CUmodule, const char*) {
    *function = &function_storage;
    return CUDA_SUCCESS;
}

CUresult cuModuleUnload(CUmodule) { return CUDA_SUCCESS; }

CUresult cuFuncSetAttribute(CUfunction, CUfunction_attribute, int) {
    return CUDA_SUCCESS;
}

CUresult cuLaunchKernel(CUfunction, unsigned int, unsigned int, unsigned int,
                        unsigned int, unsigned int, unsigned int, unsigned int,
                        CUstream, void**, void**) {
    return CUDA_SUCCESS;
}

CUresult cuLaunchKernelEx(const CUlaunchConfig*, CUfunction, void**, void**) {
    return CUDA_SUCCESS;
}
}
