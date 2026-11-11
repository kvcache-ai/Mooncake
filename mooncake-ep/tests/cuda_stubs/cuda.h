#pragma once

struct CUmod_st;
struct CUfunc_st;
struct CUctx_st;
struct CUstream_st;
using CUmodule = CUmod_st*;
using CUfunction = CUfunc_st*;
using CUcontext = CUctx_st*;
using CUstream = CUstream_st*;

enum CUresult { CUDA_SUCCESS = 0, CUDA_ERROR_UNKNOWN = 999 };
using CUjit_option = int;
using CUlaunchAttributeID = int;

constexpr CUlaunchAttributeID CU_LAUNCH_ATTRIBUTE_COOPERATIVE = 1;
constexpr CUlaunchAttributeID CU_LAUNCH_ATTRIBUTE_CLUSTER_DIMENSION = 2;
enum CUfunction_attribute {
    CU_FUNC_ATTRIBUTE_MAX_DYNAMIC_SHARED_SIZE_BYTES = 8
};

#define CU_LAUNCH_PARAM_BUFFER_POINTER reinterpret_cast<void*>(1)
#define CU_LAUNCH_PARAM_BUFFER_SIZE reinterpret_cast<void*>(2)
#define CU_LAUNCH_PARAM_END reinterpret_cast<void*>(0)

union CUlaunchAttributeValue {
    int cooperative;
    struct {
        unsigned int x;
        unsigned int y;
        unsigned int z;
    } clusterDim;
    char padding[64];
};

struct CUlaunchAttribute {
    CUlaunchAttributeID id;
    CUlaunchAttributeValue value;
};

struct CUlaunchConfig {
    unsigned int gridDimX;
    unsigned int gridDimY;
    unsigned int gridDimZ;
    unsigned int blockDimX;
    unsigned int blockDimY;
    unsigned int blockDimZ;
    unsigned int sharedMemBytes;
    CUstream hStream;
    CUlaunchAttribute* attrs;
    unsigned int numAttrs;
};

extern "C" {
CUresult cuGetErrorName(CUresult result, const char** name);
CUresult cuGetErrorString(CUresult result, const char** message);
CUresult cuCtxGetCurrent(CUcontext* context);
CUresult cuInit(unsigned int flags);
CUresult cuModuleLoadDataEx(CUmodule* module, const void* image,
                            unsigned int option_count, CUjit_option* options,
                            void** option_values);
CUresult cuModuleGetFunction(CUfunction* function, CUmodule module,
                             const char* name);
CUresult cuModuleUnload(CUmodule module);
CUresult cuFuncSetAttribute(CUfunction function, CUfunction_attribute attribute,
                            int value);
CUresult cuLaunchKernel(CUfunction function, unsigned int grid_x,
                        unsigned int grid_y, unsigned int grid_z,
                        unsigned int block_x, unsigned int block_y,
                        unsigned int block_z, unsigned int shared_bytes,
                        CUstream stream, void** params, void** extra);
CUresult cuLaunchKernelEx(const CUlaunchConfig* config, CUfunction function,
                          void** params, void** extra);
}
