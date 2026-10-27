#include <cuda.h>

#include <cstdio>
#include <cstdlib>

__attribute__((constructor)) static void recordLoad() {
    const char* marker = std::getenv("MOONCAKE_TEST_CUDA_LOAD_MARKER");
    if (marker) {
        if (FILE* file = std::fopen(marker, "a")) {
            std::fputs("loaded\n", file);
            std::fclose(file);
        }
    }
}

extern "C" CUresult CUDAAPI cuInit(unsigned int flags) {
    return flags == 0 ? CUDA_SUCCESS : CUDA_ERROR_INVALID_VALUE;
}

#ifdef MISSING_CUDA_SYMBOLS
// An older ABI must never be used as a fallback for the header-selected v2.
#undef cuMemGetAddressRange
extern "C" CUresult CUDAAPI cuMemGetAddressRange(unsigned int*, unsigned int*,
                                                 unsigned int) {
    return CUDA_SUCCESS;
}
#else
extern "C" CUresult CUDAAPI cuMemGetAddressRange(CUdeviceptr* base,
                                                 size_t* size,
                                                 CUdeviceptr pointer) {
    *base = pointer - 16;
    *size = 8192;
    return CUDA_SUCCESS;
}

#if CUDA_VERSION >= 13000
extern "C" CUresult CUDAAPI cuCtxCreate(CUcontext* context, CUctxCreateParams*,
                                        unsigned int, CUdevice) {
#else
extern "C" CUresult CUDAAPI cuCtxCreate(CUcontext* context, unsigned int,
                                        CUdevice) {
#endif
    *context = reinterpret_cast<CUcontext>(0x1234);
    return CUDA_SUCCESS;
}
#endif
