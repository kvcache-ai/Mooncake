#pragma once

// cudaMemcpyBatchAsync appeared in CUDA 12.8. A CUDA 12 build that calls it
// directly records an undefined cudaMemcpyBatchAsync@libcudart.so.12, so the
// library fails to load ("undefined symbol") whenever the process already
// has an older libcudart.so.12 -- for example after importing a PyTorch
// built for CUDA 12.6, which brings its own 12.6 runtime. CUDA 12.8-12.9
// builds therefore look the function up at run time, in the libcudart this
// library is bound to, and fall back to one cudaMemcpyAsync per copy when
// that runtime predates 12.8. Every CUDA 13 runtime has it, so CUDA 13
// builds keep calling it directly.
//
// Include this after the CUDA runtime header (cuda_runtime.h, or
// cuda_alike.h). It is empty for non-CUDA builds and for CUDA < 12.8.

#if defined(CUDART_VERSION) && CUDART_VERSION >= 12080 && CUDART_VERSION < 13000

#include <dlfcn.h>

#include <cstddef>

namespace mooncake {

using CudaMemcpyBatchAsyncFn = cudaError_t (*)(void **, void **, size_t *,
                                               size_t, cudaMemcpyAttributes *,
                                               size_t *, size_t, size_t *,
                                               cudaStream_t);

// Null when the libcudart.so.12 in use is older than 12.8.
inline CudaMemcpyBatchAsyncFn loadCudaMemcpyBatchAsync() {
    static const CudaMemcpyBatchAsyncFn fn = []() -> CudaMemcpyBatchAsyncFn {
        // Look only in the runtime our other cudart calls resolve to, never
        // in some other libcudart that happens to be loaded.
        Dl_info info{};
        if (dladdr(reinterpret_cast<void *>(&cudaRuntimeGetVersion), &info) ==
                0 ||
            info.dli_fname == nullptr) {
            return nullptr;
        }
        void *handle = dlopen(info.dli_fname, RTLD_NOLOAD | RTLD_LAZY);
        if (handle == nullptr) return nullptr;
        void *sym = dlsym(handle, "cudaMemcpyBatchAsync");
        dlclose(handle);
        return reinterpret_cast<CudaMemcpyBatchAsyncFn>(sym);
    }();
    return fn;
}

// Same contract as the CUDA 12.8 cudaMemcpyBatchAsync. Without it, each copy
// is submitted with cudaMemcpyAsync on `stream`; on an error, *failIdx is the
// index of the first copy that was not submitted.
inline cudaError_t cudaMemcpyBatchAsyncCompat(void **dsts, void **srcs,
                                              size_t *sizes, size_t count,
                                              cudaMemcpyAttributes *attrs,
                                              size_t *attrsIdxs,
                                              size_t numAttrs, size_t *failIdx,
                                              cudaStream_t stream) {
    if (CudaMemcpyBatchAsyncFn fn = loadCudaMemcpyBatchAsync()) {
        return fn(dsts, srcs, sizes, count, attrs, attrsIdxs, numAttrs, failIdx,
                  stream);
    }
    for (size_t i = 0; i < count; ++i) {
        cudaError_t err = cudaMemcpyAsync(dsts[i], srcs[i], sizes[i],
                                          cudaMemcpyDefault, stream);
        if (err != cudaSuccess) {
            if (failIdx != nullptr) *failIdx = i;
            return err;
        }
    }
    return cudaSuccess;
}

}  // namespace mooncake

#endif
