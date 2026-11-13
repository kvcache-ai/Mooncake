#include "cuda_driver.h"

// Importing this library must not load CUDA until the entry point is called.
extern "C" CUresult lazy_cuda_probe() {
    return MOONCAKE_CUDA_SYMBOL(cuInit)(0);
}
