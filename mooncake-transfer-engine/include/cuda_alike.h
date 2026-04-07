#pragma once

#ifdef USE_CUDA
#include <cuda.h>
#include <cuda_runtime.h>
#include "cuda_loader/cuda_loader.h"
#elif defined(USE_HIP)
#include "gpu_vendor/hip.h"
#elif defined(USE_MUSA)
#include "gpu_vendor/musa.h"
#elif defined(USE_MLU)
#include "gpu_vendor/mlu.h"
#elif defined(USE_UBSHMEM)
#include "gpu_vendor/ubshmem.h"
#elif defined(USE_MACA)
#include "gpu_vendor/maca.h"
#endif

#if !defined(USE_HIP) && !defined(USE_MUSA) && !defined(USE_MLU) && \
    !defined(USE_UBSHMEM) && !defined(USE_MACA)
#include <string>
const static std::string GPU_PREFIX = "cuda:";
#endif

// Runtime GPU availability check.
// Call inside #ifdef USE_CUDA / USE_HIP / ... blocks.
// - CUDA: dynamic check via cuda_loader (dlopen)
// - HIP/MUSA/UBSHMEM/MACA: always true (statically linked)
// - No GPU vendor: always false (unreachable if #ifdef is correct)
static inline bool gpu_runtime_available() {
#if defined(USE_CUDA)
    return cuda_loader_is_available() != 0;
#elif defined(USE_HIP) || defined(USE_MUSA) || defined(USE_UBSHMEM) || \
    defined(USE_MACA) || defined(USE_MLU)
    return true;
#else
    return false;
#endif
}

// Runtime GPU driver API availability check.
// For cu* (driver API) functions.
static inline bool gpu_driver_available() {
#if defined(USE_CUDA)
    return cuda_loader_driver_available() != 0;
#elif defined(USE_HIP) || defined(USE_MUSA) || defined(USE_UBSHMEM) || \
    defined(USE_MACA) || defined(USE_MLU)
    return true;
#else
    return false;
#endif
}
