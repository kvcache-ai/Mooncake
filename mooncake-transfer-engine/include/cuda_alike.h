#pragma once

#ifdef USE_CUDA
#include "gpu_vendor/cuda.h"
#elif defined(USE_HIP)
#include "gpu_vendor/hip.h"
#elif defined(USE_MUSA)
#include "gpu_vendor/musa.h"
#else
const static std::string GPU_PREFIX = "cuda:";
#endif
