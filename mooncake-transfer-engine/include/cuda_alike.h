#pragma once

#include <bits/stdint-uintn.h>
#ifdef USE_CUDA
#include <bits/stdint-uintn.h>
#include <cuda.h>
#include <cuda_runtime.h>
#elif defined(USE_HIP)
#include "gpu_vendor/hip.h"
#elif defined(USE_MUSA)
#include "gpu_vendor/musa.h"
#endif

#if !defined(USE_HIP) && !defined(USE_MUSA)
const static std::string GPU_PREFIX = "cuda:";
#endif
