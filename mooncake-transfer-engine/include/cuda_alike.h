#pragma once

#include <bits/stdint-uintn.h>
#ifdef USE_CUDA
#include <cuda.h>
#include <cuda_runtime.h>
#elif defined(USE_HIP)
#include "gpu_vendor/hip.h"
#elif defined(USE_MUSA)
#include "gpu_vendor/musa.h"
#elif defined(USE_UBSHMEM)
#include "gpu_vendor/ubshmem.h"
#endif

#if !defined(USE_HIP) && !defined(USE_MUSA) && !defined(USE_UBSHMEM)
#include <string>
const static std::string GPU_PREFIX = "cuda:";
#endif
