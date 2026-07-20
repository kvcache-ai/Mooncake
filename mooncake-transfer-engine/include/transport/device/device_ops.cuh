// Device-side memory ordering primitives — platform-portable selector.
//
// This is the ONLY file in the device API that contains a platform #ifdef.
// All other device API headers include this file and use the mc_* functions.
// The kernel itself has zero platform #ifdef branches.
#pragma once

#ifdef MOONCAKE_EP_USE_MUSA
#include "transport/device/musa/musa_ops.cuh"
#else
#include "transport/device/cuda/cuda_ops.cuh"
#endif
