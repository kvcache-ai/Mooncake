// EP device-side memory ordering primitives — platform-portable selector.
//
// This is the ONLY file in the EP device API that contains a platform #ifdef.
// All other device API headers include this file and use the ep_* functions.
// The kernel itself has zero platform #ifdef branches.
#pragma once

#ifdef MOONCAKE_EP_USE_MUSA
#include "transport/ep_device_transport/musa/ep_musa_ops.cuh"
#else
#include "transport/ep_device_transport/cuda/ep_cuda_ops.cuh"
#endif
