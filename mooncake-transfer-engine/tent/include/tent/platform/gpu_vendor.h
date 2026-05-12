// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef TENT_PLATFORM_GPU_VENDOR_H_
#define TENT_PLATFORM_GPU_VENDOR_H_

/**
 * @file gpu_vendor.h
 * @brief GPU vendor abstraction layer - unified API for multiple GPU platforms
 *
 * This header provides unified macros that map to vendor-specific APIs:
 * - CUDA (NVIDIA)
 * - MUSA (Moore Threads)
 * - HIP (AMD)
 * - MACA (Iluvatar)
 * - Ascend (Huawei)
 * - CPU (fallback)
 */

#if defined(USE_CUDA)
    #include "tent/platform/gpu_vendor/cuda.h"
#elif defined(USE_MUSA)
    #include "tent/platform/gpu_vendor/musa.h"
#elif defined(USE_HIP)
    #include "tent/platform/gpu_vendor/hip.h"
#elif defined(USE_MACA)
    #include "tent/platform/gpu_vendor/maca.h"
#elif defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
    #include "tent/platform/gpu_vendor/ascend.h"
#else
    #include "tent/platform/gpu_vendor/cpu.h"
#endif

#endif  // TENT_PLATFORM_GPU_VENDOR_H_
