/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 *
 * Ported from nccl-extensions: nccl_ep/device/jit/jit_runtime.hpp.
 * This engine is kernel agnostic -- JitKernelVariant is the only contract
 * between it and the per-kernel source generators.
 */

#pragma once

#include <cuda_runtime.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace mooncake {
namespace jit {

enum class JitKernelStatus {
    kLaunched,
    kDisabled,
    kUnsupportedDevice,
    kCompileFailed,
    kLoadFailed,
    kAttributeFailed,
    kLaunchFailed,
};

struct JitKernelVariant {
    std::string_view kernel_family;
    std::string_view variant_name;
    std::string_view source;
    std::string_view entry_name;
    const void* identity = nullptr;
    std::uint64_t runtime_key = 0;
    int num_blocks = 0;
    int block_dim = 0;
    int dynamic_smem_bytes = 0;
    int min_sm = -1;
    std::string_view target_arch;
    // Optional launch attributes (default = off).
    // Cooperative launch enables cg::this_grid().sync() inside the kernel.
    // Cluster dim > 1 enables distributed shared memory across the cluster.
    bool cooperative = false;
    int cluster_dim_x = 1;
    int cluster_dim_y = 1;
    int cluster_dim_z = 1;
};

// Allocation-free FNV-1a folding used to derive JitKernelVariant::runtime_key
// from the raw variant parameters (ints, bools, short literal tags) instead of
// hashing a heap-built name string. runtime_key only has to be stable within a
// process: it keys the in-memory fast cache; the on-disk cache key is derived
// from the full source text.
constexpr std::uint64_t kRuntimeKeySeed = 1469598103934665603ull;

constexpr std::uint64_t runtime_key_mix(std::uint64_t key,
                                        std::uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        key ^= (value >> (8 * i)) & 0xffu;
        key *= 1099511628211ull;
    }
    return key;
}

constexpr std::uint64_t runtime_key_mix(std::uint64_t key, const char* text) {
    for (; text != nullptr && *text != '\0'; ++text) {
        key ^= static_cast<unsigned char>(*text);
        key *= 1099511628211ull;
    }
    return key;
}

// Fast path only: launch the variant from the in-process kernel cache.
// Requires just identity/runtime_key and the launch configuration --
// variant_name and source may be left empty, so callers can skip building
// them entirely on the (hot) warm-cache path. Returns kDisabled on a cache
// miss; the caller then materializes the strings and calls launch_jit_kernel.
JitKernelStatus launch_jit_kernel_cached(const JitKernelVariant& variant,
                                         void* kernel_param,
                                         std::size_t kernel_param_size,
                                         cudaStream_t stream,
                                         std::string* error);

JitKernelStatus launch_jit_kernel(const JitKernelVariant& variant,
                                  void* kernel_param,
                                  std::size_t kernel_param_size,
                                  cudaStream_t stream, std::string* error);

JitKernelStatus launch_jit_kernel(const JitKernelVariant& variant,
                                  void* kernel_param, cudaStream_t stream,
                                  std::string* error);

const char* jit_kernel_status_name(JitKernelStatus status);

}  // namespace jit
}  // namespace mooncake
