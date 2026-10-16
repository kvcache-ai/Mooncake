/*
 * SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES.
 * All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

#include <cuda_runtime.h>
#include <gtest/gtest.h>
#include <jit/jit_runtime.hpp>

#include <string>

namespace {

using mooncake::jit::JitKernelStatus;
using mooncake::jit::JitKernelVariant;

struct SmokeArgs {
    int* out;
    int value;
};

// Pulls in a staged mooncake-ep header so a broken staging path fails here
// instead of much later, when a real kernel needs it.
constexpr char kSmokeSource[] = R"CUDA(
#include <mooncake_ep_device.h>

struct SmokeArgs {
    int* out;
    int value;
};

extern "C" __global__ void mooncake_ep_jit_smoke_kernel(
    const __grid_constant__ SmokeArgs p) {
    if (threadIdx.x == 0) *p.out = p.value;
}
)CUDA";

constexpr char kEntryName[] = "mooncake_ep_jit_smoke_kernel";

bool cuda_device_available() {
    int count = 0;
    return cudaGetDeviceCount(&count) == cudaSuccess && count > 0;
}

JitKernelVariant make_variant(const void* identity) {
    JitKernelVariant variant;
    variant.kernel_family = "jit_smoke";
    variant.variant_name = "smoke";
    variant.source = kSmokeSource;
    variant.entry_name = kEntryName;
    variant.identity = identity;
    variant.runtime_key = 1;
    variant.num_blocks = 1;
    variant.block_dim = 32;
    variant.min_sm = 80;
    return variant;
}

class JitSmokeTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (!cuda_device_available()) GTEST_SKIP() << "no CUDA device";
        ASSERT_EQ(cudaMalloc(&device_out_, sizeof(int)), cudaSuccess);
        ASSERT_EQ(cudaMemset(device_out_, 0, sizeof(int)), cudaSuccess);
    }

    void TearDown() override {
        if (device_out_ != nullptr) cudaFree(device_out_);
    }

    int read_back() {
        int host_out = -1;
        EXPECT_EQ(cudaMemcpy(&host_out, device_out_, sizeof(int),
                             cudaMemcpyDeviceToHost),
                  cudaSuccess);
        return host_out;
    }

    int* device_out_ = nullptr;
};

// Covers the whole chain in one go: generate source, compile with nvcc, cache
// the cubin, load it through the driver API, and launch it.
TEST_F(JitSmokeTest, CompilesLoadsAndLaunches) {
    static const int identity = 0;
    const JitKernelVariant variant = make_variant(&identity);

    SmokeArgs args{device_out_, 42};
    std::string error;
    ASSERT_EQ(mooncake::jit::launch_jit_kernel(variant, &args, nullptr, &error),
              JitKernelStatus::kLaunched)
        << error;
    ASSERT_EQ(cudaStreamSynchronize(nullptr), cudaSuccess);
    EXPECT_EQ(read_back(), 42);
}

// Every mooncake-ep kernel spins across blocks and so relies on cooperative
// launch keeping them all resident; dropping the attribute hangs instead of
// failing, and only on configurations large enough to oversubscribe the SMs.
// Nothing in the tree sets JitKernelVariant::cooperative explicitly, so the
// guarantee rests entirely on its default -- pin that down here.
//
// The lever is the grid-size cap: a cooperative launch may not ask for more
// blocks than fit on the device at once, while a plain launch may.  Running
// the same oversized grid both ways turns a silently dropped attribute into a
// test failure, without needing a grid-syncing kernel that would deadlock.
TEST_F(JitSmokeTest, CooperativeAttributeIsApplied) {
    static const int identity = 0;
    JitKernelVariant oversized = make_variant(&identity);
    oversized.num_blocks = 1 << 20;

    SmokeArgs args{device_out_, 1};
    std::string error;

    // Establish that the grid size itself is launchable before attributing the
    // failure below to the cooperative cap.
    JitKernelVariant plain = oversized;
    plain.cooperative = false;
    ASSERT_EQ(mooncake::jit::launch_jit_kernel(plain, &args, nullptr, &error),
              JitKernelStatus::kLaunched)
        << error;
    ASSERT_EQ(cudaStreamSynchronize(nullptr), cudaSuccess);

    EXPECT_EQ(
        mooncake::jit::launch_jit_kernel(oversized, &args, nullptr, &error),
        JitKernelStatus::kLaunchFailed)
        << "oversized cooperative launch succeeded, so the attribute was "
           "dropped";
}

// The warm path must serve the kernel from the in-process cache without the
// source text, which is what real callers rely on to skip building it.
TEST_F(JitSmokeTest, WarmLaunchHitsProcessCache) {
    static const int identity = 0;
    const JitKernelVariant variant = make_variant(&identity);

    SmokeArgs args{device_out_, 42};
    std::string error;
    ASSERT_EQ(mooncake::jit::launch_jit_kernel(variant, &args, nullptr, &error),
              JitKernelStatus::kLaunched)
        << error;
    ASSERT_EQ(cudaStreamSynchronize(nullptr), cudaSuccess);

    JitKernelVariant warm = variant;
    warm.source = {};
    warm.variant_name = {};
    args.value = 43;
    ASSERT_EQ(mooncake::jit::launch_jit_kernel_cached(warm, &args, 0, nullptr,
                                                      &error),
              JitKernelStatus::kLaunched)
        << error;
    ASSERT_EQ(cudaStreamSynchronize(nullptr), cudaSuccess);
    EXPECT_EQ(read_back(), 43);
}

// A variant the cache has never seen must not be served by the warm path.
// Note this one turns on the identity being fresh, so it says nothing about
// runtime_key -- that is what WarmLaunchDiscriminatesOnRuntimeKey covers.
TEST_F(JitSmokeTest, WarmLaunchMissesUnknownVariant) {
    static const int identity = 0;
    JitKernelVariant unknown = make_variant(&identity);
    unknown.source = {};
    unknown.variant_name = {};
    unknown.runtime_key = 0xdeadbeef;

    SmokeArgs args{device_out_, 7};
    std::string error;
    EXPECT_EQ(mooncake::jit::launch_jit_kernel_cached(unknown, &args, 0,
                                                      nullptr, &error),
              JitKernelStatus::kDisabled);
}

// Variants that share an identity are told apart by runtime_key alone: every
// dispatch variant will point at one identity and differ only in hidden/fp8,
// so a key comparison that dropped runtime_key would silently serve the wrong
// kernel -- a wrong result, not an error.  Prime the cache first so that a
// fresh identity cannot be the reason for the miss, leaving runtime_key as the
// only thing left to discriminate on.
TEST_F(JitSmokeTest, WarmLaunchDiscriminatesOnRuntimeKey) {
    static const int identity = 0;
    const JitKernelVariant variant = make_variant(&identity);

    SmokeArgs args{device_out_, 42};
    std::string error;
    ASSERT_EQ(mooncake::jit::launch_jit_kernel(variant, &args, nullptr, &error),
              JitKernelStatus::kLaunched)
        << error;
    ASSERT_EQ(cudaStreamSynchronize(nullptr), cudaSuccess);

    // Same identity, same everything else -- only the key moves.
    JitKernelVariant other = variant;
    other.source = {};
    other.variant_name = {};
    other.runtime_key = variant.runtime_key + 1;
    EXPECT_EQ(mooncake::jit::launch_jit_kernel_cached(other, &args, 0, nullptr,
                                                      &error),
              JitKernelStatus::kDisabled)
        << "a differing runtime_key was served from the cache, so variants "
           "sharing an identity are not being told apart";

    // The original key must still be live -- the miss above must come from
    // discrimination, not from the entry having been evicted or overwritten.
    args.value = 43;
    ASSERT_EQ(mooncake::jit::launch_jit_kernel_cached(variant, &args, 0,
                                                      nullptr, &error),
              JitKernelStatus::kLaunched)
        << error;
    ASSERT_EQ(cudaStreamSynchronize(nullptr), cudaSuccess);
    EXPECT_EQ(read_back(), 43);
}

}  // namespace
