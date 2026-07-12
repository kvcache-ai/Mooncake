// Copyright 2026 KVCache.AI
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

// Unit test for TpuPjrtShim against the mock adapter
// (mock_tpu_pjrt_adapter.cpp). Runs on any Linux host: no TPU hardware or PJRT
// runtime required.
//
// MOCK_TPU_PJRT_LIB is the path to the built mock adapter, injected by CMake.

#include "tent/platform/tpu_pjrt_shim.h"

#include <dlfcn.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <cstring>
#include <vector>

#ifndef MOCK_TPU_PJRT_LIB
#error "MOCK_TPU_PJRT_LIB must be defined by the build (path to mock adapter)"
#endif

namespace mooncake {
namespace tent {
namespace {

using RegisterFn = void *(*)(size_t, int);
using DeviceDataFn = void *(*)(const void *);
using ResetFn = void (*)();
using SetCountFn = void (*)(int);

// Loads the mock adapter a second time (dlopen is reference-counted, so this is
// the same image the shim loads) to reach the test-only registration helpers.
class TpuShimTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        // Point the shim at the mock adapter before its singleton is first
        // used.
        ::setenv("MC_TPU_PJRT_LIB", MOCK_TPU_PJRT_LIB, /*overwrite=*/1);
    }

    void SetUp() override {
        mock_ = dlopen(MOCK_TPU_PJRT_LIB, RTLD_NOW | RTLD_GLOBAL);
        ASSERT_NE(mock_, nullptr) << dlerror();
        register_ = reinterpret_cast<RegisterFn>(
            dlsym(mock_, "mock_tpu_pjrt_register_device"));
        device_data_ = reinterpret_cast<DeviceDataFn>(
            dlsym(mock_, "mock_tpu_pjrt_device_data"));
        reset_ = reinterpret_cast<ResetFn>(dlsym(mock_, "mock_tpu_pjrt_reset"));
        set_count_ = reinterpret_cast<SetCountFn>(
            dlsym(mock_, "mock_tpu_pjrt_set_device_count"));
        ASSERT_NE(register_, nullptr);
        ASSERT_NE(device_data_, nullptr);
        ASSERT_NE(reset_, nullptr);
        ASSERT_NE(set_count_, nullptr);
        reset_();
    }

    void TearDown() override {
        if (reset_) reset_();
        if (mock_) dlclose(mock_);
    }

    // Fills a fake device buffer with a repeatable pattern.
    void seedDevice(void *token, size_t size, uint8_t modulus) {
        auto *data = static_cast<uint8_t *>(device_data_(token));
        ASSERT_NE(data, nullptr);
        for (size_t i = 0; i < size; ++i) data[i] = (uint8_t)(i % modulus);
    }

    void *mock_ = nullptr;
    RegisterFn register_ = nullptr;
    DeviceDataFn device_data_ = nullptr;
    ResetFn reset_ = nullptr;
    SetCountFn set_count_ = nullptr;
};

TEST_F(TpuShimTest, AdapterLoadsAndReportsDevices) {
    auto &shim = TpuPjrtShim::instance();
    ASSERT_TRUE(shim.available());
    EXPECT_EQ(shim.deviceCount(), 4);
    EXPECT_EQ(shim.deviceNumaNode(0), 0);
    EXPECT_EQ(shim.deviceNumaNode(1), 1);
    EXPECT_EQ(shim.deviceNumaNode(99), -1);
}

TEST_F(TpuShimTest, ClassifiesRegisteredDevicePointers) {
    int host_value = 0;
    void *token = register_(64, /*index=*/2);
    ASSERT_NE(token, nullptr);

    auto &shim = TpuPjrtShim::instance();
    EXPECT_TRUE(shim.isDevicePtr(token));
    EXPECT_EQ(shim.deviceIndex(token), 2);

    // Unregistered host memory is not device memory.
    EXPECT_FALSE(shim.isDevicePtr(&host_value));
    EXPECT_EQ(shim.deviceIndex(&host_value), -1);
    EXPECT_FALSE(shim.isDevicePtr(nullptr));
}

// Regression: ProxyManager stages a transfer in chunk_size (4 MiB) pieces and
// hands the platform `token + chunk_offset` for every chunk after the first. An
// adapter that only recognises base addresses makes TENT classify TPU HBM as
// host memory, and the staging copy degrades into a memcpy from a token that on
// real PJRT is not the buffer's data -- silently corrupting every transfer
// larger than one chunk. Interior addresses must classify as device memory.
TEST_F(TpuShimTest, ClassifiesInteriorDevicePointers) {
    const size_t kSize = 8192;
    auto *token = static_cast<uint8_t *>(register_(kSize, /*index=*/3));
    ASSERT_NE(token, nullptr);

    auto &shim = TpuPjrtShim::instance();
    EXPECT_TRUE(shim.isDevicePtr(token + 1));
    EXPECT_TRUE(shim.isDevicePtr(token + 4096));
    EXPECT_TRUE(shim.isDevicePtr(token + kSize - 1));
    EXPECT_EQ(shim.deviceIndex(token + 4096), 3);

    // One past the end belongs to no buffer.
    EXPECT_FALSE(shim.isDevicePtr(token + kSize));
    EXPECT_EQ(shim.deviceIndex(token + kSize), -1);
}

TEST_F(TpuShimTest, CopyRoundTripMovesBytes) {
    auto &shim = TpuPjrtShim::instance();
    const std::vector<uint8_t> src = {1, 2, 3, 4, 5, 6, 7, 8};
    void *token = register_(src.size(), /*index=*/0);
    ASSERT_NE(token, nullptr);
    std::vector<uint8_t> dst(src.size(), 0);

    // host -> "device"
    ASSERT_TRUE(shim.copyH2D(token, src.data(), src.size()).ok());
    EXPECT_EQ(0, std::memcmp(device_data_(token), src.data(), src.size()));

    // "device" -> host
    ASSERT_TRUE(shim.copyD2H(dst.data(), token, src.size()).ok());
    EXPECT_EQ(dst, src);
}

// The chunked staging pattern end to end: copy each 4 KiB slice of a "device"
// buffer out through an interior pointer, exactly as ProxyManager would.
TEST_F(TpuShimTest, CopyFromInteriorOffsetMovesTheRightBytes) {
    auto &shim = TpuPjrtShim::instance();
    const size_t kChunk = 4096, kChunks = 4, kSize = kChunk * kChunks;
    auto *token = static_cast<uint8_t *>(register_(kSize, /*index=*/1));
    ASSERT_NE(token, nullptr);
    seedDevice(token, kSize, 251);

    std::vector<uint8_t> host(kSize, 0);
    for (size_t c = 0; c < kChunks; ++c) {
        ASSERT_TRUE(
            shim.copyD2H(host.data() + c * kChunk, token + c * kChunk, kChunk)
                .ok())
            << "chunk " << c;
    }
    EXPECT_EQ(0, std::memcmp(host.data(), device_data_(token), kSize));
    // And the bytes are the seeded pattern, not the token's poison.
    EXPECT_EQ(host[0], 0);
    EXPECT_EQ(host[kChunk + 1], (uint8_t)((kChunk + 1) % 251));
}

// A copy must never run past the end of the buffer it started in.
TEST_F(TpuShimTest, RejectsCopyRunningPastBufferEnd) {
    auto &shim = TpuPjrtShim::instance();
    auto *token = static_cast<uint8_t *>(register_(1024, /*index=*/0));
    ASSERT_NE(token, nullptr);
    std::vector<uint8_t> host(2048, 0);

    EXPECT_FALSE(shim.copyD2H(host.data(), token + 512, 1024).ok());
    EXPECT_FALSE(shim.copyH2D(token + 512, host.data(), 1024).ok());
    // Unregistered addresses are not device memory and cannot be copied.
    EXPECT_FALSE(shim.copyD2H(host.data(), host.data() + 1024, 16).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
