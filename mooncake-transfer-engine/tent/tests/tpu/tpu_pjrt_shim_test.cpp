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
#include <vector>

#ifndef MOCK_TPU_PJRT_LIB
#error "MOCK_TPU_PJRT_LIB must be defined by the build (path to mock adapter)"
#endif

namespace mooncake {
namespace tent {
namespace {

using RegisterFn = void (*)(const void *, int);
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
        reset_ = reinterpret_cast<ResetFn>(dlsym(mock_, "mock_tpu_pjrt_reset"));
        set_count_ = reinterpret_cast<SetCountFn>(
            dlsym(mock_, "mock_tpu_pjrt_set_device_count"));
        ASSERT_NE(register_, nullptr);
        ASSERT_NE(reset_, nullptr);
        ASSERT_NE(set_count_, nullptr);
        reset_();
    }

    void TearDown() override {
        if (reset_) reset_();
        if (mock_) dlclose(mock_);
    }

    void *mock_ = nullptr;
    RegisterFn register_ = nullptr;
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
    std::vector<uint8_t> fake_device(64);
    register_(fake_device.data(), /*index=*/2);

    auto &shim = TpuPjrtShim::instance();
    EXPECT_TRUE(shim.isDevicePtr(fake_device.data()));
    EXPECT_EQ(shim.deviceIndex(fake_device.data()), 2);

    // Unregistered host memory is not device memory.
    EXPECT_FALSE(shim.isDevicePtr(&host_value));
    EXPECT_EQ(shim.deviceIndex(&host_value), -1);
    EXPECT_FALSE(shim.isDevicePtr(nullptr));
}

TEST_F(TpuShimTest, CopyRoundTripMovesBytes) {
    auto &shim = TpuPjrtShim::instance();
    const std::vector<uint8_t> src = {1, 2, 3, 4, 5, 6, 7, 8};
    std::vector<uint8_t> device(src.size(), 0);
    std::vector<uint8_t> dst(src.size(), 0);

    // host -> "device"
    ASSERT_TRUE(shim.copyH2D(device.data(), src.data(), src.size()).ok());
    EXPECT_EQ(device, src);

    // "device" -> host
    ASSERT_TRUE(shim.copyD2H(dst.data(), device.data(), device.size()).ok());
    EXPECT_EQ(dst, src);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
