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

// Acceptance test for the Intel XPU platform. Drives XpuPlatform against the
// real oneAPI SYCL backend (USE_XPU is a direct-link build: libsycl is linked
// in, there is no mock adapter). The backend falls back to the OpenCL CPU
// runtime when no Intel GPU is present, so this test runs on any host with a
// SYCL runtime; when no SYCL device is visible at all it skips rather than
// fails.

#include <gtest/gtest.h>
#include <sycl/sycl.hpp>

#include <cstdint>
#include <cstring>
#include <memory>
#include <vector>

#include "tent/common/config.h"
#include "tent/platform/xpu.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {
namespace {

class XpuPlatformTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto conf = std::make_shared<Config>();
        platform_ = std::make_shared<XpuPlatform>(conf);

        // Count visible XPU devices via probe(); skip the whole suite when the
        // host has no SYCL device (e.g. no oneAPI runtime installed).
        std::vector<Topology::NicEntry> nics;
        std::vector<Topology::MemEntry> mems;
        ASSERT_TRUE(platform_->probe(nics, mems).ok());
        for (const auto &m : mems) {
            if (m.name.rfind("xpu:", 0) == 0) device_count_++;
        }
        if (device_count_ == 0) {
            GTEST_SKIP() << "no SYCL device available; skipping XPU test";
        }
    }

    std::shared_ptr<XpuPlatform> platform_;
    int device_count_ = 0;
};

TEST_F(XpuPlatformTest, TypeString) { EXPECT_EQ(platform_->type(), "xpu"); }

TEST_F(XpuPlatformTest, ProbeRegistersOneMemEntryPerDevice) {
    std::vector<Topology::NicEntry> nics;
    std::vector<Topology::MemEntry> mems;
    Status s = platform_->probe(nics, mems);
    EXPECT_TRUE(s.ok()) << s;

    int xpu_entries = 0;
    for (const auto &m : mems) {
        if (m.name.rfind("xpu:", 0) == 0) xpu_entries++;
    }
    EXPECT_EQ(xpu_entries, device_count_)
        << "probe() should register one node per device";
}

// Acceptance: device allocate -> host->device copy -> device->host copy ->
// byte-equality -> free, all driven through XpuPlatform.
TEST_F(XpuPlatformTest, DeviceAllocCopyFreeRoundTrip) {
    const size_t kSize = 4096;
    MemoryOptions options;
    options.location = "xpu:0";

    void *dev = nullptr;
    Status alloc = platform_->allocate(&dev, kSize, options);
    ASSERT_TRUE(alloc.ok()) << alloc;
    ASSERT_NE(dev, nullptr);

    // classify() == MTYPE_XPU for device USM.
    EXPECT_EQ(platform_->getMemoryType(dev), MTYPE_XPU);

    // getLocation resolves the owning device ordinal.
    auto locs = platform_->getLocation(dev, kSize);
    ASSERT_EQ(locs.size(), 1u);
    EXPECT_EQ(locs[0].location, "xpu:0");

    // host -> device -> host round trip.
    std::vector<uint8_t> src(kSize);
    for (size_t i = 0; i < kSize; ++i) src[i] = static_cast<uint8_t>(i * 7 + 1);
    std::vector<uint8_t> dst(kSize, 0);

    Status h2d = platform_->copy(dev, src.data(), kSize);
    ASSERT_TRUE(h2d.ok()) << h2d;
    Status d2h = platform_->copy(dst.data(), dev, kSize);
    ASSERT_TRUE(d2h.ok()) << d2h;

    EXPECT_EQ(std::memcmp(src.data(), dst.data(), kSize), 0);

    Status freed = platform_->free(dev, kSize);
    EXPECT_TRUE(freed.ok()) << freed;
    // Whether the freed address still classifies as device memory is up to the
    // SYCL runtime (Level Zero keeps freed USM cached and get_pointer_type
    // still reports it as device), so that is deliberately not asserted.
}

TEST_F(XpuPlatformTest, HostPointerClassifiesAsCpu) {
    int host_value = 0;
    EXPECT_EQ(platform_->getMemoryType(&host_value), MTYPE_CPU);
}

// USM device memory the platform did not allocate itself -- the way a PyTorch
// XPU tensor reaches registerLocalMemory -- must still classify as XPU memory
// and be copyable, including at an interior offset. PyTorch allocates from the
// platform default context, so mirror that here.
TEST_F(XpuPlatformTest, ForeignUsmClassifiesAsXpuAndCopies) {
    sycl::device gpu;
    bool found = false;
    for (const auto &d : sycl::device::get_devices()) {
        if (d.is_gpu() && d.has(sycl::aspect::usm_device_allocations)) {
            gpu = d;
            found = true;
            break;
        }
    }
    if (!found) GTEST_SKIP() << "no SYCL GPU device available";

    sycl::context ctx = gpu.get_platform().ext_oneapi_get_default_context();
    sycl::queue q(ctx, gpu);
    const size_t kSize = 8192;
    auto *dev = static_cast<uint8_t *>(sycl::malloc_device(kSize, gpu, ctx));
    ASSERT_NE(dev, nullptr);

    EXPECT_EQ(platform_->getMemoryType(dev), MTYPE_XPU);
    EXPECT_EQ(platform_->getMemoryType(dev + 4096), MTYPE_XPU);
    auto locs = platform_->getLocation(dev, kSize);
    ASSERT_EQ(locs.size(), 1u);
    EXPECT_EQ(locs[0].location.rfind("xpu:", 0), 0u) << locs[0].location;

    std::vector<uint8_t> src(4096), dst(4096, 0);
    for (size_t i = 0; i < src.size(); ++i)
        src[i] = static_cast<uint8_t>(i * 11 + 3);
    Status h2d = platform_->copy(dev + 4096, src.data(), src.size());
    ASSERT_TRUE(h2d.ok()) << h2d;
    Status d2h = platform_->copy(dst.data(), dev + 4096, dst.size());
    ASSERT_TRUE(d2h.ok()) << d2h;
    EXPECT_EQ(src, dst);

    // Cross-check against the owner's own queue: the platform really wrote
    // into this allocation, not a stage.
    std::vector<uint8_t> direct(4096, 0);
    q.memcpy(direct.data(), dev + 4096, direct.size()).wait();
    EXPECT_EQ(src, direct);

    // Host and shared USM are host-accessible and stay classified as CPU.
    void *host_usm = sycl::malloc_host(4096, ctx);
    ASSERT_NE(host_usm, nullptr);
    EXPECT_EQ(platform_->getMemoryType(host_usm), MTYPE_CPU);
    sycl::free(host_usm, ctx);

    sycl::free(dev, ctx);
}

// Interior addresses (chunked staging hands the backend base + offset) must
// classify as device memory, or transfers larger than one chunk corrupt.
TEST_F(XpuPlatformTest, InteriorPointerClassifiesAsXpu) {
    const size_t kSize = 8192;
    MemoryOptions options;
    options.location = "xpu:0";

    void *dev = nullptr;
    ASSERT_TRUE(platform_->allocate(&dev, kSize, options).ok());
    auto *bytes = static_cast<uint8_t *>(dev);

    EXPECT_EQ(platform_->getMemoryType(bytes + 4096), MTYPE_XPU);
    auto locs = platform_->getLocation(bytes + 4096, kSize - 4096);
    ASSERT_EQ(locs.size(), 1u);
    EXPECT_EQ(locs[0].location, "xpu:0");

    // Interior-offset staging copy must round-trip correctly.
    std::vector<uint8_t> src(4096), dst(4096, 0);
    for (size_t i = 0; i < src.size(); ++i)
        src[i] = static_cast<uint8_t>(i * 13 + 5);
    ASSERT_TRUE(platform_->copy(bytes + 4096, src.data(), 4096).ok());
    ASSERT_TRUE(platform_->copy(dst.data(), bytes + 4096, 4096).ok());
    EXPECT_EQ(std::memcmp(src.data(), dst.data(), 4096), 0);

    EXPECT_TRUE(platform_->free(dev, kSize).ok());
}

// A bare "xpu" location (no ordinal) must allocate on device 0, not fall
// through to the host allocator.
TEST_F(XpuPlatformTest, BareXpuLocationAllocatesDeviceZero) {
    MemoryOptions options;
    options.location = "xpu";

    void *dev = nullptr;
    Status alloc = platform_->allocate(&dev, 4096, options);
    ASSERT_TRUE(alloc.ok()) << alloc;
    ASSERT_NE(dev, nullptr);
    EXPECT_EQ(platform_->getMemoryType(dev), MTYPE_XPU);
    EXPECT_TRUE(platform_->free(dev, 4096).ok());
}

// A malformed ordinal ("xpu:<garbage>") must be rejected rather than silently
// allocating on device 0.
TEST_F(XpuPlatformTest, MalformedXpuOrdinalIsRejected) {
    MemoryOptions options;
    options.location = "xpu:notanumber";

    void *dev = nullptr;
    Status alloc = platform_->allocate(&dev, 4096, options);
    EXPECT_FALSE(alloc.ok());
    EXPECT_EQ(dev, nullptr);
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
