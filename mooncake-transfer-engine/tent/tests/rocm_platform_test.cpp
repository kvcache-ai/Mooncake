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

#include <gtest/gtest.h>

#include <hip/hip_runtime.h>
#include <memory>
#include <string>
#include <vector>

#include "tent/common/config.h"
#include "tent/platform/rocm.h"
#include "tent/runtime/platform.h"
#include "tent/runtime/topology.h"

namespace mooncake {
namespace tent {
namespace {

// Helper: returns the number of visible AMD GPUs (0 if ROCm not available).
static int getHipDeviceCount() {
    int count = 0;
    if (hipGetDeviceCount(&count) != hipSuccess) return 0;
    return count;
}

// ---------------------------------------------------------------------------
// HIPStreamPool unit tests
// ---------------------------------------------------------------------------

TEST(HIPStreamPoolTest, AcquireAndRelease) {
    if (getHipDeviceCount() == 0) GTEST_SKIP() << "No AMD GPU available";

    HIPStreamPool pool;
    HIPStreamHandle handle;
    Status s = pool.acquire(handle);
    EXPECT_TRUE(s.ok()) << s;
    EXPECT_NE(handle.get(), nullptr);
    // Destructor releases back to pool automatically — no assertion needed.
}

TEST(HIPStreamPoolTest, AcquireMultipleStreams) {
    if (getHipDeviceCount() == 0) GTEST_SKIP() << "No AMD GPU available";

    HIPStreamPool pool;
    constexpr int kStreams = 4;
    std::vector<HIPStreamHandle> handles(kStreams);
    for (int i = 0; i < kStreams; i++) {
        Status s = pool.acquire(handles[i]);
        EXPECT_TRUE(s.ok()) << s;
        EXPECT_NE(handles[i].get(), nullptr);
    }
}

TEST(HIPStreamPoolTest, AcquireByExplicitDeviceId) {
    if (getHipDeviceCount() == 0) GTEST_SKIP() << "No AMD GPU available";

    HIPStreamPool pool;
    HIPStreamHandle handle;
    Status s = pool.acquire(handle, 0);
    EXPECT_TRUE(s.ok()) << s;
    EXPECT_NE(handle.get(), nullptr);
}

TEST(HIPStreamPoolTest, InvalidDeviceIdReturnsError) {
    HIPStreamPool pool;
    HIPStreamHandle handle;
    // Device ID -5 is always invalid.
    Status s = pool.acquire(handle, -5);
    EXPECT_FALSE(s.ok());
}

// ---------------------------------------------------------------------------
// RocmPlatform unit tests
// ---------------------------------------------------------------------------

class RocmPlatformTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (getHipDeviceCount() == 0) {
            GTEST_SKIP() << "No AMD GPU available";
        }
        auto conf = std::make_shared<Config>();
        platform_ = std::make_shared<RocmPlatform>(conf);
    }

    std::shared_ptr<RocmPlatform> platform_;
};

TEST_F(RocmPlatformTest, TypeString) { EXPECT_EQ(platform_->type(), "rocm"); }

TEST_F(RocmPlatformTest, ProbeDiscoversMems) {
    std::vector<Topology::NicEntry> nics;
    std::vector<Topology::MemEntry> mems;
    Status s = platform_->probe(nics, mems);
    EXPECT_TRUE(s.ok()) << s;
    // At least one ROCm memory entry should be found.
    bool found_rocm = false;
    for (const auto& m : mems) {
        if (m.type == Topology::MEM_ROCM) {
            found_rocm = true;
            EXPECT_EQ(m.name.substr(0, 5), "rocm:");
        }
    }
    EXPECT_TRUE(found_rocm) << "probe() found no MEM_ROCM entries";
}

TEST_F(RocmPlatformTest, AllocateAndFreeDeviceMemory) {
    MemoryOptions opts;
    opts.location = "rocm:0";
    void* ptr = nullptr;
    constexpr size_t kSize = 1024 * 1024;  // 1 MiB

    Status s = platform_->allocate(&ptr, kSize, opts);
    ASSERT_TRUE(s.ok()) << s;
    ASSERT_NE(ptr, nullptr);

    EXPECT_EQ(platform_->getMemoryType(ptr), MTYPE_ROCM);

    Status fs = platform_->free(ptr, kSize);
    EXPECT_TRUE(fs.ok()) << fs;
}

TEST_F(RocmPlatformTest, AllocateAndFreeCpuMemory) {
    MemoryOptions opts;
    opts.location = "cpu:0";
    void* ptr = nullptr;
    constexpr size_t kSize = 4096;

    Status s = platform_->allocate(&ptr, kSize, opts);
    ASSERT_TRUE(s.ok()) << s;
    ASSERT_NE(ptr, nullptr);

    EXPECT_EQ(platform_->getMemoryType(ptr), MTYPE_CPU);

    Status fs = platform_->free(ptr, kSize);
    EXPECT_TRUE(fs.ok()) << fs;
}

TEST_F(RocmPlatformTest, CopyDeviceToDevice) {
    MemoryOptions opts;
    opts.location = "rocm:0";
    constexpr size_t kSize = 256;
    void *src = nullptr, *dst = nullptr;

    ASSERT_TRUE(platform_->allocate(&src, kSize, opts).ok());
    ASSERT_TRUE(platform_->allocate(&dst, kSize, opts).ok());

    // Fill src with a pattern via hipMemset.
    ASSERT_EQ(hipMemset(src, 0xAB, kSize), hipSuccess);

    Status cs = platform_->copy(dst, src, kSize);
    EXPECT_TRUE(cs.ok()) << cs;

    // Verify the copy by reading back to host.
    std::vector<uint8_t> host_buf(kSize, 0);
    ASSERT_EQ(hipMemcpy(host_buf.data(), dst, kSize, hipMemcpyDeviceToHost),
              hipSuccess);
    for (size_t i = 0; i < kSize; i++) {
        EXPECT_EQ(host_buf[i], 0xAB) << "mismatch at byte " << i;
    }

    EXPECT_TRUE(platform_->free(src, kSize).ok());
    EXPECT_TRUE(platform_->free(dst, kSize).ok());
}

TEST_F(RocmPlatformTest, CopyHostToDevice) {
    MemoryOptions dev_opts;
    dev_opts.location = "rocm:0";
    MemoryOptions cpu_opts;
    cpu_opts.location = "cpu:0";
    constexpr size_t kSize = 512;
    void *dev = nullptr, *host_mem = nullptr;

    ASSERT_TRUE(platform_->allocate(&dev, kSize, dev_opts).ok());
    ASSERT_TRUE(platform_->allocate(&host_mem, kSize, cpu_opts).ok());

    memset(host_mem, 0xCD, kSize);

    Status cs = platform_->copy(dev, host_mem, kSize);
    EXPECT_TRUE(cs.ok()) << cs;

    std::vector<uint8_t> verify(kSize, 0);
    ASSERT_EQ(hipMemcpy(verify.data(), dev, kSize, hipMemcpyDeviceToHost),
              hipSuccess);
    for (size_t i = 0; i < kSize; i++) {
        EXPECT_EQ(verify[i], 0xCD) << "mismatch at byte " << i;
    }

    EXPECT_TRUE(platform_->free(dev, kSize).ok());
    EXPECT_TRUE(platform_->free(host_mem, kSize).ok());
}

TEST_F(RocmPlatformTest, GetLocationDeviceMemory) {
    MemoryOptions opts;
    opts.location = "rocm:0";
    constexpr size_t kSize = 4096;
    void* ptr = nullptr;

    ASSERT_TRUE(platform_->allocate(&ptr, kSize, opts).ok());
    auto locs = platform_->getLocation(ptr, kSize);
    ASSERT_FALSE(locs.empty());
    EXPECT_EQ(locs[0].location, "rocm:0");
    EXPECT_TRUE(platform_->free(ptr, kSize).ok());
}

TEST_F(RocmPlatformTest, GetMemoryTypeUnknownPointer) {
    // An arbitrary user-space stack pointer should not be MTYPE_ROCM.
    int local_var = 42;
    MemoryType mtype = platform_->getMemoryType(&local_var);
    // CPU or UNKNOWN — never MTYPE_ROCM.
    EXPECT_NE(mtype, MTYPE_ROCM);
}

// ---------------------------------------------------------------------------
// Multi-GPU tests (skipped when only one GPU is present)
// ---------------------------------------------------------------------------

TEST_F(RocmPlatformTest, AllocateOnSecondGpu) {
    if (getHipDeviceCount() < 2) GTEST_SKIP() << "Requires >= 2 AMD GPUs";

    MemoryOptions opts;
    opts.location = "rocm:1";
    void* ptr = nullptr;
    constexpr size_t kSize = 1024;

    Status s = platform_->allocate(&ptr, kSize, opts);
    ASSERT_TRUE(s.ok()) << s;
    ASSERT_NE(ptr, nullptr);

    EXPECT_EQ(platform_->getMemoryType(ptr), MTYPE_ROCM);

    EXPECT_TRUE(platform_->free(ptr, kSize).ok());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
