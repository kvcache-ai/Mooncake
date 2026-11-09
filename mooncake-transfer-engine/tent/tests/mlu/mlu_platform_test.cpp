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

#include <cnrt.h>
#include <cn_api.h>
#include <gtest/gtest.h>

#include <limits>
#include <thread>
#include <vector>

#include "tent/platform/mlu.h"

namespace mooncake::tent {
namespace {
constexpr size_t kSize = 1 << 20;

class MluPlatformTest : public ::testing::Test {
   protected:
    void SetUp() override {
        unsigned int count = 0;
        ASSERT_EQ(cnrtGetDeviceCount(&count), cnrtSuccess);
        if (count == 0) GTEST_SKIP() << "No MLU device visible";
        ASSERT_EQ(cnrtSetDevice(0), cnrtSuccess);
        ASSERT_EQ(cnrtMalloc(&device_, kSize), cnrtSuccess);
    }
    void TearDown() override {
        if (device_) EXPECT_EQ(cnrtFree(device_), cnrtSuccess);
    }
    MluPlatform platform_{std::make_shared<Config>()};
    void* device_ = nullptr;
};

TEST(MluLocationTest, CpuPlatformRejectsMluAllocation) {
    CpuPlatform cpu(std::make_shared<Config>());
    for (const auto& location : {"mlu", "mlu:0"}) {
        MemoryOptions options;
        options.location = location;
        void* ptr = nullptr;
        EXPECT_FALSE(cpu.allocate(&ptr, 4096, options).ok());
        EXPECT_EQ(ptr, nullptr);
    }
}

TEST_F(MluPlatformTest, TopologyResolvesHostStaging) {
    Topology topology;
    ASSERT_TRUE(topology.discover({&platform_}).ok());
    auto* mem = topology.getMemEntry("mlu:0");
    ASSERT_NE(mem, nullptr);
    EXPECT_EQ(mem->type, Topology::MEM_MLU);
    EXPECT_FALSE(mem->pci_bus_id.empty());
    auto host = topology.findNearMem("mlu:0");
    EXPECT_EQ(LocationParser(host).type(), "cpu");
    Topology restored;
    ASSERT_TRUE(restored.parse(topology.toString()).ok());
    EXPECT_EQ(restored.findNearMem("mlu:0"), host);
}

TEST_F(MluPlatformTest, ExternalInteriorPointerOnFreshWorker) {
    std::vector<uint8_t> seed(kSize), readback(kSize);
    for (size_t i = 0; i < seed.size(); ++i) seed[i] = i % 251;
    std::thread worker([&] {
        auto* interior = static_cast<uint8_t*>(device_) + 17;
        EXPECT_EQ(platform_.getMemoryType(interior), MTYPE_MLU);
        auto locations = platform_.getLocation(interior, kSize - 17);
        ASSERT_EQ(locations.size(), 1u);
        EXPECT_EQ(locations[0].location, "mlu:0");
        EXPECT_EQ(platform_.getMemoryType(seed.data()), MTYPE_CPU);
        ASSERT_TRUE(platform_.copy(interior, seed.data(), kSize - 17).ok());
        ASSERT_TRUE(platform_.copy(readback.data(), interior, kSize - 17).ok());
    });
    worker.join();
    EXPECT_TRUE(std::equal(seed.begin(), seed.end() - 17, readback.begin()));
}

// A second driver context on the same card catches context restoration bugs
// even on a one-card worker. The allocation is not owned by MluPlatform/CNRT.
TEST_F(MluPlatformTest, CopiesExternalDriverAllocationAndRestoresContext) {
    struct Allocation {
        CNcontext previous = nullptr, owner = nullptr;
        CNaddr address = 0;
        ~Allocation() {
            if (owner) {
                cnCtxSetCurrent(owner);
                if (address) cnFree(address);
                cnCtxSetCurrent(previous);
                cnCtxDestroy(owner);
            }
        }
    } allocation;
    ASSERT_EQ(cnCtxGetCurrent(&allocation.previous), CN_SUCCESS);
    CNdev device;
    ASSERT_EQ(cnDeviceGet(&device, 0), CN_SUCCESS);
    ASSERT_EQ(cnCtxCreate(&allocation.owner, 0, device), CN_SUCCESS);
    ASSERT_EQ(cnMalloc(&allocation.address, 4096), CN_SUCCESS);
    ASSERT_EQ(cnCtxSetCurrent(allocation.previous), CN_SUCCESS);
    auto* ptr = reinterpret_cast<void*>(allocation.address);
    std::vector<uint8_t> seed(4096, 53), actual(4096);
    EXPECT_EQ(platform_.getMemoryType(ptr), MTYPE_MLU);
    ASSERT_TRUE(platform_.copy(ptr, seed.data(), seed.size()).ok());
    ASSERT_TRUE(platform_.copy(actual.data(), ptr, actual.size()).ok());
    EXPECT_EQ(actual, seed);
    CNcontext after = nullptr;
    ASSERT_EQ(cnCtxGetCurrent(&after), CN_SUCCESS);
    EXPECT_EQ(after, allocation.previous);
}

TEST_F(MluPlatformTest, RejectsDeviceOverrunAndInteriorFree) {
    auto* tail = static_cast<uint8_t*>(device_) + kSize - 1;
    uint8_t host[2] = {1, 2};
    EXPECT_FALSE(platform_.copy(tail, host, 2).ok());
    EXPECT_FALSE(platform_.copy(host, tail, 2).ok());
    EXPECT_TRUE(platform_.getLocation(tail, 2).empty());
    EXPECT_TRUE(
        platform_.getLocation(device_, std::numeric_limits<size_t>::max())
            .empty());
    EXPECT_FALSE(platform_.free(tail, 1).ok());
    EXPECT_FALSE(platform_.copy(device_, device_, 1).ok());
    EXPECT_EQ(host[0], 1);
    EXPECT_EQ(host[1], 2);
}

TEST_F(MluPlatformTest, AllocationValidationAndContextPreservation) {
    for (const auto& location :
         {"mlu:-1", "mlu:", "mlu:0junk", "mlu:99999", "cuda:0"}) {
        MemoryOptions options;
        options.location = location;
        void* ptr = nullptr;
        EXPECT_FALSE(platform_.allocate(&ptr, 4096, options).ok()) << location;
        EXPECT_EQ(ptr, nullptr);
    }
    for (const auto& location : {"mlu", "mlu:0", "cpu:0"}) {
        CNcontext before = nullptr, after = nullptr;
        ASSERT_EQ(cnCtxGetCurrent(&before), CN_SUCCESS);
        MemoryOptions options;
        options.location = location;
        void* ptr = nullptr;
        ASSERT_TRUE(platform_.allocate(&ptr, 4096, options).ok());
        EXPECT_EQ(platform_.getMemoryType(ptr),
                  options.location == "cpu:0" ? MTYPE_CPU : MTYPE_MLU);
        uint8_t host[4] = {1, 2, 3, 4};
        ASSERT_TRUE(platform_.copy(ptr, host, sizeof(host)).ok());
        ASSERT_TRUE(platform_.free(ptr, 4096).ok());
        ASSERT_EQ(cnCtxGetCurrent(&after), CN_SUCCESS);
        EXPECT_EQ(before, after);
    }
}
}  // namespace
}  // namespace mooncake::tent
