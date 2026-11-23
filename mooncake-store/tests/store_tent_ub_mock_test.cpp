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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "allocator.h"
#include "client_service.h"
#include "common/client_buffer_allocation.h"
#include "loopback_urma_adapter.h"
#include "real_client.h"
#include "test_server_helpers.h"

namespace mooncake {
namespace testing {
namespace {

long hugepagesFree() {
    std::ifstream file("/proc/meminfo");
    std::string key;
    long value = -1;
    while (file >> key) {
        if (key == "HugePages_Free:") {
            file >> value;
            break;
        }
        file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    return value;
}

class StoreTentUbMockTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("StoreTentUbMockTest");
        FLAGS_logtostderr = 1;
        // Set before RealClient first reads its segment allocation policy.
        setenv("MC_STORE_USE_HUGEPAGE", "1", 1);
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        ASSERT_TRUE(tent::ub::mock::loopbackRequested());
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
    }

    void TearDown() override { master_.Stop(); }

    InProcMaster master_;
};

TEST_F(StoreTentUbMockTest, PutAndGetUseUb) {
    namespace ub_mock = tent::ub::mock;
    constexpr size_t kSize = 1024 * 1024;
    constexpr size_t kSegmentSize = 512 * 1024 * 1024;
    constexpr size_t kBufferSize = 128 * 1024 * 1024;

    auto provider = Client::Create("localhost:17812", "P2PHANDSHAKE", "ub",
                                   std::nullopt, master_.master_address());
    ASSERT_TRUE(provider.has_value());
    auto client = Client::Create("localhost:17813", "P2PHANDSHAKE", "ub",
                                 std::nullopt, master_.master_address());
    ASSERT_TRUE(client.has_value());

    void* segment = allocate_buffer_allocator_memory(kSegmentSize);
    ASSERT_NE(segment, nullptr);
    auto mounted = (*provider)->MountSegment(segment, kSegmentSize, "ub");
    ASSERT_TRUE(mounted.has_value()) << toString(mounted.error());

    SimpleAllocator buffers(kBufferSize);
    auto registered = (*client)->RegisterLocalMemory(
        buffers.getBase(), kBufferSize, "cpu:0", false, false);
    ASSERT_TRUE(registered.has_value()) << toString(registered.error());

    void* source = buffers.allocate(kSize);
    void* target = buffers.allocate(kSize);
    ASSERT_NE(source, nullptr);
    ASSERT_NE(target, nullptr);
    for (size_t i = 0; i < kSize; ++i) {
        static_cast<uint8_t*>(source)[i] = static_cast<uint8_t>(i * 13 + 7);
    }
    std::memset(target, 0, kSize);
    ub_mock::resetLoopbackStats();

    ReplicateConfig config;
    config.replica_num = 1;
    std::vector<Slice> slices{{source, kSize}};
    auto put = (*client)->Put("ub_mock_put_get_key", slices, config);
    ASSERT_TRUE(put.has_value()) << toString(put.error());
    const auto after_put = ub_mock::loopbackStats();
    EXPECT_GT(after_put.write_requests, 0u);
    EXPECT_GE(after_put.bytes, kSize);

    slices = {{target, kSize}};
    auto get = (*client)->Get("ub_mock_put_get_key", slices);
    ASSERT_TRUE(get.has_value()) << toString(get.error());
    EXPECT_EQ(std::memcmp(target, source, kSize), 0);
    const auto after_get = ub_mock::loopbackStats();
    EXPECT_GT(after_get.read_requests, 0u);
    EXPECT_GE(after_get.bytes - after_put.bytes, kSize);

    buffers.deallocate(source, kSize);
    buffers.deallocate(target, kSize);
    EXPECT_TRUE(
        (*client)->unregisterLocalMemory(buffers.getBase(), false).has_value());
    EXPECT_TRUE((*provider)->UnmountSegment(segment, kSegmentSize).has_value());
    std::free(segment);
}

TEST_F(StoreTentUbMockTest, HugepageSegmentIsReleasedOnTeardown) {
    const long before = hugepagesFree();
    if (before <= 0) GTEST_SKIP() << "No free hugepages on this host";

    auto client = RealClient::create();
    ASSERT_EQ(client->setup_real("localhost:17817", "P2PHANDSHAKE",
                                 16 * 1024 * 1024, 16 * 1024 * 1024, "ub", "",
                                 master_.master_address()),
              0);
    ASSERT_LT(hugepagesFree(), before) << "setup did not map hugepages";

    client->tearDownAll();
    client.reset();
    EXPECT_EQ(hugepagesFree(), before)
        << "hugepages mapped by setup were not released on teardown";
}

}  // namespace
}  // namespace testing
}  // namespace mooncake
