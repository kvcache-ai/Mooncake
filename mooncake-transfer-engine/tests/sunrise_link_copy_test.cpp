#include <gtest/gtest.h>

#include <cstring>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "gpu_vendor/sunrise.h"
#include "sunrise_allocator.h"
#include "transfer_engine.h"
#include "transport/transport.h"

using namespace mooncake;
using namespace mooncake::sunrise_alloc_detail;

namespace {

static constexpr size_t kTestSize = 4096;
static constexpr size_t kLargeTestSize = 4 * 1024 * 1024;

class SunriseLinkCopyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        int gpu_count = 0;
        if (tangGetDeviceCount(&gpu_count) != tangSuccess || gpu_count <= 0) {
            GTEST_SKIP() << "Sunrise device is unavailable";
        }
        ASSERT_EQ(tangSetDevice(0), tangSuccess);
    }

    static std::vector<char> makePattern(size_t size, char seed) {
        std::vector<char> data(size);
        for (size_t i = 0; i < size; ++i)
            data[i] = static_cast<char>((seed + i) % 256);
        return data;
    }

    static bool verifyHostBuffer(const void* host_ptr, size_t size, char seed) {
        auto expected = makePattern(size, seed);
        return memcmp(host_ptr, expected.data(), size) == 0;
    }
};

}  // namespace

TEST_F(SunriseLinkCopyTest, DeviceToHostAllocStaging) {
    void* dev_buf = nullptr;
    ASSERT_EQ(tangMalloc(&dev_buf, kTestSize), tangSuccess);
    void* host_buf = nullptr;
    ASSERT_EQ(tangHostAlloc(&host_buf, kTestSize, 0), tangSuccess);

    auto pattern = makePattern(kTestSize, 'A');
    ASSERT_EQ(
        tangMemcpy(dev_buf, pattern.data(), kTestSize, tangMemcpyHostToDevice),
        tangSuccess);

    std::vector<char> staging(kTestSize);
    ASSERT_EQ(
        tangMemcpy(staging.data(), dev_buf, kTestSize, tangMemcpyDeviceToHost),
        tangSuccess);
    memcpy(host_buf, staging.data(), kTestSize);
    EXPECT_TRUE(verifyHostBuffer(host_buf, kTestSize, 'A'));

    tangFree(dev_buf);
    tangFreeHost(host_buf);
}

TEST_F(SunriseLinkCopyTest, HostAllocToDeviceStaging) {
    void* host_buf = nullptr;
    ASSERT_EQ(tangHostAlloc(&host_buf, kTestSize, 0), tangSuccess);
    void* dev_buf = nullptr;
    ASSERT_EQ(tangMalloc(&dev_buf, kTestSize), tangSuccess);

    auto pattern = makePattern(kTestSize, 'B');
    memcpy(host_buf, pattern.data(), kTestSize);

    std::vector<char> staging(kTestSize);
    memcpy(staging.data(), host_buf, kTestSize);
    ASSERT_EQ(
        tangMemcpy(dev_buf, staging.data(), kTestSize, tangMemcpyHostToDevice),
        tangSuccess);

    std::vector<char> verify(kTestSize);
    ASSERT_EQ(
        tangMemcpy(verify.data(), dev_buf, kTestSize, tangMemcpyDeviceToHost),
        tangSuccess);
    EXPECT_EQ(memcmp(verify.data(), pattern.data(), kTestSize), 0);

    tangFree(dev_buf);
    tangFreeHost(host_buf);
}

TEST_F(SunriseLinkCopyTest, DeviceToHostAllocLargeData) {
    void* dev_buf = nullptr;
    ASSERT_EQ(tangMalloc(&dev_buf, kLargeTestSize), tangSuccess);
    void* host_buf = nullptr;
    ASSERT_EQ(tangHostAlloc(&host_buf, kLargeTestSize, 0), tangSuccess);

    auto pattern = makePattern(kLargeTestSize, 'C');
    ASSERT_EQ(tangMemcpy(dev_buf, pattern.data(), kLargeTestSize,
                         tangMemcpyHostToDevice),
              tangSuccess);

    std::vector<char> staging(kLargeTestSize);
    ASSERT_EQ(tangMemcpy(staging.data(), dev_buf, kLargeTestSize,
                         tangMemcpyDeviceToHost),
              tangSuccess);
    memcpy(host_buf, staging.data(), kLargeTestSize);
    EXPECT_TRUE(verifyHostBuffer(host_buf, kLargeTestSize, 'C'));

    tangFree(dev_buf);
    tangFreeHost(host_buf);
}

TEST_F(SunriseLinkCopyTest, DeviceToHostAllocStagingConsistency) {
    for (int trial = 0; trial < 5; ++trial) {
        void* dev_buf = nullptr;
        ASSERT_EQ(tangMalloc(&dev_buf, kTestSize), tangSuccess);
        void* host_buf = nullptr;
        ASSERT_EQ(tangHostAlloc(&host_buf, kTestSize, 0), tangSuccess);

        char seed = static_cast<char>('D' + trial);
        auto pattern = makePattern(kTestSize, seed);
        ASSERT_EQ(tangMemcpy(dev_buf, pattern.data(), kTestSize,
                             tangMemcpyHostToDevice),
                  tangSuccess);

        std::vector<char> staging(kTestSize);
        ASSERT_EQ(tangMemcpy(staging.data(), dev_buf, kTestSize,
                             tangMemcpyDeviceToHost),
                  tangSuccess);
        memcpy(host_buf, staging.data(), kTestSize);
        ASSERT_TRUE(verifyHostBuffer(host_buf, kTestSize, seed));

        tangFree(dev_buf);
        tangFreeHost(host_buf);
    }
}

TEST_F(SunriseLinkCopyTest, IsDeviceMemoryRangeOnRealAlloc) {
    void* dev_buf = nullptr;
    ASSERT_EQ(tangMalloc(&dev_buf, kTestSize), tangSuccess);
    addStoreMemRange(dev_buf, kTestSize);
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().insert(dev_buf);
    }

    EXPECT_TRUE(sunrise_is_device_memory_range(dev_buf));
    EXPECT_TRUE(
        sunrise_is_device_memory_range(static_cast<char*>(dev_buf) + 100));
    EXPECT_FALSE(sunrise_is_host_allocated(dev_buf));

    removeStoreMemRange(dev_buf);
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().erase(dev_buf);
    }
    tangFree(dev_buf);
}

TEST_F(SunriseLinkCopyTest, IsHostAllocatedOnRealAlloc) {
    void* host_buf = nullptr;
    ASSERT_EQ(tangHostAlloc(&host_buf, kTestSize, 0), tangSuccess);
    addStoreMemRange(host_buf, kTestSize);
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangHostAllocatedSet().insert(host_buf);
    }

    EXPECT_TRUE(sunrise_is_host_allocated(host_buf));
    EXPECT_TRUE(sunrise_is_host_allocated(static_cast<char*>(host_buf) + 100));
    EXPECT_FALSE(sunrise_is_device_memory_range(host_buf));

    removeStoreMemRange(host_buf);
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangHostAllocatedSet().erase(host_buf);
    }
    tangFreeHost(host_buf);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
