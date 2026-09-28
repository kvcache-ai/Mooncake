#include <gtest/gtest.h>

#include <cstdint>
#include <mutex>
#include <unordered_set>
#include <vector>

#include "sunrise_allocator.h"

using namespace mooncake::sunrise_alloc_detail;

class SunriseAllocatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        saved_host_set_ = tangHostAllocatedSet();
        saved_dev_set_ = tangDeviceAllocatedSet();
        saved_ranges_ = storeMemRanges();
        tangHostAllocatedSet().clear();
        tangDeviceAllocatedSet().clear();
        storeMemRanges().clear();
    }

    void TearDown() override {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangHostAllocatedSet() = std::move(saved_host_set_);
        tangDeviceAllocatedSet() = std::move(saved_dev_set_);
        storeMemRanges() = std::move(saved_ranges_);
    }

   private:
    std::unordered_set<void*> saved_host_set_;
    std::unordered_set<void*> saved_dev_set_;
    std::vector<MemRange> saved_ranges_;
};

TEST_F(SunriseAllocatorTest, IsDeviceMemoryRangeNullptr) {
    EXPECT_FALSE(sunrise_is_device_memory_range(nullptr));
}

TEST_F(SunriseAllocatorTest, IsDeviceMemoryRangeUntracked) {
    int x;
    EXPECT_FALSE(sunrise_is_device_memory_range(&x));
}

TEST_F(SunriseAllocatorTest, IsDeviceMemoryRangeBasePointer) {
    void* dev_ptr = reinterpret_cast<void*>(0x1000);
    size_t size = 4096;
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().insert(dev_ptr);
    }
    addStoreMemRange(dev_ptr, size);

    EXPECT_TRUE(sunrise_is_device_memory_range(dev_ptr));

    removeStoreMemRange(dev_ptr);
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().erase(dev_ptr);
    }
}

TEST_F(SunriseAllocatorTest, IsDeviceMemoryRangeSubPointer) {
    void* base = reinterpret_cast<void*>(0x2000);
    size_t size = 8192;
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().insert(base);
    }
    addStoreMemRange(base, size);

    void* mid = reinterpret_cast<void*>(0x2000 + 100);
    void* end_minus_one = reinterpret_cast<void*>(0x2000 + size - 1);
    EXPECT_TRUE(sunrise_is_device_memory_range(mid));
    EXPECT_TRUE(sunrise_is_device_memory_range(end_minus_one));

    void* past_end = reinterpret_cast<void*>(0x2000 + size);
    EXPECT_FALSE(sunrise_is_device_memory_range(past_end));

    removeStoreMemRange(base);
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().erase(base);
    }
}

TEST_F(SunriseAllocatorTest, IsHostAllocatedBasePointer) {
    void* host_ptr = reinterpret_cast<void*>(0x3000);
    size_t size = 4096;
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangHostAllocatedSet().insert(host_ptr);
    }
    addStoreMemRange(host_ptr, size);

    EXPECT_TRUE(sunrise_is_host_allocated(host_ptr));
    EXPECT_FALSE(sunrise_is_device_memory_range(host_ptr));

    removeStoreMemRange(host_ptr);
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangHostAllocatedSet().erase(host_ptr);
    }
}

TEST_F(SunriseAllocatorTest, IsHostAllocatedSubPointer) {
    void* base = reinterpret_cast<void*>(0x4000);
    size_t size = 8192;
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangHostAllocatedSet().insert(base);
    }
    addStoreMemRange(base, size);

    void* mid = reinterpret_cast<void*>(0x4000 + 500);
    EXPECT_TRUE(sunrise_is_host_allocated(mid));
    EXPECT_FALSE(sunrise_is_device_memory_range(mid));

    removeStoreMemRange(base);
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangHostAllocatedSet().erase(base);
    }
}

TEST_F(SunriseAllocatorTest, HostAllocNotConfusedWithDevice) {
    void* host_ptr = reinterpret_cast<void*>(0x5000);
    size_t size = 4096;
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangHostAllocatedSet().insert(host_ptr);
    }
    addStoreMemRange(host_ptr, size);

    EXPECT_TRUE(sunrise_is_host_allocated(host_ptr));
    EXPECT_FALSE(sunrise_is_device_memory_range(host_ptr));

    removeStoreMemRange(host_ptr);
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangHostAllocatedSet().erase(host_ptr);
    }
}

TEST_F(SunriseAllocatorTest, DeviceAllocNotConfusedWithHost) {
    void* dev_ptr = reinterpret_cast<void*>(0x6000);
    size_t size = 4096;
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().insert(dev_ptr);
    }
    addStoreMemRange(dev_ptr, size);

    EXPECT_TRUE(sunrise_is_device_memory_range(dev_ptr));
    EXPECT_FALSE(sunrise_is_host_allocated(dev_ptr));

    removeStoreMemRange(dev_ptr);
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().erase(dev_ptr);
    }
}

TEST_F(SunriseAllocatorTest, RemoveStoreMemRangeCleansUp) {
    void* base = reinterpret_cast<void*>(0x8000);
    size_t size = 4096;
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().insert(base);
    }
    addStoreMemRange(base, size);
    EXPECT_TRUE(sunrise_is_device_memory_range(base));

    removeStoreMemRange(base);
    EXPECT_FALSE(sunrise_is_device_memory_range(base));

    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().erase(base);
    }
}

TEST_F(SunriseAllocatorTest, MultipleRanges) {
    void* dev_base = reinterpret_cast<void*>(0x9000);
    size_t dev_size = 4096;
    void* host_base = reinterpret_cast<void*>(0xA000);
    size_t host_size = 8192;

    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().insert(dev_base);
        tangHostAllocatedSet().insert(host_base);
    }
    addStoreMemRange(dev_base, dev_size);
    addStoreMemRange(host_base, host_size);

    EXPECT_TRUE(sunrise_is_device_memory_range(dev_base));
    EXPECT_FALSE(sunrise_is_device_memory_range(host_base));
    EXPECT_TRUE(sunrise_is_host_allocated(host_base));
    EXPECT_FALSE(sunrise_is_host_allocated(dev_base));

    void* dev_mid = reinterpret_cast<void*>(0x9000 + 50);
    void* host_mid = reinterpret_cast<void*>(0xA000 + 50);
    EXPECT_TRUE(sunrise_is_device_memory_range(dev_mid));
    EXPECT_TRUE(sunrise_is_host_allocated(host_mid));

    removeStoreMemRange(dev_base);
    removeStoreMemRange(host_base);
    {
        std::lock_guard<std::mutex> lock(tangAllocMutex());
        tangDeviceAllocatedSet().erase(dev_base);
        tangHostAllocatedSet().erase(host_base);
    }
}
