// buffer_allocator_test.cpp
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstddef>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>
#include <chrono>

#include "allocator.h"
#include "types.h"

namespace mooncake {

// Test fixture for BufferAllocator tests
class BufferAllocatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize glog for logging
        google::InitGoogleLogging("BufferAllocatorTest");
        FLAGS_logtostderr = 1;  // Output logs to stderr
    }

    void TearDown() override {
        // Cleanup glog
        google::ShutdownGoogleLogging();
    }

    // Helper function to create a BufferAllocator for testing
    std::shared_ptr<BufferAllocatorBase> CreateTestAllocator(
        const std::string& segment_name, size_t base_offset, size_t size,
        BufferAllocatorType allocator_type) {
        const size_t base = 0x100000000ULL + base_offset;  // 4GB + offset
        switch (allocator_type) {
            case BufferAllocatorType::CACHELIB:
                return std::make_shared<CachelibBufferAllocator>(
                    segment_name, base, size, segment_name, generate_uuid());
            case BufferAllocatorType::OFFSET:
                return std::make_shared<OffsetBufferAllocator>(
                    segment_name, base, size, segment_name, generate_uuid());
            default:
                throw std::invalid_argument("Invalid allocator type");
        }
    }

    void VerifyAllocatedBuffer(const AllocatedBuffer& bufHandle,
                               size_t alloc_size,
                               const std::string& segment_name,
                               const std::string& transport_endpoint) {
        auto descriptor = bufHandle.get_descriptor();
        EXPECT_EQ(bufHandle.getSegmentName(), segment_name);
        EXPECT_EQ(descriptor.transport_endpoint_, transport_endpoint);
        EXPECT_EQ(descriptor.size_, alloc_size);
        EXPECT_NE(bufHandle.data(), nullptr);
    }

    std::vector<BufferAllocatorType> allocator_types_ = {
        BufferAllocatorType::CACHELIB, BufferAllocatorType::OFFSET};
};

// Test basic allocation and deallocation functionality
TEST_F(BufferAllocatorTest, AllocateAndDeallocate) {
    for (const auto& allocator_type : allocator_types_) {
        std::string segment_name = "1";
        size_t size = 1024 * 1024 * 16;  // 16MB (multiple of 4MB)
        auto allocator =
            CreateTestAllocator(segment_name, 0, size, allocator_type);

        // Allocate memory block
        size_t alloc_size = 1024;
        auto bufHandle = allocator->allocate(alloc_size);
        auto descriptor = bufHandle->get_descriptor();
        // Verify allocation success and properties
        ASSERT_NE(bufHandle, nullptr);
        VerifyAllocatedBuffer(*bufHandle, alloc_size, segment_name,
                              segment_name);

        // Release memory
        bufHandle.reset();
    }
}

// Test multiple allocations within the buffer
TEST_F(BufferAllocatorTest, AllocateMultiple) {
    for (const auto& allocator_type : allocator_types_) {
        std::string segment_name = "1";
        size_t size = 1024 * 1024 * 16;  // 16MB (must be multiple of 4MB)
        auto allocator =
            CreateTestAllocator(segment_name, 0, size, allocator_type);

        // Allocate multiple memory blocks
        size_t alloc_size = 1024 * 1024;  // 1MB per block
        std::vector<std::unique_ptr<AllocatedBuffer>> handles;

        // Attempt to allocate 8 blocks (should succeed as total size is less
        // than buffer size)
        for (int i = 0; i < 8; ++i) {
            auto bufHandle = allocator->allocate(alloc_size);
            ASSERT_NE(bufHandle, nullptr);
            VerifyAllocatedBuffer(*bufHandle, alloc_size, segment_name,
                                  segment_name);
            handles.push_back(std::move(bufHandle));
        }

        // Clean up allocated memory
        handles.clear();
        LOG(INFO) << "Cleaned up handles in AllocateMultiple test";
    }
}

// Test allocation request larger than available space
TEST_F(BufferAllocatorTest, AllocateTooLarge) {
    for (const auto& allocator_type : allocator_types_) {
        std::string segment_name = "3";
        size_t size = 1024 * 1024 * 16;  // 16MB (must be multiple of 4MB)

        auto allocator = CreateTestAllocator(segment_name, 0x20000000ULL, size,
                                             allocator_type);

        // Attempt to allocate more than total buffer size
        size_t alloc_size = size + 1;
        auto bufHandle = allocator->allocate(alloc_size);
        EXPECT_EQ(bufHandle, nullptr);
    }
}

// Test repeated allocation and deallocation until the total allocated size
// larger than the buffer size
TEST_F(BufferAllocatorTest, RepeatAllocateAndDeallocate) {
    for (const auto& allocator_type : allocator_types_) {
        std::string segment_name = "test";
        size_t size = 1024 * 1024 * 16;  // 16MB (must be multiple of 4MB)
        auto allocator = CreateTestAllocator(segment_name, 0x20000000ULL, size,
                                             allocator_type);

        // Allocate and deallocate multiple times
        size_t alloc_size = 1024;
        for (size_t i = 0; i < size / alloc_size * 2; ++i) {
            auto bufHandle = allocator->allocate(alloc_size);
            ASSERT_NE(bufHandle, nullptr);
            VerifyAllocatedBuffer(*bufHandle, alloc_size, segment_name,
                                  segment_name);
        }
    }
}

// Test parallel allocation and deallocation
TEST_F(BufferAllocatorTest, ParallelAllocation) {
    for (const auto& allocator_type : allocator_types_) {
        std::string segment_name = "test";
        size_t size = 1024 * 1024 * 16;  // 16MB (must be multiple of 4MB)
        auto allocator = CreateTestAllocator(segment_name, 0x20000000ULL, size,
                                             allocator_type);

        const int num_threads = 4;
        const auto test_duration = std::chrono::seconds(1);
        std::vector<std::thread> threads;

        // Create 4 threads, each performing repeated allocation and
        // deallocation for 1 second
        for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
            threads.emplace_back(
                [this, &allocator, test_duration, segment_name]() {
                    auto start_time = std::chrono::steady_clock::now();

                    while (std::chrono::steady_clock::now() - start_time <
                           test_duration) {
                        // Allocate memory of varying sizes
                        size_t alloc_size = 477;
                        auto bufHandle = allocator->allocate(alloc_size);

                        ASSERT_NE(bufHandle, nullptr);
                        VerifyAllocatedBuffer(*bufHandle, alloc_size,
                                              segment_name, segment_name);
                    }
                });
        }

        // Wait for all threads to complete
        for (auto& thread : threads) {
            thread.join();
        }

        LOG(INFO) << "Completed parallel allocation/deallocation test for "
                  << (allocator_type == BufferAllocatorType::CACHELIB
                          ? "CACHELIB"
                          : "OFFSET");
    }
}

// Test fixture for SimpleAllocator tests
class SimpleAllocatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("SimpleAllocatorTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

// Test basic memory allocation and deallocation
TEST_F(SimpleAllocatorTest, BasicAllocationAndDeallocation) {
    const size_t total_size = 1024 * 1024 * 16;  // 16MB (multiple of 4MB)
    SimpleAllocator allocator(total_size);

    // Test basic allocation
    size_t alloc_size = 1024;  // 1KB
    void* ptr = allocator.allocate(alloc_size);
    ASSERT_NE(ptr, nullptr);

    // Verify memory alignment
    EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 8, 0)
        << "Memory not 8-byte aligned";

    // Verify memory is usable
    std::memset(ptr, 0xFF, alloc_size);

    // Clean up
    allocator.deallocate(ptr, alloc_size);
}

// Test multiple allocations and deallocations
TEST_F(SimpleAllocatorTest, MultipleAllocations) {
    const size_t total_size = 1024 * 1024 * 16;  // 16MB
    SimpleAllocator allocator(total_size);

    std::vector<std::pair<void*, size_t>> allocations;
    const size_t alloc_size = 1024 * 1024;  // 1MB per block

    // Allocate multiple blocks
    for (int i = 0; i < 8; ++i) {
        void* ptr = allocator.allocate(alloc_size);
        ASSERT_NE(ptr, nullptr) << "Failed to allocate block " << i;
        allocations.emplace_back(ptr, alloc_size);
    }

    // Verify and deallocate all blocks
    for (const auto& [ptr, size] : allocations) {
        EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 8, 0)
            << "Memory not 8-byte aligned";
        allocator.deallocate(ptr, size);
    }
}

// Test allocation request larger than available space
TEST_F(SimpleAllocatorTest, AllocationTooLarge) {
    const size_t total_size = 1024 * 1024 * 16;  // 16MB
    SimpleAllocator allocator(total_size);

    void* ptr = allocator.allocate(total_size + 1);
    EXPECT_EQ(ptr, nullptr);
}

// Stress test with many small allocations
TEST_F(SimpleAllocatorTest, StressTest) {
    const size_t total_size = 1024 * 1024 * 256;  // 256MB for stress testing
    SimpleAllocator allocator(total_size);

    std::vector<std::pair<void*, size_t>> allocations;
    const size_t num_allocations = 100;

    // Perform multiple allocations of varying sizes
    for (size_t i = 0; i < num_allocations; ++i) {
        size_t size = 1024 * (1 + (i % 10));  // Vary between 1KB and 10KB
        void* ptr = allocator.allocate(size);
        if (ptr) {
            EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % 8, 0)
                << "Memory not 8-byte aligned";
            allocations.emplace_back(ptr, size);
        }
    }

    // Clean up all allocations in reverse order
    while (!allocations.empty()) {
        auto [ptr, size] = allocations.back();
        allocator.deallocate(ptr, size);
        allocations.pop_back();
    }
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
