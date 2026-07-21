// buffer_allocator_test.cpp
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstddef>
#include <cstring>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

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
                    segment_name, base, size, segment_name);
            case BufferAllocatorType::OFFSET:
                return std::make_shared<OffsetBufferAllocator>(
                    segment_name, base, size, segment_name);
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

TEST_F(BufferAllocatorTest, RestoreOffsetAllocationsAtOriginalAddresses) {
    constexpr uintptr_t kBase = 0x180000000ULL;
    constexpr size_t kCapacity = 16 * 1024 * 1024;
    const std::string segment = "restore-segment";
    const std::string endpoint = "restore-endpoint";

    auto original = std::make_shared<OffsetBufferAllocator>(
        segment, kBase, kCapacity, endpoint);
    auto first = original->allocate(123);
    auto removed = original->allocate(5003);
    auto last = original->allocate(777);
    ASSERT_NE(first, nullptr);
    ASSERT_NE(removed, nullptr);
    ASSERT_NE(last, nullptr);

    std::vector<AllocatedBuffer::Descriptor> descriptors = {
        first->get_descriptor(), last->get_descriptor()};
    removed.reset();

    auto restored = RestoreOffsetBufferAllocator(segment, kBase, kCapacity,
                                                 endpoint, descriptors);
    ASSERT_TRUE(restored.has_value());
    ASSERT_EQ(restored->buffers.size(), descriptors.size());
    EXPECT_EQ(restored->buffers[0]->get_descriptor().buffer_address_,
              descriptors[0].buffer_address_);
    EXPECT_EQ(restored->buffers[1]->get_descriptor().buffer_address_,
              descriptors[1].buffer_address_);

    auto new_buffer = restored->allocator->allocate(1024);
    ASSERT_NE(new_buffer, nullptr);
    const auto new_address = reinterpret_cast<uintptr_t>(new_buffer->data());
    for (const auto& descriptor : descriptors) {
        EXPECT_TRUE(
            new_address + new_buffer->size() <= descriptor.buffer_address_ ||
            descriptor.buffer_address_ + descriptor.size_ <= new_address);
    }

    auto wrong_endpoint = descriptors;
    wrong_endpoint[0].transport_endpoint_ = "other-endpoint";
    EXPECT_FALSE(RestoreOffsetBufferAllocator(segment, kBase, kCapacity,
                                              endpoint, wrong_endpoint)
                     .has_value());

    auto duplicate = descriptors;
    duplicate.push_back(descriptors.front());
    EXPECT_FALSE(RestoreOffsetBufferAllocator(segment, kBase, kCapacity,
                                              endpoint, duplicate)
                     .has_value());

    auto out_of_range = descriptors;
    out_of_range[0].buffer_address_ = kBase + kCapacity;
    EXPECT_FALSE(RestoreOffsetBufferAllocator(segment, kBase, kCapacity,
                                              endpoint, out_of_range)
                     .has_value());
}

TEST_F(BufferAllocatorTest, RestoreOffsetAllocationsValidatesRangesAndOrder) {
    constexpr uintptr_t kBase = 0x190000000ULL;
    constexpr size_t kCapacity = 4096;
    const std::string segment = "restore-validation";
    const std::string endpoint = "restore-validation-endpoint";
    auto descriptor = [&](uintptr_t address, uint64_t size) {
        return AllocatedBuffer::Descriptor{size, address, "tcp", endpoint};
    };

    std::vector<AllocatedBuffer::Descriptor> unsorted = {
        descriptor(kBase + 512, 64), descriptor(kBase + 128, 64)};
    auto restored = RestoreOffsetBufferAllocator(segment, kBase, kCapacity,
                                                 endpoint, unsorted);
    ASSERT_TRUE(restored.has_value());
    ASSERT_EQ(restored->buffers.size(), unsorted.size());
    EXPECT_EQ(reinterpret_cast<uintptr_t>(restored->buffers[0]->data()),
              unsorted[0].buffer_address_);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(restored->buffers[1]->data()),
              unsorted[1].buffer_address_);

    std::vector<AllocatedBuffer::Descriptor> overlapping = {
        descriptor(kBase + 128, 100), descriptor(kBase + 200, 32)};
    EXPECT_FALSE(RestoreOffsetBufferAllocator(segment, kBase, kCapacity,
                                              endpoint, overlapping)
                     .has_value());

    std::vector<AllocatedBuffer::Descriptor> normalized_past_end = {
        descriptor(kBase + kCapacity - 100, 100)};
    EXPECT_FALSE(RestoreOffsetBufferAllocator(segment, kBase, kCapacity,
                                              endpoint, normalized_past_end)
                     .has_value());

    EXPECT_FALSE(RestoreOffsetBufferAllocator(
                     segment, std::numeric_limits<size_t>::max() - 100, 200,
                     endpoint, {})
                     .has_value());
    std::vector<AllocatedBuffer::Descriptor> descriptor_overflow = {
        descriptor(std::numeric_limits<uintptr_t>::max() - 10, 20)};
    EXPECT_FALSE(RestoreOffsetBufferAllocator(segment, kBase, kCapacity,
                                              endpoint, descriptor_overflow)
                     .has_value());
}

TEST_F(BufferAllocatorTest, RestoredOffsetHandleReleasesItsExactAddress) {
    constexpr uintptr_t kBase = 0x1A0000000ULL;
    constexpr size_t kCapacity = 4096;
    const std::string endpoint = "restore-release";
    std::vector<AllocatedBuffer::Descriptor> descriptors = {
        {64, kBase + 128, "tcp", endpoint}, {64, kBase + 512, "tcp", endpoint}};
    auto restored = RestoreOffsetBufferAllocator(
        "restore-release", kBase, kCapacity, endpoint, descriptors);
    ASSERT_TRUE(restored.has_value());

    restored->buffers[0].reset();
    auto replacement = restored->allocator->allocate(64);
    ASSERT_NE(replacement, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(replacement->data()),
              descriptors[0].buffer_address_);
}

TEST_F(BufferAllocatorTest, RestoreOffsetAllocationsHasNoArbitraryGapLimit) {
    constexpr uintptr_t kBase = 0x1B0000000ULL;
    constexpr size_t kGapCount = 65537;
    const std::string endpoint = "restore-many-gaps";
    std::vector<AllocatedBuffer::Descriptor> descriptors;
    descriptors.reserve(kGapCount);
    for (size_t i = 0; i < kGapCount; ++i) {
        descriptors.push_back({1, kBase + 1 + i * 2, "tcp", endpoint});
    }

    auto restored = RestoreOffsetBufferAllocator(
        "restore-many-gaps", kBase, kGapCount * 2 + 1, endpoint, descriptors);
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->buffers.size(), descriptors.size());
    EXPECT_EQ(reinterpret_cast<uintptr_t>(restored->buffers.back()->data()),
              descriptors.back().buffer_address_);
}

TEST_F(BufferAllocatorTest, RestoreCachelibAllocationsAtOriginalAddresses) {
    constexpr uintptr_t kBase = 0x1C0000000ULL;
    constexpr size_t kCapacity = 4 * facebook::cachelib::Slab::kSize;
    const std::string segment = "cachelib-restore";
    const std::string endpoint = "cachelib-restore-endpoint";
    auto original = std::make_shared<CachelibBufferAllocator>(
        segment, kBase, kCapacity, endpoint);

    auto small_first = original->allocate(64);
    auto small_hole = original->allocate(64);
    auto small_last = original->allocate(64);
    auto large_first = original->allocate(4096);
    auto large_hole = original->allocate(4096);
    auto large_last = original->allocate(4096);
    ASSERT_NE(small_first, nullptr);
    ASSERT_NE(small_hole, nullptr);
    ASSERT_NE(small_last, nullptr);
    ASSERT_NE(large_first, nullptr);
    ASSERT_NE(large_hole, nullptr);
    ASSERT_NE(large_last, nullptr);

    std::vector<AllocatedBuffer::Descriptor> descriptors = {
        large_last->get_descriptor(), small_first->get_descriptor(),
        large_first->get_descriptor(), small_last->get_descriptor()};
    small_hole.reset();
    large_hole.reset();

    auto restored = RestoreCachelibBufferAllocator(segment, kBase, kCapacity,
                                                   endpoint, descriptors);
    ASSERT_TRUE(restored.has_value());
    ASSERT_EQ(restored->buffers.size(), descriptors.size());
    for (size_t i = 0; i < descriptors.size(); ++i) {
        EXPECT_EQ(reinterpret_cast<uintptr_t>(restored->buffers[i]->data()),
                  descriptors[i].buffer_address_);
    }

    auto new_buffer = restored->allocator->allocate(64);
    ASSERT_NE(new_buffer, nullptr);
    const auto new_address = reinterpret_cast<uintptr_t>(new_buffer->data());
    for (const auto& descriptor : descriptors) {
        EXPECT_NE(new_address, descriptor.buffer_address_);
    }

    const uintptr_t released = descriptors[1].buffer_address_;
    restored->buffers[1].reset();
    auto replacement = restored->allocator->allocate(descriptors[1].size_);
    ASSERT_NE(replacement, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(replacement->data()), released);
}

TEST_F(BufferAllocatorTest, RestoreCachelibAllocationsRejectsInvalidLayouts) {
    constexpr uintptr_t kBase = 0x1D0000000ULL;
    constexpr size_t kCapacity = 4 * facebook::cachelib::Slab::kSize;
    constexpr size_t kSlabSize = facebook::cachelib::Slab::kSize;
    const std::string endpoint = "cachelib-invalid-endpoint";
    auto descriptor = [&](uintptr_t address, uint64_t size) {
        return AllocatedBuffer::Descriptor{size, address, "tcp", endpoint};
    };
    auto restore = [&](const std::vector<AllocatedBuffer::Descriptor>& descs) {
        return RestoreCachelibBufferAllocator("cachelib-invalid", kBase,
                                              kCapacity, endpoint, descs);
    };

    EXPECT_FALSE(
        restore({descriptor(kBase, 64), descriptor(kBase, 4096)}).has_value());
    EXPECT_FALSE(restore({descriptor(kBase + 1, 64)}).has_value());
    EXPECT_FALSE(
        restore({descriptor(kBase, 64), descriptor(kBase, 64)}).has_value());

    auto wrong_endpoint = descriptor(kBase, 64);
    wrong_endpoint.transport_endpoint_ = "wrong";
    EXPECT_FALSE(restore({wrong_endpoint}).has_value());
    EXPECT_FALSE(restore({descriptor(kBase + kCapacity, 64)}).has_value());
    EXPECT_FALSE(RestoreCachelibBufferAllocator("cachelib-invalid", kBase + 1,
                                                kCapacity, endpoint, {})
                     .has_value());
    EXPECT_FALSE(RestoreCachelibBufferAllocator(
                     "cachelib-invalid",
                     std::numeric_limits<size_t>::max() - kSlabSize,
                     2 * kSlabSize, endpoint, {})
                     .has_value());

    auto valid_after_fail = restore({descriptor(kBase + kSlabSize, 4096)});
    ASSERT_TRUE(valid_after_fail.has_value());
    EXPECT_EQ(reinterpret_cast<uintptr_t>(valid_after_fail->buffers[0]->data()),
              kBase + kSlabSize);
}

TEST_F(BufferAllocatorTest, CachelibImportRejectsChunkInSlabTail) {
    constexpr uintptr_t kBase = 0x1E0000000ULL;
    constexpr size_t kCapacity = 2 * facebook::cachelib::Slab::kSize;
    constexpr uint32_t kAllocSize = facebook::cachelib::Slab::kSize - 16;
    const size_t header_size =
        sizeof(facebook::cachelib::SlabHeader) * 2 + 1;
    auto headers = std::make_unique<char[]>(header_size);
    facebook::cachelib::MemoryAllocator allocator(
        facebook::cachelib::MemoryAllocator::Config({kAllocSize}),
        headers.get(), header_size, reinterpret_cast<void*>(kBase), kCapacity);
    const auto pool = allocator.addPool("main", kCapacity);

    EXPECT_FALSE(allocator.importAllocations(
        pool, {{reinterpret_cast<void*>(kBase + kAllocSize), kAllocSize}}));
}

TEST_F(BufferAllocatorTest, RestoreCachelibRejectsNonMemoryDescriptors) {
    constexpr uintptr_t kBase = 0x1F0000000ULL;
    constexpr size_t kCapacity = 2 * facebook::cachelib::Slab::kSize;
    const std::string endpoint = "cachelib-memory-only";
    std::vector<AllocatedBuffer::Descriptor> descriptors = {
        {64, kBase, "tcp", endpoint}};

    EXPECT_FALSE(RestoreCachelibBufferAllocator(
                     "cachelib-memory-only", kBase, kCapacity, endpoint,
                     descriptors, ReplicaType::NOF_SSD)
                     .has_value());

    descriptors[0].protocol_ = "cxl";
    EXPECT_FALSE(RestoreCachelibBufferAllocator(
                     "cachelib-memory-only", kBase, kCapacity, endpoint,
                     descriptors)
                     .has_value());

    descriptors[0].protocol_ = "rdma";
    auto rdma = RestoreCachelibBufferAllocator(
        "cachelib-memory-only", kBase, kCapacity, endpoint, descriptors);
    ASSERT_TRUE(rdma.has_value());
    const auto restored = rdma->buffers[0]->get_descriptor();
    EXPECT_EQ(restored.protocol_, descriptors[0].protocol_);
    EXPECT_EQ(restored.buffer_address_, descriptors[0].buffer_address_);
    EXPECT_EQ(restored.transport_endpoint_,
              descriptors[0].transport_endpoint_);
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
        size_t size = 1024 * 1024 * 32;  // 32MB (must be multiple of 4MB)
        auto allocator = CreateTestAllocator(segment_name, 0x20000000ULL, size,
                                             allocator_type);

        const int num_threads = 4;
        const auto test_duration = std::chrono::seconds(1);
        std::vector<std::thread> threads;
        std::atomic<int> success_count{0};
        std::atomic<bool> saw_invalid_buffer{false};

        // Create 4 threads, each performing repeated allocation and
        // deallocation for 1 second
        for (int thread_id = 0; thread_id < num_threads; ++thread_id) {
            threads.emplace_back([&allocator, test_duration, segment_name,
                                  &success_count, &saw_invalid_buffer]() {
                auto start_time = std::chrono::steady_clock::now();

                while (std::chrono::steady_clock::now() - start_time <
                       test_duration) {
                    size_t alloc_size = 477;
                    auto bufHandle = allocator->allocate(alloc_size);
                    if (!bufHandle) {
                        std::this_thread::yield();
                        continue;
                    }

                    auto descriptor = bufHandle->get_descriptor();
                    if (bufHandle->getSegmentName() != segment_name ||
                        descriptor.transport_endpoint_ != segment_name ||
                        descriptor.size_ != alloc_size ||
                        bufHandle->data() == nullptr) {
                        saw_invalid_buffer.store(true,
                                                 std::memory_order_relaxed);
                        bufHandle.reset();
                        break;
                    }
                    success_count.fetch_add(1, std::memory_order_relaxed);
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
        EXPECT_FALSE(saw_invalid_buffer.load(std::memory_order_relaxed));
        EXPECT_GT(success_count.load(std::memory_order_relaxed), 0);
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
