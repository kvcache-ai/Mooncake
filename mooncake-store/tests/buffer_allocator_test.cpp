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

namespace {

LiveAllocation ToLiveAllocation(uintptr_t base,
                                const AllocatedBuffer::Descriptor& descriptor) {
    return {descriptor.buffer_address_ - base, descriptor.size_};
}

}  // namespace

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
        auto allocator = CreateBufferAllocator(allocator_type, segment_name,
                                               base, size, segment_name);
        if (!allocator) {
            throw std::invalid_argument("Invalid allocator test parameters");
        }
        return std::move(*allocator);
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

TEST_F(BufferAllocatorTest, OffsetLargestFreeRegionRemainsExact) {
    constexpr size_t CAPACITY = 16 * 1024 * 1024;
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "exact-largest-free-region", 0x140000000ULL, CAPACITY,
        "exact-largest-free-region");

    EXPECT_EQ(allocator->getLargestFreeRegion(), CAPACITY);

    auto buffer = allocator->allocate(CAPACITY / 2);
    ASSERT_NE(buffer, nullptr);
    const auto internal_allocator = allocator->getOffsetAllocator();

    // Successful allocations intentionally leave the fast-fail hint high, but
    // the segment-selection query must still return the authoritative value.
    EXPECT_GT(internal_allocator->getLargestFreeRegion(),
              allocator->getLargestFreeRegion());
    EXPECT_EQ(allocator->getLargestFreeRegion(),
              internal_allocator->storageReport().largestFreeRegion);

    buffer.reset();
    EXPECT_EQ(allocator->getLargestFreeRegion(), CAPACITY);
}

TEST_F(BufferAllocatorTest, ImportOffsetAllocationsAtOriginalAddresses) {
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

    const std::vector<AllocatedBuffer::Descriptor> descriptors = {
        first->get_descriptor(), last->get_descriptor()};
    std::vector<LiveAllocation> allocations = {
        ToLiveAllocation(kBase, descriptors[0]),
        ToLiveAllocation(kBase, descriptors[1])};
    const auto removed_descriptor = removed->get_descriptor();
    removed.reset();

    auto restored = ImportOffsetBufferAllocator(segment, kBase, kCapacity,
                                                endpoint, allocations);
    ASSERT_TRUE(restored.has_value());
    ASSERT_EQ(restored->buffers.size(), descriptors.size());
    EXPECT_EQ(restored->allocator->size(),
              descriptors[0].size_ + descriptors[1].size_);
    EXPECT_EQ(restored->buffers[0]->get_descriptor().buffer_address_,
              descriptors[0].buffer_address_);
    EXPECT_EQ(restored->buffers[1]->get_descriptor().buffer_address_,
              descriptors[1].buffer_address_);

    auto new_buffer = restored->allocator->allocate(removed_descriptor.size_);
    ASSERT_NE(new_buffer, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(new_buffer->data()),
              removed_descriptor.buffer_address_);
}

TEST_F(BufferAllocatorTest, ImportOffsetAllocationsValidatesRangesAndOrder) {
    constexpr uintptr_t kBase = 0x190000000ULL;
    constexpr size_t kCapacity = 4096;
    const std::string segment = "restore-validation";
    const std::string endpoint = "restore-validation-endpoint";
    auto allocation = [&](uintptr_t address, uint64_t size) {
        return LiveAllocation{address - kBase, size};
    };

    std::vector<LiveAllocation> unsorted = {allocation(kBase + 512, 64),
                                            allocation(kBase + 128, 64)};
    auto restored = ImportOffsetBufferAllocator(segment, kBase, kCapacity,
                                                endpoint, unsorted);
    ASSERT_TRUE(restored.has_value());
    ASSERT_EQ(restored->buffers.size(), unsorted.size());
    EXPECT_EQ(reinterpret_cast<uintptr_t>(restored->buffers[0]->data()),
              kBase + unsorted[0].offset_from_base);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(restored->buffers[1]->data()),
              kBase + unsorted[1].offset_from_base);

    std::vector<LiveAllocation> overlapping = {allocation(kBase + 128, 100),
                                               allocation(kBase + 200, 32)};
    EXPECT_FALSE(ImportOffsetBufferAllocator(segment, kBase, kCapacity,
                                             endpoint, overlapping)
                     .has_value());

    auto normalization_probe =
        offset_allocator::OffsetAllocator::create(0, kCapacity);
    size_t rounded_request = 0;
    for (size_t request = kCapacity / 2; request < kCapacity; ++request) {
        const size_t normalized =
            normalization_probe->normalizedAllocationSize(request);
        if (normalized > request && normalized <= kCapacity) {
            rounded_request = request;
            break;
        }
    }
    ASSERT_NE(rounded_request, 0);
    std::vector<LiveAllocation> normalized_past_end = {
        allocation(kBase + kCapacity - rounded_request, rounded_request)};
    EXPECT_FALSE(ImportOffsetBufferAllocator(segment, kBase, kCapacity,
                                             endpoint, normalized_past_end)
                     .has_value());

    EXPECT_FALSE(ImportOffsetBufferAllocator(
                     segment, std::numeric_limits<size_t>::max() - 100, 200,
                     endpoint, {})
                     .has_value());
    std::vector<LiveAllocation> allocation_overflow = {
        {std::numeric_limits<uintptr_t>::max() - kBase - 10, 20}};
    EXPECT_FALSE(ImportOffsetBufferAllocator(segment, kBase, kCapacity,
                                             endpoint, allocation_overflow)
                     .has_value());
}

TEST_F(BufferAllocatorTest, ImportedOffsetHandleReleasesItsExactAddress) {
    constexpr uintptr_t kBase = 0x1A0000000ULL;
    constexpr size_t kCapacity = 4096;
    const std::string endpoint = "restore-release";
    std::vector<LiveAllocation> allocations = {{0, 64}, {512, 64}};
    auto restored = ImportOffsetBufferAllocator(
        "restore-release", kBase, kCapacity, endpoint, allocations);
    ASSERT_TRUE(restored.has_value());

    restored->buffers[0].reset();
    auto replacement = restored->allocator->allocate(64);
    ASSERT_NE(replacement, nullptr);
    EXPECT_EQ(reinterpret_cast<uintptr_t>(replacement->data()),
              kBase + allocations[0].offset_from_base);
}

TEST_F(BufferAllocatorTest, ImportOffsetAllocationsHasNoArbitraryGapLimit) {
    constexpr uintptr_t kBase = 0x1B0000000ULL;
    constexpr size_t kGapCount = 65537;
    const std::string endpoint = "restore-many-gaps";
    std::vector<LiveAllocation> allocations;
    allocations.reserve(kGapCount);
    for (size_t i = 0; i < kGapCount; ++i) {
        allocations.push_back({1 + i * 2, 1});
    }

    auto restored = ImportOffsetBufferAllocator(
        "restore-many-gaps", kBase, kGapCount * 2 + 1, endpoint, allocations);
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->buffers.size(), allocations.size());
    EXPECT_EQ(reinterpret_cast<uintptr_t>(restored->buffers.back()->data()),
              kBase + allocations.back().offset_from_base);
}

TEST_F(BufferAllocatorTest, CachelibCreateRejectsInvalidMemoryLayout) {
    constexpr size_t kSlabSize = facebook::cachelib::Slab::kSize;
    constexpr uintptr_t kBase = 0x1C0000000ULL;

    auto expect_invalid = [](size_t base, size_t size) {
        auto result = CachelibBufferAllocator::Create("cachelib-invalid", base,
                                                      size, "endpoint");
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    };

    expect_invalid(kBase + 1, kSlabSize);
    expect_invalid(kBase, kSlabSize + 1);
    expect_invalid(std::numeric_limits<size_t>::max() - kSlabSize,
                   2 * kSlabSize);
    if constexpr (std::numeric_limits<size_t>::max() / kSlabSize >
                  std::numeric_limits<unsigned int>::max()) {
        const size_t too_many_slabs =
            (static_cast<size_t>(std::numeric_limits<unsigned int>::max()) +
             1) *
            kSlabSize;
        expect_invalid(kBase, too_many_slabs);
    }
}

TEST_F(BufferAllocatorTest, ImportCachelibAllocationsAtOriginalAddresses) {
    constexpr uintptr_t kBase = 0x1C0000000ULL;
    constexpr size_t kCapacity = 4 * facebook::cachelib::Slab::kSize;
    const std::string segment = "cachelib-restore";
    const std::string endpoint = "cachelib-restore-endpoint";
    auto created =
        CachelibBufferAllocator::Create(segment, kBase, kCapacity, endpoint);
    ASSERT_TRUE(created.has_value());
    auto original = std::move(*created);

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
    std::vector<LiveAllocation> allocations;
    allocations.reserve(descriptors.size());
    for (const auto& descriptor : descriptors) {
        allocations.push_back(ToLiveAllocation(kBase, descriptor));
    }
    small_hole.reset();
    large_hole.reset();

    auto restored = ImportCachelibBufferAllocator(segment, kBase, kCapacity,
                                                  endpoint, allocations);
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

TEST_F(BufferAllocatorTest, ImportCachelibAllocationsRejectsInvalidLayouts) {
    constexpr uintptr_t kBase = 0x1D0000000ULL;
    constexpr size_t kCapacity = 4 * facebook::cachelib::Slab::kSize;
    const std::string endpoint = "cachelib-invalid-endpoint";
    auto allocation = [&](uintptr_t address, uint64_t size) {
        return LiveAllocation{address - kBase, size};
    };
    auto import = [&](const std::vector<LiveAllocation>& allocations) {
        return ImportCachelibBufferAllocator("cachelib-invalid", kBase,
                                             kCapacity, endpoint, allocations);
    };

    EXPECT_FALSE(
        import({allocation(kBase, 64), allocation(kBase, 4096)}).has_value());
    EXPECT_FALSE(import({allocation(kBase + 1, 64)}).has_value());
    EXPECT_FALSE(
        import({allocation(kBase, 64), allocation(kBase, 64)}).has_value());
    EXPECT_FALSE(import({allocation(kBase + kCapacity, 64)}).has_value());
}

TEST_F(BufferAllocatorTest, CachelibImportRejectsChunkInSlabTail) {
    constexpr uintptr_t kBase = 0x1E0000000ULL;
    constexpr size_t kCapacity = 2 * facebook::cachelib::Slab::kSize;
    constexpr uint32_t kAllocSize = facebook::cachelib::Slab::kSize - 16;
    const size_t header_size = sizeof(facebook::cachelib::SlabHeader) * 2 + 1;
    auto headers = std::make_unique<char[]>(header_size);
    facebook::cachelib::MemoryAllocator allocator(
        facebook::cachelib::MemoryAllocator::Config({kAllocSize}),
        headers.get(), header_size, reinterpret_cast<void*>(kBase), kCapacity);
    const auto pool = allocator.addPool("main", kCapacity);

    EXPECT_FALSE(allocator.importAllocations(
        pool, {{reinterpret_cast<void*>(kBase + kAllocSize), kAllocSize}}));
}

TEST_F(BufferAllocatorTest, CachelibImportRejectsNonMemoryReplicaType) {
    constexpr uintptr_t kBase = 0x1F0000000ULL;
    constexpr size_t kCapacity = 2 * facebook::cachelib::Slab::kSize;
    const std::string endpoint = "cachelib-memory-only";
    std::vector<LiveAllocation> allocations = {{0, 64}};

    EXPECT_FALSE(ImportCachelibBufferAllocator("cachelib-memory-only", kBase,
                                               kCapacity, endpoint, allocations,
                                               ReplicaType::NOF_SSD)
                     .has_value());
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
