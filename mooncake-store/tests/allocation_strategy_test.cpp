#include "allocation_strategy.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "allocator.h"
#include "types.h"

namespace mooncake {

class AllocationStrategyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        strategy_ = std::make_unique<RandomAllocationStrategy>();
    }

    // Helper function to create a BufferAllocator for testing
    std::shared_ptr<BufferAllocator> CreateTestAllocator(
        const std::string& segment_name, size_t base_offset = 0) {
        const size_t base = 0x100000000ULL + base_offset;  // 4GB + offset
        const size_t size = 1024 * 1024 * 16;  // 16MB (multiple of 4MB)
        return std::make_shared<BufferAllocator>(segment_name, base, size);
    }

    std::unique_ptr<RandomAllocationStrategy> strategy_;
};

// Test basic functionality with empty allocators map
TEST_F(AllocationStrategyTest, EmptyAllocatorsMap) {
    std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>
        empty_allocators;
    ReplicateConfig config{1, "local"};

    auto result = strategy_->Allocate(empty_allocators, 100, config);
    EXPECT_EQ(result, nullptr);
}

// Test preferred segment behavior with empty allocators
TEST_F(AllocationStrategyTest, PreferredSegmentWithEmptyAllocators) {
    std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>
        allocators;
    ReplicateConfig config{1, "preferred_segment"};

    auto result = strategy_->Allocate(allocators, 100, config);
    EXPECT_EQ(result, nullptr);  // Should return nullptr for empty allocators
}

// Test preferred segment allocation when available
TEST_F(AllocationStrategyTest, PreferredSegmentAllocation) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("preferred", 0x10000000ULL);

    std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>
        allocators;
    allocators["segment1"] = allocator1;
    allocators["preferred"] = allocator2;

    ReplicateConfig config{1, "preferred"};
    size_t alloc_size = 1024;

    auto result = strategy_->Allocate(allocators, alloc_size, config);
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->get_descriptor().segment_name_, "preferred");
    EXPECT_EQ(result->get_descriptor().size_, alloc_size);
}

// Test fallback to random allocation when preferred segment doesn't exist
TEST_F(AllocationStrategyTest, PreferredSegmentNotFound) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL);

    std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>
        allocators;
    allocators["segment1"] = allocator1;
    allocators["segment2"] = allocator2;

    ReplicateConfig config{1, "nonexistent"};
    size_t alloc_size = 1024;

    auto result = strategy_->Allocate(allocators, alloc_size, config);
    ASSERT_NE(result, nullptr);
    // Should allocate from one of the available segments
    std::string segment_name = result->get_descriptor().segment_name_;
    EXPECT_TRUE(segment_name == "segment1" || segment_name == "segment2");
    EXPECT_EQ(result->get_descriptor().size_, alloc_size);
}

// Test multiple allocators with random selection
TEST_F(AllocationStrategyTest, MultipleAllocatorsRandomSelection) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL);
    auto allocator3 = CreateTestAllocator("segment3", 0x20000000ULL);

    std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>
        allocators;
    allocators["segment1"] = allocator1;
    allocators["segment2"] = allocator2;
    allocators["segment3"] = allocator3;

    ReplicateConfig config{1, ""};  // No preferred segment
    size_t alloc_size = 1024;

    // Perform multiple allocations to test randomness
    std::vector<std::string> allocated_segments;
    for (int i = 0; i < 10; ++i) {
        auto result = strategy_->Allocate(allocators, alloc_size, config);
        ASSERT_NE(result, nullptr);
        allocated_segments.push_back(result->get_descriptor().segment_name_);
        EXPECT_EQ(result->get_descriptor().size_, alloc_size);
    }

    // Verify that allocations happened on available segments
    for (const auto& segment : allocated_segments) {
        EXPECT_TRUE(segment == "segment1" || segment == "segment2" ||
                    segment == "segment3");
    }
}

// Test allocation when preferred segment has insufficient space
TEST_F(AllocationStrategyTest, PreferredSegmentInsufficientSpace) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("preferred", 0x10000000ULL);

    std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>
        allocators;
    allocators["segment1"] = allocator1;
    allocators["preferred"] = allocator2;

    // First, fill up the preferred allocator
    ReplicateConfig config{1, "preferred"};
    std::vector<std::unique_ptr<AllocatedBuffer>> buffers;

    // Allocate most of the space in preferred segment
    size_t large_alloc = 15 * 1024 * 1024;  // 15MB out of 16MB
    auto large_buffer = strategy_->Allocate(allocators, large_alloc, config);
    ASSERT_NE(large_buffer, nullptr);
    EXPECT_EQ(large_buffer->get_descriptor().segment_name_, "preferred");
    buffers.push_back(std::move(large_buffer));

    // Now try to allocate more than remaining space in preferred segment
    size_t small_alloc = 2 * 1024 * 1024;  // 2MB (more than remaining ~1MB)
    auto result = strategy_->Allocate(allocators, small_alloc, config);
    ASSERT_NE(result, nullptr);
    // Should fall back to segment1 since preferred doesn't have enough space
    EXPECT_EQ(result->get_descriptor().segment_name_, "segment1");
    EXPECT_EQ(result->get_descriptor().size_, small_alloc);
}

// Test allocation when all allocators are full
TEST_F(AllocationStrategyTest, AllAllocatorsFull) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL);

    std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>
        allocators;
    allocators["segment1"] = allocator1;
    allocators["segment2"] = allocator2;

    ReplicateConfig config{1, ""};
    std::vector<std::unique_ptr<AllocatedBuffer>> buffers;

    // Fill up both allocators
    size_t large_alloc = 15 * 1024 * 1024;  // 15MB each
    auto buffer1 = strategy_->Allocate(allocators, large_alloc, config);
    auto buffer2 = strategy_->Allocate(allocators, large_alloc, config);
    ASSERT_NE(buffer1, nullptr);
    ASSERT_NE(buffer2, nullptr);
    buffers.push_back(std::move(buffer1));
    buffers.push_back(std::move(buffer2));

    // Try to allocate more than remaining space
    size_t impossible_alloc = 5 * 1024 * 1024;  // 5MB (more than remaining)
    auto result = strategy_->Allocate(allocators, impossible_alloc, config);
    EXPECT_EQ(result, nullptr);  // Should fail
}

// Test allocation with zero size
TEST_F(AllocationStrategyTest, ZeroSizeAllocation) {
    auto allocator = CreateTestAllocator("segment1");
    std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>
        allocators;
    allocators["segment1"] = allocator;

    ReplicateConfig config{1, ""};

    auto result = strategy_->Allocate(allocators, 0, config);
    // Zero-size allocation behavior depends on BufferAllocator implementation
    // This test documents the current behavior
    if (result != nullptr) {
        EXPECT_EQ(result->get_descriptor().segment_name_, "segment1");
    }
}

// Test allocation with very large size
TEST_F(AllocationStrategyTest, VeryLargeSizeAllocation) {
    auto allocator = CreateTestAllocator("segment1");
    std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>
        allocators;
    allocators["segment1"] = allocator;

    ReplicateConfig config{1, ""};
    size_t huge_size = 100 * 1024 * 1024;  // 100MB (larger than 16MB capacity)

    auto result = strategy_->Allocate(allocators, huge_size, config);
    EXPECT_EQ(result, nullptr);  // Should fail due to insufficient capacity
}

}  // namespace mooncake
