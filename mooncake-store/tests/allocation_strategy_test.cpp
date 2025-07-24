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
    std::shared_ptr<BufferAllocatorBase> CreateTestAllocator(
        const std::string& segment_name, size_t base_offset,
        BufferAllocatorType allocator_type) {
        const size_t base = 0x100000000ULL + base_offset;  // 4GB + offset
        const size_t size = 1024 * 1024 * 16;  // 16MB (multiple of 4MB)
        switch (allocator_type) {
            case BufferAllocatorType::CACHELIB:
                return std::make_shared<CachelibBufferAllocator>(segment_name,
                                                                 base, size);
            case BufferAllocatorType::OFFSET:
                return std::make_shared<OffsetBufferAllocator>(segment_name,
                                                               base, size);
            default:
                throw std::invalid_argument("Invalid allocator type");
        }
    }

    std::vector<BufferAllocatorType> allocator_types_ = {
        BufferAllocatorType::CACHELIB, BufferAllocatorType::OFFSET};

    std::unique_ptr<RandomAllocationStrategy> strategy_;
};

// Test basic functionality with empty allocators map
TEST_F(AllocationStrategyTest, EmptyAllocatorsMap) {
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        empty_allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> empty_allocators;
    ReplicateConfig config{1, false, "local"};

    auto result = strategy_->Allocate(empty_allocators,
                                      empty_allocators_by_name, 100, config);
    EXPECT_EQ(result, nullptr);
}

// Test preferred segment behavior with empty allocators
TEST_F(AllocationStrategyTest, PreferredSegmentWithEmptyAllocators) {
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        empty_allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> empty_allocators;
    ReplicateConfig config{1, false, "preferred_segment"};

    auto result = strategy_->Allocate(empty_allocators,
                                      empty_allocators_by_name, 100, config);
    EXPECT_EQ(result, nullptr);  // Should return nullptr for empty allocators
}

// Test preferred segment allocation when available
TEST_F(AllocationStrategyTest, PreferredSegmentAllocation) {
    for (const auto& allocator_type : allocator_types_) {
        auto allocator1 = CreateTestAllocator("segment1", 0, allocator_type);
        auto allocator2 =
            CreateTestAllocator("preferred", 0x10000000ULL, allocator_type);

        std::unordered_map<std::string,
                           std::vector<std::shared_ptr<BufferAllocatorBase>>>
            allocators_by_name;
        std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

        allocators_by_name["segment1"].push_back(allocator1);
        allocators_by_name["preferred"].push_back(allocator2);
        allocators.push_back(allocator1);
        allocators.push_back(allocator2);

        ReplicateConfig config{1, false, "preferred"};
        size_t alloc_size = 1024;

        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          alloc_size, config);
        ASSERT_NE(result, nullptr);
        EXPECT_EQ(result->get_descriptor().segment_name_, "preferred");
        EXPECT_EQ(result->get_descriptor().size_, alloc_size);
    }
}

// Test fallback to random allocation when preferred segment doesn't exist
TEST_F(AllocationStrategyTest, PreferredSegmentNotFound) {
    for (const auto& allocator_type : allocator_types_) {
        auto allocator1 = CreateTestAllocator("segment1", 0, allocator_type);
        auto allocator2 =
            CreateTestAllocator("segment2", 0x10000000ULL, allocator_type);

        std::unordered_map<std::string,
                           std::vector<std::shared_ptr<BufferAllocatorBase>>>
            allocators_by_name;
        std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

        allocators_by_name["segment1"].push_back(allocator1);
        allocators_by_name["segment2"].push_back(allocator2);
        allocators.push_back(allocator1);
        allocators.push_back(allocator2);

        ReplicateConfig config{1, false, "nonexistent"};
        size_t alloc_size = 1024;

        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          alloc_size, config);
        ASSERT_NE(result, nullptr);
        // Should allocate from one of the available segments
        std::string segment_name = result->get_descriptor().segment_name_;
        EXPECT_TRUE(segment_name == "segment1" || segment_name == "segment2");
        EXPECT_EQ(result->get_descriptor().size_, alloc_size);
    }
}

// Test multiple allocators with random selection
TEST_F(AllocationStrategyTest, MultipleAllocatorsRandomSelection) {
    for (const auto& allocator_type : allocator_types_) {
        auto allocator1 = CreateTestAllocator("segment1", 0, allocator_type);
        auto allocator2 =
            CreateTestAllocator("segment2", 0x10000000ULL, allocator_type);
        auto allocator3 =
            CreateTestAllocator("segment3", 0x20000000ULL, allocator_type);

        std::unordered_map<std::string,
                           std::vector<std::shared_ptr<BufferAllocatorBase>>>
            allocators_by_name;
        std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

        allocators_by_name["segment1"].push_back(allocator1);
        allocators_by_name["segment2"].push_back(allocator2);
        allocators_by_name["segment3"].push_back(allocator3);
        allocators.push_back(allocator1);
        allocators.push_back(allocator2);
        allocators.push_back(allocator3);

        ReplicateConfig config{1, false, ""};  // No preferred segment
        size_t alloc_size = 1024;

        // Perform multiple allocations to test randomness
        std::vector<std::string> allocated_segments;
        for (int i = 0; i < 10; ++i) {
            auto result = strategy_->Allocate(allocators, allocators_by_name,
                                              alloc_size, config);
            ASSERT_NE(result, nullptr);
            allocated_segments.push_back(
                result->get_descriptor().segment_name_);
            EXPECT_EQ(result->get_descriptor().size_, alloc_size);
        }

        // Verify that allocations happened on available segments
        for (const auto& segment : allocated_segments) {
            EXPECT_TRUE(segment == "segment1" || segment == "segment2" ||
                        segment == "segment3");
        }
    }
}

// Test allocation when preferred segment has insufficient space
TEST_F(AllocationStrategyTest, PreferredSegmentInsufficientSpace) {
    for (const auto& allocator_type : allocator_types_) {
        auto allocator1 = CreateTestAllocator("segment1", 0, allocator_type);
        auto allocator2 =
            CreateTestAllocator("preferred", 0x10000000ULL, allocator_type);

        std::unordered_map<std::string,
                           std::vector<std::shared_ptr<BufferAllocatorBase>>>
            allocators_by_name;
        std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

        allocators_by_name["segment1"].push_back(allocator1);
        allocators_by_name["preferred"].push_back(allocator2);
        allocators.push_back(allocator1);
        allocators.push_back(allocator2);

        // First, fill up the preferred allocator
        ReplicateConfig config{1, false, "preferred"};
        std::vector<std::unique_ptr<AllocatedBuffer>> buffers;

        // Allocate most of the space in preferred segment
        size_t large_alloc = 15 * 1024 * 1024;  // 15MB out of 16MB
        auto large_buffer = strategy_->Allocate(allocators, allocators_by_name,
                                                large_alloc, config);
        ASSERT_NE(large_buffer, nullptr);
        EXPECT_EQ(large_buffer->get_descriptor().segment_name_, "preferred");
        buffers.push_back(std::move(large_buffer));

        // Now try to allocate more than remaining space in preferred segment
        size_t small_alloc = 2 * 1024 * 1024;  // 2MB (more than remaining ~1MB)
        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          small_alloc, config);
        ASSERT_NE(result, nullptr);
        // Should fall back to segment1 since preferred doesn't have enough
        // space
        EXPECT_EQ(result->get_descriptor().segment_name_, "segment1");
        EXPECT_EQ(result->get_descriptor().size_, small_alloc);
    }
}

// Test allocation when all allocators are full
TEST_F(AllocationStrategyTest, AllAllocatorsFull) {
    for (const auto& allocator_type : allocator_types_) {
        auto allocator1 = CreateTestAllocator("segment1", 0, allocator_type);
        auto allocator2 =
            CreateTestAllocator("segment2", 0x10000000ULL, allocator_type);

        std::unordered_map<std::string,
                           std::vector<std::shared_ptr<BufferAllocatorBase>>>
            allocators_by_name;
        std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

        allocators_by_name["segment1"].push_back(allocator1);
        allocators_by_name["segment2"].push_back(allocator2);
        allocators.push_back(allocator1);
        allocators.push_back(allocator2);

        ReplicateConfig config{1, false, ""};
        std::vector<std::unique_ptr<AllocatedBuffer>> buffers;

        // Fill up both allocators
        size_t large_alloc = 15 * 1024 * 1024;  // 15MB each
        auto buffer1 = strategy_->Allocate(allocators, allocators_by_name,
                                           large_alloc, config);
        auto buffer2 = strategy_->Allocate(allocators, allocators_by_name,
                                           large_alloc, config);
        ASSERT_NE(buffer1, nullptr);
        ASSERT_NE(buffer2, nullptr);
        buffers.push_back(std::move(buffer1));
        buffers.push_back(std::move(buffer2));

        // Try to allocate more than remaining space
        size_t impossible_alloc = 5 * 1024 * 1024;  // 5MB (more than remaining)
        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          impossible_alloc, config);
        EXPECT_EQ(result, nullptr);  // Should fail
    }
}

// Test allocation with zero size
TEST_F(AllocationStrategyTest, ZeroSizeAllocation) {
    for (const auto& allocator_type : allocator_types_) {
        auto allocator = CreateTestAllocator("segment1", 0, allocator_type);
        std::unordered_map<std::string,
                           std::vector<std::shared_ptr<BufferAllocatorBase>>>
            allocators_by_name;
        std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

        allocators_by_name["segment1"].push_back(allocator);
        allocators.push_back(allocator);

        ReplicateConfig config{1, false, ""};

        auto result =
            strategy_->Allocate(allocators, allocators_by_name, 0, config);
        // Zero-size allocation behavior depends on BufferAllocator
        // implementation This test documents the current behavior
        if (result != nullptr) {
            EXPECT_EQ(result->get_descriptor().segment_name_, "segment1");
        }
    }
}

// Test allocation with very large size
TEST_F(AllocationStrategyTest, VeryLargeSizeAllocation) {
    for (const auto& allocator_type : allocator_types_) {
        auto allocator = CreateTestAllocator("segment1", 0, allocator_type);
        std::unordered_map<std::string,
                           std::vector<std::shared_ptr<BufferAllocatorBase>>>
            allocators_by_name;
        std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

        allocators_by_name["segment1"].push_back(allocator);
        allocators.push_back(allocator);

        ReplicateConfig config{1, false, ""};
        size_t huge_size =
            100 * 1024 * 1024;  // 100MB (larger than 16MB capacity)

        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          huge_size, config);
        EXPECT_EQ(result, nullptr);  // Should fail due to insufficient capacity
    }
}

}  // namespace mooncake