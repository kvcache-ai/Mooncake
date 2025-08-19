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
        const size_t size =
            1024 * 1024 * 64;  // 64MB (for multiple slabs in cachelib)
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

    std::vector<size_t> slice_sizes = {100};
    auto result = strategy_->Allocate(
        empty_allocators, empty_allocators_by_name, slice_sizes, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test preferred segment behavior with empty allocators
TEST_F(AllocationStrategyTest, PreferredSegmentWithEmptyAllocators) {
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        empty_allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> empty_allocators;
    ReplicateConfig config{1, false, "preferred_segment"};

    std::vector<size_t> slice_sizes = {100};
    auto result = strategy_->Allocate(
        empty_allocators, empty_allocators_by_name, slice_sizes, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
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
        std::vector<size_t> slice_sizes = {1024};

        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          slice_sizes, config);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 1);
        ASSERT_FALSE(result.value().empty());

        const auto& replica = result.value()[0];
        auto descriptor = replica.get_descriptor();
        ASSERT_TRUE(descriptor.is_memory_replica());
        const auto& mem_desc = descriptor.get_memory_descriptor();
        ASSERT_EQ(mem_desc.buffer_descriptors.size(), 1);
        EXPECT_EQ(mem_desc.buffer_descriptors[0].segment_name_, "preferred");
        EXPECT_EQ(mem_desc.buffer_descriptors[0].size_, 1024);
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
        std::vector<size_t> slice_sizes = {1024};

        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          slice_sizes, config);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 1);

        const auto& replica = result.value()[0];
        auto descriptor = replica.get_descriptor();
        ASSERT_TRUE(descriptor.is_memory_replica());
        const auto& mem_desc = descriptor.get_memory_descriptor();
        ASSERT_EQ(mem_desc.buffer_descriptors.size(), 1);
        std::string segment_name = mem_desc.buffer_descriptors[0].segment_name_;
        EXPECT_TRUE(segment_name == "segment1" || segment_name == "segment2");
        EXPECT_EQ(mem_desc.buffer_descriptors[0].size_, 1024);
    }
}

// Test multiple slices allocation
TEST_F(AllocationStrategyTest, MultipleSlicesAllocation) {
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
        std::vector<size_t> slice_sizes = {1024, 2048, 512};

        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          slice_sizes, config);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 1);

        const auto& replica = result.value()[0];
        auto descriptor = replica.get_descriptor();
        ASSERT_TRUE(descriptor.is_memory_replica());
        const auto& mem_desc = descriptor.get_memory_descriptor();
        ASSERT_EQ(mem_desc.buffer_descriptors.size(), 3);
        EXPECT_EQ(mem_desc.buffer_descriptors[0].size_, 1024);
        EXPECT_EQ(mem_desc.buffer_descriptors[1].size_, 2048);
        EXPECT_EQ(mem_desc.buffer_descriptors[2].size_, 512);
    }
}

// Test multiple replicas allocation
TEST_F(AllocationStrategyTest, MultipleReplicasAllocation) {
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

        ReplicateConfig config{3, false, ""};  // Request 3 replicas
        std::vector<size_t> slice_sizes = {1024, 2048};

        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          slice_sizes, config);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 3);

        // Check each replica has all slices
        for (const auto& replica : result.value()) {
            auto descriptor = replica.get_descriptor();
            ASSERT_TRUE(descriptor.is_memory_replica());
            const auto& mem_desc = descriptor.get_memory_descriptor();
            ASSERT_EQ(mem_desc.buffer_descriptors.size(), 2);
            EXPECT_EQ(mem_desc.buffer_descriptors[0].size_, 1024);
            EXPECT_EQ(mem_desc.buffer_descriptors[1].size_, 2048);
        }

        // Check that replicas are on different segments
        std::set<std::string> used_segments;
        for (const auto& replica : result.value()) {
            auto segment_names = replica.get_segment_names();
            for (const auto& name_ptr : segment_names) {
                if (name_ptr) {
                    used_segments.insert(*name_ptr);
                }
            }
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
        std::vector<size_t> large_slices = {
            10 * 1024 * 1024, 10 * 1024 * 1024, 10 * 1024 * 1024,
            10 * 1024 * 1024, 10 * 1024 * 1024, 10 * 1024 * 1024,
            3 * 1024 * 1024};  // 63MB out of 64MB

        auto large_result = strategy_->Allocate(allocators, allocators_by_name,
                                                large_slices, config);
        ASSERT_TRUE(large_result.has_value());
        auto large_desc = large_result.value()[0].get_descriptor();
        ASSERT_TRUE(large_desc.is_memory_replica());
        EXPECT_EQ(large_desc.get_memory_descriptor()
                      .buffer_descriptors[0]
                      .segment_name_,
                  "preferred");

        // Now try to allocate more than remaining space in preferred segment
        std::vector<size_t> small_slice = {2 * 1024 * 1024};
        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          small_slice, config);
        ASSERT_TRUE(result.has_value());
        auto small_desc = result.value()[0].get_descriptor();
        ASSERT_TRUE(small_desc.is_memory_replica());
        const auto& mem_desc = small_desc.get_memory_descriptor();
        EXPECT_EQ(mem_desc.buffer_descriptors[0].segment_name_, "segment1");
        EXPECT_EQ(mem_desc.buffer_descriptors[0].size_, 2 * 1024 * 1024);
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

        // Fill up both allocators
        std::vector<size_t> large_slices = {15 * 1024 * 1024, 15 * 1024 * 1024,
                                            15 * 1024 * 1024,
                                            15 * 1024 * 1024};  // 60MB
        auto result1 = strategy_->Allocate(allocators, allocators_by_name,
                                           large_slices, config);
        ASSERT_TRUE(result1.has_value());
        auto result2 = strategy_->Allocate(allocators, allocators_by_name,
                                           large_slices, config);
        ASSERT_TRUE(result2.has_value());

        // Try to allocate more than remaining space
        std::vector<size_t> impossible_slice = {
            5 * 1024 * 1024};  // 5MB (more than remaining)
        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          impossible_slice, config);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
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
        std::vector<size_t> zero_slice = {0};

        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          zero_slice, config);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
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
        std::vector<size_t> huge_slice = {
            100 * 1024 * 1024};  // 100MB (larger than 64MB capacity)

        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          huge_slice, config);
        EXPECT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    }
}

// Test empty slice sizes
TEST_F(AllocationStrategyTest, EmptySliceSizes) {
    auto allocator =
        CreateTestAllocator("segment1", 0, BufferAllocatorType::OFFSET);
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator);
    allocators.push_back(allocator);

    ReplicateConfig config{1, false, ""};
    std::vector<size_t> empty_slices;

    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      empty_slices, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test invalid replication count
TEST_F(AllocationStrategyTest, InvalidReplicationCount) {
    auto allocator =
        CreateTestAllocator("segment1", 0, BufferAllocatorType::OFFSET);
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator);
    allocators.push_back(allocator);

    ReplicateConfig config{0, false, ""};  // Invalid: 0 replicas
    std::vector<size_t> slice_sizes = {1024};

    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      slice_sizes, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test insufficient allocators for requested replica count
TEST_F(AllocationStrategyTest, InsufficientAllocatorsForReplicas) {
    auto allocator1 =
        CreateTestAllocator("segment1", 0, BufferAllocatorType::OFFSET);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL,
                                          BufferAllocatorType::OFFSET);

    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator1);
    allocators_by_name["segment2"].push_back(allocator2);
    allocators.push_back(allocator1);
    allocators.push_back(allocator2);

    ReplicateConfig config{
        5, false, ""};  // Request 5 replicas, but only 2 segments available
    std::vector<size_t> slice_sizes = {1024};

    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      slice_sizes, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

}  // namespace mooncake