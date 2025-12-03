#include "allocation_strategy.h"

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "allocator.h"
#include "types.h"

namespace mooncake {

// Size units for better readability
static constexpr size_t MB = 1024 * 1024;

// Base class for non-parameterized tests
class AllocationStrategyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        strategy_ = std::make_unique<RandomAllocationStrategy>();
    }

    std::unique_ptr<RandomAllocationStrategy> strategy_;
};

// Parameterized test class for allocator type variations
class AllocationStrategyParameterizedTest
    : public ::testing::TestWithParam<BufferAllocatorType> {
   protected:
    void SetUp() override {
        strategy_ = std::make_unique<RandomAllocationStrategy>();
        allocator_type_ = GetParam();
    }

    // Helper function to create a BufferAllocator for testing
    // Using segment_name as transport_endpoint for simplicity
    std::shared_ptr<BufferAllocatorBase> CreateTestAllocator(
        const std::string& segment_name, size_t base_offset,
        size_t size = 64 * MB) {
        const size_t base = 0x100000000ULL + base_offset;  // 4GB + offset
        switch (allocator_type_) {
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

    BufferAllocatorType allocator_type_;
    std::unique_ptr<RandomAllocationStrategy> strategy_;
};

// Instantiate parameterized tests for all allocator types
INSTANTIATE_TEST_SUITE_P(
    AllAllocatorTypes, AllocationStrategyParameterizedTest,
    ::testing::Values(BufferAllocatorType::CACHELIB,
                      BufferAllocatorType::OFFSET),
    [](const ::testing::TestParamInfo<BufferAllocatorType>& info) {
        switch (info.param) {
            case BufferAllocatorType::CACHELIB:
                return "Cachelib";
            case BufferAllocatorType::OFFSET:
                return "Offset";
            default:
                return "Unknown";
        }
    });

// Test basic functionality with empty allocators map (non-parameterized)
TEST_F(AllocationStrategyTest, EmptyAllocatorsMap) {
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        empty_allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> empty_allocators;
    ReplicateConfig config{1, false, {"local"}};

    size_t slice_length = 100;
    auto result = strategy_->Allocate(
        empty_allocators, empty_allocators_by_name, slice_length, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test preferred segment behavior with empty allocators (non-parameterized)
TEST_F(AllocationStrategyTest, PreferredSegmentWithEmptyAllocators) {
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        empty_allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> empty_allocators;
    ReplicateConfig config{1, false, {"preferred_segment"}};

    size_t slice_length = 100;
    auto result = strategy_->Allocate(
        empty_allocators, empty_allocators_by_name, slice_length, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test preferred segment allocation when available
TEST_P(AllocationStrategyParameterizedTest, PreferredSegmentAllocation) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("preferred", 0x10000000ULL);

    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator1);
    allocators_by_name["preferred"].push_back(allocator2);
    allocators.push_back(allocator1);
    allocators.push_back(allocator2);

    ReplicateConfig config{1, false, {"preferred"}};
    size_t slice_length = 1024;

    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      slice_length, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 1);
    ASSERT_FALSE(result.value().empty());

    const auto& replica = result.value()[0];
    auto descriptor = replica.get_descriptor();
    ASSERT_TRUE(descriptor.is_memory_replica());
    const auto& mem_desc = descriptor.get_memory_descriptor();
    EXPECT_EQ(mem_desc.buffer_descriptor.transport_endpoint_, "preferred");
    EXPECT_EQ(mem_desc.buffer_descriptor.size_, 1024);
}

// Test fallback to random allocation when preferred segment doesn't exist
TEST_P(AllocationStrategyParameterizedTest, PreferredSegmentNotFound) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL);

    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator1);
    allocators_by_name["segment2"].push_back(allocator2);
    allocators.push_back(allocator1);
    allocators.push_back(allocator2);

    ReplicateConfig config{1, false, {"nonexistent"}};
    size_t slice_length = 1024;

    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      slice_length, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 1);

    const auto& replica = result.value()[0];
    auto descriptor = replica.get_descriptor();
    ASSERT_TRUE(descriptor.is_memory_replica());
    const auto& mem_desc = descriptor.get_memory_descriptor();
    std::string segment_ep = mem_desc.buffer_descriptor.transport_endpoint_;
    EXPECT_TRUE(segment_ep == "segment1" || segment_ep == "segment2");
    EXPECT_EQ(mem_desc.buffer_descriptor.size_, 1024);
}

// Test single slice allocation
TEST_P(AllocationStrategyParameterizedTest, SingleSliceAllocation) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL);

    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator1);
    allocators_by_name["segment2"].push_back(allocator2);
    allocators.push_back(allocator1);
    allocators.push_back(allocator2);

    ReplicateConfig config{1, false, {""}};
    size_t slice_length = 1024;

    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      slice_length, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 1);

    const auto& replica = result.value()[0];
    auto descriptor = replica.get_descriptor();
    ASSERT_TRUE(descriptor.is_memory_replica());
    const auto& mem_desc = descriptor.get_memory_descriptor();
    EXPECT_EQ(mem_desc.buffer_descriptor.size_, 1024);
}

// Test multiple replicas allocation
TEST_P(AllocationStrategyParameterizedTest, MultipleReplicasAllocation) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL);
    auto allocator3 = CreateTestAllocator("segment3", 0x20000000ULL);

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

    ReplicateConfig config{3, false, {""}};  // Request 3 replicas
    size_t slice_length = 1024;

    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      slice_length, config);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 3);

    // Check each replica has the correct slice size
    for (const auto& replica : result.value()) {
        auto descriptor = replica.get_descriptor();
        ASSERT_TRUE(descriptor.is_memory_replica());
        const auto& mem_desc = descriptor.get_memory_descriptor();
        EXPECT_EQ(mem_desc.buffer_descriptor.size_, 1024);
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

// Test allocation when preferred segment has insufficient space
TEST_P(AllocationStrategyParameterizedTest, PreferredSegmentInsufficientSpace) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("preferred", 0x10000000ULL);

    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator1);
    allocators_by_name["preferred"].push_back(allocator2);
    allocators.push_back(allocator1);
    allocators.push_back(allocator2);

    // First, fill up the preferred allocator
    ReplicateConfig config{1, false, {"preferred"}};
    // Store the results of the allocations to avoid deallocation of the buffers
    // before the test is done
    std::vector<std::vector<Replica>> results;
    // Allocate multiple times to fill up the preferred allocator
    for (int i = 0; i < 4; ++i) {
        size_t large_slice = 15 * 1024 * 1024;  // 10MB
        auto large_result = strategy_->Allocate(allocators, allocators_by_name,
                                                large_slice, config);
        ASSERT_TRUE(large_result.has_value());
        auto last_desc = large_result.value()[0].get_descriptor();
        ASSERT_TRUE(last_desc.is_memory_replica());
        EXPECT_EQ(last_desc.get_memory_descriptor()
                      .buffer_descriptor.transport_endpoint_,
                  "preferred");
        results.emplace_back(std::move(large_result.value()));
    }

    // Now try to allocate more than remaining space in preferred segment
    size_t small_slice = 5 * 1024 * 1024;  // 2MB
    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      small_slice, config);
    ASSERT_TRUE(result.has_value());
    auto small_desc = result.value()[0].get_descriptor();
    ASSERT_TRUE(small_desc.is_memory_replica());
    const auto& mem_desc = small_desc.get_memory_descriptor();
    EXPECT_EQ(mem_desc.buffer_descriptor.transport_endpoint_, "segment1");
    EXPECT_EQ(mem_desc.buffer_descriptor.size_, small_slice);
}

// Test allocation when all allocators are full
TEST_P(AllocationStrategyParameterizedTest, AllAllocatorsFull) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL);

    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator1);
    allocators_by_name["segment2"].push_back(allocator2);
    allocators.push_back(allocator1);
    allocators.push_back(allocator2);

    ReplicateConfig config{1, false, {""}};

    // Fill up both allocators
    size_t large_slice = 15 * 1024 * 1024;  // 15MB
    // Store the results of the allocations to avoid deallocation of the buffers
    // before the test is done
    std::vector<std::vector<Replica>> results;
    // Allocate 8 times to use 120MB total
    for (int i = 0; i < 8; ++i) {
        auto result = strategy_->Allocate(allocators, allocators_by_name,
                                          large_slice, config);
        ASSERT_TRUE(result.has_value());
        results.emplace_back(std::move(result.value()));
    }

    // Try to allocate more than remaining space
    size_t impossible_slice = 5 * 1024 * 1024;  // 5MB (more than remaining)
    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      impossible_slice, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test allocation with zero size
TEST_P(AllocationStrategyParameterizedTest, ZeroSizeAllocation) {
    auto allocator = CreateTestAllocator("segment1", 0);
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator);
    allocators.push_back(allocator);

    ReplicateConfig config{1, false, {""}};
    size_t zero_slice = 0;

    auto result =
        strategy_->Allocate(allocators, allocators_by_name, zero_slice, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test allocation with very large size
TEST_P(AllocationStrategyParameterizedTest, VeryLargeSizeAllocation) {
    auto allocator = CreateTestAllocator("segment1", 0);
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator);
    allocators.push_back(allocator);

    ReplicateConfig config{1, false, {""}};
    size_t huge_slice = 100 * 1024 * 1024;  // 100MB (larger than 64MB capacity)

    auto result =
        strategy_->Allocate(allocators, allocators_by_name, huge_slice, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test zero slice length (already covered by ZeroSizeAllocation test)

// Test invalid replication count
TEST_F(AllocationStrategyTest, InvalidReplicationCount) {
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "segment1", 0x100000000ULL, 64 * MB, "segment1");
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator);
    allocators.push_back(allocator);

    ReplicateConfig config{0, false, {""}};  // Invalid: 0 replicas
    size_t slice_length = 1024;

    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      slice_length, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test best-effort behavior when insufficient allocators for requested replica
// count
TEST_F(AllocationStrategyTest, InsufficientAllocatorsForReplicas) {
    auto allocator1 = std::make_shared<OffsetBufferAllocator>(
        "segment1", 0x100000000ULL, 64 * MB, "segment1");
    auto allocator2 = std::make_shared<OffsetBufferAllocator>(
        "segment2", 0x100000000ULL + 0x10000000ULL, 64 * MB, "segment2");

    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators;

    allocators_by_name["segment1"].push_back(allocator1);
    allocators_by_name["segment2"].push_back(allocator2);
    allocators.push_back(allocator1);
    allocators.push_back(allocator2);

    ReplicateConfig config{
        5, false, {""}};  // Request 5 replicas, but only 2 segments available
    size_t slice_length = 1024;

    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      slice_length, config);
    // With best-effort semantics, should succeed with available replicas
    EXPECT_TRUE(result.has_value());
    // Should get 2 replicas (limited by number of segments)
    EXPECT_EQ(2u, result.value().size());

    // Verify each replica has the expected slice structure
    for (const auto& replica : result.value()) {
        auto descriptor = replica.get_descriptor();
        ASSERT_TRUE(descriptor.is_memory_replica());
        const auto& mem_desc = descriptor.get_memory_descriptor();
        EXPECT_EQ(mem_desc.buffer_descriptor.size_, 1024u);
    }

    // Verify replicas are on different segments
    std::unordered_set<std::string> segment_names;
    for (const auto& replica : result.value()) {
        auto descriptor = replica.get_descriptor();
        const auto& mem_desc = descriptor.get_memory_descriptor();
        segment_names.insert(mem_desc.buffer_descriptor.transport_endpoint_);
    }
    EXPECT_EQ(2u, segment_names.size());
}

// Note: The following unit tests for internal helper methods have been removed
// because those methods (allocateSingleBuffer, tryRandomAllocate,
// allocateSlice, resetRetryCount, getRetryCount) are no longer part of the
// public API. The functionality is now encapsulated within the Allocate()
// method.

}  // namespace mooncake
