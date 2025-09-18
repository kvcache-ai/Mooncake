#include "allocation_strategy.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
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

// Unit test class for testing individual functions
class AllocationStrategyUnitTest : public ::testing::Test {
   protected:
    void SetUp() override {
        strategy_ = std::make_unique<RandomAllocationStrategy>();
    }

    // Helper function to create test allocators
    std::shared_ptr<BufferAllocatorBase> CreateTestAllocator(
        const std::string& segment_name, size_t base_offset,
        BufferAllocatorType type, size_t size = 64 * MB) {
        const size_t base = 0x100000000ULL + base_offset;  // 4GB + offset
        switch (type) {
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

    std::unique_ptr<RandomAllocationStrategy> strategy_;
};

// Test basic functionality with empty allocators map (non-parameterized)
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

// Test preferred segment behavior with empty allocators (non-parameterized)
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
    EXPECT_EQ(mem_desc.buffer_descriptors[0].transport_endpoint_, "preferred");
    EXPECT_EQ(mem_desc.buffer_descriptors[0].size_, 1024);
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
    std::string segment_ep = mem_desc.buffer_descriptors[0].transport_endpoint_;
    EXPECT_TRUE(segment_ep == "segment1" || segment_ep == "segment2");
    EXPECT_EQ(mem_desc.buffer_descriptors[0].size_, 1024);
}

// Test multiple slices allocation
TEST_P(AllocationStrategyParameterizedTest, MultipleSlicesAllocation) {
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
    ReplicateConfig config{1, false, "preferred"};
    std::vector<size_t> large_slices = {10 * 1024 * 1024, 10 * 1024 * 1024,
                                        10 * 1024 * 1024, 10 * 1024 * 1024,
                                        10 * 1024 * 1024, 10 * 1024 * 1024,
                                        3 * 1024 * 1024};  // 63MB out of 64MB

    auto large_result = strategy_->Allocate(allocators, allocators_by_name,
                                            large_slices, config);
    ASSERT_TRUE(large_result.has_value());
    auto large_desc = large_result.value()[0].get_descriptor();
    ASSERT_TRUE(large_desc.is_memory_replica());
    EXPECT_EQ(large_desc.get_memory_descriptor()
                  .buffer_descriptors[0]
                  .transport_endpoint_,
              "preferred");

    // Now try to allocate more than remaining space in preferred segment
    std::vector<size_t> small_slice = {2 * 1024 * 1024};
    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      small_slice, config);
    ASSERT_TRUE(result.has_value());
    auto small_desc = result.value()[0].get_descriptor();
    ASSERT_TRUE(small_desc.is_memory_replica());
    const auto& mem_desc = small_desc.get_memory_descriptor();
    EXPECT_EQ(mem_desc.buffer_descriptors[0].transport_endpoint_, "segment1");
    EXPECT_EQ(mem_desc.buffer_descriptors[0].size_, 2 * 1024 * 1024);
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
    std::vector<size_t> impossible_slice = {5 * 1024 *
                                            1024};  // 5MB (more than remaining)
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

    ReplicateConfig config{1, false, ""};
    std::vector<size_t> zero_slice = {0};

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

    ReplicateConfig config{1, false, ""};
    std::vector<size_t> huge_slice = {
        100 * 1024 * 1024};  // 100MB (larger than 64MB capacity)

    auto result =
        strategy_->Allocate(allocators, allocators_by_name, huge_slice, config);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test empty slice sizes
TEST_F(AllocationStrategyTest, EmptySliceSizes) {
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "segment1", 0x100000000ULL, 64 * MB, "segment1");
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
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "segment1", 0x100000000ULL, 64 * MB, "segment1");
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
        5, false, ""};  // Request 5 replicas, but only 2 segments available
    std::vector<size_t> slice_sizes = {1024};

    auto result = strategy_->Allocate(allocators, allocators_by_name,
                                      slice_sizes, config);
    // With best-effort semantics, should succeed with available replicas
    EXPECT_TRUE(result.has_value());
    // Should get 2 replicas (limited by number of segments)
    EXPECT_EQ(2u, result.value().size());

    // Verify each replica has the expected slice structure
    for (const auto& replica : result.value()) {
        auto descriptor = replica.get_descriptor();
        ASSERT_TRUE(descriptor.is_memory_replica());
        const auto& mem_desc = descriptor.get_memory_descriptor();
        ASSERT_EQ(mem_desc.buffer_descriptors.size(), 1u);
        EXPECT_EQ(mem_desc.buffer_descriptors[0].size_, 1024u);
    }

    // Verify replicas are on different segments
    std::unordered_set<std::string> segment_names;
    for (const auto& replica : result.value()) {
        auto descriptor = replica.get_descriptor();
        const auto& mem_desc = descriptor.get_memory_descriptor();
        segment_names.insert(
            mem_desc.buffer_descriptors[0].transport_endpoint_);
    }
    EXPECT_EQ(2u, segment_names.size());
}

TEST_F(AllocationStrategyUnitTest,
       AllocateSingleBuffer_PreferredSegmentNotFound) {
    auto allocator1 =
        CreateTestAllocator("segment1", 0, BufferAllocatorType::OFFSET);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL,
                                          BufferAllocatorType::OFFSET);

    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators = {allocator1,
                                                                    allocator2};
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    allocators_by_name["segment1"] = {allocator1};
    allocators_by_name["segment2"] = {allocator2};

    ReplicateConfig config{1, false, "nonexistent"};
    std::unordered_set<std::string> excluded_segments;

    auto buffer = strategy_->allocateSingleBuffer(
        allocators, allocators_by_name, 1024, config, excluded_segments);

    ASSERT_TRUE(buffer != nullptr);
    std::string segment_name = buffer->getSegmentName();
    EXPECT_TRUE(segment_name == "segment1" || segment_name == "segment2");
}

TEST_F(AllocationStrategyUnitTest, AllocateSingleBuffer_EmptyPreferredSegment) {
    auto allocator1 =
        CreateTestAllocator("segment1", 0, BufferAllocatorType::OFFSET);

    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators = {allocator1};
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    allocators_by_name["segment1"] = {allocator1};

    ReplicateConfig config{1, false, ""};  // Empty preferred segment
    std::unordered_set<std::string> excluded_segments;

    auto buffer = strategy_->allocateSingleBuffer(
        allocators, allocators_by_name, 1024, config, excluded_segments);

    ASSERT_TRUE(buffer != nullptr);
    EXPECT_EQ(buffer->getSegmentName(), "segment1");
}

// Test tryRandomAllocate function
TEST_F(AllocationStrategyUnitTest, TryRandomAllocate_Success) {
    auto allocator1 =
        CreateTestAllocator("segment1", 0, BufferAllocatorType::OFFSET);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL,
                                          BufferAllocatorType::OFFSET);

    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators = {allocator1,
                                                                    allocator2};
    std::unordered_set<std::string> excluded_segments;

    auto buffer =
        strategy_->tryRandomAllocate(allocators, 1024, excluded_segments);
    ASSERT_TRUE(buffer != nullptr);
    EXPECT_EQ(buffer->size(), 1024);
}

TEST_F(AllocationStrategyUnitTest, TryRandomAllocate_AllSegmentsExcluded) {
    auto allocator1 =
        CreateTestAllocator("segment1", 0, BufferAllocatorType::OFFSET);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL,
                                          BufferAllocatorType::OFFSET);

    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators = {allocator1,
                                                                    allocator2};
    std::unordered_set<std::string> excluded_segments = {"segment1",
                                                         "segment2"};

    auto buffer =
        strategy_->tryRandomAllocate(allocators, 1024, excluded_segments);
    EXPECT_TRUE(buffer == nullptr);
}

TEST_F(AllocationStrategyUnitTest, TryRandomAllocate_InsufficientSpace) {
    auto allocator = CreateTestAllocator(
        "segment1", 0, BufferAllocatorType::OFFSET, 1024);  // Only 1KB
    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators = {allocator};
    std::unordered_set<std::string> excluded_segments;

    auto buffer = strategy_->tryRandomAllocate(
        allocators, 2048, excluded_segments);  // Request 2KB
    EXPECT_TRUE(buffer == nullptr);
}

// Test allocateSlice function
TEST_F(AllocationStrategyUnitTest, AllocateSlice_SingleReplica) {
    auto allocator1 =
        CreateTestAllocator("segment1", 0, BufferAllocatorType::OFFSET);

    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators = {allocator1};
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    allocators_by_name["segment1"] = {allocator1};

    ReplicateConfig config{1, false, ""};
    auto buffers = strategy_->allocateSlice(allocators, allocators_by_name,
                                            1024, 1, config);

    ASSERT_EQ(buffers.size(), 1);
    EXPECT_EQ(buffers[0]->size(), 1024);
    EXPECT_EQ(buffers[0]->getSegmentName(), "segment1");
}

TEST_F(AllocationStrategyUnitTest, AllocateSlice_MultipleReplicas) {
    auto allocator1 =
        CreateTestAllocator("segment1", 0, BufferAllocatorType::OFFSET);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL,
                                          BufferAllocatorType::OFFSET);
    auto allocator3 = CreateTestAllocator("segment3", 0x20000000ULL,
                                          BufferAllocatorType::OFFSET);

    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators = {
        allocator1, allocator2, allocator3};
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    allocators_by_name["segment1"] = {allocator1};
    allocators_by_name["segment2"] = {allocator2};
    allocators_by_name["segment3"] = {allocator3};

    ReplicateConfig config{3, false, ""};
    auto buffers = strategy_->allocateSlice(allocators, allocators_by_name,
                                            1024, 3, config);

    ASSERT_EQ(buffers.size(), 3);

    // Verify all buffers have correct size
    for (const auto& buffer : buffers) {
        EXPECT_EQ(buffer->size(), 1024);
    }

    // Verify replicas are on different segments
    std::unordered_set<std::string> used_segments;
    for (const auto& buffer : buffers) {
        used_segments.insert(buffer->getSegmentName());
    }
    EXPECT_EQ(used_segments.size(), 3);
}

TEST_F(AllocationStrategyUnitTest, AllocateSlice_InsufficientAllocators) {
    auto allocator1 =
        CreateTestAllocator("segment1", 0, BufferAllocatorType::OFFSET);

    std::vector<std::shared_ptr<BufferAllocatorBase>> allocators = {allocator1};
    std::unordered_map<std::string,
                       std::vector<std::shared_ptr<BufferAllocatorBase>>>
        allocators_by_name;
    allocators_by_name["segment1"] = {allocator1};

    ReplicateConfig config{3, false,
                           ""};  // Request 3 replicas but only 1 allocator
    auto buffers = strategy_->allocateSlice(allocators, allocators_by_name,
                                            1024, 3, config);

    // Should allocate as many as possible (best-effort)
    ASSERT_EQ(buffers.size(), 1);
    EXPECT_EQ(buffers[0]->getSegmentName(), "segment1");
}

// Test getLargestFreeRegion() filtering logic with fragmented allocators
TEST_F(AllocationStrategyUnitTest,
       TryRandomAllocate_LargestFreeRegionFiltering) {
    // Run the test 10 times to account for randomness
    for (int run = 0; run < 10; ++run) {
        // Create two OffsetBufferAllocators with 10MB each
        auto allocator1 = CreateTestAllocator(
            "segment1", 0, BufferAllocatorType::OFFSET, 10 * MB);
        auto allocator2 = CreateTestAllocator(
            "segment2", 0x10000000ULL, BufferAllocatorType::OFFSET, 10 * MB);

        // Fragment allocator1 heavily - leave only small free regions
        std::vector<std::unique_ptr<AllocatedBuffer>> fragments1;
        for (int i = 0; i < 9; ++i) {
            fragments1.push_back(allocator1->allocate(1 * MB));
        }
        // allocator1: 9MB allocated, only 1MB free

        // Leave allocator2 with enough contiguous space
        auto fragment2 = allocator2->allocate(5 * MB);
        // allocator2: 5MB allocated, 5MB contiguous free

        std::vector<std::shared_ptr<BufferAllocatorBase>> allocators = {
            allocator1, allocator2};
        std::unordered_set<std::string> excluded_segments;

        // Reset retry counter before test
        strategy_->resetRetryCount();

        auto buffer =
            strategy_->tryRandomAllocate(allocators, 4 * MB, excluded_segments);

        ASSERT_TRUE(buffer != nullptr) << "Failed on run " << run;
        EXPECT_EQ(buffer->size(), 4 * MB) << "Failed on run " << run;
        EXPECT_EQ(buffer->getSegmentName(), "segment2")
            << "Failed on run " << run;
        EXPECT_EQ(strategy_->getRetryCount(), 0) << "Failed on run " << run;
    }
}

}  // namespace mooncake
