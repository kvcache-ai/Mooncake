#include "allocation_strategy.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <cmath>
#include <iomanip>
#include <memory>
#include <numeric>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "allocator.h"
#include "types.h"

namespace mooncake {

// Size units for better readability
static constexpr size_t MiB = 1024 * 1024;

// Strategy types for parameterized tests
const auto kStrategyTypes = ::testing::Values(AllocationStrategyType::RANDOM,
                                              AllocationStrategyType::P2C);

const auto kAllocatorTypes = ::testing::Values(BufferAllocatorType::CACHELIB,
                                               BufferAllocatorType::OFFSET);

// Base class for non-parameterized tests
class AllocationStrategyTest : public ::testing::Test {
   protected:
    void SetUp() override {
        strategy_ = std::make_unique<RandomAllocationStrategy>();
    }

    std::unique_ptr<RandomAllocationStrategy> strategy_;
};

// Parameterized test class for strategy and allocator type variations
class AllocationStrategyParameterizedTest
    : public ::testing::TestWithParam<
          std::tuple<AllocationStrategyType, BufferAllocatorType>> {
   protected:
    void SetUp() override {
        auto [strategy_type, allocator_type] = GetParam();
        strategy_ = CreateAllocationStrategy(strategy_type);
        allocator_type_ = allocator_type;
    }

    // Helper function to create a BufferAllocator for testing
    // Using segment_name as transport_endpoint for simplicity
    std::shared_ptr<BufferAllocatorBase> CreateTestAllocator(
        const std::string& segment_name, size_t base_offset,
        size_t size = 64 * MiB) {
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
    std::shared_ptr<AllocationStrategy> strategy_;
};

// Instantiate parameterized tests for all strategy and allocator combinations
INSTANTIATE_TEST_SUITE_P(
    AllCombinations, AllocationStrategyParameterizedTest,
    ::testing::Combine(kStrategyTypes, kAllocatorTypes),
    [](const ::testing::TestParamInfo<
        std::tuple<AllocationStrategyType, BufferAllocatorType>>& info) {
        AllocationStrategyType strategy_type = std::get<0>(info.param);
        BufferAllocatorType allocator_type = std::get<1>(info.param);
        std::string strategy_str;
        switch (strategy_type) {
            case AllocationStrategyType::RANDOM:
                strategy_str = "Random";
                break;
            case AllocationStrategyType::P2C:
                strategy_str = "P2C";
                break;
            default:
                strategy_str = "Unknown";
        }
        std::string allocator_str =
            (allocator_type == BufferAllocatorType::CACHELIB) ? "Cachelib"
                                                              : "Offset";
        return strategy_str + "_" + allocator_str;
    });

// Test basic functionality with empty allocators map (non-parameterized)
TEST_F(AllocationStrategyTest, EmptyAllocatorsMap) {
    AllocatorManager allocator_manager;

    size_t slice_length = 100;
    auto result =
        strategy_->Allocate(allocator_manager, slice_length, 1, {}, {});
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test preferred segment behavior with empty allocators (non-parameterized)
TEST_F(AllocationStrategyTest, PreferredSegmentWithEmptyAllocators) {
    AllocatorManager allocator_manager;

    size_t slice_length = 100;
    std::vector<std::string> preferred_segments = {"preferred_segment"};
    auto result = strategy_->Allocate(allocator_manager, slice_length, 1,
                                      preferred_segments, {});
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test preferred segment allocation when available
TEST_P(AllocationStrategyParameterizedTest, PreferredSegmentAllocation) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("preferred", 0x10000000ULL);

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);
    allocator_manager.addAllocator("preferred", allocator2);

    size_t slice_length = 1024;
    std::vector<std::string> preferred_segments = {"preferred"};

    auto result = strategy_->Allocate(allocator_manager, slice_length, 1,
                                      preferred_segments, {});
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

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);
    allocator_manager.addAllocator("segment2", allocator2);

    size_t slice_length = 1024;
    std::vector<std::string> preferred_segments = {"nonexistent"};

    auto result = strategy_->Allocate(allocator_manager, slice_length, 1,
                                      preferred_segments, {});
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

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);
    allocator_manager.addAllocator("segment2", allocator2);

    size_t slice_length = 1024;

    auto result =
        strategy_->Allocate(allocator_manager, slice_length, 1, {}, {});
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

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);
    allocator_manager.addAllocator("segment2", allocator2);
    allocator_manager.addAllocator("segment3", allocator3);

    size_t slice_length = 1024;

    auto result = strategy_->Allocate(allocator_manager, slice_length, 3, {},
                                      {});  // Request 3 replicas
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

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);
    allocator_manager.addAllocator("preferred", allocator2);

    // First, fill up the preferred allocator
    std::vector<std::string> preferred_segments = {"preferred"};
    // Store the results of the allocations to avoid deallocation of the buffers
    // before the test is done
    std::vector<std::vector<Replica>> results;
    // Allocate multiple times to fill up the preferred allocator
    for (int i = 0; i < 4; ++i) {
        size_t large_slice = 15 * 1024 * 1024;  // 15MB
        auto large_result = strategy_->Allocate(allocator_manager, large_slice,
                                                1, preferred_segments, {});
        ASSERT_TRUE(large_result.has_value());
        auto last_desc = large_result.value()[0].get_descriptor();
        ASSERT_TRUE(last_desc.is_memory_replica());
        EXPECT_EQ(last_desc.get_memory_descriptor()
                      .buffer_descriptor.transport_endpoint_,
                  "preferred");
        results.emplace_back(std::move(large_result.value()));
    }

    // Now try to allocate more than remaining space in preferred segment
    size_t small_slice = 5 * 1024 * 1024;  // 5MB
    auto result = strategy_->Allocate(allocator_manager, small_slice, 1,
                                      preferred_segments, {});
    ASSERT_TRUE(result.has_value());
    auto small_desc = result.value()[0].get_descriptor();
    ASSERT_TRUE(small_desc.is_memory_replica());
    const auto& mem_desc = small_desc.get_memory_descriptor();
    EXPECT_EQ(mem_desc.buffer_descriptor.transport_endpoint_,
              "segment1");  // Falls back to other segment
    EXPECT_EQ(mem_desc.buffer_descriptor.size_, small_slice);
}

// Test allocation when all allocators are full
TEST_P(AllocationStrategyParameterizedTest, AllAllocatorsFull) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL);

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);
    allocator_manager.addAllocator("segment2", allocator2);

    // Fill up both allocators
    size_t large_slice = 15 * 1024 * 1024;  // 15MB
    // Store the results of the allocations to avoid deallocation of the buffers
    // before the test is done
    std::vector<std::vector<Replica>> results;
    // Allocate 8 times to use 120MB total
    for (int i = 0; i < 8; ++i) {
        auto result =
            strategy_->Allocate(allocator_manager, large_slice, 1, {}, {});
        ASSERT_TRUE(result.has_value());
        results.emplace_back(std::move(result.value()));
    }

    // Try to allocate more than remaining space
    size_t impossible_slice = 5 * 1024 * 1024;  // 5MB (more than remaining)
    auto result =
        strategy_->Allocate(allocator_manager, impossible_slice, 1, {}, {});
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test allocation with zero size
TEST_P(AllocationStrategyParameterizedTest, ZeroSizeAllocation) {
    auto allocator = CreateTestAllocator("segment1", 0);

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator);

    size_t zero_slice = 0;

    auto result = strategy_->Allocate(allocator_manager, zero_slice, 1, {}, {});
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test allocation with very large size
TEST_P(AllocationStrategyParameterizedTest, VeryLargeSizeAllocation) {
    auto allocator = CreateTestAllocator("segment1", 0);

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator);

    size_t huge_slice = 100 * 1024 * 1024;  // 100MB (larger than 64MB capacity)

    auto result = strategy_->Allocate(allocator_manager, huge_slice, 1, {}, {});
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test zero slice length (already covered by ZeroSizeAllocation test)

// Test invalid replication count
TEST_F(AllocationStrategyTest, InvalidReplicationCount) {
    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "segment1", 0x100000000ULL, 64 * MiB, "segment1");

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator);

    size_t slice_length = 1024;

    auto result = strategy_->Allocate(allocator_manager, slice_length, 0, {},
                                      {});  // Invalid: 0 replicas
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

// Test best-effort behavior when insufficient allocators for requested replica
// count
TEST_F(AllocationStrategyTest, InsufficientAllocatorsForReplicas) {
    auto allocator1 = std::make_shared<OffsetBufferAllocator>(
        "segment1", 0x100000000ULL, 64 * MiB, "segment1");
    auto allocator2 = std::make_shared<OffsetBufferAllocator>(
        "segment2", 0x100000000ULL + 0x10000000ULL, 64 * MiB, "segment2");

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);
    allocator_manager.addAllocator("segment2", allocator2);

    size_t slice_length = 1024;

    auto result = strategy_->Allocate(
        allocator_manager, slice_length, 5, {},
        {});  // Request 5 replicas, but only 2 segments available
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

// Test allocation with multiple preferred segments
TEST_P(AllocationStrategyParameterizedTest,
       MultiplePreferredSegmentsAllocation) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("preferred1", 0x10000000ULL);
    auto allocator3 = CreateTestAllocator("preferred2", 0x20000000ULL);
    auto allocator4 = CreateTestAllocator("segment4", 0x30000000ULL);

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);
    allocator_manager.addAllocator("preferred1", allocator2);
    allocator_manager.addAllocator("preferred2", allocator3);
    allocator_manager.addAllocator("segment4", allocator4);

    size_t slice_length = 1024;
    std::vector<std::string> preferred_segments = {
        "preferred1", "preferred2"};  // Multiple preferred segments

    auto result = strategy_->Allocate(allocator_manager, slice_length, 2,
                                      preferred_segments, {});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 2);

    for (const auto& replica : result.value()) {
        auto descriptor = replica.get_descriptor();
        ASSERT_TRUE(descriptor.is_memory_replica());
        const auto& mem_desc = descriptor.get_memory_descriptor();
        std::string segment_ep = mem_desc.buffer_descriptor.transport_endpoint_;
        EXPECT_TRUE(segment_ep == "preferred1" || segment_ep == "preferred2");
        EXPECT_EQ(mem_desc.buffer_descriptor.size_, 1024);
    }
}

// Test allocation with excluded segments
TEST_P(AllocationStrategyParameterizedTest, ExcludedSegmentsAllocation) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL);
    auto allocator3 = CreateTestAllocator("segment3", 0x20000000ULL);
    auto allocator4 = CreateTestAllocator("segment4", 0x30000000ULL);

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);
    allocator_manager.addAllocator("segment2", allocator2);
    allocator_manager.addAllocator("segment3", allocator3);
    allocator_manager.addAllocator("segment4", allocator4);

    size_t slice_length = 1024;
    std::set<std::string> excluded_segments = {"segment1", "segment3"};

    auto result = strategy_->Allocate(allocator_manager, slice_length,
                                      3,  // Requires 3 replicas
                                      {}, excluded_segments);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 2);  // Only 2 replicas should be allocated

    for (const auto& replica : result.value()) {
        auto descriptor = replica.get_descriptor();
        ASSERT_TRUE(descriptor.is_memory_replica());
        const auto& mem_desc = descriptor.get_memory_descriptor();
        std::string segment_ep = mem_desc.buffer_descriptor.transport_endpoint_;
        // Should not be allocated from excluded segments
        EXPECT_NE(segment_ep, "segment1");
        EXPECT_NE(segment_ep, "segment3");
        EXPECT_TRUE(segment_ep == "segment2" || segment_ep == "segment4");
        EXPECT_EQ(mem_desc.buffer_descriptor.size_, 1024);
    }
}

// Test allocation when all available segments are excluded
TEST_F(AllocationStrategyTest, AllSegmentsExcluded) {
    auto allocator1 = std::make_shared<OffsetBufferAllocator>(
        "segment1", 0x100000000ULL, 64 * MiB, "segment1");

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);

    size_t slice_length = 1024;
    std::set<std::string> excluded_segments = {"segment1"};

    auto result = strategy_->Allocate(allocator_manager, slice_length, 1, {},
                                      excluded_segments);
    EXPECT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

// Test allocation with preferred segments and excluded segments combination
TEST_P(AllocationStrategyParameterizedTest,
       PreferredAndExcludedSegmentsCombination) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("preferred", 0x10000000ULL);
    auto allocator3 = CreateTestAllocator("segment3", 0x20000000ULL);
    auto allocator4 = CreateTestAllocator("segment4", 0x30000000ULL);

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);
    allocator_manager.addAllocator("preferred", allocator2);
    allocator_manager.addAllocator("segment3", allocator3);
    allocator_manager.addAllocator("segment4", allocator4);

    size_t slice_length = 1024;
    std::vector<std::string> preferred_segments = {"preferred"};
    std::set<std::string> excluded_segments = {
        "segment1"};  // Exclude a different segment

    auto result = strategy_->Allocate(allocator_manager, slice_length,
                                      3,  // Requires 3 replicas
                                      preferred_segments, excluded_segments);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 3);

    bool has_preferred_replica = false;
    for (const auto& replica : result.value()) {
        auto descriptor = replica.get_descriptor();
        ASSERT_TRUE(descriptor.is_memory_replica());
        const auto& mem_desc = descriptor.get_memory_descriptor();
        std::string segment_ep = mem_desc.buffer_descriptor.transport_endpoint_;
        // Should not be allocated from excluded segments
        EXPECT_NE(segment_ep, "segment1");
        if (segment_ep == "preferred") {
            has_preferred_replica = true;
        }
        EXPECT_EQ(mem_desc.buffer_descriptor.size_, 1024);
    }
    EXPECT_TRUE(has_preferred_replica);
}

// Test allocation with preferred segments that are also excluded (exclude takes
// precedence)
TEST_P(AllocationStrategyParameterizedTest,
       PreferredAndExcludedSegmentsConflict) {
    auto allocator1 = CreateTestAllocator("segment1", 0);
    auto allocator2 = CreateTestAllocator("segment2", 0x10000000ULL);
    auto allocator3 = CreateTestAllocator("segment3", 0x20000000ULL);

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator1);
    allocator_manager.addAllocator("segment2", allocator2);
    allocator_manager.addAllocator("segment3", allocator3);

    size_t slice_length = 1024;
    std::vector<std::string> preferred_segments = {
        "segment1"};  // Will be excluded
    std::set<std::string> excluded_segments = {
        "segment1"};  // Exclude the preferred

    auto result = strategy_->Allocate(allocator_manager, slice_length,
                                      3,  // Requires 3 replicas
                                      preferred_segments, excluded_segments);
    ASSERT_TRUE(result.has_value());  // Should still succeed by falling back to
                                      // other segments
    EXPECT_EQ(result.value().size(), 2);  // Only 2 replicas should be allocated

    for (const auto& replica : result.value()) {
        auto descriptor = replica.get_descriptor();
        ASSERT_TRUE(descriptor.is_memory_replica());
        const auto& mem_desc = descriptor.get_memory_descriptor();
        std::string segment_ep = mem_desc.buffer_descriptor.transport_endpoint_;
        EXPECT_NE(
            segment_ep,
            "segment1");  // Should not be allocated from excluded segments
        EXPECT_TRUE(segment_ep == "segment2" || segment_ep == "segment3");
        EXPECT_EQ(mem_desc.buffer_descriptor.size_, 1024);
    }
}

// Test the performance of AllocationStrategy.
// Test P2C load balancing distribution with different sized segments
TEST_P(AllocationStrategyParameterizedTest, P2CLoadBalancingDistribution) {
    auto [strategy_type, allocator_type] = GetParam();
    if (strategy_type != AllocationStrategyType::P2C) {
        // This test is only for P2C strategy
        GTEST_SKIP();
    }

    const auto kNumSegments = 3;
    // Different sized segments to test utilization ratio balancing
    std::array<size_t, kNumSegments> kSegmentSizes = {32 * MiB, 64 * MiB,
                                                      128 * MiB};

    AllocatorManager allocator_manager;
    for (size_t i = 0; i < kNumSegments; i++) {
        const auto name = std::to_string(i) + "-segment";
        allocator_manager.addAllocator(
            name, CreateTestAllocator(name, i * 128 * MiB, kSegmentSizes[i]));
    }

    std::array<size_t, kNumSegments> count = {0};
    size_t slice_length = 64 * 1024;      // 64KB per allocation
    const size_t kNumAllocations = 3000;  // Total 192MB allocated
    std::vector<std::vector<Replica>> test_replicas;

    for (size_t i = 0; i < kNumAllocations; i++) {
        auto result = strategy_->Allocate(allocator_manager, slice_length);
        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().size(), 1);

        for (const auto& replica : result.value()) {
            auto descriptor = replica.get_descriptor();
            ASSERT_TRUE(descriptor.is_memory_replica());
            const auto& mem_desc = descriptor.get_memory_descriptor();
            std::string segment_name =
                mem_desc.buffer_descriptor.transport_endpoint_;
            EXPECT_EQ(mem_desc.buffer_descriptor.size_, slice_length);

            // Extract segment index from name "X-segment"
            size_t segment_idx = segment_name[0] - '0';
            ASSERT_LT(segment_idx, kNumSegments);
            count[segment_idx]++;
        }

        test_replicas.push_back(std::move(result.value()));
    }

    // Calculate utilization ratio for each segment
    std::cout << "\nP2C Load Balancing Results (Different Sized Segments):\n";
    std::cout << "Total allocations: " << kNumAllocations << " x "
              << (slice_length / 1024)
              << "KB = " << (kNumAllocations * slice_length / MiB) << "MB\n\n";

    std::array<double, kNumSegments> utilization_ratios;
    for (size_t i = 0; i < kNumSegments; i++) {
        size_t allocated_bytes = count[i] * slice_length;
        double utilization = (allocated_bytes * 100.0) / kSegmentSizes[i];
        utilization_ratios[i] = utilization;

        std::cout << "Segment " << i << " (" << (kSegmentSizes[i] / MiB)
                  << "MB capacity):\n"
                  << "  Allocations: " << count[i] << " (" << std::fixed
                  << std::setprecision(1)
                  << (count[i] * 100.0 / kNumAllocations) << "% of total)\n"
                  << "  Allocated: " << (allocated_bytes / MiB) << "MB\n"
                  << "  Utilization: " << std::setprecision(1) << utilization
                  << "%\n\n";
    }

    // P2C should balance utilization ratios across segments
    // Even though segments have different capacities (32MB, 64MB, 128MB),
    // their utilization ratios should be similar (within 15% difference)
    double max_util =
        *std::max_element(utilization_ratios.begin(), utilization_ratios.end());
    double min_util =
        *std::min_element(utilization_ratios.begin(), utilization_ratios.end());
    double util_diff = max_util - min_util;

    std::cout << "Utilization difference: " << std::setprecision(1) << util_diff
              << "%\n";
    std::cout << "Expected: < 15% for good load balancing\n\n";

    // Verify that utilization ratios are balanced (within 15%)
    EXPECT_LT(util_diff, 15.0) << "P2C should balance utilization ratios";
}

// Test the performance comparison between strategies
TEST_F(AllocationStrategyTest, PerformanceComparison) {
    const auto kNumSegments = 512;
    const auto kSegmentBase = 0x100000000ULL;
    const auto kSegmentSize = 64 * MiB;
    const auto kNumAllocations = 5000;
    const auto kAllocationSize = 4 * MiB;

    // Construct and add allocators
    AllocatorManager allocator_manager;
    for (size_t i = 0; i < kNumSegments; i++) {
        const auto name = "segment_" + std::to_string(i);
        allocator_manager.addAllocator(
            name, std::make_shared<OffsetBufferAllocator>(name, kSegmentBase,
                                                          kSegmentSize, name));
    }

    // Test Random strategy
    auto random_strategy = std::make_unique<RandomAllocationStrategy>();
    std::vector<std::vector<Replica>> random_replicas;
    random_replicas.reserve(kNumAllocations);

    auto random_start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < kNumAllocations; i++) {
        auto result =
            random_strategy->Allocate(allocator_manager, kAllocationSize);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result.value().size(), 1);
        random_replicas.emplace_back(std::move(result.value()));
    }
    auto random_elapsed_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - random_start);

    random_replicas.clear();

    // Test P2C strategy
    auto p2c_strategy = std::make_unique<P2CAllocationStrategy>();
    std::vector<std::vector<Replica>> p2c_replicas;
    p2c_replicas.reserve(kNumAllocations);

    auto p2c_start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < kNumAllocations; i++) {
        auto result =
            p2c_strategy->Allocate(allocator_manager, kAllocationSize);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result.value().size(), 1);
        p2c_replicas.emplace_back(std::move(result.value()));
    }
    auto p2c_elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - p2c_start);

    std::cout << "\nAllocation Strategy Performance Comparison:\n"
              << "Num segments: " << kNumSegments << "\n"
              << "Num allocations: " << kNumAllocations << "\n"
              << "Random strategy: " << random_elapsed_us.count() << " us\n"
              << "P2C strategy: " << p2c_elapsed_us.count() << " us\n"
              << "Speedup: " << std::fixed << std::setprecision(2)
              << (static_cast<double>(random_elapsed_us.count()) /
                  p2c_elapsed_us.count())
              << "x\n\n";
}

TEST_F(AllocationStrategyTest, PerformanceTest) {
    const auto kNumSegments = 512;
    const auto kSegmentBase = 0x100000000ULL;
    const auto kSegmentSize = 64 * MiB;
    const auto kNumAllocations = 5000;
    const auto kAllocationSize = 4 * MiB;

    // Construct and add allocators.
    AllocatorManager allocator_manager;
    for (size_t i = 0; i < kNumSegments; i++) {
        const auto name = "segment_" + std::to_string(i);
        allocator_manager.addAllocator(
            name, std::make_shared<OffsetBufferAllocator>(name, kSegmentBase,
                                                          kSegmentSize, name));
    }

    std::vector<std::vector<Replica>> replicas;
    replicas.reserve(kNumAllocations);

    // Do allocations.
    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < kNumAllocations; i++) {
        auto result = strategy_->Allocate(allocator_manager, kAllocationSize);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result.value().size(), 1);
        replicas.emplace_back(std::move(result.value()));
    }
    auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - start);

    std::cout << "\nAllocation Strategy Performance Test:\n"
              << "Num segments: " << kNumSegments << "\n"
              << "Num allocations: " << kNumAllocations << "\n"
              << "Time elapsed: " << elapsed_us.count() << " us\n\n";
}

// Note: The following unit tests for internal helper methods have been removed
// because those methods (allocateSingleBuffer, tryRandomAllocate,
// allocateSlice, resetRetryCount, getRetryCount) are no longer part of the
// public API. The functionality is now encapsulated within the Allocate()
// method.

}  // namespace mooncake
