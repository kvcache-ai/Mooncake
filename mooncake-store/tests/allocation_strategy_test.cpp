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
const auto kStrategyTypes = ::testing::Values(
    AllocationStrategyType::RANDOM, AllocationStrategyType::FREE_RATIO_FIRST);

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

class ScopedRandomSeed {
   public:
    explicit ScopedRandomSeed(uint64_t seed)
        : saved_engine_(threadLocalRandomEngine()) {
        threadLocalRandomEngine().seed(seed);
    }

    ~ScopedRandomSeed() { threadLocalRandomEngine() = saved_engine_; }

   private:
    RandomEngine saved_engine_;
};

class HintTestAllocator final
    : public BufferAllocatorBase,
      public std::enable_shared_from_this<HintTestAllocator> {
   public:
    HintTestAllocator(std::string name, size_t hint, bool succeeds)
        : name_(std::move(name)), hint_(hint), succeeds_(succeeds) {}

    std::unique_ptr<AllocatedBuffer> allocate(size_t size) override {
        ++allocation_calls_;
        if (!succeeds_) {
            return nullptr;
        }
        return std::make_unique<AllocatedBuffer>(
            shared_from_this(), reinterpret_cast<void*>(0x1000), size);
    }

    void deallocate(AllocatedBuffer*) override {}
    size_t capacity() const override { return MiB; }
    size_t size() const override { return 0; }
    std::string getSegmentName() const override { return name_; }
    std::string getTransportEndpoint() const override { return name_; }
    size_t getLargestFreeRegion() const override { return hint_; }
    size_t getLargestFreeRegionHint() const override {
        ++hint_queries_;
        return hint_;
    }
    size_t allocationCalls() const { return allocation_calls_; }
    size_t hintQueries() const { return hint_queries_; }

   private:
    std::string name_;
    size_t hint_;
    bool succeeds_;
    size_t allocation_calls_{0};
    mutable size_t hint_queries_{0};
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
            case AllocationStrategyType::FREE_RATIO_FIRST:
                strategy_str = "FreeRatioFirst";
                break;
            case AllocationStrategyType::SSD_FREE_RATIO_FIRST:
                strategy_str = "SsdFreeRatioFirst";
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
// Test FreeRatioFirst load balancing distribution with different sized segments
TEST_P(AllocationStrategyParameterizedTest,
       FreeRatioFirstLoadBalancingDistribution) {
    auto [strategy_type, allocator_type] = GetParam();
    if (strategy_type != AllocationStrategyType::FREE_RATIO_FIRST) {
        // This test is only for FreeRatioFirst strategy
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
    std::cout << "\nFreeRatioFirst Load Balancing Results (Different Sized "
                 "Segments):\n";
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

    // FreeRatioFirst should balance utilization ratios across segments
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
    EXPECT_LT(util_diff, 15.0)
        << "FreeRatioFirst should balance utilization ratios";
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

    // Test FreeRatioFirst strategy
    auto frf_strategy = std::make_unique<FreeRatioFirstAllocationStrategy>();
    std::vector<std::vector<Replica>> frf_replicas;
    frf_replicas.reserve(kNumAllocations);

    auto frf_start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < kNumAllocations; i++) {
        auto result =
            frf_strategy->Allocate(allocator_manager, kAllocationSize);
        ASSERT_TRUE(result.has_value());
        ASSERT_EQ(result.value().size(), 1);
        frf_replicas.emplace_back(std::move(result.value()));
    }
    auto frf_elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - frf_start);

    std::cout << "\nAllocation Strategy Performance Comparison:\n"
              << "Num segments: " << kNumSegments << "\n"
              << "Num allocations: " << kNumAllocations << "\n"
              << "Random strategy: " << random_elapsed_us.count() << " us\n"
              << "FreeRatioFirst strategy: " << frf_elapsed_us.count()
              << " us\n"
              << "Speedup: " << std::fixed << std::setprecision(2)
              << (static_cast<double>(random_elapsed_us.count()) /
                  frf_elapsed_us.count())
              << "x\n\n";
}

TEST_F(AllocationStrategyTest,
       FreeRatioFirstPreservesFallbackBudgetForKnownNoFitSegments) {
    constexpr size_t kSegmentCount = 256;
    constexpr size_t kRequestSize = 4096;
    constexpr uint64_t kSeed = 0xCCF2026;

    RandomEngine predictor(kSeed);
    const size_t sample_start = randomIndex(kSegmentCount, predictor);
    const size_t fallback_start = randomIndex(kSegmentCount, predictor);
    const auto is_sampled = [&](size_t index) {
        const size_t offset = index >= sample_start
                                  ? index - sample_start
                                  : kSegmentCount - sample_start + index;
        return offset < 6;
    };

    size_t fit_offset = 150;
    while (fit_offset < 200 &&
           is_sampled((fallback_start + fit_offset) % kSegmentCount)) {
        ++fit_offset;
    }
    ASSERT_LT(fit_offset, 200);
    const size_t fit_index = (fallback_start + fit_offset) % kSegmentCount;

    AllocatorManager allocator_manager;
    std::vector<std::shared_ptr<HintTestAllocator>> allocators;
    allocators.reserve(kSegmentCount);
    for (size_t i = 0; i < kSegmentCount; ++i) {
        const std::string name = "fit-budget-segment-" + std::to_string(i);
        const bool can_fit = i == fit_index;
        auto allocator = std::make_shared<HintTestAllocator>(
            name, can_fit ? kRequestSize : 0, can_fit);
        allocator_manager.addAllocator(name, allocator);
        allocators.push_back(std::move(allocator));
    }

    ScopedRandomSeed seed(kSeed);
    FreeRatioFirstAllocationStrategy strategy;
    auto& metrics = MasterMetricManager::instance();
    const auto requests_before = metrics.get_fit_aware_fallback_requests();
    const auto checked_before =
        metrics.get_fit_aware_eligible_candidates_checked();
    const auto credits_before = metrics.get_fit_aware_no_fit_budget_credits();
    const auto segment_calls_before =
        metrics.get_fit_aware_segment_allocate_calls();
    const auto recovered_before = metrics.get_fit_aware_recovered_allocations();
    auto result = strategy.Allocate(allocator_manager, kRequestSize);

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);
    EXPECT_EQ(allocators[fit_index]->allocationCalls(), 1);
    const size_t total_calls =
        std::accumulate(allocators.begin(), allocators.end(), size_t{0},
                        [](size_t total, const auto& allocator) {
                            return total + allocator->allocationCalls();
                        });
    EXPECT_EQ(total_calls, 7);
    EXPECT_EQ(metrics.get_fit_aware_fallback_requests() - requests_before, 1);
    EXPECT_EQ(
        metrics.get_fit_aware_eligible_candidates_checked() - checked_before,
        fit_offset + 1);
    EXPECT_EQ(metrics.get_fit_aware_no_fit_budget_credits() - credits_before,
              fit_offset);
    EXPECT_EQ(
        metrics.get_fit_aware_segment_allocate_calls() - segment_calls_before,
        1);
    EXPECT_EQ(metrics.get_fit_aware_recovered_allocations() - recovered_before,
              1);
}

TEST_F(AllocationStrategyTest,
       FreeRatioFirstUnknownHintsFailOpenWithinOriginalAttemptLimit) {
    constexpr size_t kSegmentCount = 256;
    constexpr size_t kRequestSize = 4096;
    constexpr uint64_t kSeed = 0xFA11BAC;

    RandomEngine predictor(kSeed);
    const size_t sample_start = randomIndex(kSegmentCount, predictor);
    const size_t fallback_start = randomIndex(kSegmentCount, predictor);
    const auto is_sampled = [&](size_t index) {
        const size_t offset = index >= sample_start
                                  ? index - sample_start
                                  : kSegmentCount - sample_start + index;
        return offset < 6;
    };

    size_t fit_offset = 150;
    while (fit_offset < 200 &&
           is_sampled((fallback_start + fit_offset) % kSegmentCount)) {
        ++fit_offset;
    }
    ASSERT_LT(fit_offset, 200);
    const size_t fit_index = (fallback_start + fit_offset) % kSegmentCount;

    AllocatorManager allocator_manager;
    std::vector<std::shared_ptr<HintTestAllocator>> allocators;
    allocators.reserve(kSegmentCount);
    for (size_t i = 0; i < kSegmentCount; ++i) {
        const std::string name = "unknown-hint-segment-" + std::to_string(i);
        const bool can_fit = i == fit_index;
        auto allocator = std::make_shared<HintTestAllocator>(
            name, can_fit ? kRequestSize : kAllocatorUnknownFreeSpace, can_fit);
        allocator_manager.addAllocator(name, allocator);
        allocators.push_back(std::move(allocator));
    }

    ScopedRandomSeed seed(kSeed);
    FreeRatioFirstAllocationStrategy strategy;
    auto& metrics = MasterMetricManager::instance();
    const auto requests_before = metrics.get_fit_aware_fallback_requests();
    const auto checked_before =
        metrics.get_fit_aware_eligible_candidates_checked();
    const auto credits_before = metrics.get_fit_aware_no_fit_budget_credits();
    const auto segment_calls_before =
        metrics.get_fit_aware_segment_allocate_calls();
    const auto chargeable_exhaustions_before =
        metrics.get_fit_aware_chargeable_budget_exhaustions();
    auto result = strategy.Allocate(allocator_manager, kRequestSize);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(allocators[fit_index]->allocationCalls(), 0);
    const size_t total_calls =
        std::accumulate(allocators.begin(), allocators.end(), size_t{0},
                        [](size_t total, const auto& allocator) {
                            return total + allocator->allocationCalls();
                        });
    EXPECT_EQ(total_calls, 106);
    EXPECT_EQ(metrics.get_fit_aware_fallback_requests() - requests_before, 1);
    EXPECT_EQ(
        metrics.get_fit_aware_eligible_candidates_checked() - checked_before,
        100);
    EXPECT_EQ(metrics.get_fit_aware_no_fit_budget_credits() - credits_before,
              0);
    EXPECT_EQ(
        metrics.get_fit_aware_segment_allocate_calls() - segment_calls_before,
        100);
    EXPECT_EQ(metrics.get_fit_aware_chargeable_budget_exhaustions() -
                  chargeable_exhaustions_before,
              1);
}

TEST_F(AllocationStrategyTest,
       FreeRatioFirstAvoidsHintOverheadWhenRetryBudgetCoversAllSegments) {
    constexpr size_t kSegmentCount = 64;
    constexpr size_t kRequestSize = 4096;

    AllocatorManager allocator_manager;
    std::vector<std::shared_ptr<HintTestAllocator>> allocators;
    allocators.reserve(kSegmentCount);
    for (size_t i = 0; i < kSegmentCount; ++i) {
        const std::string name = "small-cluster-segment-" + std::to_string(i);
        auto allocator = std::make_shared<HintTestAllocator>(name, 0, false);
        allocator_manager.addAllocator(name, allocator);
        allocators.push_back(std::move(allocator));
    }

    ScopedRandomSeed seed(0x5A11C1A5);
    FreeRatioFirstAllocationStrategy strategy;
    auto& metrics = MasterMetricManager::instance();
    const auto requests_before = metrics.get_fit_aware_fallback_requests();
    auto result = strategy.Allocate(allocator_manager, kRequestSize);

    ASSERT_FALSE(result.has_value());
    const size_t total_hint_queries =
        std::accumulate(allocators.begin(), allocators.end(), size_t{0},
                        [](size_t total, const auto& allocator) {
                            return total + allocator->hintQueries();
                        });
    EXPECT_EQ(total_hint_queries, 0);
    EXPECT_EQ(metrics.get_fit_aware_fallback_requests() - requests_before, 0);
}

TEST_F(AllocationStrategyTest,
       FreeRatioFirstReportsFitAwareScanLimitExhaustion) {
    constexpr size_t kSegmentCount = 256;
    constexpr size_t kRequestSize = 4096;

    AllocatorManager allocator_manager;
    for (size_t i = 0; i < kSegmentCount; ++i) {
        const std::string name = "scan-limit-segment-" + std::to_string(i);
        allocator_manager.addAllocator(
            name, std::make_shared<HintTestAllocator>(name, 0, false));
    }

    auto& metrics = MasterMetricManager::instance();
    const auto requests_before = metrics.get_fit_aware_fallback_requests();
    const auto checked_before =
        metrics.get_fit_aware_eligible_candidates_checked();
    const auto credits_before = metrics.get_fit_aware_no_fit_budget_credits();
    const auto scan_exhaustions_before =
        metrics.get_fit_aware_scan_cap_exhaustions();

    ScopedRandomSeed seed(0x5CA11E17);
    FreeRatioFirstAllocationStrategy strategy;
    auto result = strategy.Allocate(allocator_manager, kRequestSize);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(metrics.get_fit_aware_fallback_requests() - requests_before, 1);
    EXPECT_EQ(
        metrics.get_fit_aware_eligible_candidates_checked() - checked_before,
        200);
    EXPECT_EQ(metrics.get_fit_aware_no_fit_budget_credits() - credits_before,
              200);
    EXPECT_EQ(
        metrics.get_fit_aware_scan_cap_exhaustions() - scan_exhaustions_before,
        1);
}

TEST_F(AllocationStrategyTest,
       FreeRatioFirstReportsSegmentCallsNotBottomAllocatorCalls) {
    constexpr size_t kSegmentCount = 128;
    constexpr size_t kRequestSize = 4096;

    AllocatorManager allocator_manager;
    std::vector<std::shared_ptr<HintTestAllocator>> allocators;
    allocators.reserve(2 * kSegmentCount);
    for (size_t i = 0; i < kSegmentCount; ++i) {
        const std::string name = "multi-allocator-segment-" + std::to_string(i);
        for (size_t j = 0; j < 2; ++j) {
            auto allocator = std::make_shared<HintTestAllocator>(
                name, kAllocatorUnknownFreeSpace, false);
            allocator_manager.addAllocator(name, allocator);
            allocators.push_back(std::move(allocator));
        }
    }

    auto& metrics = MasterMetricManager::instance();
    const auto segment_calls_before =
        metrics.get_fit_aware_segment_allocate_calls();

    ScopedRandomSeed seed(0xA110CA7E);
    FreeRatioFirstAllocationStrategy strategy;
    auto result = strategy.Allocate(allocator_manager, kRequestSize);

    ASSERT_FALSE(result.has_value());
    const size_t bottom_allocator_calls =
        std::accumulate(allocators.begin(), allocators.end(), size_t{0},
                        [](size_t total, const auto& allocator) {
                            return total + allocator->allocationCalls();
                        });
    EXPECT_EQ(bottom_allocator_calls, 212);
    EXPECT_EQ(
        metrics.get_fit_aware_segment_allocate_calls() - segment_calls_before,
        100);
}

TEST_F(AllocationStrategyTest,
       FreeRatioFirstDoesNotRefundBudgetForExcludedSegments) {
    constexpr size_t kSegmentCount = 256;
    constexpr size_t kRequestSize = 4096;
    constexpr uint64_t kSeed = 0xE7C1DDED;

    RandomEngine predictor(kSeed);
    const size_t sample_start = randomIndex(kSegmentCount, predictor);
    const size_t fallback_start = randomIndex(kSegmentCount, predictor);
    const auto is_sampled = [&](size_t index) {
        const size_t offset = index >= sample_start
                                  ? index - sample_start
                                  : kSegmentCount - sample_start + index;
        return offset < 6;
    };

    size_t fit_offset = 150;
    while (fit_offset < 200 &&
           is_sampled((fallback_start + fit_offset) % kSegmentCount)) {
        ++fit_offset;
    }
    ASSERT_LT(fit_offset, 200);
    const size_t fit_index = (fallback_start + fit_offset) % kSegmentCount;

    AllocatorManager allocator_manager;
    std::vector<std::shared_ptr<HintTestAllocator>> allocators;
    allocators.reserve(kSegmentCount);
    std::set<std::string> excluded_segments;
    for (size_t i = 0; i < kSegmentCount; ++i) {
        const std::string name = "excluded-budget-segment-" + std::to_string(i);
        const bool can_fit = i == fit_index;
        auto allocator = std::make_shared<HintTestAllocator>(
            name, can_fit ? kRequestSize : 0, can_fit);
        allocator_manager.addAllocator(name, allocator);
        allocators.push_back(std::move(allocator));
    }
    for (size_t offset = 0; offset < 100; ++offset) {
        excluded_segments.insert(
            "excluded-budget-segment-" +
            std::to_string((fallback_start + offset) % kSegmentCount));
    }

    ScopedRandomSeed seed(kSeed);
    FreeRatioFirstAllocationStrategy strategy;
    auto result = strategy.Allocate(allocator_manager, kRequestSize, 1, {},
                                    excluded_segments);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    EXPECT_EQ(allocators[fit_index]->allocationCalls(), 0);
}

TEST_F(AllocationStrategyTest,
       FreeRatioFirstUsesRealOffsetHintsBeyondLegacyFallbackBudget) {
    constexpr size_t kSegmentCount = 256;
    constexpr size_t kRequestSize = 128 * 1024;
    constexpr uint64_t kSeed = 0x0FF5E7;

    RandomEngine predictor(kSeed);
    const size_t sample_start = randomIndex(kSegmentCount, predictor);
    const size_t fallback_start = randomIndex(kSegmentCount, predictor);
    const auto is_sampled = [&](size_t index) {
        const size_t offset = index >= sample_start
                                  ? index - sample_start
                                  : kSegmentCount - sample_start + index;
        return offset < 6;
    };

    size_t fit_offset = 150;
    while (fit_offset < 200 &&
           is_sampled((fallback_start + fit_offset) % kSegmentCount)) {
        ++fit_offset;
    }
    ASSERT_LT(fit_offset, 200);
    const size_t fit_index = (fallback_start + fit_offset) % kSegmentCount;

    AllocatorManager allocator_manager;
    std::vector<std::shared_ptr<OffsetBufferAllocator>> allocators;
    std::vector<std::unique_ptr<AllocatedBuffer>> held_buffers;
    allocators.reserve(kSegmentCount);
    held_buffers.reserve(kSegmentCount - 1);
    for (size_t i = 0; i < kSegmentCount; ++i) {
        const std::string name = "real-hint-segment-" + std::to_string(i);
        const uintptr_t base =
            0x300000000ULL + static_cast<uintptr_t>(i) * kRequestSize;
        auto allocator = std::make_shared<OffsetBufferAllocator>(
            name, base, kRequestSize, name);
        if (i != fit_index) {
            auto held = allocator->allocate(kRequestSize);
            ASSERT_NE(held, nullptr);
            ASSERT_EQ(allocator->allocate(kRequestSize), nullptr);
            ASSERT_LT(allocator->getLargestFreeRegionHint(), kRequestSize);
            held_buffers.push_back(std::move(held));
        }
        allocator_manager.addAllocator(name, allocator);
        allocators.push_back(std::move(allocator));
    }

    ScopedRandomSeed seed(kSeed);
    FreeRatioFirstAllocationStrategy strategy;
    auto result = strategy.Allocate(allocator_manager, kRequestSize);

    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result->size(), 1);
    const auto segment_names = result->front().get_segment_names();
    ASSERT_EQ(segment_names.size(), 1);
    ASSERT_TRUE(segment_names.front().has_value());
    EXPECT_EQ(segment_names.front().value(),
              "real-hint-segment-" + std::to_string(fit_index));
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

// Mock SsdMetricsProvider for testing SSD-aware allocation
class MockSsdMetricsProvider : public SsdMetricsProvider {
   public:
    std::unordered_map<std::string, int64_t> total_capacity;
    std::unordered_map<std::string, int64_t> used_bytes;

    int64_t getSsdTotalCapacity(const std::string& name) const override {
        auto it = total_capacity.find(name);
        return it != total_capacity.end() ? it->second : 0;
    }

    int64_t getSsdUsedBytes(const std::string& name) const override {
        auto it = used_bytes.find(name);
        return it != used_bytes.end() ? it->second : 0;
    }
};

TEST_F(AllocationStrategyTest, SsdFreeRatioFirstChoosesHighestFreeRatio) {
    auto ssd_strategy = std::make_unique<SsdFreeRatioFirstAllocationStrategy>();

    const int kNumSegments = 3;
    const size_t kSegmentSize = 64 * MiB;

    AllocatorManager allocator_manager;
    for (int i = 0; i < kNumSegments; i++) {
        const auto name = std::to_string(i) + "-segment";
        allocator_manager.addAllocator(
            name,
            std::make_shared<OffsetBufferAllocator>(
                name, 0x100000000ULL + i * kSegmentSize, kSegmentSize, name));
    }

    // SSD free ratios: segment 0 = 20%, segment 1 = 60%, segment 2 = 90%
    MockSsdMetricsProvider ssd_provider;
    for (int i = 0; i < kNumSegments; i++) {
        const auto name = std::to_string(i) + "-segment";
        ssd_provider.total_capacity[name] = 1000 * MiB;
    }
    ssd_provider.used_bytes["0-segment"] = 800 * MiB;  // 20% free
    ssd_provider.used_bytes["1-segment"] = 400 * MiB;  // 60% free
    ssd_provider.used_bytes["2-segment"] = 100 * MiB;  // 90% free

    auto result =
        ssd_strategy->Allocate(allocator_manager, 64 * 1024, 1, {}, {},
                               ReplicaType::MEMORY, &ssd_provider);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().size(), 1u);

    const auto& replica = result.value()[0];
    auto descriptor = replica.get_descriptor();
    ASSERT_TRUE(descriptor.is_memory_replica());
    const auto& mem_desc = descriptor.get_memory_descriptor();
    EXPECT_EQ(mem_desc.buffer_descriptor.transport_endpoint_, "2-segment");
}

// Test that SsdFreeRatioFirstAllocationStrategy works without an
// SsdMetricsProvider (delegates to base class random allocation).
TEST_F(AllocationStrategyTest,
       SsdFreeRatioFirstWithoutMetricsProviderAllocates) {
    auto ssd_strategy = std::make_unique<SsdFreeRatioFirstAllocationStrategy>();

    auto allocator = std::make_shared<OffsetBufferAllocator>(
        "segment1", 0x100000000ULL, 64 * MiB, "segment1");
    AllocatorManager allocator_manager;
    allocator_manager.addAllocator("segment1", allocator);

    auto result = ssd_strategy->Allocate(allocator_manager, 1024, 1, {}, {});
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().size(), 1u);
}

// Test that SsdFreeRatioFirstAllocationStrategy skips excluded segments even
// when they have the highest SSD free ratio.
TEST_F(AllocationStrategyTest, SsdFreeRatioFirstExcludedSegmentsSkipped) {
    auto ssd_strategy = std::make_unique<SsdFreeRatioFirstAllocationStrategy>();

    const int kNumSegments = 3;
    const size_t kSegmentSize = 64 * MiB;

    AllocatorManager allocator_manager;
    for (int i = 0; i < kNumSegments; i++) {
        const auto name = std::to_string(i) + "-segment";
        allocator_manager.addAllocator(
            name,
            std::make_shared<OffsetBufferAllocator>(
                name, 0x100000000ULL + i * kSegmentSize, kSegmentSize, name));
    }

    // SSD free ratios: 0-segment=95%, 1-segment=50%, 2-segment=30%
    MockSsdMetricsProvider ssd_provider;
    for (int i = 0; i < kNumSegments; i++) {
        const auto name = std::to_string(i) + "-segment";
        ssd_provider.total_capacity[name] = 1000 * MiB;
    }
    ssd_provider.used_bytes["0-segment"] = 50 * MiB;   // 95% free (highest)
    ssd_provider.used_bytes["1-segment"] = 500 * MiB;  // 50% free
    ssd_provider.used_bytes["2-segment"] = 700 * MiB;  // 30% free

    // Exclude 0-segment which has the highest SSD free ratio
    std::set<std::string> excluded = {"0-segment"};

    auto result =
        ssd_strategy->Allocate(allocator_manager, 64 * 1024, 1, {}, excluded,
                               ReplicaType::MEMORY, &ssd_provider);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().size(), 1u);

    const auto& replica = result.value()[0];
    auto descriptor = replica.get_descriptor();
    ASSERT_TRUE(descriptor.is_memory_replica());
    const auto& mem_desc = descriptor.get_memory_descriptor();
    // Must NOT be allocated to the excluded segment despite its high free ratio
    EXPECT_NE(mem_desc.buffer_descriptor.transport_endpoint_, "0-segment");
}

// Test that ssd_used_bytes > ssd_total_capacity is clamped to 0% free,
// so the other segment (with normal usage) is preferred.
TEST_F(AllocationStrategyTest, SsdFreeRatioFirstUsedExceedsTotalIsClamped) {
    auto ssd_strategy = std::make_unique<SsdFreeRatioFirstAllocationStrategy>();

    const size_t kSegmentSize = 64 * MiB;

    AllocatorManager allocator_manager;
    allocator_manager.addAllocator(
        "0-segment",
        std::make_shared<OffsetBufferAllocator>("0-segment", 0x100000000ULL,
                                                kSegmentSize, "0-segment"));
    allocator_manager.addAllocator(
        "1-segment", std::make_shared<OffsetBufferAllocator>(
                         "1-segment", 0x100000000ULL + kSegmentSize,
                         kSegmentSize, "1-segment"));

    // 0-segment: used exceeds total (concurrent drift) → clamped to 0% free
    // 1-segment: used=100, total=1000 → 90% free
    MockSsdMetricsProvider ssd_provider;
    ssd_provider.total_capacity["0-segment"] = 1000 * MiB;
    ssd_provider.total_capacity["1-segment"] = 1000 * MiB;
    ssd_provider.used_bytes["0-segment"] = 1500 * MiB;  // exceeds total
    ssd_provider.used_bytes["1-segment"] = 100 * MiB;   // 90% free

    auto result =
        ssd_strategy->Allocate(allocator_manager, 64 * 1024, 1, {}, {},
                               ReplicaType::MEMORY, &ssd_provider);
    ASSERT_TRUE(result.has_value());
    ASSERT_EQ(result.value().size(), 1u);

    const auto& replica = result.value()[0];
    auto descriptor = replica.get_descriptor();
    ASSERT_TRUE(descriptor.is_memory_replica());
    const auto& mem_desc = descriptor.get_memory_descriptor();
    // 0-segment is clamped to 0% free; 1-segment is 90% free → pick 1-segment
    EXPECT_EQ(mem_desc.buffer_descriptor.transport_endpoint_, "1-segment");
}

// Strategy-level performance comparison: Random vs SsdFreeRatioFirst.
// Uses OffsetBufferAllocator (real physical memory via aligned_alloc) and
// MockSsdMetricsProvider. Measures pure strategy overhead without MasterService
// locking, reflecting only the allocation algorithm cost.
TEST_F(AllocationStrategyTest, SsdFreeRatioFirstVsRandomStrategyPerformance) {
    constexpr size_t kNumSegments = 64;
    constexpr size_t kSegmentSize = 8 * MiB;   // 64 * 8MB = 512MB physical
    constexpr size_t kAllocSize = 128 * 1024;  // 128KB per allocation
    constexpr int kWarmupRounds = 200;
    constexpr int kBenchmarkRounds = 2000;

    // Build AllocatorManager: 64 segments, each 8 MiB
    AllocatorManager allocator_manager;
    for (size_t i = 0; i < kNumSegments; i++) {
        const auto name = "perf_seg_" + std::to_string(i);
        allocator_manager.addAllocator(
            name,
            std::make_shared<OffsetBufferAllocator>(
                name, 0x200000000ULL + i * kSegmentSize, kSegmentSize, name));
    }

    // SSD free ratios vary uniformly from ~10% to ~90% across segments
    MockSsdMetricsProvider ssd_provider;
    for (size_t i = 0; i < kNumSegments; i++) {
        const auto name = "perf_seg_" + std::to_string(i);
        ssd_provider.total_capacity[name] = 1000 * MiB;
        // used: 100 MiB (10% used) to 900 MiB (90% used)
        ssd_provider.used_bytes[name] =
            static_cast<int64_t>((100 + i * 800 / kNumSegments)) * MiB;
    }

    // -------- Random strategy --------
    auto random_strategy = std::make_unique<RandomAllocationStrategy>();
    {
        // Warmup (results discarded)
        for (int i = 0; i < kWarmupRounds; i++) {
            (void)random_strategy->Allocate(allocator_manager, kAllocSize);
        }
    }
    std::vector<std::vector<Replica>> random_replicas;
    random_replicas.reserve(kBenchmarkRounds);

    auto t_rand_start = std::chrono::steady_clock::now();
    for (int i = 0; i < kBenchmarkRounds; i++) {
        auto r = random_strategy->Allocate(allocator_manager, kAllocSize);
        ASSERT_TRUE(r.has_value());
        random_replicas.emplace_back(std::move(r.value()));
    }
    auto rand_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t_rand_start);
    random_replicas.clear();  // deallocate so SSD strategy starts fresh

    // -------- SsdFreeRatioFirst strategy --------
    auto ssd_strategy = std::make_unique<SsdFreeRatioFirstAllocationStrategy>();
    {
        // Warmup
        for (int i = 0; i < kWarmupRounds; i++) {
            (void)ssd_strategy->Allocate(allocator_manager, kAllocSize, 1, {},
                                         {}, ReplicaType::MEMORY,
                                         &ssd_provider);
        }
    }
    std::vector<std::vector<Replica>> ssd_replicas;
    ssd_replicas.reserve(kBenchmarkRounds);

    auto t_ssd_start = std::chrono::steady_clock::now();
    for (int i = 0; i < kBenchmarkRounds; i++) {
        auto r = ssd_strategy->Allocate(allocator_manager, kAllocSize, 1, {},
                                        {}, ReplicaType::MEMORY, &ssd_provider);
        ASSERT_TRUE(r.has_value());
        ssd_replicas.emplace_back(std::move(r.value()));
    }
    auto ssd_us = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - t_ssd_start);
    ssd_replicas.clear();

    double rand_us_per_op =
        static_cast<double>(rand_us.count()) / kBenchmarkRounds;
    double ssd_us_per_op =
        static_cast<double>(ssd_us.count()) / kBenchmarkRounds;
    double overhead_ratio =
        static_cast<double>(ssd_us.count()) / rand_us.count();

    std::cout
        << "\n=== Strategy-Level Performance: Random vs SsdFreeRatioFirst ===\n"
        << "Segments: " << kNumSegments
        << " | Alloc size: " << (kAllocSize / 1024) << " KB"
        << " | Rounds: " << kBenchmarkRounds << "\n"
        << "Random:              " << rand_us.count() << " us total  |  "
        << std::fixed << std::setprecision(3) << rand_us_per_op << " us/op\n"
        << "SsdFreeRatioFirst:   " << ssd_us.count() << " us total  |  "
        << ssd_us_per_op << " us/op\n"
        << "Overhead ratio:      " << std::setprecision(2) << overhead_ratio
        << "x  (" << std::setprecision(1) << (overhead_ratio - 1.0) * 100.0
        << "% slower)\n\n";
}

}  // namespace mooncake
