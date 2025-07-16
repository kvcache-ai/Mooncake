#include <gtest/gtest.h>

#include <random>
#include <span>
#include <vector>

#include "offset_allocator/offsetAllocator.hpp"

using namespace mooncake::offset_allocator;

// Constants for allocator configuration
constexpr uint32 ALLOCATOR_SIZE = 1024 * 1024 * 1024;  // 1GB
constexpr uint32 MAX_ALLOCS = 1000;

// Helper function to check if any allocations overlap
void assertNoOverlap(std::span<AllocationHandle> handles) {
    for (size_t i = 0; i < handles.size(); ++i) {
        if (!handles[i].isValid()) continue;

        uint32 start_i = handles[i].address();
        uint32 end_i = start_i + handles[i].size();

        for (size_t j = i + 1; j < handles.size(); ++j) {
            if (!handles[j].isValid()) continue;

            uint32 start_j = handles[j].address();
            uint32 end_j = start_j + handles[j].size();

            // Check for overlap: two ranges [start_i, end_i) and [start_j,
            // end_j) overlap if: start_i < end_j && start_j < end_i
            ASSERT_FALSE(start_i < end_j && start_j < end_i)
                << "Allocation overlap detected: " << "Handle " << i << " ["
                << start_i << ", " << end_i << ") " << "overlaps with Handle "
                << j << " [" << start_j << ", " << end_j << ")";
        }
    }
}

class OffsetAllocatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        allocator = Allocator::create(0, ALLOCATOR_SIZE,
                                      MAX_ALLOCS);  // 1GB, 1000 max allocs
    }

    void TearDown() override { allocator.reset(); }

    std::shared_ptr<Allocator> allocator;
};

// Test basic allocation and deallocation
TEST_F(OffsetAllocatorTest, BasicAllocation) {
    // Allocate handle
    auto handle = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_TRUE(handle.has_value());
    EXPECT_TRUE(handle->isValid());
    EXPECT_NE(handle->address(), Allocation::NO_SPACE);
    EXPECT_EQ(handle->size(), ALLOCATOR_SIZE);

    // Try allocate new handle
    auto handle2 = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_FALSE(handle2.has_value());

    // Release handle
    handle.reset();

    // Try allocate again
    handle2 = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_TRUE(handle2.has_value());
    EXPECT_TRUE(handle2->isValid());
    EXPECT_NE(handle2->address(), Allocation::NO_SPACE);
}

// Test allocation failure when out of space
TEST_F(OffsetAllocatorTest, AllocationFailure) {
    // Try to allocate more than available space
    auto handle =
        allocator->allocate(2 * ALLOCATOR_SIZE);  // 2GB > 1GB available
    EXPECT_FALSE(handle.has_value());
}

// Test multiple allocations
TEST_F(OffsetAllocatorTest, MultipleAllocations) {
    std::vector<AllocationHandle> handles;

    for (int i = 0; i < 10; ++i) {
        auto handle = allocator->allocate(1000);
        ASSERT_TRUE(handle.has_value());
        handles.push_back(std::move(*handle));
    }

    // All handles should be valid and have different offsets
    for (size_t i = 0; i < handles.size(); ++i) {
        EXPECT_TRUE(handles[i].isValid());
        EXPECT_EQ(handles[i].size(), 1000);
        for (size_t j = i + 1; j < handles.size(); ++j) {
            EXPECT_NE(handles[i].address(), handles[j].address());
        }
    }

    // Check that no allocations overlap
    assertNoOverlap(std::span<AllocationHandle>(handles));
}

// Test allocations with different sizes don't overlap
TEST_F(OffsetAllocatorTest, DifferentSizesNoOverlap) {
    std::vector<AllocationHandle> handles;
    std::vector<uint32> sizes = {100, 500, 1000, 2000, 50, 1500, 800, 300};

    for (uint32 size : sizes) {
        auto handle = allocator->allocate(size);
        ASSERT_TRUE(handle.has_value()) << "Failed to allocate size: " << size;
        EXPECT_EQ(handle->size(), size);
        handles.push_back(std::move(*handle));
    }

    // Verify all handles are valid
    for (const auto& handle : handles) {
        EXPECT_TRUE(handle.isValid());
    }

    // Check that no allocations overlap
    assertNoOverlap(std::span<AllocationHandle>(handles));
}

// Test storage reports
TEST_F(OffsetAllocatorTest, StorageReports) {
    StorageReport report = allocator->storageReport();
    EXPECT_GT(report.totalFreeSpace, 0);
    EXPECT_GT(report.largestFreeRegion, 0);

    // Allocate some space
    auto handle = allocator->allocate(1000);
    ASSERT_TRUE(handle.has_value());

    StorageReport newReport = allocator->storageReport();
    EXPECT_LT(newReport.totalFreeSpace, report.totalFreeSpace);
}

// Test continuous allocation and deallocation with random sizes
TEST_F(OffsetAllocatorTest, ContinuousRandomAllocationDeallocation) {
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<uint32> size_dist(1,
                                                    1024 * 64);  // 1B to 64KB

    const int max_iterations = 20000;

    // Allocate and deallocate random sizes
    for (int i = 0; i < max_iterations; ++i) {
        uint32_t size = size_dist(gen);
        auto handle = allocator->allocate(size);
        EXPECT_TRUE(handle.has_value()) << "Failed to allocate size: " << size;
        // It will free automatically when handle goes out of scope
    }

    auto full_space_handle = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_TRUE(full_space_handle.has_value());
    EXPECT_EQ(full_space_handle->size(), ALLOCATOR_SIZE);
}

// Verify how much contiguous memory is still available after many
// small random allocations.
TEST_F(OffsetAllocatorTest,
       MaxContiguousAllocationAfterRandomSmallAllocations) {
    // 1 B … 64 KiB
    std::uniform_int_distribution<std::uint32_t> size_dist(1, 64 * 1024);
    std::mt19937 rng(std::random_device{}());

    std::vector<AllocationHandle> small_blocks;

    // 1) Allocate 400 small random-sized blocks.
    for (std::size_t i = 0; i < 400; ++i) {
        std::uint32_t size = size_dist(rng);
        auto h = allocator->allocate(size);
        ASSERT_TRUE(h.has_value())
            << "Allocation of " << size << " bytes failed";
        small_blocks.emplace_back(std::move(*h));
    }

    // 2) Find the largest contiguous block that can still be allocated.
    for (std::uint32_t sz = ALLOCATOR_SIZE; sz >= 1 * 1024 * 1024;
         sz -= 1 * 1024 * 1024) {
        auto h = allocator->allocate(sz);
        if (h.has_value()) {
            std::cout << "Largest allocatable block after random allocations: "
                      << sz << " bytes\n";
            std::cout << "Utilization: "
                      << static_cast<double>(sz) /
                             static_cast<double>(ALLOCATOR_SIZE)
                      << '\n';
            break;
        }
    }

    // 3) Free all small blocks.
    small_blocks.clear();

    // 4) The entire region should now be available again.
    auto full = allocator->allocate(ALLOCATOR_SIZE);
    ASSERT_TRUE(full.has_value());
    EXPECT_EQ(full->size(), ALLOCATOR_SIZE);
}
