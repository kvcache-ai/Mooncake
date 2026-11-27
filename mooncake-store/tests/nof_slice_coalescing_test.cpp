#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <vector>

#include "transfer_task.h"

namespace mooncake {
namespace {

constexpr size_t kBlocks = 4096;
constexpr size_t kHalfBlock = kBlocks / 2;

// Pointers only have to be distinct and ordered; nothing here dereferences
// them, so any non-null address works.
void* At(uintptr_t base, size_t offset) {
    return reinterpret_cast<void*>(base + offset);
}

// Base address for the synthetic buffers below.
constexpr uintptr_t kBase = 0x10000000;

TEST(NofSliceCoalescingTest, SingleSliceIsReturnedUnchanged) {
    const std::vector<Slice> slices{{At(kBase, 0), kBlocks}};

    const auto runs = CoalesceAdjacentSlices(slices);

    ASSERT_TRUE(runs.has_value());
    ASSERT_EQ(runs->size(), 1u);
    EXPECT_EQ((*runs)[0].ptr, slices[0].ptr);
    EXPECT_EQ((*runs)[0].size, kBlocks);
}

// The regression this exists for: two half-block slices of one block-aligned
// buffer are individually unaligned but describe an aligned run.
TEST(NofSliceCoalescingTest, AdjacentSlicesMergeIntoOneRun) {
    const std::vector<Slice> slices{{At(kBase, 0), kHalfBlock},
                                    {At(kBase, kHalfBlock), kHalfBlock}};

    const auto runs = CoalesceAdjacentSlices(slices);

    ASSERT_TRUE(runs.has_value());
    ASSERT_EQ(runs->size(), 1u);
    EXPECT_EQ((*runs)[0].ptr, slices[0].ptr);
    EXPECT_EQ((*runs)[0].size, kBlocks);
}

TEST(NofSliceCoalescingTest, ManyAdjacentSlicesMergeIntoOneRun) {
    std::vector<Slice> slices;
    for (size_t i = 0; i < 8; ++i) {
        slices.push_back({At(kBase, i * kHalfBlock), kHalfBlock});
    }

    const auto runs = CoalesceAdjacentSlices(slices);

    ASSERT_TRUE(runs.has_value());
    ASSERT_EQ(runs->size(), 1u);
    EXPECT_EQ((*runs)[0].ptr, slices[0].ptr);
    EXPECT_EQ((*runs)[0].size, 8 * kHalfBlock);
}

TEST(NofSliceCoalescingTest, SeparateBuffersStaySeparate) {
    const std::vector<Slice> slices{{At(kBase, 0), kBlocks},
                                    {At(kBase, 4 * kBlocks), kBlocks}};

    const auto runs = CoalesceAdjacentSlices(slices);

    ASSERT_TRUE(runs.has_value());
    ASSERT_EQ(runs->size(), 2u);
    EXPECT_EQ((*runs)[0].ptr, slices[0].ptr);
    EXPECT_EQ((*runs)[0].size, kBlocks);
    EXPECT_EQ((*runs)[1].ptr, slices[1].ptr);
    EXPECT_EQ((*runs)[1].size, kBlocks);
}

TEST(NofSliceCoalescingTest, RunsAlternateWithGaps) {
    // [A][A][gap][B][B][gap][C] -> three runs.
    const std::vector<Slice> slices{
        {At(kBase, 0), kHalfBlock},
        {At(kBase, kHalfBlock), kHalfBlock},
        {At(kBase, 8 * kBlocks), kHalfBlock},
        {At(kBase, 8 * kBlocks + kHalfBlock), kHalfBlock},
        {At(kBase, 16 * kBlocks), kBlocks}};

    const auto runs = CoalesceAdjacentSlices(slices);

    ASSERT_TRUE(runs.has_value());
    ASSERT_EQ(runs->size(), 3u);
    EXPECT_EQ((*runs)[0].size, kBlocks);
    EXPECT_EQ((*runs)[1].size, kBlocks);
    EXPECT_EQ((*runs)[2].size, kBlocks);
    EXPECT_EQ((*runs)[2].ptr, slices[4].ptr);
}

TEST(NofSliceCoalescingTest, OutOfOrderSlicesAreNotReordered) {
    // Descending addresses: adjacent in the vector but not in memory.
    const std::vector<Slice> slices{{At(kBase, 4 * kBlocks), kBlocks},
                                    {At(kBase, 0), kBlocks}};

    const auto runs = CoalesceAdjacentSlices(slices);

    ASSERT_TRUE(runs.has_value());
    ASSERT_EQ(runs->size(), 2u);
    EXPECT_EQ((*runs)[0].ptr, slices[0].ptr);
    EXPECT_EQ((*runs)[1].ptr, slices[1].ptr);
}

TEST(NofSliceCoalescingTest, ZeroLengthSlicesAreDropped) {
    const std::vector<Slice> slices{
        {At(kBase, 0), 0}, {At(kBase, 0), kBlocks}, {At(kBase, kBlocks), 0}};

    const auto runs = CoalesceAdjacentSlices(slices);

    ASSERT_TRUE(runs.has_value());
    ASSERT_EQ(runs->size(), 1u);
    EXPECT_EQ((*runs)[0].ptr, At(kBase, 0));
    EXPECT_EQ((*runs)[0].size, kBlocks);
}

TEST(NofSliceCoalescingTest, ZeroLengthSliceBetweenRunsDoesNotBreakThem) {
    // The dropped slice sits exactly on the boundary, so the two real slices
    // must still coalesce with each other.
    const std::vector<Slice> slices{{At(kBase, 0), kHalfBlock},
                                    {At(kBase, kHalfBlock), 0},
                                    {At(kBase, kHalfBlock), kHalfBlock}};

    const auto runs = CoalesceAdjacentSlices(slices);

    ASSERT_TRUE(runs.has_value());
    ASSERT_EQ(runs->size(), 1u);
    EXPECT_EQ((*runs)[0].ptr, At(kBase, 0));
    EXPECT_EQ((*runs)[0].size, kBlocks);
}

TEST(NofSliceCoalescingTest, AllZeroLengthSlicesYieldAnEmptyVector) {
    const std::vector<Slice> slices{{At(kBase, 0), 0}, {At(kBase, kBlocks), 0}};

    const auto runs = CoalesceAdjacentSlices(slices);

    ASSERT_TRUE(runs.has_value());
    EXPECT_TRUE(runs->empty());
}

TEST(NofSliceCoalescingTest, EmptyInputYieldsAnEmptyVector) {
    const auto runs = CoalesceAdjacentSlices({});

    ASSERT_TRUE(runs.has_value());
    EXPECT_TRUE(runs->empty());
}

TEST(NofSliceCoalescingTest, NullPointerIsRejected) {
    // Rejected even when empty: a null slice is a caller bug, and reporting it
    // beats silently submitting a transfer with fewer bytes than requested.
    for (const auto& slices : {std::vector<Slice>{{nullptr, kBlocks}},
                               std::vector<Slice>{{nullptr, 0}}}) {
        const auto runs = CoalesceAdjacentSlices(slices);
        EXPECT_FALSE(runs.has_value());
    }
}

TEST(NofSliceCoalescingTest, NullPointerAmongValidSlicesIsRejected) {
    const std::vector<Slice> slices{{At(kBase, 0), kBlocks},
                                    {nullptr, kBlocks}};

    EXPECT_FALSE(CoalesceAdjacentSlices(slices).has_value());
}

TEST(NofSliceCoalescingTest, PointerPlusSizeOverflowIsRejected) {
    const auto near_top = std::numeric_limits<uintptr_t>::max() - 16;
    const std::vector<Slice> slices{{At(near_top, 0), kBlocks}};

    EXPECT_FALSE(CoalesceAdjacentSlices(slices).has_value());
}

TEST(NofSliceCoalescingTest, LargeButInRangeSliceIsAccepted) {
    // One byte short of the top of the address space: the overflow check must
    // not reject a legitimate (if implausible) range.
    const auto near_top = std::numeric_limits<uintptr_t>::max() - 16;
    const std::vector<Slice> slices{{At(near_top, 0), 16}};

    const auto runs = CoalesceAdjacentSlices(slices);

    ASSERT_TRUE(runs.has_value());
    ASSERT_EQ(runs->size(), 1u);
    EXPECT_EQ((*runs)[0].size, 16);
}

TEST(NofSliceCoalescingTest, CoalescingPreservesContiguousByteCount) {
    const std::vector<Slice> slices{{At(kBase, 0), 1024},
                                    {At(kBase, 1024), 3072},
                                    {At(kBase, 16 * kBlocks), 512},
                                    {At(kBase, 16 * kBlocks + 512), 3584}};

    const auto runs = CoalesceAdjacentSlices(slices);

    ASSERT_TRUE(runs.has_value());
    ASSERT_EQ(runs->size(), 2u);
    size_t total = 0;
    for (const auto& slice : slices) {
        total += slice.size;
    }
    size_t run_total = 0;
    for (const auto& run : *runs) {
        run_total += run.size;
    }
    EXPECT_EQ(run_total, total);
}

}  // namespace
}  // namespace mooncake
