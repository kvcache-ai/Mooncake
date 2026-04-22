#include <gtest/gtest.h>

#include <algorithm>
#include <numeric>
#include <vector>

#include "real_client.h"

namespace mooncake {

static constexpr size_t kGiB = 1ULL << 30;

// Helper: verify invariants that must hold for every valid result.
static void checkInvariants(const std::vector<size_t>& sizes,
                            size_t total_bytes, size_t max_mr_size) {
    ASSERT_FALSE(sizes.empty());
    // (c) sum == total_bytes
    size_t sum = std::accumulate(sizes.begin(), sizes.end(), size_t{0});
    EXPECT_EQ(sum, total_bytes) << "segments do not sum to total_bytes";
    // (a) every segment <= max_mr_size
    for (size_t i = 0; i < sizes.size(); i++) {
        EXPECT_LE(sizes[i], max_mr_size)
            << "segment " << i << " (" << sizes[i]
            << " B) exceeds max_mr_size (" << max_mr_size << " B)";
    }
    // (b) adjacent segments differ by at most 1 GiB
    for (size_t i = 1; i < sizes.size(); i++) {
        size_t lo = std::min(sizes[i - 1], sizes[i]);
        size_t hi = std::max(sizes[i - 1], sizes[i]);
        EXPECT_LE(hi - lo, kGiB) << "segments " << i - 1 << " and " << i
                                 << " differ by more than 1 GiB";
    }
}

// Non-RDMA: always a single segment regardless of size.
TEST(ComputeSegmentSizesTest, NonRdmaAlwaysSingleSegment) {
    const size_t total = 1401 * kGiB;
    const size_t max_mr = 1024 * kGiB;  // would split under RDMA
    auto sizes = computeSegmentSizes(total, max_mr, /*is_rdma=*/false);
    ASSERT_EQ(sizes.size(), 1u);
    EXPECT_EQ(sizes[0], total);
}

// RDMA, GiB-aligned total: balanced split, all segments are exact GiB.
// 1401 GiB / 1 TiB limit -> 2 segments: 700 GiB + 701 GiB.
TEST(ComputeSegmentSizesTest, RdmaGiBAlignedBalancedSplit) {
    const size_t total = 1401 * kGiB;
    const size_t max_mr = 1024 * kGiB;  // 1 TiB
    auto sizes = computeSegmentSizes(total, max_mr, /*is_rdma=*/true);
    checkInvariants(sizes, total, max_mr);
    ASSERT_EQ(sizes.size(), 2u);
    EXPECT_EQ(sizes[0], 700 * kGiB);
    EXPECT_EQ(sizes[1], 701 * kGiB);
}

// RDMA, sub-GiB tail: last segment absorbs the tail, must not exceed
// max_mr_size even when max_mr_size is not GiB-aligned.
// 7 GiB + 500 MiB, max = 3 GiB -> 3 segments: [2 GiB, 3 GiB, 2 GiB+500 MiB].
TEST(ComputeSegmentSizesTest, RdmaSubGiBTailAbsorbedInLastSegment) {
    const size_t tail = 500 * 1024 * 1024;  // 500 MiB
    const size_t total = 7 * kGiB + tail;
    const size_t max_mr = 3 * kGiB;
    auto sizes = computeSegmentSizes(total, max_mr, /*is_rdma=*/true);
    checkInvariants(sizes, total, max_mr);
    ASSERT_EQ(sizes.size(), 3u);
    EXPECT_EQ(sizes[0], 2 * kGiB);
    EXPECT_EQ(sizes[1], 3 * kGiB);
    EXPECT_EQ(sizes[2], 2 * kGiB + tail);
}

// RDMA, total slightly over one max_mr_size: must produce 2 segments, not 1.
// floor(total_gib/max_seg_gib) == 1 but the 1-byte tail would push the single
// segment over max_mr_size.
TEST(ComputeSegmentSizesTest, RdmaTotalJustOverMaxMrSizeRequiresTwoSegments) {
    const size_t max_mr = 2 * kGiB;
    const size_t total = max_mr + 1;  // 2 GiB + 1 byte
    auto sizes = computeSegmentSizes(total, max_mr, /*is_rdma=*/true);
    checkInvariants(sizes, total, max_mr);
    ASSERT_EQ(sizes.size(), 2u);
}

// RDMA, total exactly equals max_mr_size: exactly 1 segment, no split.
TEST(ComputeSegmentSizesTest, RdmaTotalEqualsMaxMrSizeSingleSegment) {
    const size_t max_mr = 4 * kGiB;
    const size_t total = max_mr;
    auto sizes = computeSegmentSizes(total, max_mr, /*is_rdma=*/true);
    checkInvariants(sizes, total, max_mr);
    ASSERT_EQ(sizes.size(), 1u);
    EXPECT_EQ(sizes[0], total);
}

// RDMA, total below max_mr_size with sub-GiB tail: 1 segment containing tail.
TEST(ComputeSegmentSizesTest, RdmaTotalBelowMaxMrSizeWithTailSingleSegment) {
    const size_t tail = 123 * 1024 * 1024;  // 123 MiB
    const size_t total = 3 * kGiB + tail;
    const size_t max_mr = 8 * kGiB;
    auto sizes = computeSegmentSizes(total, max_mr, /*is_rdma=*/true);
    checkInvariants(sizes, total, max_mr);
    ASSERT_EQ(sizes.size(), 1u);
    EXPECT_EQ(sizes[0], total);
}

// RDMA, max_mr_size exactly 1 GiB: the boundary of the supported range.
// Every segment must be exactly 1 GiB; invariant (a) must hold strictly.
TEST(ComputeSegmentSizesTest, RdmaMaxMrSizeExactlyOneGiBHonorsCap) {
    const size_t max_mr = kGiB;
    const size_t total = 3 * kGiB;
    auto sizes = computeSegmentSizes(total, max_mr, /*is_rdma=*/true);
    checkInvariants(sizes, total, max_mr);
    ASSERT_EQ(sizes.size(), 3u);
    for (size_t s : sizes) EXPECT_EQ(s, kGiB);
}

// RDMA, max_mr_size < 1 GiB: unsupported precondition. The function logs
// an error and returns an empty vector so callers fail loudly rather than
// mounting segments that violate the cap.
TEST(ComputeSegmentSizesTest, RdmaMaxMrSizeBelowOneGiBReturnsEmpty) {
    const size_t max_mr = 512 * 1024 * 1024;  // 512 MiB, < 1 GiB
    const size_t total = 3 * kGiB;
    auto sizes = computeSegmentSizes(total, max_mr, /*is_rdma=*/true);
    EXPECT_TRUE(sizes.empty());
}

}  // namespace mooncake
