#include "batch_read_fanout.h"

#include <cstring>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

struct FakeOp {
    std::string key;
};

TEST(PlanDuplicateKeysTest, UniqueKeysAreAllPrimariesInOrder) {
    std::vector<FakeOp> ops{{"a"}, {"b"}, {"c"}};

    const auto plan = PlanDuplicateKeys(ops);

    EXPECT_EQ(plan.primaries, (std::vector<size_t>{0, 1, 2}));
    EXPECT_TRUE(plan.duplicates.empty());
}

TEST(PlanDuplicateKeysTest, DuplicatesPairWithFirstOccurrence) {
    std::vector<FakeOp> ops{{"a"}, {"b"}, {"a"}, {"a"}, {"b"}};

    const auto plan = PlanDuplicateKeys(ops);

    EXPECT_EQ(plan.primaries, (std::vector<size_t>{0, 1}));
    EXPECT_EQ(plan.duplicates,
              (std::vector<std::pair<size_t, size_t>>{{2, 0}, {3, 0}, {4, 1}}));
}

TEST(CopySlicesMaybeDeviceTest, ContiguousSingleSlice) {
    char src[] = "hello";
    char dst[sizeof(src)] = {};

    auto r =
        CopySlicesMaybeDevice({Slice{dst, sizeof(src)}},
                              {Slice{src, sizeof(src)}}, sizeof(src), "test");

    ASSERT_TRUE(r.has_value());
    EXPECT_STREQ(dst, src);
}

TEST(CopySlicesMaybeDeviceTest, MismatchedChunkingCopiesAcrossBoundaries) {
    const std::string payload = "0123456789abcdef";
    std::vector<char> src_buf(payload.begin(), payload.end());
    std::vector<char> dst_buf(payload.size(), '\0');

    // Source in 3 chunks, destination in 5; bytes must land identically.
    std::vector<Slice> src{
        {src_buf.data(), 5}, {src_buf.data() + 5, 7}, {src_buf.data() + 12, 4}};
    std::vector<Slice> dst{{dst_buf.data(), 2},
                           {dst_buf.data() + 2, 3},
                           {dst_buf.data() + 5, 1},
                           {dst_buf.data() + 6, 6},
                           {dst_buf.data() + 12, 4}};

    auto r = CopySlicesMaybeDevice(dst, src, payload.size(), "test");

    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(std::string(dst_buf.begin(), dst_buf.end()), payload);
}

TEST(CopySlicesMaybeDeviceTest, ZeroSizeSlicesAreSkipped) {
    char src[] = "ab";
    char dst[sizeof(src)] = {};

    auto r =
        CopySlicesMaybeDevice({Slice{nullptr, 0}, Slice{dst, sizeof(src)}},
                              {Slice{src, sizeof(src)}}, sizeof(src), "test");

    ASSERT_TRUE(r.has_value());
    EXPECT_STREQ(dst, src);
}

TEST(CopySlicesMaybeDeviceTest, ShortDestinationUnderflows) {
    char src[] = "abcd";
    char dst[2] = {};

    auto r = CopySlicesMaybeDevice({Slice{dst, sizeof(dst)}},
                                   {Slice{src, sizeof(src) - 1}},
                                   sizeof(src) - 1, "test");

    ASSERT_FALSE(r.has_value());
    EXPECT_EQ(r.error(), ErrorCode::INVALID_PARAMS);
}

TEST(FanOutDuplicatesTest, SuccessMirrorsPrimaryResult) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(2);
    results[0] = 16;
    char src[] = "data";
    char dst[sizeof(src)] = {};

    FanOutDuplicates({DuplicateFanOutJob{.dup_result_index = 1,
                                         .primary_result_index = 0,
                                         .key = "k",
                                         .dup_bytes = sizeof(src),
                                         .primary_bytes = sizeof(src),
                                         .copy =
                                             [&]() {
                                                 return CopyMaybeDevice(
                                                     dst, src, sizeof(src),
                                                     "test");
                                             }}},
                     results);

    ASSERT_TRUE(results[1].has_value());
    EXPECT_EQ(*results[1], 16);
    EXPECT_STREQ(dst, src);
}

TEST(FanOutDuplicatesTest, PrimaryFailurePropagatesError) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(2);
    results[0] = tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    bool copy_ran = false;

    FanOutDuplicates(
        {DuplicateFanOutJob{.dup_result_index = 1,
                            .primary_result_index = 0,
                            .key = "k",
                            .dup_bytes = 4,
                            .primary_bytes = 4,
                            .copy =
                                [&]() {
                                    copy_ran = true;
                                    return tl::expected<void, ErrorCode>();
                                }}},
        results);

    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::OBJECT_NOT_FOUND);
    EXPECT_FALSE(copy_ran);
}

TEST(FanOutDuplicatesTest, SizeMismatchFailsLoudlyWithoutCopying) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(2);
    results[0] = 4;
    bool copy_ran = false;

    FanOutDuplicates(
        {DuplicateFanOutJob{.dup_result_index = 1,
                            .primary_result_index = 0,
                            .key = "k",
                            .dup_bytes = 8,
                            .primary_bytes = 4,
                            .copy =
                                [&]() {
                                    copy_ran = true;
                                    return tl::expected<void, ErrorCode>();
                                }}},
        results);

    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::INTERNAL_ERROR);
    EXPECT_FALSE(copy_ran);
}

TEST(FanOutDuplicatesTest, CopyFailureRecordsCopyError) {
    std::vector<tl::expected<int64_t, ErrorCode>> results(2);
    results[0] = 4;

    FanOutDuplicates({DuplicateFanOutJob{.dup_result_index = 1,
                                         .primary_result_index = 0,
                                         .key = "k",
                                         .dup_bytes = 4,
                                         .primary_bytes = 4,
                                         .copy =
                                             []() {
                                                 return tl::make_unexpected(
                                                     ErrorCode::TRANSFER_FAIL);
                                             }}},
                     results);

    ASSERT_FALSE(results[1].has_value());
    EXPECT_EQ(results[1].error(), ErrorCode::TRANSFER_FAIL);
}

}  // namespace
}  // namespace mooncake
