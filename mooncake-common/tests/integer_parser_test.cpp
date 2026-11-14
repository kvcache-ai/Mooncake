#include "integer_parser.h"

#include <cstdint>
#include <limits>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(IntegerParserTest, ParsesStrictlyWithRangeChecks) {
    EXPECT_EQ(TryParseInteger<int>("-42"), -42);
    EXPECT_EQ(TryParseInteger<uint64_t>("18446744073709551615"),
              std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(TryParseInteger<uint64_t>("18446744073709551616"), std::nullopt);
    EXPECT_EQ(TryParseInteger<uint32_t>("4294967296"), std::nullopt);
    EXPECT_EQ(TryParseInteger<int>("12suffix"), std::nullopt);
    EXPECT_EQ(TryParseInteger<int>(" 12 "), std::nullopt);
    EXPECT_EQ(TryParseInteger<int>("+12"), std::nullopt);
    EXPECT_EQ(TryParseInteger<int>("+-12", {.allow_leading_plus = true}),
              std::nullopt);
    EXPECT_EQ(TryParseInteger<int>(" +12 ", {.trim_ascii_whitespace = true,
                                             .allow_leading_plus = true}),
              12);
}

}  // namespace
}  // namespace mooncake
