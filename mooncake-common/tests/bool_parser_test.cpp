#include "bool_parser.h"

#include <string_view>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(BoolParserTest, ParsesCanonicalGrammar) {
    for (std::string_view value : {"1", "true", "YES", "On", "enable"}) {
        ASSERT_EQ(TryParseBool(value), true) << value;
    }
    for (std::string_view value : {"0", "false", "NO", "Off", "disable"}) {
        ASSERT_EQ(TryParseBool(value), false) << value;
    }
    EXPECT_EQ(TryParseBool(" \tTrUe\r\n"), true);
    EXPECT_EQ(TryParseBool(""), std::nullopt);
    EXPECT_EQ(TryParseBool("maybe"), std::nullopt);
}

TEST(BoolParserTest, SupportsRestrictedTokenSets) {
    const BoolParseOptions true_false_only{
        .token_set = BoolTokenSet::kTrueFalse, .trim_ascii_whitespace = false};
    EXPECT_EQ(TryParseBool("1", true_false_only), true);
    EXPECT_EQ(TryParseBool("0", true_false_only), false);
    EXPECT_EQ(TryParseBool("TRUE", true_false_only), true);
    EXPECT_EQ(TryParseBool("yes", true_false_only), std::nullopt);
    EXPECT_EQ(TryParseBool(" true ", true_false_only), std::nullopt);
}

}  // namespace
}  // namespace mooncake
