#include "environment_value_parser.h"

#include <gtest/gtest.h>

#include <cerrno>
#include <cmath>
#include <cstdint>
#include <string>

namespace mooncake::test {

TEST(EnvironmentValueParserTest, ParsesSupportedTypes) {
    EXPECT_EQ(TryParseEnvironmentValue<int64_t>(" \t+42\r\n"), 42);
    EXPECT_EQ(TryParseEnvironmentValue<bool>("ON"), true);
    EXPECT_EQ(TryParseEnvironmentValue<std::string>("value"), "value");

    const auto ratio = TryParseEnvironmentValue<double>("0.75");
    ASSERT_TRUE(ratio.has_value());
    EXPECT_DOUBLE_EQ(*ratio, 0.75);
}

TEST(EnvironmentValueParserTest, RejectsInvalidTypedValues) {
    EXPECT_FALSE(TryParseEnvironmentValue<int64_t>("").has_value());
    EXPECT_FALSE(
        TryParseEnvironmentValue<int64_t>("not-an-integer").has_value());
    EXPECT_FALSE(TryParseEnvironmentValue<bool>("").has_value());
    EXPECT_FALSE(TryParseEnvironmentValue<bool>("unknown").has_value());
    EXPECT_FALSE(TryParseEnvironmentValue<double>("").has_value());
    EXPECT_FALSE(TryParseEnvironmentValue<double>("not-a-ratio").has_value());
}

TEST(EnvironmentValueParserTest, RequiresFiniteCompleteDoubleValues) {
    EXPECT_FALSE(TryParseEnvironmentValue<double>("0.75suffix").has_value());
    EXPECT_FALSE(TryParseEnvironmentValue<double>("nan").has_value());

    const auto ratio = TryParseEnvironmentValue<double>(" 0.75 ");
    ASSERT_TRUE(ratio.has_value());
    EXPECT_DOUBLE_EQ(*ratio, 0.75);
}

TEST(EnvironmentValueParserTest, SupportsExplicitLenientDoubleParsing) {
    const EnvironmentDoubleParseOptions options{
        .allow_trailing_characters = true,
        .allow_non_finite = true,
    };

    const auto suffixed = TryParseEnvironmentDouble("0.75suffix", options);
    ASSERT_TRUE(suffixed.has_value());
    EXPECT_DOUBLE_EQ(*suffixed, 0.75);

    const auto nan = TryParseEnvironmentDouble("nan", options);
    ASSERT_TRUE(nan.has_value());
    EXPECT_TRUE(std::isnan(*nan));

    EXPECT_FALSE(TryParseEnvironmentDouble(" invalid", options).has_value());
}

TEST(EnvironmentValueParserTest, PreservesEmptyStrings) {
    const auto empty = TryParseEnvironmentValue<std::string>("");
    ASSERT_TRUE(empty.has_value());
    EXPECT_TRUE(empty->empty());
}

TEST(EnvironmentValueParserTest, PreservesErrno) {
    for (const char* value : {"0.75", "not-a-ratio", "1e9999"}) {
        errno = EDOM;
        static_cast<void>(TryParseEnvironmentValue<double>(value));
        EXPECT_EQ(errno, EDOM);
    }
}

}  // namespace mooncake::test
