#include "ascii_string.h"

#include <string>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TEST(AsciiStringTest, TrimsOnlyAsciiWhitespace) {
    EXPECT_EQ(TrimAsciiWhitespace(" \t\n\r\f\vvalue \t\n\r\f\v"), "value");
    EXPECT_TRUE(TrimAsciiWhitespace(" \t\n\r\f\v").empty());

    const std::string non_ascii =
        std::string("\xC2\xA0") + "value" + std::string("\xC2\xA0");
    EXPECT_EQ(TrimAsciiWhitespace(non_ascii), non_ascii);
}

TEST(AsciiStringTest, NormalizesAndComparesWithoutLocale) {
    EXPECT_EQ(AsciiToLower("AbC-123"), "abc-123");
    EXPECT_TRUE(AsciiCaseInsensitiveEquals("EnAbLe", "enable"));
    EXPECT_FALSE(AsciiCaseInsensitiveEquals("enabled", "enable"));
}

}  // namespace
}  // namespace mooncake
