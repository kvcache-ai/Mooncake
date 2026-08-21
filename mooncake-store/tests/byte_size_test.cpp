#include "byte_size.h"

#include <gtest/gtest.h>

using namespace mooncake;

TEST(UtilsTest, ByteSizeToString) {
    EXPECT_EQ(byte_size_to_string(999), "999 B");
    EXPECT_EQ(byte_size_to_string(2048), "2.00 KB");
    EXPECT_EQ(byte_size_to_string(5ULL * 1024 * 1024 + 1234), "5.00 MB");
    EXPECT_EQ(byte_size_to_string(15ULL * 1024 * 1024 * 1024), "15.00 GB");
    EXPECT_EQ(byte_size_to_string(0), "0 B");
    EXPECT_EQ(byte_size_to_string(1), "1 B");
    EXPECT_EQ(byte_size_to_string(1024), "1.00 KB");
    EXPECT_EQ(byte_size_to_string(1024 * 1024), "1.00 MB");
    EXPECT_EQ(byte_size_to_string(1024ULL * 1024 * 1024), "1.00 GB");
    EXPECT_EQ(byte_size_to_string(1024ULL * 1024 * 1024 * 1024), "1.00 TB");
    EXPECT_EQ(byte_size_to_string(15 * 1024 + 134), "15.13 KB");
    EXPECT_EQ(byte_size_to_string(15 * 1024 * 1024 + 44048), "15.04 MB");
}

TEST(UtilsTest, StringToByteSize) {
    auto parsed = try_string_to_byte_size("16 MB");
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed.value(), 16ULL * 1024 * 1024);

    parsed = try_string_to_byte_size("0");
    ASSERT_TRUE(parsed.has_value());
    EXPECT_EQ(parsed.value(), 0);

    EXPECT_FALSE(try_string_to_byte_size("-5").has_value());
    EXPECT_FALSE(try_string_to_byte_size("16XB").has_value());
    EXPECT_EQ(string_to_byte_size("-5"), 0);
}
