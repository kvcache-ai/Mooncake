#include "common/byte_size.h"

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

TEST(UtilsTest, BinaryByteSizeSuffixes) {
    struct Case {
        const char* input;
        uint64_t expected;
    };
    const Case cases[] = {
        {"1KiB", 1024},
        {"1mib", 1048576},
        {"1GIB", 1073741824},
        {"1tIb", 1099511627776},
        {"256MiB", 268435456},
        {"1.5 KiB", 1536},
        {"1.5 MiB", 1572864},
        {"1.5 GiB", 1610612736},
        {"1.5 TiB", 1649267441664},
        {" \t2\tMiB\r\n", 2097152},
        {"0KiB", 0},
        {"0MiB", 0},
        {"0GiB", 0},
        {"0TiB", 0},
        {"1K", 1024},
        {"1KB", 1024},
        {"1M", 1048576},
        {"1MB", 1048576},
        {"1G", 1073741824},
        {"1GB", 1073741824},
        {"1T", 1099511627776},
        {"1TB", 1099511627776},
    };
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.input);
        const auto parsed = try_string_to_byte_size(entry.input);
        ASSERT_TRUE(parsed.has_value());
        EXPECT_EQ(*parsed, entry.expected);
        EXPECT_EQ(string_to_byte_size(entry.input), entry.expected);
    }
}

TEST(UtilsTest, RejectsInvalidBinaryByteSizeSuffixes) {
    for (const char* input : {"KiB", "MiB", "GiB", "TiB", "1Ki", "1MiBB",
                              "256XiB", "1PiB", "1MiBjunk", "1Mi B", "-1MiB"}) {
        SCOPED_TRACE(input);
        EXPECT_FALSE(try_string_to_byte_size(input).has_value());
    }
}
