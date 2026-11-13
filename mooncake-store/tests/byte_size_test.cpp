#include "common/byte_size.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <utility>

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

TEST(UtilsTest, ParsesIecByteSizeSuffixes) {
    const std::pair<const char*, uint64_t> cases[] = {
        {"1KiB", 1024ULL},
        {"512MiB", 512ULL * 1024 * 1024},
        {"8GiB", 8ULL * 1024 * 1024 * 1024},
        {"2TiB", 2ULL * 1024 * 1024 * 1024 * 1024},
        {"1.5 GiB", 1536ULL * 1024 * 1024},
        {"8mIb", 8ULL * 1024 * 1024},
        {"0KiB", 0},
        {"\t16 MiB \n", 16ULL * 1024 * 1024},
    };
    for (const auto& [input, expected] : cases) {
        SCOPED_TRACE(input);
        auto parsed = try_string_to_byte_size(input);
        ASSERT_TRUE(parsed.has_value());
        EXPECT_EQ(parsed.value(), expected);
        EXPECT_EQ(string_to_byte_size(input), expected);
    }
}

TEST(UtilsTest, IecUnitsRetainLegacyBinaryMultipliers) {
    const std::pair<const char*, const char*> units[] = {
        {"KiB", "KB"}, {"MiB", "MB"}, {"GiB", "GB"}, {"TiB", "TB"}};
    for (const auto& [iec, legacy] : units) {
        for (const char* number : {"0", "1", "1.5", "256"}) {
            const auto input = std::string(number) + iec;
            SCOPED_TRACE(input);
            auto parsed = try_string_to_byte_size(input);
            ASSERT_TRUE(parsed.has_value());
            EXPECT_EQ(parsed,
                      try_string_to_byte_size(std::string(number) + legacy));
        }
    }
}

TEST(UtilsTest, RejectsOutOfRangeByteSizes) {
    for (const char* input :
         {"18446744073709551616", "18446744073709551616B",
          "18014398509481984KB", "18014398509481984KiB", "17592186044416MB",
          "17592186044416MiB", "17179869184GB", "17179869184GiB", "16777216TB",
          "16777216TiB", "1e300TiB"}) {
        SCOPED_TRACE(input);
        EXPECT_FALSE(try_string_to_byte_size(input).has_value());
    }
    auto largest_exact_double =
        try_string_to_byte_size("18446744073709549568B");
    ASSERT_TRUE(largest_exact_double.has_value());
    EXPECT_EQ(largest_exact_double.value(), 18446744073709549568ULL);
}

TEST(UtilsTest, RejectsMalformedIecSizes) {
    for (const char* input : {"MiB", "1Ki", "1MiBB", "1GiBS", "1PiB", "-2MiB",
                              "nanGiB", "infMiB"}) {
        SCOPED_TRACE(input);
        EXPECT_FALSE(try_string_to_byte_size(input).has_value());
    }
}
