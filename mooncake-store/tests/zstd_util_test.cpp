#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "utils/zstd_util.h"

namespace mooncake::test {

class ZstdUtilTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ZstdUtilTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

// ========== Compress / Decompress Roundtrip ==========

TEST_F(ZstdUtilTest, Roundtrip_String) {
    std::string input = "Mooncake snapshot roundtrip test data!";
    auto compressed = zstd_compress(input);
    EXPECT_FALSE(compressed.empty());
    auto decompressed = zstd_decompress_to_string(compressed);
    EXPECT_EQ(decompressed, input);
}

TEST_F(ZstdUtilTest, Roundtrip_Vector) {
    std::vector<uint8_t> input = {0, 1, 2, 128, 254, 255, 0, 127};
    auto compressed = zstd_compress(input);
    EXPECT_FALSE(compressed.empty());
    auto decompressed = zstd_decompress(compressed);
    EXPECT_EQ(decompressed, input);
}

TEST_F(ZstdUtilTest, Roundtrip_RawPointer) {
    std::vector<uint8_t> input = {10, 20, 30, 40, 50};
    auto compressed = zstd_compress(input.data(), input.size());
    EXPECT_FALSE(compressed.empty());
    auto decompressed = zstd_decompress(compressed.data(), compressed.size());
    EXPECT_EQ(decompressed, input);
}

TEST_F(ZstdUtilTest, Roundtrip_WithExplicitSize) {
    std::string input = "Explicit size decompression test.";
    auto compressed = zstd_compress(input);
    auto decompressed = zstd_decompress(compressed, input.size());
    std::string decompressed_str(decompressed.begin(), decompressed.end());
    EXPECT_EQ(decompressed_str, input);
}

TEST_F(ZstdUtilTest, Roundtrip_WithMaxSize) {
    std::string input = "Max size decompression test.";
    auto compressed = zstd_compress(input);
    auto decompressed =
        zstd_decompress(compressed.data(), compressed.size(), input.size() * 2);
    std::string decompressed_str(decompressed.begin(), decompressed.end());
    EXPECT_EQ(decompressed_str, input);
}

// ========== Error Handling ==========

TEST_F(ZstdUtilTest, Decompress_InvalidData_Throws) {
    std::vector<uint8_t> invalid = {0xDE, 0xAD, 0xBE, 0xEF};
    EXPECT_THROW(zstd_decompress(invalid), std::runtime_error);
    EXPECT_THROW(zstd_decompress_to_string(invalid), std::runtime_error);
}

TEST_F(ZstdUtilTest, DecompressWithMaxSize_ExceedsLimit_Throws) {
    std::string input = "This data exceeds the max size limit.";
    auto compressed = zstd_compress(input);
    EXPECT_THROW(
        zstd_decompress(compressed.data(), compressed.size(), /*max=*/5),
        std::runtime_error);
}

TEST_F(ZstdUtilTest, DecompressWithMaxSize_NullInput_Throws) {
    EXPECT_THROW(zstd_decompress(nullptr, 0, 1024), std::runtime_error);
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
