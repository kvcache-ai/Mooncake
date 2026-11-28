#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>
#include <utility>

#include "config/offload_read_chunk_config.h"

namespace mooncake {
namespace {

constexpr const char* kChunkBytes = "MOONCAKE_OFFLOAD_READ_CHUNK_BYTES";
constexpr const char* kConcurrency = "MOONCAKE_OFFLOAD_READ_CHUNK_CONCURRENCY";

class OffloadReadChunkConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OffloadReadChunkConfigTest");
        FLAGS_logtostderr = 1;
        for (auto& [name, original] : originals_) {
            if (const char* value = std::getenv(name)) original = value;
            ASSERT_EQ(unsetenv(name), 0);
        }
    }

    void TearDown() override {
        for (const auto& [name, original] : originals_) {
            if (original) {
                EXPECT_EQ(setenv(name, original->c_str(), 1), 0);
            } else {
                EXPECT_EQ(unsetenv(name), 0);
            }
        }
        google::ShutdownGoogleLogging();
    }

   private:
    std::pair<const char*, std::optional<std::string>> originals_[2] = {
        {kChunkBytes, std::nullopt}, {kConcurrency, std::nullopt}};
};

// Chunking is off by default: each owner's keys go in one RPC, as before.
TEST_F(OffloadReadChunkConfigTest, DefaultsSilentlyForUnsetAndEmpty) {
    testing::internal::CaptureStderr();
    auto config = OffloadReadChunkConfig::FromEnvironment();
    EXPECT_EQ(config.chunk_bytes, 0u);
    EXPECT_EQ(config.chunk_concurrency, 4u);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());

    ASSERT_EQ(setenv(kChunkBytes, "", 1), 0);
    ASSERT_EQ(setenv(kConcurrency, "", 1), 0);
    testing::internal::CaptureStderr();
    config = OffloadReadChunkConfig::FromEnvironment();
    EXPECT_EQ(config.chunk_bytes, 0u);
    EXPECT_EQ(config.chunk_concurrency, 4u);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
}

TEST_F(OffloadReadChunkConfigTest, ParsesValues) {
    ASSERT_EQ(setenv(kChunkBytes, "67108864", 1), 0);
    ASSERT_EQ(setenv(kConcurrency, " 8 ", 1), 0);
    testing::internal::CaptureStderr();
    auto config = OffloadReadChunkConfig::FromEnvironment();
    EXPECT_EQ(config.chunk_bytes, 64ULL << 20);
    EXPECT_EQ(config.chunk_concurrency, 8u);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());

    // 0 keeps chunking off; one chunk in flight reads chunks serially; 64 is
    // the largest concurrency taken as is.
    ASSERT_EQ(setenv(kChunkBytes, "0", 1), 0);
    ASSERT_EQ(setenv(kConcurrency, "1", 1), 0);
    config = OffloadReadChunkConfig::FromEnvironment();
    EXPECT_EQ(config.chunk_bytes, 0u);
    EXPECT_EQ(config.chunk_concurrency, 1u);
    ASSERT_EQ(setenv(kConcurrency, "64", 1), 0);
    testing::internal::CaptureStderr();
    EXPECT_EQ(OffloadReadChunkConfig::FromEnvironment().chunk_concurrency, 64u);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
}

TEST_F(OffloadReadChunkConfigTest, ConcurrencyOutOfRangeWarnsAndClamps) {
    for (const auto& [value, expected] :
         {std::pair<const char*, size_t>{"0", 1},
          std::pair<const char*, size_t>{"65", 64},
          std::pair<const char*, size_t>{"100000", 64}}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv(kConcurrency, value, 1), 0);
        testing::internal::CaptureStderr();
        EXPECT_EQ(OffloadReadChunkConfig::FromEnvironment().chunk_concurrency,
                  expected);
        EXPECT_NE(
            testing::internal::GetCapturedStderr().find(
                std::string("MOONCAKE_OFFLOAD_READ_CHUNK_CONCURRENCY=") +
                value + " is outside 1..64, using " + std::to_string(expected)),
            std::string::npos);
    }
}

TEST_F(OffloadReadChunkConfigTest, InvalidValuesWarnAndFallBack) {
    for (const char* value : {"-1", "abc", "64MiB", "18446744073709551616"}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv(kChunkBytes, value, 1), 0);
        testing::internal::CaptureStderr();
        EXPECT_EQ(OffloadReadChunkConfig::FromEnvironment().chunk_bytes, 0u);
        EXPECT_NE(testing::internal::GetCapturedStderr().find(
                      std::string("Invalid value for "
                                  "MOONCAKE_OFFLOAD_READ_CHUNK_BYTES: ") +
                      value),
                  std::string::npos);
    }
    for (const char* value : {"-2", "x", "4 threads"}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv(kConcurrency, value, 1), 0);
        testing::internal::CaptureStderr();
        EXPECT_EQ(OffloadReadChunkConfig::FromEnvironment().chunk_concurrency,
                  4u);
        EXPECT_NE(testing::internal::GetCapturedStderr().find(
                      std::string("Invalid value for "
                                  "MOONCAKE_OFFLOAD_READ_CHUNK_CONCURRENCY: ") +
                      value),
                  std::string::npos);
    }
}

}  // namespace
}  // namespace mooncake
