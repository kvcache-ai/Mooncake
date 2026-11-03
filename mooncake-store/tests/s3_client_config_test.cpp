#include "../src/config/s3_client_config.h"

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstdlib>
#include <mutex>
#include <optional>
#include <string>

namespace mooncake {
namespace {

std::mutex environment_mutex;

class S3ClientConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        environment_lock_ = std::unique_lock<std::mutex>(environment_mutex);
        for (size_t i = 0; i < kVariables.size(); ++i) {
            if (const char* value = std::getenv(kVariables[i])) {
                original_[i] = value;
            }
            ASSERT_EQ(unsetenv(kVariables[i]), 0);
        }
    }

    void TearDown() override {
        for (size_t i = 0; i < kVariables.size(); ++i) {
            if (original_[i]) {
                EXPECT_EQ(setenv(kVariables[i], original_[i]->c_str(), 1), 0);
            } else {
                EXPECT_EQ(unsetenv(kVariables[i]), 0);
            }
        }
    }

    void Set(const char* name, const char* value) {
        ASSERT_EQ(setenv(name, value, 1), 0);
    }

   private:
    inline static constexpr std::array<const char*, 11> kVariables = {
        "MOONCAKE_AWS_REGION",
        "MOONCAKE_AWS_S3_ENDPOINT",
        "MOONCAKE_AWS_BUCKET_NAME",
        "MOONCAKE_AWS_ACCESS_KEY_ID",
        "MOONCAKE_AWS_SECRET_ACCESS_KEY",
        "MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING",
        "MOONCAKE_AWS_USE_HTTPS",
        "MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION",
        "MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION",
        "MOONCAKE_AWS_CONNECT_TIMEOUT_MS",
        "MOONCAKE_AWS_REQUEST_TIMEOUT_MS"};
    std::array<std::optional<std::string>, kVariables.size()> original_;
    std::unique_lock<std::mutex> environment_lock_;
};

TEST_F(S3ClientConfigTest, UsesExistingDefaults) {
    const auto config = S3ClientConfig::FromEnvironment();
    EXPECT_TRUE(config.region.empty());
    EXPECT_TRUE(config.s3_endpoint.empty());
    EXPECT_TRUE(config.bucket_name.empty());
    EXPECT_TRUE(config.access_key_id.empty());
    EXPECT_TRUE(config.secret_access_key.empty());
    EXPECT_TRUE(config.use_virtual_addressing);
    EXPECT_TRUE(config.use_https);
    EXPECT_TRUE(config.request_checksum_calculation.empty());
    EXPECT_TRUE(config.response_checksum_validation.empty());
    EXPECT_EQ(config.connect_timeout, std::chrono::milliseconds(10000));
    EXPECT_EQ(config.request_timeout, std::chrono::milliseconds(30000));
}

TEST_F(S3ClientConfigTest, ReadsAllExistingSettings) {
    Set("MOONCAKE_AWS_REGION", "us-east-1");
    Set("MOONCAKE_AWS_S3_ENDPOINT", "https://s3.example.com");
    Set("MOONCAKE_AWS_BUCKET_NAME", "bucket");
    Set("MOONCAKE_AWS_ACCESS_KEY_ID", "access");
    Set("MOONCAKE_AWS_SECRET_ACCESS_KEY", "secret");
    Set("MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING", "0");
    Set("MOONCAKE_AWS_USE_HTTPS", "false");
    Set("MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION", "when_required");
    Set("MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION", "when_supported");
    Set("MOONCAKE_AWS_CONNECT_TIMEOUT_MS", "5000");
    Set("MOONCAKE_AWS_REQUEST_TIMEOUT_MS", "6000");

    const auto config = S3ClientConfig::FromEnvironment();
    EXPECT_EQ(config.region, "us-east-1");
    EXPECT_EQ(config.s3_endpoint, "https://s3.example.com");
    EXPECT_EQ(config.bucket_name, "bucket");
    EXPECT_EQ(config.access_key_id, "access");
    EXPECT_EQ(config.secret_access_key, "secret");
    EXPECT_FALSE(config.use_virtual_addressing);
    EXPECT_FALSE(config.use_https);
    EXPECT_EQ(config.request_checksum_calculation, "when_required");
    EXPECT_EQ(config.response_checksum_validation, "when_supported");
    EXPECT_EQ(config.connect_timeout, std::chrono::milliseconds(5000));
    EXPECT_EQ(config.request_timeout, std::chrono::milliseconds(6000));
}

TEST_F(S3ClientConfigTest, PreservesExplicitlyEmptyStrings) {
    Set("MOONCAKE_AWS_REGION", "");
    Set("MOONCAKE_AWS_S3_ENDPOINT", "");
    Set("MOONCAKE_AWS_BUCKET_NAME", "");
    Set("MOONCAKE_AWS_ACCESS_KEY_ID", "");
    Set("MOONCAKE_AWS_SECRET_ACCESS_KEY", "");
    Set("MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION", "");
    Set("MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION", "");

    const auto config = S3ClientConfig::FromEnvironment();
    EXPECT_TRUE(config.region.empty());
    EXPECT_TRUE(config.s3_endpoint.empty());
    EXPECT_TRUE(config.bucket_name.empty());
    EXPECT_TRUE(config.access_key_id.empty());
    EXPECT_TRUE(config.secret_access_key.empty());
    EXPECT_TRUE(config.request_checksum_calculation.empty());
    EXPECT_TRUE(config.response_checksum_validation.empty());
}

TEST_F(S3ClientConfigTest, InvalidTypedValuesUseExistingDefaultsAndWarn) {
    Set("MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING", "invalid");
    Set("MOONCAKE_AWS_USE_HTTPS", "invalid");
    Set("MOONCAKE_AWS_CONNECT_TIMEOUT_MS", "invalid");
    Set("MOONCAKE_AWS_REQUEST_TIMEOUT_MS", "invalid");

    testing::internal::CaptureStderr();
    const auto config = S3ClientConfig::FromEnvironment();
    const std::string diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.use_virtual_addressing);
    EXPECT_TRUE(config.use_https);
    EXPECT_EQ(config.connect_timeout, std::chrono::milliseconds(10000));
    EXPECT_EQ(config.request_timeout, std::chrono::milliseconds(30000));
    EXPECT_NE(diagnostics.find("MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING"),
              std::string::npos);
    EXPECT_NE(diagnostics.find("MOONCAKE_AWS_USE_HTTPS"), std::string::npos);
    EXPECT_NE(diagnostics.find("MOONCAKE_AWS_CONNECT_TIMEOUT_MS"),
              std::string::npos);
    EXPECT_NE(diagnostics.find("MOONCAKE_AWS_REQUEST_TIMEOUT_MS"),
              std::string::npos);
}

TEST_F(S3ClientConfigTest, NewConfigsReadCurrentEnvironment) {
    Set("MOONCAKE_AWS_REGION", "first");
    const auto first = S3ClientConfig::FromEnvironment();
    Set("MOONCAKE_AWS_REGION", "second");
    const auto second = S3ClientConfig::FromEnvironment();

    EXPECT_EQ(first.region, "first");
    EXPECT_EQ(second.region, "second");
}

}  // namespace
}  // namespace mooncake
