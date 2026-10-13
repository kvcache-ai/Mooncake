#include "../src/config/s3_client_config.h"

#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "environ.h"

namespace mooncake {
namespace {

class S3ClientConfigTest : public ::testing::Test {
   protected:
    S3ClientConfig Load() const {
        return S3ClientConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(S3ClientConfigTest, UsesExistingDefaults) {
    const auto config = Load();
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
    source_.Set("MOONCAKE_AWS_REGION", "us-east-1");
    source_.Set("MOONCAKE_AWS_S3_ENDPOINT", "https://s3.example.com");
    source_.Set("MOONCAKE_AWS_BUCKET_NAME", "bucket");
    source_.Set("MOONCAKE_AWS_ACCESS_KEY_ID", "access");
    source_.Set("MOONCAKE_AWS_SECRET_ACCESS_KEY", "secret");
    source_.Set("MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING", "0");
    source_.Set("MOONCAKE_AWS_USE_HTTPS", "false");
    source_.Set("MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION", "when_required");
    source_.Set("MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION", "when_supported");
    source_.Set("MOONCAKE_AWS_CONNECT_TIMEOUT_MS", "5000");
    source_.Set("MOONCAKE_AWS_REQUEST_TIMEOUT_MS", "6000");

    const auto config = Load();
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
    source_.Set("MOONCAKE_AWS_REGION", "");
    source_.Set("MOONCAKE_AWS_S3_ENDPOINT", "");
    source_.Set("MOONCAKE_AWS_BUCKET_NAME", "");
    source_.Set("MOONCAKE_AWS_ACCESS_KEY_ID", "");
    source_.Set("MOONCAKE_AWS_SECRET_ACCESS_KEY", "");
    source_.Set("MOONCAKE_AWS_REQUEST_CHECKSUM_CALCULATION", "");
    source_.Set("MOONCAKE_AWS_RESPONSE_CHECKSUM_VALIDATION", "");

    const auto config = Load();
    EXPECT_TRUE(config.region.empty());
    EXPECT_TRUE(config.s3_endpoint.empty());
    EXPECT_TRUE(config.bucket_name.empty());
    EXPECT_TRUE(config.access_key_id.empty());
    EXPECT_TRUE(config.secret_access_key.empty());
    EXPECT_TRUE(config.request_checksum_calculation.empty());
    EXPECT_TRUE(config.response_checksum_validation.empty());
}

TEST_F(S3ClientConfigTest, InvalidTypedValuesUseExistingDefaultsAndWarn) {
    source_.Set("MOONCAKE_AWS_USE_VIRTUAL_ADDRESSING", "invalid");
    source_.Set("MOONCAKE_AWS_USE_HTTPS", "invalid");
    source_.Set("MOONCAKE_AWS_CONNECT_TIMEOUT_MS", "invalid");
    source_.Set("MOONCAKE_AWS_REQUEST_TIMEOUT_MS", "invalid");

    testing::internal::CaptureStderr();
    const auto config = Load();
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
    source_.Set("MOONCAKE_AWS_REGION", "first");
    const auto first = Load();
    source_.Set("MOONCAKE_AWS_REGION", "second");
    const auto second = Load();

    EXPECT_EQ(first.region, "first");
    EXPECT_EQ(second.region, "second");
}

}  // namespace
}  // namespace mooncake
