#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <map>
#include <mutex>
#include <optional>
#include <string>

#include "../src/config/oss_adapter_config.h"
#include "storage/distributed/oss_adapter.h"

namespace mooncake {
namespace {

std::mutex environment_mutex;

class OssAdapterConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        environment_lock_ = std::unique_lock<std::mutex>(environment_mutex);
        for (const char* name : kVariables) {
            if (const char* value = std::getenv(name)) {
                original_values_[name] = value;
            } else {
                original_values_[name] = std::nullopt;
            }
            ASSERT_EQ(unsetenv(name), 0) << name;
        }
    }

    void TearDown() override {
        for (const auto& [name, value] : original_values_) {
            if (value) {
                EXPECT_EQ(setenv(name.c_str(), value->c_str(), 1), 0) << name;
            } else {
                EXPECT_EQ(unsetenv(name.c_str()), 0) << name;
            }
        }
    }

    void Set(const char* name, const std::string& value) {
        ASSERT_EQ(setenv(name, value.c_str(), 1), 0) << name;
    }

    void SetRequiredPrimary() {
        Set("MOONCAKE_OSS_ENDPOINT", "https://oss.example.com");
        Set("MOONCAKE_OSS_BUCKET", "bucket");
        Set("MOONCAKE_OSS_REGION", "region");
    }

   private:
    inline static constexpr std::array<const char*, 17> kVariables = {
        "MOONCAKE_OSS_ENDPOINT",
        "OSS_ENDPOINT",
        "MOONCAKE_OSS_BUCKET",
        "OSS_BUCKET",
        "MOONCAKE_OSS_REGION",
        "OSS_REGION",
        "MOONCAKE_OSS_ACCESS_KEY_ID",
        "OSS_ACCESS_KEY_ID",
        "MOONCAKE_OSS_ACCESS_KEY_SECRET",
        "OSS_ACCESS_KEY_SECRET",
        "MOONCAKE_OSS_SECURITY_TOKEN",
        "OSS_SESSION_TOKEN",
        "MOONCAKE_OSS_PATH_STYLE",
        "MOONCAKE_OSS_ANONYMOUS",
        "MOONCAKE_OSS_MAX_CONNECTIONS",
        "MOONCAKE_OSS_RECEIVE_BUFFER_SIZE",
        "MOONCAKE_OSS_UPLOAD_BUFFER_SIZE",
    };

    std::map<std::string, std::optional<std::string>> original_values_;
    std::unique_lock<std::mutex> environment_lock_;
};

TEST_F(OssAdapterConfigTest, InitUsesCompatibilityAliases) {
    Set("OSS_ENDPOINT", "https://oss.example.com");
    Set("OSS_BUCKET", "bucket");
    Set("OSS_REGION", "region");
    Set("MOONCAKE_OSS_ANONYMOUS", "true");

    OssObjectStorageAdapter adapter("prefix");
    EXPECT_TRUE(adapter.Init());
}

TEST_F(OssAdapterConfigTest, EmptyPrimaryOverridesCompatibilityAlias) {
    Set("OSS_ENDPOINT", "https://oss.example.com");
    Set("MOONCAKE_OSS_ENDPOINT", "");
    Set("MOONCAKE_OSS_BUCKET", "bucket");
    Set("MOONCAKE_OSS_REGION", "region");
    Set("MOONCAKE_OSS_ANONYMOUS", "true");

    OssObjectStorageAdapter adapter("prefix");
    const auto result = adapter.Init();

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(OssAdapterConfigTest, CredentialsAreRequiredUnlessAnonymous) {
    SetRequiredPrimary();

    OssObjectStorageAdapter authenticated("prefix");
    const auto missing_credentials = authenticated.Init();
    ASSERT_FALSE(missing_credentials);
    EXPECT_EQ(missing_credentials.error(), ErrorCode::INVALID_PARAMS);

    Set("MOONCAKE_OSS_ANONYMOUS", "true");
    OssObjectStorageAdapter anonymous("prefix");
    EXPECT_TRUE(anonymous.Init());
}

TEST_F(OssAdapterConfigTest, InvalidAnonymousValueWarnsAndUsesFalse) {
    SetRequiredPrimary();
    Set("MOONCAKE_OSS_ANONYMOUS", "invalid");

    testing::internal::CaptureStderr();
    OssObjectStorageAdapter adapter("prefix");
    const auto result = adapter.Init();
    const std::string diagnostics = testing::internal::GetCapturedStderr();

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_NE(diagnostics.find("invalid value 'invalid' for env "
                               "MOONCAKE_OSS_ANONYMOUS, using default 0"),
              std::string::npos);
}

TEST_F(OssAdapterConfigTest, InitReadsCurrentEnvironmentEachTime) {
    SetRequiredPrimary();
    Set("MOONCAKE_OSS_ANONYMOUS", "true");

    OssObjectStorageAdapter adapter("prefix");
    EXPECT_TRUE(adapter.Init());

    Set("MOONCAKE_OSS_ANONYMOUS", "false");
    const auto second = adapter.Init();
    ASSERT_FALSE(second);
    EXPECT_EQ(second.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(OssAdapterConfigTest, LoadsPrimaryValuesAndNormalizesStrings) {
    Set("MOONCAKE_OSS_ENDPOINT", "https://oss.example.com///");
    Set("MOONCAKE_OSS_BUCKET", "primary-bucket");
    Set("MOONCAKE_OSS_REGION", "primary-region");
    Set("MOONCAKE_OSS_ACCESS_KEY_ID", "primary-id");
    Set("MOONCAKE_OSS_ACCESS_KEY_SECRET", "primary-secret");
    Set("MOONCAKE_OSS_SECURITY_TOKEN", "  primary-token\t");
    Set("MOONCAKE_OSS_PATH_STYLE", "TRUE");
    Set("MOONCAKE_OSS_ANONYMOUS", "On");

    const auto config = OssAdapterConfig::FromEnvironment();

    ASSERT_TRUE(config);
    EXPECT_EQ(config->endpoint, "https://oss.example.com");
    EXPECT_EQ(config->bucket, "primary-bucket");
    EXPECT_EQ(config->region, "primary-region");
    EXPECT_EQ(config->access_key_id, "primary-id");
    EXPECT_EQ(config->access_key_secret, "primary-secret");
    EXPECT_EQ(config->security_token, "primary-token");
    EXPECT_TRUE(config->path_style);
    EXPECT_TRUE(config->anonymous);
}

TEST_F(OssAdapterConfigTest, LoadsCompatibilityAliases) {
    Set("OSS_ENDPOINT", "https://alias.example.com");
    Set("OSS_BUCKET", "alias-bucket");
    Set("OSS_REGION", "alias-region");
    Set("OSS_ACCESS_KEY_ID", "alias-id");
    Set("OSS_ACCESS_KEY_SECRET", "alias-secret");
    Set("OSS_SESSION_TOKEN", "alias-token");

    const auto config = OssAdapterConfig::FromEnvironment();

    ASSERT_TRUE(config);
    EXPECT_EQ(config->endpoint, "https://alias.example.com");
    EXPECT_EQ(config->bucket, "alias-bucket");
    EXPECT_EQ(config->region, "alias-region");
    EXPECT_EQ(config->access_key_id, "alias-id");
    EXPECT_EQ(config->access_key_secret, "alias-secret");
    EXPECT_EQ(config->security_token, "alias-token");
    EXPECT_FALSE(config->path_style);
    EXPECT_FALSE(config->anonymous);
}

TEST_F(OssAdapterConfigTest, PresentPrimaryOverridesCompatibilityAlias) {
    Set("OSS_ENDPOINT", "https://alias.example.com");
    Set("MOONCAKE_OSS_ENDPOINT", "https://primary.example.com");
    Set("OSS_BUCKET", "alias-bucket");
    Set("MOONCAKE_OSS_BUCKET", "primary-bucket");
    Set("OSS_REGION", "alias-region");
    Set("MOONCAKE_OSS_REGION", "primary-region");
    Set("OSS_ACCESS_KEY_ID", "alias-id");
    Set("MOONCAKE_OSS_ACCESS_KEY_ID", "primary-id");
    Set("OSS_ACCESS_KEY_SECRET", "alias-secret");
    Set("MOONCAKE_OSS_ACCESS_KEY_SECRET", "primary-secret");
    Set("OSS_SESSION_TOKEN", "alias-token");
    Set("MOONCAKE_OSS_SECURITY_TOKEN", "");

    const auto config = OssAdapterConfig::FromEnvironment();

    ASSERT_TRUE(config);
    EXPECT_EQ(config->endpoint, "https://primary.example.com");
    EXPECT_EQ(config->bucket, "primary-bucket");
    EXPECT_EQ(config->region, "primary-region");
    EXPECT_EQ(config->access_key_id, "primary-id");
    EXPECT_EQ(config->access_key_secret, "primary-secret");
    EXPECT_TRUE(config->security_token.empty());
}

TEST_F(OssAdapterConfigTest, InvalidBoolValuesWarnAndUseFalse) {
    SetRequiredPrimary();
    Set("MOONCAKE_OSS_ACCESS_KEY_ID", "id");
    Set("MOONCAKE_OSS_ACCESS_KEY_SECRET", "secret");
    Set("MOONCAKE_OSS_PATH_STYLE", "invalid-path-style");
    Set("MOONCAKE_OSS_ANONYMOUS", "invalid-anonymous");

    testing::internal::CaptureStderr();
    const auto config = OssAdapterConfig::FromEnvironment();
    const std::string diagnostics = testing::internal::GetCapturedStderr();

    ASSERT_TRUE(config);
    EXPECT_FALSE(config->path_style);
    EXPECT_FALSE(config->anonymous);
    const size_t path_warning = diagnostics.find(
        "invalid value 'invalid-path-style' for env "
        "MOONCAKE_OSS_PATH_STYLE, using default 0");
    const size_t anonymous_warning = diagnostics.find(
        "invalid value 'invalid-anonymous' for env MOONCAKE_OSS_ANONYMOUS, "
        "using default 0");
    EXPECT_NE(path_warning, std::string::npos);
    EXPECT_NE(anonymous_warning, std::string::npos);
    EXPECT_LT(path_warning, anonymous_warning);
}

TEST_F(OssAdapterConfigTest, RejectsMissingRequiredValuesBeforeCredentials) {
    Set("MOONCAKE_OSS_BUCKET", "bucket");
    Set("MOONCAKE_OSS_REGION", "region");
    Set("MOONCAKE_OSS_ACCESS_KEY_ID", "id");
    Set("MOONCAKE_OSS_ACCESS_KEY_SECRET", "secret");

    const auto config = OssAdapterConfig::FromEnvironment();

    ASSERT_FALSE(config);
    EXPECT_EQ(config.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(OssAdapterConfigTest, AllowsMissingCredentialsOnlyWhenAnonymous) {
    SetRequiredPrimary();

    const auto authenticated = OssAdapterConfig::FromEnvironment();
    ASSERT_FALSE(authenticated);
    EXPECT_EQ(authenticated.error(), ErrorCode::INVALID_PARAMS);

    Set("MOONCAKE_OSS_ANONYMOUS", "yes");
    const auto anonymous = OssAdapterConfig::FromEnvironment();
    ASSERT_TRUE(anonymous);
    EXPECT_TRUE(anonymous->anonymous);
    EXPECT_TRUE(anonymous->access_key_id.empty());
    EXPECT_TRUE(anonymous->access_key_secret.empty());
}

TEST_F(OssAdapterConfigTest, NewConfigsReadCurrentEnvironment) {
    SetRequiredPrimary();
    Set("MOONCAKE_OSS_ANONYMOUS", "true");
    const auto first = OssAdapterConfig::FromEnvironment();

    Set("MOONCAKE_OSS_ENDPOINT", "https://second.example.com");
    const auto second = OssAdapterConfig::FromEnvironment();

    ASSERT_TRUE(first);
    ASSERT_TRUE(second);
    EXPECT_EQ(first->endpoint, "https://oss.example.com");
    EXPECT_EQ(second->endpoint, "https://second.example.com");
}

TEST_F(OssAdapterConfigTest, TuningDefaultsAndOverrides) {
    SetRequiredPrimary();
    Set("MOONCAKE_OSS_ANONYMOUS", "true");
    const auto defaults = OssAdapterConfig::FromEnvironment();
    ASSERT_TRUE(defaults);
    EXPECT_EQ(defaults->max_connections, 64);
    EXPECT_EQ(defaults->receive_buffer_size, 1024 * 1024);
    EXPECT_EQ(defaults->upload_buffer_size, 1024 * 1024);

    Set("MOONCAKE_OSS_MAX_CONNECTIONS", "128");
    Set("MOONCAKE_OSS_RECEIVE_BUFFER_SIZE", "65536");
    Set("MOONCAKE_OSS_UPLOAD_BUFFER_SIZE", "131072");
    const auto configured = OssAdapterConfig::FromEnvironment();
    ASSERT_TRUE(configured);
    EXPECT_EQ(configured->max_connections, 128);
    EXPECT_EQ(configured->receive_buffer_size, 65536);
    EXPECT_EQ(configured->upload_buffer_size, 131072);
}

TEST_F(OssAdapterConfigTest, TuningValuesKeepExistingBounds) {
    SetRequiredPrimary();
    Set("MOONCAKE_OSS_ANONYMOUS", "true");
    for (const char* value : {"-1", "0"}) {
        SCOPED_TRACE(value);
        Set("MOONCAKE_OSS_MAX_CONNECTIONS", value);
        Set("MOONCAKE_OSS_RECEIVE_BUFFER_SIZE", value);
        Set("MOONCAKE_OSS_UPLOAD_BUFFER_SIZE", value);
        const auto config = OssAdapterConfig::FromEnvironment();
        ASSERT_TRUE(config);
        EXPECT_EQ(config->max_connections, 1);
        EXPECT_EQ(config->receive_buffer_size, 16 * 1024);
        EXPECT_EQ(config->upload_buffer_size, 16 * 1024);
    }

    Set("MOONCAKE_OSS_MAX_CONNECTIONS", "2147483647");
    Set("MOONCAKE_OSS_RECEIVE_BUFFER_SIZE", "2147483647");
    Set("MOONCAKE_OSS_UPLOAD_BUFFER_SIZE", "2147483647");
    const auto config = OssAdapterConfig::FromEnvironment();
    ASSERT_TRUE(config);
    EXPECT_EQ(config->max_connections, 2147483647);
    EXPECT_EQ(config->receive_buffer_size, 10 * 1024 * 1024);
    EXPECT_EQ(config->upload_buffer_size, 2 * 1024 * 1024);
}

TEST_F(OssAdapterConfigTest, InvalidTuningValuesWarnAndUseDefaults) {
    SetRequiredPrimary();
    Set("MOONCAKE_OSS_ANONYMOUS", "true");
    for (const char* value :
         {"invalid", "", "65536bytes", "2147483648", "-2147483649"}) {
        SCOPED_TRACE(value);
        Set("MOONCAKE_OSS_MAX_CONNECTIONS", value);
        Set("MOONCAKE_OSS_RECEIVE_BUFFER_SIZE", value);
        Set("MOONCAKE_OSS_UPLOAD_BUFFER_SIZE", value);
        testing::internal::CaptureStderr();
        const auto config = OssAdapterConfig::FromEnvironment();
        const std::string diagnostics = testing::internal::GetCapturedStderr();
        ASSERT_TRUE(config);
        EXPECT_EQ(config->max_connections, 64);
        EXPECT_EQ(config->receive_buffer_size, 1024 * 1024);
        EXPECT_EQ(config->upload_buffer_size, 1024 * 1024);
        EXPECT_NE(
            diagnostics.find("MOONCAKE_OSS_MAX_CONNECTIONS, using default 64"),
            std::string::npos);
        EXPECT_NE(
            diagnostics.find(
                "MOONCAKE_OSS_RECEIVE_BUFFER_SIZE, using default 1048576"),
            std::string::npos);
        EXPECT_NE(diagnostics.find(
                      "MOONCAKE_OSS_UPLOAD_BUFFER_SIZE, using default 1048576"),
                  std::string::npos);
    }
}

}  // namespace
}  // namespace mooncake
