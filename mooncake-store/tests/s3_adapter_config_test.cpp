#include <gtest/gtest.h>

#include <cstdlib>
#include <map>
#include <optional>
#include <string>

#include "../src/config/s3_adapter_config.h"

namespace mooncake {
namespace {

class ScopedEnvironment {
   public:
    ScopedEnvironment() {
        for (const char* name :
             {"MOONCAKE_S3_ENDPOINT", "AWS_ENDPOINT_URL", "MOONCAKE_S3_BUCKET",
              "MOONCAKE_S3_REGION", "AWS_REGION", "MOONCAKE_S3_ACCESS_KEY_ID",
              "AWS_ACCESS_KEY_ID", "MOONCAKE_S3_SECRET_ACCESS_KEY",
              "AWS_SECRET_ACCESS_KEY", "MOONCAKE_S3_SESSION_TOKEN",
              "AWS_SESSION_TOKEN", "AWS_DEFAULT_REGION",
              "MOONCAKE_S3_PATH_STYLE", "MOONCAKE_S3_ANONYMOUS",
              "MOONCAKE_S3_MAX_CONNECTIONS", "MOONCAKE_S3_RECEIVE_BUFFER_SIZE",
              "MOONCAKE_S3_UPLOAD_BUFFER_SIZE"}) {
            Unset(name);
        }
    }

    ~ScopedEnvironment() {
        for (const auto& [name, value] : original_values_) {
            if (value) {
                setenv(name.c_str(), value->c_str(), 1);
            } else {
                unsetenv(name.c_str());
            }
        }
    }

    void Set(const std::string& name, const std::string& value) {
        Remember(name);
        setenv(name.c_str(), value.c_str(), 1);
    }

   private:
    void Unset(const std::string& name) {
        Remember(name);
        unsetenv(name.c_str());
    }

    void Remember(const std::string& name) {
        if (!original_values_.contains(name)) {
            const char* original = std::getenv(name.c_str());
            original_values_[name] =
                original ? std::optional<std::string>(original) : std::nullopt;
        }
    }

    std::map<std::string, std::optional<std::string>> original_values_;
};

void SetMinimal(ScopedEnvironment& env) {
    env.Set("MOONCAKE_S3_ENDPOINT", "http://127.0.0.1:8333/");
    env.Set("MOONCAKE_S3_BUCKET", "kv");
    env.Set("MOONCAKE_S3_ACCESS_KEY_ID", "id");
    env.Set("MOONCAKE_S3_SECRET_ACCESS_KEY", "secret");
}

TEST(S3AdapterConfigTest, AppliesDefaults) {
    ScopedEnvironment env;
    SetMinimal(env);
    auto config = S3AdapterConfig::FromEnvironment();
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->endpoint, "http://127.0.0.1:8333");
    EXPECT_EQ(config->region, "us-east-1");
    // 127.0.0.1 cannot carry a bucket subdomain.
    EXPECT_TRUE(config->path_style);
    EXPECT_FALSE(config->anonymous);
    EXPECT_EQ(config->max_connections, 64);
}

TEST(S3AdapterConfigTest, FallsBackToStandardAwsVariables) {
    ScopedEnvironment env;
    env.Set("AWS_ENDPOINT_URL", "https://s3.us-west-2.amazonaws.com");
    env.Set("MOONCAKE_S3_BUCKET", "kv");
    env.Set("AWS_REGION", "us-west-2");
    env.Set("AWS_ACCESS_KEY_ID", "aws-id");
    env.Set("AWS_SECRET_ACCESS_KEY", "aws-secret");
    env.Set("AWS_SESSION_TOKEN", " token \n");
    auto config = S3AdapterConfig::FromEnvironment();
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->endpoint, "https://s3.us-west-2.amazonaws.com");
    EXPECT_EQ(config->region, "us-west-2");
    EXPECT_EQ(config->access_key_id, "aws-id");
    EXPECT_EQ(config->access_key_secret, "aws-secret");
    EXPECT_EQ(config->security_token, "token");
}

TEST(S3AdapterConfigTest, MooncakeVariablesOverrideAwsVariables) {
    ScopedEnvironment env;
    SetMinimal(env);
    env.Set("AWS_ENDPOINT_URL", "https://ignored.example.com");
    env.Set("AWS_REGION", "ignored-region");
    env.Set("MOONCAKE_S3_REGION", "eu-central-1");
    env.Set("AWS_ACCESS_KEY_ID", "ignored-id");
    auto config = S3AdapterConfig::FromEnvironment();
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->endpoint, "http://127.0.0.1:8333");
    EXPECT_EQ(config->region, "eu-central-1");
    EXPECT_EQ(config->access_key_id, "id");
}

TEST(S3AdapterConfigTest, DoesNotMixCredentialSources) {
    // Static MOONCAKE_S3_* keys must not pick up a session token left in the
    // shell by aws sso or similar.
    ScopedEnvironment env;
    SetMinimal(env);
    env.Set("AWS_SESSION_TOKEN", "unrelated-sso-token");
    env.Set("AWS_ACCESS_KEY_ID", "aws-id");
    auto config = S3AdapterConfig::FromEnvironment();
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->access_key_id, "id");
    EXPECT_EQ(config->access_key_secret, "secret");
    EXPECT_TRUE(config->security_token.empty());

    env.Set("MOONCAKE_S3_SESSION_TOKEN", "own-token");
    config = S3AdapterConfig::FromEnvironment();
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->security_token, "own-token");
}

TEST(S3AdapterConfigTest, DoesNotCompleteAwsKeysFromMooncakeVariables) {
    // A MOONCAKE_S3_ secret alone selects the MOONCAKE_S3_ set; the key ID is
    // not borrowed from AWS_ACCESS_KEY_ID.
    ScopedEnvironment env;
    env.Set("MOONCAKE_S3_ENDPOINT", "https://s3.example.com");
    env.Set("MOONCAKE_S3_BUCKET", "kv");
    env.Set("MOONCAKE_S3_SECRET_ACCESS_KEY", "secret");
    env.Set("AWS_ACCESS_KEY_ID", "aws-id");
    EXPECT_FALSE(S3AdapterConfig::FromEnvironment().has_value());
}

TEST(S3AdapterConfigTest, ReadsAwsDefaultRegion) {
    ScopedEnvironment env;
    SetMinimal(env);
    env.Set("AWS_DEFAULT_REGION", "ap-southeast-1");
    auto config = S3AdapterConfig::FromEnvironment();
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->region, "ap-southeast-1");
    env.Set("AWS_REGION", "eu-west-1");
    config = S3AdapterConfig::FromEnvironment();
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->region, "eu-west-1");
}

TEST(S3AdapterConfigTest, RequiresExplicitScheme) {
    ScopedEnvironment env;
    SetMinimal(env);
    env.Set("MOONCAKE_S3_ENDPOINT", "s3.example.com:9000");
    EXPECT_FALSE(S3AdapterConfig::FromEnvironment().has_value());
    env.Set("MOONCAKE_S3_ENDPOINT", "HTTPS://s3.example.com");
    EXPECT_TRUE(S3AdapterConfig::FromEnvironment().has_value());
}

TEST(S3AdapterConfigTest, UsesPathStyleForIpAndLocalhostEndpoints) {
    for (const char* endpoint :
         {"http://127.0.0.1:9000", "http://localhost:8333", "http://[::1]:9000",
          "https://10.0.0.5"}) {
        ScopedEnvironment env;
        SetMinimal(env);
        env.Set("MOONCAKE_S3_ENDPOINT", endpoint);
        auto config = S3AdapterConfig::FromEnvironment();
        ASSERT_TRUE(config.has_value()) << endpoint;
        EXPECT_TRUE(config->path_style) << endpoint;
    }
    ScopedEnvironment env;
    SetMinimal(env);
    env.Set("MOONCAKE_S3_ENDPOINT", "https://s3.us-west-2.amazonaws.com");
    auto config = S3AdapterConfig::FromEnvironment();
    ASSERT_TRUE(config.has_value());
    EXPECT_FALSE(config->path_style);
}

TEST(S3AdapterConfigTest, RejectsMissingEndpointOrBucket) {
    {
        ScopedEnvironment env;
        SetMinimal(env);
        env.Set("MOONCAKE_S3_ENDPOINT", "");
        EXPECT_FALSE(S3AdapterConfig::FromEnvironment().has_value());
    }
    {
        ScopedEnvironment env;
        SetMinimal(env);
        env.Set("MOONCAKE_S3_BUCKET", "");
        EXPECT_FALSE(S3AdapterConfig::FromEnvironment().has_value());
    }
}

TEST(S3AdapterConfigTest, RejectsEndpointWithPath) {
    ScopedEnvironment env;
    SetMinimal(env);
    env.Set("MOONCAKE_S3_ENDPOINT", "http://127.0.0.1:8333/bucket");
    EXPECT_FALSE(S3AdapterConfig::FromEnvironment().has_value());
}

TEST(S3AdapterConfigTest, RequiresCredentialsUnlessAnonymous) {
    ScopedEnvironment env;
    SetMinimal(env);
    env.Set("MOONCAKE_S3_SECRET_ACCESS_KEY", "");
    EXPECT_FALSE(S3AdapterConfig::FromEnvironment().has_value());
    env.Set("MOONCAKE_S3_ANONYMOUS", "true");
    EXPECT_TRUE(S3AdapterConfig::FromEnvironment().has_value());
}

TEST(S3AdapterConfigTest, ClampsConnectionAndBufferSettings) {
    ScopedEnvironment env;
    SetMinimal(env);
    env.Set("MOONCAKE_S3_MAX_CONNECTIONS", "0");
    env.Set("MOONCAKE_S3_RECEIVE_BUFFER_SIZE", "1");
    env.Set("MOONCAKE_S3_UPLOAD_BUFFER_SIZE", "999999999");
    auto config = S3AdapterConfig::FromEnvironment();
    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->max_connections, 1);
    EXPECT_EQ(config->receive_buffer_size, 16 * 1024);
    EXPECT_EQ(config->upload_buffer_size, 2 * 1024 * 1024);
}

}  // namespace
}  // namespace mooncake
