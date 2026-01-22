// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "tent/metrics/config_loader.h"

namespace mooncake {
namespace tent {
namespace {

//------------------------------------------------------------------------------
// Helper class for managing environment variables in tests
//------------------------------------------------------------------------------

class EnvVarGuard {
   public:
    EnvVarGuard(const char* name, const char* value) : name_(name) {
        const char* old = std::getenv(name);
        if (old) {
            old_value_ = old;
            had_value_ = true;
        }
        setenv(name, value, 1);
    }

    ~EnvVarGuard() {
        if (had_value_) {
            setenv(name_.c_str(), old_value_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    std::string old_value_;
    bool had_value_ = false;
};

//------------------------------------------------------------------------------
// MetricsConfig Default Values Tests
//------------------------------------------------------------------------------

TEST(MetricsConfigLoaderTest, GetDefaultConfigReturnsExpectedDefaults) {
    MetricsConfig config = MetricsConfigLoader::getDefaultConfig();

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.http_host, "0.0.0.0");
    EXPECT_EQ(config.http_port, 9100);
    EXPECT_EQ(config.http_server_threads, 2);
    EXPECT_EQ(config.report_interval_seconds, 30);
    EXPECT_TRUE(config.enable_prometheus);
    EXPECT_TRUE(config.enable_json);
    EXPECT_TRUE(config.latency_buckets.empty());
    EXPECT_TRUE(config.size_buckets.empty());
}

//------------------------------------------------------------------------------
// MetricsConfigLoader::loadFromConfig Tests
//------------------------------------------------------------------------------

TEST(MetricsConfigLoaderTest, LoadFromConfigWithAllValues) {
    Config config;
    std::string json_content = R"({
        "metrics/enabled": false,
        "metrics/http_port": 8080,
        "metrics/http_host": "127.0.0.1",
        "metrics/http_server_threads": 4,
        "metrics/report_interval_seconds": 60,
        "metrics/enable_prometheus": false,
        "metrics/enable_json": true,
        "metrics/latency_buckets": [0.001, 0.01, 0.1, 1.0],
        "metrics/size_buckets": [1024, 10240, 102400]
    })";
    ASSERT_TRUE(config.load(json_content).ok());

    MetricsConfig metrics_config = MetricsConfigLoader::loadFromConfig(config);

    EXPECT_FALSE(metrics_config.enabled);
    EXPECT_EQ(metrics_config.http_port, 8080);
    EXPECT_EQ(metrics_config.http_host, "127.0.0.1");
    EXPECT_EQ(metrics_config.http_server_threads, 4);
    EXPECT_EQ(metrics_config.report_interval_seconds, 60);
    EXPECT_FALSE(metrics_config.enable_prometheus);
    EXPECT_TRUE(metrics_config.enable_json);
    ASSERT_EQ(metrics_config.latency_buckets.size(), 4);
    EXPECT_DOUBLE_EQ(metrics_config.latency_buckets[0], 0.001);
    EXPECT_DOUBLE_EQ(metrics_config.latency_buckets[3], 1.0);
    ASSERT_EQ(metrics_config.size_buckets.size(), 3);
    EXPECT_DOUBLE_EQ(metrics_config.size_buckets[0], 1024);
}

TEST(MetricsConfigLoaderTest, LoadFromConfigWithPartialValues) {
    Config config;
    std::string json_content = R"({
        "metrics/http_port": 9200
    })";
    ASSERT_TRUE(config.load(json_content).ok());

    MetricsConfig metrics_config = MetricsConfigLoader::loadFromConfig(config);

    // Only http_port should be overridden, others should be defaults
    EXPECT_TRUE(metrics_config.enabled);
    EXPECT_EQ(metrics_config.http_port, 9200);
    EXPECT_EQ(metrics_config.http_host, "0.0.0.0");
    EXPECT_EQ(metrics_config.http_server_threads, 2);
}

TEST(MetricsConfigLoaderTest, LoadFromConfigWithEmptyConfig) {
    Config config;
    std::string json_content = "{}";
    ASSERT_TRUE(config.load(json_content).ok());

    MetricsConfig metrics_config = MetricsConfigLoader::loadFromConfig(config);

    // All values should be defaults
    MetricsConfig default_config = MetricsConfigLoader::getDefaultConfig();
    EXPECT_EQ(metrics_config.enabled, default_config.enabled);
    EXPECT_EQ(metrics_config.http_port, default_config.http_port);
    EXPECT_EQ(metrics_config.http_host, default_config.http_host);
}

//------------------------------------------------------------------------------
// MetricsConfigLoader::loadFromEnvironment Tests
//------------------------------------------------------------------------------

TEST(MetricsConfigLoaderTest, LoadFromEnvironmentWithAllVars) {
    EnvVarGuard g1(config_keys::ENV_METRICS_ENABLED, "false");
    EnvVarGuard g2(config_keys::ENV_METRICS_HTTP_PORT, "9999");
    EnvVarGuard g3(config_keys::ENV_METRICS_HTTP_HOST, "192.168.1.1");
    EnvVarGuard g4(config_keys::ENV_METRICS_HTTP_SERVER_THREADS, "8");
    EnvVarGuard g5(config_keys::ENV_METRICS_REPORT_INTERVAL, "120");
    EnvVarGuard g6(config_keys::ENV_METRICS_ENABLE_PROMETHEUS, "true");
    EnvVarGuard g7(config_keys::ENV_METRICS_ENABLE_JSON, "false");

    MetricsConfig config = MetricsConfigLoader::loadFromEnvironment();

    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.http_port, 9999);
    EXPECT_EQ(config.http_host, "192.168.1.1");
    EXPECT_EQ(config.http_server_threads, 8);
    EXPECT_EQ(config.report_interval_seconds, 120);
    EXPECT_TRUE(config.enable_prometheus);
    EXPECT_FALSE(config.enable_json);
}

TEST(MetricsConfigLoaderTest, LoadFromEnvironmentWithPartialVars) {
    EnvVarGuard g1(config_keys::ENV_METRICS_HTTP_PORT, "8888");

    MetricsConfig config = MetricsConfigLoader::loadFromEnvironment();

    EXPECT_EQ(config.http_port, 8888);
    // Other values should be defaults
    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.http_host, "0.0.0.0");
}

TEST(MetricsConfigLoaderTest, LoadFromEnvironmentWithLatencyBuckets) {
    EnvVarGuard g1(config_keys::ENV_METRICS_LATENCY_BUCKETS,
                   "0.001,0.005,0.01");

    MetricsConfig config = MetricsConfigLoader::loadFromEnvironment();

    ASSERT_EQ(config.latency_buckets.size(), 3);
    EXPECT_DOUBLE_EQ(config.latency_buckets[0], 0.001);
    EXPECT_DOUBLE_EQ(config.latency_buckets[1], 0.005);
    EXPECT_DOUBLE_EQ(config.latency_buckets[2], 0.01);
}

TEST(MetricsConfigLoaderTest, LoadFromEnvironmentWithSizeBuckets) {
    EnvVarGuard g1(config_keys::ENV_METRICS_SIZE_BUCKETS, "1024,2048,4096");

    MetricsConfig config = MetricsConfigLoader::loadFromEnvironment();

    ASSERT_EQ(config.size_buckets.size(), 3);
    EXPECT_DOUBLE_EQ(config.size_buckets[0], 1024);
    EXPECT_DOUBLE_EQ(config.size_buckets[1], 2048);
    EXPECT_DOUBLE_EQ(config.size_buckets[2], 4096);
}

//------------------------------------------------------------------------------
// MetricsConfigLoader::loadWithDefaults Tests
//------------------------------------------------------------------------------

TEST(MetricsConfigLoaderTest, LoadWithDefaultsNoConfigNoEnv) {
    MetricsConfig config = MetricsConfigLoader::loadWithDefaults(nullptr);

    MetricsConfig default_config = MetricsConfigLoader::getDefaultConfig();
    EXPECT_EQ(config.enabled, default_config.enabled);
    EXPECT_EQ(config.http_port, default_config.http_port);
}

TEST(MetricsConfigLoaderTest, LoadWithDefaultsConfigOverridesEnv) {
    // Set environment variable
    EnvVarGuard g1(config_keys::ENV_METRICS_HTTP_PORT, "7777");

    // Create config with different value
    Config file_config;
    std::string json_content = R"({
        "metrics/http_port": 6666
    })";
    ASSERT_TRUE(file_config.load(json_content).ok());

    // Config file should take priority over environment
    MetricsConfig config = MetricsConfigLoader::loadWithDefaults(&file_config);

    EXPECT_EQ(config.http_port, 6666);
}

TEST(MetricsConfigLoaderTest, LoadWithDefaultsEnvOverridesDefault) {
    EnvVarGuard g1(config_keys::ENV_METRICS_HTTP_PORT, "5555");

    MetricsConfig config = MetricsConfigLoader::loadWithDefaults(nullptr);

    EXPECT_EQ(config.http_port, 5555);
}

//------------------------------------------------------------------------------
// MetricsConfigLoader::validateConfig Tests
//------------------------------------------------------------------------------

TEST(MetricsConfigLoaderTest, ValidateConfigValidConfig) {
    MetricsConfig config = MetricsConfigLoader::getDefaultConfig();
    std::string error_msg;

    EXPECT_TRUE(MetricsConfigLoader::validateConfig(config, &error_msg));
    EXPECT_TRUE(error_msg.empty());
}

TEST(MetricsConfigLoaderTest, ValidateConfigInvalidPortZero) {
    MetricsConfig config = MetricsConfigLoader::getDefaultConfig();
    config.http_port = 0;
    std::string error_msg;

    EXPECT_FALSE(MetricsConfigLoader::validateConfig(config, &error_msg));
    EXPECT_FALSE(error_msg.empty());
    EXPECT_NE(error_msg.find("port"), std::string::npos);
}

TEST(MetricsConfigLoaderTest, ValidateConfigInvalidThreadsZero) {
    MetricsConfig config = MetricsConfigLoader::getDefaultConfig();
    config.http_server_threads = 0;
    std::string error_msg;

    EXPECT_FALSE(MetricsConfigLoader::validateConfig(config, &error_msg));
    EXPECT_FALSE(error_msg.empty());
    EXPECT_NE(error_msg.find("threads"), std::string::npos);
}

TEST(MetricsConfigLoaderTest, ValidateConfigNoOutputFormatEnabled) {
    MetricsConfig config = MetricsConfigLoader::getDefaultConfig();
    config.enable_prometheus = false;
    config.enable_json = false;
    std::string error_msg;

    EXPECT_FALSE(MetricsConfigLoader::validateConfig(config, &error_msg));
    EXPECT_FALSE(error_msg.empty());
    EXPECT_NE(error_msg.find("format"), std::string::npos);
}

TEST(MetricsConfigLoaderTest, ValidateConfigUnsortedLatencyBuckets) {
    MetricsConfig config = MetricsConfigLoader::getDefaultConfig();
    config.latency_buckets = {0.1, 0.05, 0.2};  // Not sorted
    std::string error_msg;

    EXPECT_FALSE(MetricsConfigLoader::validateConfig(config, &error_msg));
    EXPECT_FALSE(error_msg.empty());
    EXPECT_NE(error_msg.find("Latency"), std::string::npos);
}

TEST(MetricsConfigLoaderTest, ValidateConfigUnsortedSizeBuckets) {
    MetricsConfig config = MetricsConfigLoader::getDefaultConfig();
    config.size_buckets = {1024, 512, 2048};  // Not sorted
    std::string error_msg;

    EXPECT_FALSE(MetricsConfigLoader::validateConfig(config, &error_msg));
    EXPECT_FALSE(error_msg.empty());
    EXPECT_NE(error_msg.find("Size"), std::string::npos);
}

TEST(MetricsConfigLoaderTest, ValidateConfigDuplicateBucketValues) {
    MetricsConfig config = MetricsConfigLoader::getDefaultConfig();
    config.latency_buckets = {0.1, 0.1, 0.2};  // Duplicate values
    std::string error_msg;

    EXPECT_FALSE(MetricsConfigLoader::validateConfig(config, &error_msg));
    EXPECT_FALSE(error_msg.empty());
}

TEST(MetricsConfigLoaderTest, ValidateConfigValidSortedBuckets) {
    MetricsConfig config = MetricsConfigLoader::getDefaultConfig();
    config.latency_buckets = {0.001, 0.01, 0.1, 1.0};
    config.size_buckets = {1024, 10240, 102400};
    std::string error_msg;

    EXPECT_TRUE(MetricsConfigLoader::validateConfig(config, &error_msg));
    EXPECT_TRUE(error_msg.empty());
}

TEST(MetricsConfigLoaderTest, ValidateConfigNullErrorMsg) {
    MetricsConfig config = MetricsConfigLoader::getDefaultConfig();
    config.http_port = 0;

    // Should not crash when error_msg is nullptr
    EXPECT_FALSE(MetricsConfigLoader::validateConfig(config, nullptr));
}

//------------------------------------------------------------------------------
// ConfigHelper Parsing Tests
//------------------------------------------------------------------------------

TEST(ConfigHelperTest, ParseBoolTrue) {
    EXPECT_TRUE(ConfigHelper::parseBool("true", false));
    EXPECT_TRUE(ConfigHelper::parseBool("TRUE", false));
    EXPECT_TRUE(ConfigHelper::parseBool("True", false));
    EXPECT_TRUE(ConfigHelper::parseBool("1", false));
    EXPECT_TRUE(ConfigHelper::parseBool("yes", false));
    EXPECT_TRUE(ConfigHelper::parseBool("YES", false));
}

TEST(ConfigHelperTest, ParseBoolFalse) {
    EXPECT_FALSE(ConfigHelper::parseBool("false", true));
    EXPECT_FALSE(ConfigHelper::parseBool("FALSE", true));
    EXPECT_FALSE(ConfigHelper::parseBool("False", true));
    EXPECT_FALSE(ConfigHelper::parseBool("0", true));
    EXPECT_FALSE(ConfigHelper::parseBool("no", true));
    EXPECT_FALSE(ConfigHelper::parseBool("NO", true));
}

TEST(ConfigHelperTest, ParseBoolInvalid) {
    EXPECT_TRUE(ConfigHelper::parseBool("invalid", true));
    EXPECT_FALSE(ConfigHelper::parseBool("invalid", false));
}

TEST(ConfigHelperTest, ParseIntValid) {
    EXPECT_EQ(ConfigHelper::parseInt("42", 0), 42);
    EXPECT_EQ(ConfigHelper::parseInt("-10", 0), -10);
    EXPECT_EQ(ConfigHelper::parseInt("0", 100), 0);
}

TEST(ConfigHelperTest, ParseIntInvalid) {
    EXPECT_EQ(ConfigHelper::parseInt("not-a-number", 99), 99);
    EXPECT_EQ(ConfigHelper::parseInt("", 50), 50);
}

TEST(ConfigHelperTest, ParsePortValid) {
    EXPECT_EQ(ConfigHelper::parsePort("8080", 0), 8080);
    EXPECT_EQ(ConfigHelper::parsePort("65535", 0), 65535);
    EXPECT_EQ(ConfigHelper::parsePort("1", 0), 1);
}

TEST(ConfigHelperTest, ParsePortInvalid) {
    EXPECT_EQ(ConfigHelper::parsePort("not-a-port", 9100), 9100);
    EXPECT_EQ(ConfigHelper::parsePort("", 9100), 9100);
}

TEST(ConfigHelperTest, ParseDoubleArrayValid) {
    auto result = ConfigHelper::parseDoubleArray("1.0,2.5,3.14");
    ASSERT_EQ(result.size(), 3);
    EXPECT_DOUBLE_EQ(result[0], 1.0);
    EXPECT_DOUBLE_EQ(result[1], 2.5);
    EXPECT_DOUBLE_EQ(result[2], 3.14);
}

TEST(ConfigHelperTest, ParseDoubleArrayEmpty) {
    auto result = ConfigHelper::parseDoubleArray("");
    EXPECT_TRUE(result.empty());
}

TEST(ConfigHelperTest, ParseDoubleArraySingleValue) {
    auto result = ConfigHelper::parseDoubleArray("42.0");
    ASSERT_EQ(result.size(), 1);
    EXPECT_DOUBLE_EQ(result[0], 42.0);
}

//------------------------------------------------------------------------------
// Config Keys Constants Tests
//------------------------------------------------------------------------------

TEST(ConfigKeysTest, ConfigKeyConstants) {
    // Verify config key constants are correctly defined
    EXPECT_STREQ(config_keys::METRICS_ENABLED, "metrics/enabled");
    EXPECT_STREQ(config_keys::METRICS_HTTP_PORT, "metrics/http_port");
    EXPECT_STREQ(config_keys::METRICS_HTTP_HOST, "metrics/http_host");
    EXPECT_STREQ(config_keys::METRICS_HTTP_SERVER_THREADS,
                 "metrics/http_server_threads");
    EXPECT_STREQ(config_keys::METRICS_REPORT_INTERVAL,
                 "metrics/report_interval_seconds");
    EXPECT_STREQ(config_keys::METRICS_ENABLE_PROMETHEUS,
                 "metrics/enable_prometheus");
    EXPECT_STREQ(config_keys::METRICS_ENABLE_JSON, "metrics/enable_json");
    EXPECT_STREQ(config_keys::METRICS_LATENCY_BUCKETS,
                 "metrics/latency_buckets");
    EXPECT_STREQ(config_keys::METRICS_SIZE_BUCKETS, "metrics/size_buckets");
}

TEST(ConfigKeysTest, EnvVarConstants) {
    // Verify environment variable constants are correctly defined
    EXPECT_STREQ(config_keys::ENV_METRICS_ENABLED, "TENT_METRICS_ENABLED");
    EXPECT_STREQ(config_keys::ENV_METRICS_HTTP_PORT, "TENT_METRICS_HTTP_PORT");
    EXPECT_STREQ(config_keys::ENV_METRICS_HTTP_HOST, "TENT_METRICS_HTTP_HOST");
    EXPECT_STREQ(config_keys::ENV_METRICS_HTTP_SERVER_THREADS,
                 "TENT_METRICS_HTTP_SERVER_THREADS");
    EXPECT_STREQ(config_keys::ENV_METRICS_REPORT_INTERVAL,
                 "TENT_METRICS_REPORT_INTERVAL");
    EXPECT_STREQ(config_keys::ENV_METRICS_ENABLE_PROMETHEUS,
                 "TENT_METRICS_ENABLE_PROMETHEUS");
    EXPECT_STREQ(config_keys::ENV_METRICS_ENABLE_JSON,
                 "TENT_METRICS_ENABLE_JSON");
    EXPECT_STREQ(config_keys::ENV_METRICS_LATENCY_BUCKETS,
                 "TENT_METRICS_LATENCY_BUCKETS");
    EXPECT_STREQ(config_keys::ENV_METRICS_SIZE_BUCKETS,
                 "TENT_METRICS_SIZE_BUCKETS");
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
