#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "client_metric.h"
#include "environment_variables.h"

namespace mooncake {
namespace {

class ScopedEnvVar {
   public:
    explicit ScopedEnvVar(const char* name) : name_(name) {
        const char* value = std::getenv(name);
        if (value != nullptr) {
            original_ = value;
        }
        unsetenv(name);
    }

    ~ScopedEnvVar() {
        if (original_.has_value()) {
            setenv(name_.c_str(), original_->c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    ScopedEnvVar(const ScopedEnvVar&) = delete;
    ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

    void Set(const char* value) { setenv(name_.c_str(), value, 1); }

   private:
    std::string name_;
    std::optional<std::string> original_;
};

struct ClientMetricEnvironment {
    using Variables = ClientMetricEnvironmentVariables;

    ScopedEnvVar enabled{Variables::MC_STORE_CLIENT_METRIC.name};
    ScopedEnvVar interval{Variables::MC_STORE_CLIENT_METRIC_INTERVAL.name};
    ScopedEnvVar bandwidth{Variables::MC_STORE_CLIENT_METRIC_BANDWIDTH.name};
};

class ClientMetricConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ClientMetricConfigTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    ClientMetricEnvironment env;
};

TEST_F(ClientMetricConfigTest, UsesDefaultsWhenEnvironmentIsUnset) {
    const auto config = ClientMetricConfig::FromEnvironment();

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.reporting_interval_seconds, 0);
    EXPECT_TRUE(config.bandwidth_reporting_enabled);
}

TEST_F(ClientMetricConfigTest, ReadsValidEnvironmentValues) {
    env.enabled.Set("true");
    env.interval.Set(" +15 ");
    env.bandwidth.Set("false");

    ::testing::internal::CaptureStderr();
    const auto config = ClientMetricConfig::FromEnvironment();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.reporting_interval_seconds, 15);
    EXPECT_FALSE(config.bandwidth_reporting_enabled);
    EXPECT_NE(logs.find("Client metrics interval set to 15s via "
                        "MC_STORE_CLIENT_METRIC_INTERVAL"),
              std::string::npos);
}

TEST_F(ClientMetricConfigTest, InvalidEnableValueSilentlyDisablesMetrics) {
    env.enabled.Set("invalid");
    env.interval.Set("invalid");
    env.bandwidth.Set("invalid");

    ::testing::internal::CaptureStderr();
    const auto config = ClientMetricConfig::FromEnvironment();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.reporting_interval_seconds, 0);
    EXPECT_TRUE(config.bandwidth_reporting_enabled);
    EXPECT_EQ(logs.find("MC_STORE_CLIENT_METRIC_INTERVAL"), std::string::npos);
    EXPECT_EQ(logs.find("MC_STORE_CLIENT_METRIC_BANDWIDTH"), std::string::npos);
}

TEST_F(ClientMetricConfigTest, EmptyEnableValueSilentlyDisablesMetrics) {
    env.enabled.Set("");

    ::testing::internal::CaptureStderr();
    const auto config = ClientMetricConfig::FromEnvironment();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.enabled);
    EXPECT_TRUE(logs.empty());
}

TEST_F(ClientMetricConfigTest, InvalidIntervalValuesUseDefaultAndWarn) {
    for (const char* value : {"invalid", "-1", "18446744073709551616", ""}) {
        SCOPED_TRACE(value);
        env.interval.Set(value);

        ::testing::internal::CaptureStderr();
        const auto config = ClientMetricConfig::FromEnvironment();
        const std::string logs = ::testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.reporting_interval_seconds, 0);
        EXPECT_NE(logs.find("Failed to parse "
                            "MC_STORE_CLIENT_METRIC_INTERVAL"),
                  std::string::npos);
    }
}

TEST_F(ClientMetricConfigTest, InvalidBandwidthValuesUseDefaultAndWarn) {
    for (const char* value : {"invalid", ""}) {
        SCOPED_TRACE(value);
        env.bandwidth.Set(value);

        ::testing::internal::CaptureStderr();
        const auto config = ClientMetricConfig::FromEnvironment();
        const std::string logs = ::testing::internal::GetCapturedStderr();

        EXPECT_TRUE(config.bandwidth_reporting_enabled);
        EXPECT_NE(logs.find("Failed to parse "
                            "MC_STORE_CLIENT_METRIC_BANDWIDTH"),
                  std::string::npos);
    }
}

TEST_F(ClientMetricConfigTest, ZeroIntervalRemainsEnabled) {
    env.interval.Set("0");

    ::testing::internal::CaptureStderr();
    const auto config = ClientMetricConfig::FromEnvironment();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.reporting_interval_seconds, 0);
    EXPECT_NE(logs.find("Client metrics reporting disabled (interval=0) via "
                        "MC_STORE_CLIENT_METRIC_INTERVAL"),
              std::string::npos);
}

}  // namespace
}  // namespace mooncake
