#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <string>

#include "client_metric.h"
#include "environ.h"
#include "../src/config/client_environment_variables.h"

namespace mooncake {
namespace {

class ClientMetricConfigTest : public ::testing::Test {
   protected:
    using Variables = ClientEnvironmentVariables::Metric;

    void SetUp() override {
        google::InitGoogleLogging("ClientMetricConfigTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    ClientMetricConfig Load() const {
        return ClientMetricConfig::FromEnvironment(Environ(source_));
    }

    void SetEnabled(const char* value) {
        source_.Set(Variables::MC_STORE_CLIENT_METRIC.name, value);
    }

    void SetInterval(const char* value) {
        source_.Set(Variables::MC_STORE_CLIENT_METRIC_INTERVAL.name, value);
    }

    void SetBandwidth(const char* value) {
        source_.Set(Variables::MC_STORE_CLIENT_METRIC_BANDWIDTH.name, value);
    }

    MapEnvironSource source_;
};

TEST_F(ClientMetricConfigTest, UsesDefaultsWhenEnvironmentIsUnset) {
    const auto config = Load();

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.reporting_interval, std::chrono::milliseconds::zero());
    EXPECT_TRUE(config.bandwidth_reporting_enabled);
}

TEST_F(ClientMetricConfigTest, ReadsValidEnvironmentValues) {
    SetEnabled("true");
    SetInterval(" +15 ");
    SetBandwidth("false");

    ::testing::internal::CaptureStderr();
    const auto config = Load();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.reporting_interval, std::chrono::seconds(15));
    EXPECT_FALSE(config.bandwidth_reporting_enabled);
    EXPECT_NE(logs.find("Client metrics interval set to 15s via "
                        "MC_STORE_CLIENT_METRIC_INTERVAL"),
              std::string::npos);
}

TEST_F(ClientMetricConfigTest, InvalidEnableValueSilentlyDisablesMetrics) {
    SetEnabled("invalid");
    SetInterval("invalid");
    SetBandwidth("invalid");

    ::testing::internal::CaptureStderr();
    const auto config = Load();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.enabled);
    EXPECT_EQ(config.reporting_interval, std::chrono::milliseconds::zero());
    EXPECT_TRUE(config.bandwidth_reporting_enabled);
    EXPECT_EQ(logs.find("MC_STORE_CLIENT_METRIC_INTERVAL"), std::string::npos);
    EXPECT_EQ(logs.find("MC_STORE_CLIENT_METRIC_BANDWIDTH"), std::string::npos);
}

TEST_F(ClientMetricConfigTest, EmptyEnableValueSilentlyDisablesMetrics) {
    SetEnabled("");

    ::testing::internal::CaptureStderr();
    const auto config = Load();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.enabled);
    EXPECT_TRUE(logs.empty());
}

TEST_F(ClientMetricConfigTest, InvalidIntervalValuesUseDefaultAndWarn) {
    for (const char* value : {"invalid", "-1", "18446744073709551616", ""}) {
        SCOPED_TRACE(value);
        SetInterval(value);

        ::testing::internal::CaptureStderr();
        const auto config = Load();
        const std::string logs = ::testing::internal::GetCapturedStderr();

        EXPECT_EQ(config.reporting_interval, std::chrono::milliseconds::zero());
        EXPECT_NE(logs.find("Failed to parse "
                            "MC_STORE_CLIENT_METRIC_INTERVAL"),
                  std::string::npos);
    }
}

TEST_F(ClientMetricConfigTest, InvalidBandwidthValuesUseDefaultAndWarn) {
    for (const char* value : {"invalid", ""}) {
        SCOPED_TRACE(value);
        SetBandwidth(value);

        ::testing::internal::CaptureStderr();
        const auto config = Load();
        const std::string logs = ::testing::internal::GetCapturedStderr();

        EXPECT_TRUE(config.bandwidth_reporting_enabled);
        EXPECT_NE(logs.find("Failed to parse "
                            "MC_STORE_CLIENT_METRIC_BANDWIDTH"),
                  std::string::npos);
    }
}

TEST_F(ClientMetricConfigTest, ZeroIntervalRemainsEnabled) {
    SetInterval("0");

    ::testing::internal::CaptureStderr();
    const auto config = Load();
    const std::string logs = ::testing::internal::GetCapturedStderr();

    EXPECT_TRUE(config.enabled);
    EXPECT_EQ(config.reporting_interval, std::chrono::milliseconds::zero());
    EXPECT_NE(logs.find("Client metrics reporting disabled (interval=0) via "
                        "MC_STORE_CLIENT_METRIC_INTERVAL"),
              std::string::npos);
}

TEST(ClientMetricClusterIdTest, MergeLabelsUsesConfiguredClusterId) {
    const char* cluster_id = std::getenv("MC_STORE_CLUSTER_ID");
    if (cluster_id == nullptr || cluster_id[0] == '\0') {
        GTEST_SKIP() << "MC_STORE_CLUSTER_ID is not configured";
    }

    const auto labels = merge_labels({{"operation", "test"}});
    EXPECT_EQ(labels.at("cluster_id"), cluster_id);
    EXPECT_EQ(labels.at("operation"), "test");
}

}  // namespace
}  // namespace mooncake
