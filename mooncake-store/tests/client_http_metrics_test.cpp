#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <thread>
#include <chrono>

#include "client_config_builder.h"
#include "client_metric.h"
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_http/coro_http_client.hpp>

namespace mooncake::test {

class ClientHttpMetricsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ClientHttpMetricsTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

// Test config builder with metrics settings
TEST_F(ClientHttpMetricsTest, ConfigBuilderMetricsSettings) {
    // Test P2P config with default metrics settings
    auto p2p_config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1", "http://127.0.0.1:8080/metadata", "tcp", std::nullopt,
        "127.0.0.1:50051",
        R"({"tiers": [{"type": "memory", "capacity": 1073741824}]})");

    EXPECT_EQ(p2p_config.metrics_port, 9003);
    EXPECT_TRUE(p2p_config.enable_metrics_http);

    // Test P2P config with custom metrics settings
    auto p2p_config_custom = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1", "http://127.0.0.1:8080/metadata", "tcp", std::nullopt,
        "127.0.0.1:50051",
        R"({"tiers": [{"type": "memory", "capacity": 1073741824}]})", 0,
        nullptr, "", 12345, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000, "te", 32,
        9005,   // metrics_port
        false,  // enable_metrics_http
        {}      // labels
    );

    EXPECT_EQ(p2p_config_custom.metrics_port, 9005);
    EXPECT_FALSE(p2p_config_custom.enable_metrics_http);

    // Test Centralized config with default metrics settings
    auto centralized_config =
        ClientConfigBuilder::build_centralized_real_client(
            "127.0.0.1", "http://127.0.0.1:8080/metadata");

    EXPECT_EQ(centralized_config.metrics_port, 9003);
    EXPECT_TRUE(centralized_config.enable_metrics_http);

    // Test Centralized config with custom metrics settings
    auto centralized_config_custom =
        ClientConfigBuilder::build_centralized_real_client(
            "127.0.0.1", "http://127.0.0.1:8080/metadata", "tcp", std::nullopt,
            "127.0.0.1:50051", 0, 0, nullptr, "",
            false,  // enable_offload
            9006,   // metrics_port
            false,  // enable_metrics_http
            {}      // labels
        );

    EXPECT_EQ(centralized_config_custom.metrics_port, 9006);
    EXPECT_FALSE(centralized_config_custom.enable_metrics_http);
}

// Test metrics disabled scenario
TEST_F(ClientHttpMetricsTest, MetricsDisabled) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1", "http://127.0.0.1:8080/metadata", "tcp", std::nullopt,
        "127.0.0.1:50051",
        R"({"tiers": [{"type": "memory", "capacity": 1073741824}]})", 0,
        nullptr, "", 12345, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000, "te", 32,
        9003,  // metrics_port
        false  // enable_metrics_http = false
    );

    EXPECT_EQ(config.metrics_port, 9003);
    EXPECT_FALSE(config.enable_metrics_http);
}

// Test with labels
TEST_F(ClientHttpMetricsTest, ConfigWithLabels) {
    std::map<std::string, std::string> labels = {
        {"instance_id", "test_instance"}, {"cluster_id", "test_cluster"}};

    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1", "http://127.0.0.1:8080/metadata", "tcp", std::nullopt,
        "127.0.0.1:50051",
        R"({"tiers": [{"type": "memory", "capacity": 1073741824}]})", 0,
        nullptr, "", 12345, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000, "te", 32,
        9003,   // metrics_port
        true,   // enable_metrics_http
        labels  // labels
    );

    EXPECT_EQ(config.labels.size(), 2);
    EXPECT_EQ(config.labels["instance_id"], "test_instance");
    EXPECT_EQ(config.labels["cluster_id"], "test_cluster");
    EXPECT_EQ(config.metrics_port, 9003);
    EXPECT_TRUE(config.enable_metrics_http);
}

// Test HTTP server endpoints directly
TEST_F(ClientHttpMetricsTest, HttpEndpointsTest) {
    // Create a simple HTTP server that mimics the metrics server behavior
    const uint16_t test_port =
        19003;  // Use a non-standard port to avoid conflicts

    // Create ClientMetric instance
    auto metrics = ClientMetric::Create({{"test_label", "test_value"}});
    ASSERT_NE(metrics, nullptr);

    // Add some test data to metrics
    metrics->transfer_metric.total_read_bytes.inc(1024);
    metrics->transfer_metric.total_write_bytes.inc(2048);
    metrics->transfer_metric.get_latency_us.observe(100);
    metrics->transfer_metric.get_latency_us.observe(200);

    // Create and start HTTP server
    coro_http::coro_http_server server(1, test_port);

    using namespace coro_http;

    // Register handlers similar to ClientService
    server.set_http_handler<GET>("/metrics", [&metrics](
                                                 coro_http_request& req,
                                                 coro_http_response& resp) {
        std::string metrics_str;
        metrics->serialize(metrics_str);
        resp.add_header("Content-Type", "text/plain; version=0.0.4");
        resp.set_status_and_content(status_type::ok, std::move(metrics_str));
    });

    server.set_http_handler<GET>(
        "/metrics/summary",
        [&metrics](coro_http_request& req, coro_http_response& resp) {
            std::string summary = metrics->summary_metrics();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    server.set_http_handler<GET>(
        "/health", [](coro_http_request& req, coro_http_response& resp) {
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, "OK");
        });

    server.async_start();

    // Wait for server to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Test /health endpoint
    {
        coro_http::coro_http_client client;
        auto resp = client.get("http://127.0.0.1:" + std::to_string(test_port) +
                               "/health");
        EXPECT_EQ(resp.status, 200) << "Health endpoint returned wrong status";
        EXPECT_EQ(resp.resp_body, "OK")
            << "Health endpoint returned wrong body";
    }

    // Test /metrics endpoint
    {
        coro_http::coro_http_client client;
        auto resp = client.get("http://127.0.0.1:" + std::to_string(test_port) +
                               "/metrics");
        EXPECT_EQ(resp.status, 200) << "Metrics endpoint returned wrong status";
        // Check that metrics contain expected data
        EXPECT_TRUE(resp.resp_body.find("mooncake_transfer_read_bytes") !=
                    std::string::npos)
            << "Metrics should contain read bytes metric";
        EXPECT_TRUE(resp.resp_body.find("mooncake_transfer_write_bytes") !=
                    std::string::npos)
            << "Metrics should contain write bytes metric";
        EXPECT_TRUE(resp.resp_body.find("test_label") != std::string::npos)
            << "Metrics should contain label";
        EXPECT_TRUE(resp.resp_body.find("test_value") != std::string::npos)
            << "Metrics should contain label value";
    }

    // Test /metrics/summary endpoint
    {
        coro_http::coro_http_client client;
        auto resp = client.get("http://127.0.0.1:" + std::to_string(test_port) +
                               "/metrics/summary");
        EXPECT_EQ(resp.status, 200)
            << "Metrics summary endpoint returned wrong status";
        // Check that summary contains expected data
        EXPECT_TRUE(resp.resp_body.find("Transfer Metrics Summary") !=
                    std::string::npos)
            << "Summary should contain transfer metrics header";
        EXPECT_TRUE(resp.resp_body.find("Total Read") != std::string::npos)
            << "Summary should contain total read";
        EXPECT_TRUE(resp.resp_body.find("Total Write") != std::string::npos)
            << "Summary should contain total write";
    }

    // Stop server
    server.stop();
}

}  // namespace mooncake::test