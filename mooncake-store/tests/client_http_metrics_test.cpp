#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <string>
#include <thread>
#include <chrono>

#include <csignal>
#include "client_config_builder.h"
#include "client_metric.h"
#include "p2p_client_metric.h"
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

    EXPECT_EQ(p2p_config.http_port, 9003);
    EXPECT_TRUE(p2p_config.enable_http_server);

    // Test P2P config with custom metrics settings
    auto p2p_config_custom = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1", "http://127.0.0.1:8080/metadata", "tcp", std::nullopt,
        "127.0.0.1:50051",
        R"({"tiers": [{"type": "memory", "capacity": 1073741824}]})", 0,
        nullptr, "", 12345, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000, "te", 32,
        9005,   // http_port
        false,  // enable_http_server
        {}      // labels
    );

    EXPECT_EQ(p2p_config_custom.http_port, 9005);
    EXPECT_FALSE(p2p_config_custom.enable_http_server);

    // Test Centralized config with default metrics settings
    auto centralized_config =
        ClientConfigBuilder::build_centralized_real_client(
            "127.0.0.1", "http://127.0.0.1:8080/metadata");

    EXPECT_EQ(centralized_config.http_port, 9003);
    EXPECT_TRUE(centralized_config.enable_http_server);

    // Test Centralized config with custom metrics settings
    auto centralized_config_custom =
        ClientConfigBuilder::build_centralized_real_client(
            "127.0.0.1", "http://127.0.0.1:8080/metadata", "tcp", std::nullopt,
            "127.0.0.1:50051", 0, 0, nullptr, "",
            false,  // enable_offload
            9006,   // http_port
            false,  // enable_http_server
            {}      // labels
        );

    EXPECT_EQ(centralized_config_custom.http_port, 9006);
    EXPECT_FALSE(centralized_config_custom.enable_http_server);
}

// Test metrics disabled scenario
TEST_F(ClientHttpMetricsTest, MetricsDisabled) {
    auto config = ClientConfigBuilder::build_p2p_real_client(
        "127.0.0.1", "http://127.0.0.1:8080/metadata", "tcp", std::nullopt,
        "127.0.0.1:50051",
        R"({"tiers": [{"type": "memory", "capacity": 1073741824}]})", 0,
        nullptr, "", 12345, 2, 1024, 300 * 1024 * 1024, 5 * 60 * 1000, "te", 32,
        9003,  // http_port
        false  // enable_http_server = false
    );

    EXPECT_EQ(config.http_port, 9003);
    EXPECT_FALSE(config.enable_http_server);
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
        9003,   // http_port
        true,   // enable_http_server
        labels  // labels
    );

    EXPECT_EQ(config.labels.size(), 2);
    EXPECT_EQ(config.labels["instance_id"], "test_instance");
    EXPECT_EQ(config.labels["cluster_id"], "test_cluster");
    EXPECT_EQ(config.http_port, 9003);
    EXPECT_TRUE(config.enable_http_server);
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

// Test P2P client metrics HTTP endpoints
TEST_F(ClientHttpMetricsTest, P2PClientMetricsHttpEndpointsTest) {
    const uint16_t test_port = 19004;

    // Create P2PClientMetric instance
    auto p2p_metrics = P2PClientMetric::Create({{"p2p_label", "p2p_value"}});
    ASSERT_NE(p2p_metrics, nullptr);

    // Add test data to P2P metrics
    // Local Put metrics
    p2p_metrics->local_request.put_requests.inc(10);
    p2p_metrics->local_request.put_bytes.inc(5 * 1024 * 1024);  // 5 MB
    p2p_metrics->local_request.put_latency_success.observe(200);
    p2p_metrics->local_request.put_latency_success.observe(300);
    p2p_metrics->local_request.put_latency_failure.observe(500);

    // Local Get metrics
    p2p_metrics->local_request.get_requests.inc(100);
    p2p_metrics->local_request.get_hits.inc(80);
    p2p_metrics->local_request.get_misses.inc(15);
    p2p_metrics->local_request.get_failures.inc(5);
    p2p_metrics->local_request.get_bytes.inc(20 * 1024 * 1024);  // 20 MB
    p2p_metrics->local_request.get_latency_success.observe(100);
    p2p_metrics->local_request.get_latency_success.observe(150);
    p2p_metrics->local_request.get_latency_failure.observe(400);

    // Create and start HTTP server
    coro_http::coro_http_server server(1, test_port);

    using namespace coro_http;

    // Register handlers for P2P metrics
    server.set_http_handler<GET>("/metrics", [&p2p_metrics](
                                                 coro_http_request& req,
                                                 coro_http_response& resp) {
        std::string metrics_str;
        p2p_metrics->serialize(metrics_str);
        resp.add_header("Content-Type", "text/plain; version=0.0.4");
        resp.set_status_and_content(status_type::ok, std::move(metrics_str));
    });

    server.set_http_handler<GET>(
        "/metrics/summary",
        [&p2p_metrics](coro_http_request& req, coro_http_response& resp) {
            std::string summary = p2p_metrics->summary_metrics();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    server.async_start();

    // Wait for server to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Test /metrics endpoint for P2P metrics
    {
        coro_http::coro_http_client client;
        auto resp = client.get("http://127.0.0.1:" + std::to_string(test_port) +
                               "/metrics");
        EXPECT_EQ(resp.status, 200)
            << "P2P metrics endpoint returned wrong status";

        // Check P2P Put metrics
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_put_requests_total") !=
            std::string::npos)
            << "Metrics should contain P2P put requests metric";
        EXPECT_TRUE(resp.resp_body.find("mooncake_p2p_local_put_bytes_total") !=
                    std::string::npos)
            << "Metrics should contain P2P put bytes metric";
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_put_latency_success_us") !=
            std::string::npos)
            << "Metrics should contain P2P put latency success metric";
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_put_latency_failure_us") !=
            std::string::npos)
            << "Metrics should contain P2P put latency failure metric";

        // Check P2P Get metrics
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_get_requests_total") !=
            std::string::npos)
            << "Metrics should contain P2P get requests metric";
        EXPECT_TRUE(resp.resp_body.find("mooncake_p2p_local_get_hits_total") !=
                    std::string::npos)
            << "Metrics should contain P2P get hits metric";
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_get_misses_total") !=
            std::string::npos)
            << "Metrics should contain P2P get misses metric";
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_get_failures_total") !=
            std::string::npos)
            << "Metrics should contain P2P get failures metric";
        EXPECT_TRUE(resp.resp_body.find("mooncake_p2p_local_get_bytes_total") !=
                    std::string::npos)
            << "Metrics should contain P2P get bytes metric";
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_get_latency_success_us") !=
            std::string::npos)
            << "Metrics should contain P2P get latency success metric";
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_get_latency_failure_us") !=
            std::string::npos)
            << "Metrics should contain P2P get latency failure metric";

        // Check labels
        EXPECT_TRUE(resp.resp_body.find("p2p_label") != std::string::npos)
            << "Metrics should contain P2P label";
        EXPECT_TRUE(resp.resp_body.find("p2p_value") != std::string::npos)
            << "Metrics should contain P2P label value";

        // Check actual values (Prometheus format with labels:
        // metric_name{label="value"} number)
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_put_requests_total") !=
                std::string::npos &&
            resp.resp_body.find("} 10\n") != std::string::npos)
            << "P2P put requests should be 10";
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_get_requests_total") !=
                std::string::npos &&
            resp.resp_body.find("} 100\n") != std::string::npos)
            << "P2P get requests should be 100";
        EXPECT_TRUE(resp.resp_body.find("mooncake_p2p_local_get_hits_total") !=
                        std::string::npos &&
                    resp.resp_body.find("} 80\n") != std::string::npos)
            << "P2P get hits should be 80";
    }

    // Test /metrics/summary endpoint for P2P metrics
    {
        coro_http::coro_http_client client;
        auto resp = client.get("http://127.0.0.1:" + std::to_string(test_port) +
                               "/metrics/summary");
        EXPECT_EQ(resp.status, 200)
            << "P2P metrics summary endpoint returned wrong status";

        // Check summary contains expected P2P metrics
        EXPECT_TRUE(resp.resp_body.find("P2P Local Request Metrics") !=
                    std::string::npos)
            << "Summary should contain P2P metrics header";
        EXPECT_TRUE(resp.resp_body.find("Put:") != std::string::npos)
            << "Summary should contain put section";
        EXPECT_TRUE(resp.resp_body.find("Get:") != std::string::npos)
            << "Summary should contain get section";
        EXPECT_TRUE(resp.resp_body.find("10 requests") != std::string::npos)
            << "Summary should show 10 put requests";
        EXPECT_TRUE(resp.resp_body.find("100 requests") != std::string::npos)
            << "Summary should show 100 get requests";
        EXPECT_TRUE(resp.resp_body.find("80 hits") != std::string::npos)
            << "Summary should show 80 get hits";
        EXPECT_TRUE(resp.resp_body.find("15 misses") != std::string::npos)
            << "Summary should show 15 get misses";
        EXPECT_TRUE(resp.resp_body.find("5.00 MB") != std::string::npos)
            << "Summary should show 5.00 MB written";
        EXPECT_TRUE(resp.resp_body.find("20.00 MB") != std::string::npos)
            << "Summary should show 20.00 MB read";
    }

    server.stop();
}

// Test combined ClientMetric and P2PClientMetric HTTP endpoints
TEST_F(ClientHttpMetricsTest, CombinedMetricsHttpEndpointsTest) {
    const uint16_t test_port = 19005;

    // Create both ClientMetric and P2PClientMetric instances
    auto metrics = ClientMetric::Create({{"instance", "combined_test"}});
    ASSERT_NE(metrics, nullptr);

    auto p2p_metrics = P2PClientMetric::Create({{"instance", "combined_test"}});
    ASSERT_NE(p2p_metrics, nullptr);

    // Add test data to both metrics
    metrics->transfer_metric.total_read_bytes.inc(1024 * 1024);
    metrics->transfer_metric.total_write_bytes.inc(2 * 1024 * 1024);

    p2p_metrics->local_request.get_requests.inc(50);
    p2p_metrics->local_request.get_hits.inc(40);
    p2p_metrics->local_request.get_bytes.inc(10 * 1024 * 1024);
    p2p_metrics->local_request.put_requests.inc(20);
    p2p_metrics->local_request.put_bytes.inc(5 * 1024 * 1024);

    // Create and start HTTP server
    coro_http::coro_http_server server(1, test_port);

    using namespace coro_http;

    // Register combined metrics handler
    server.set_http_handler<GET>("/metrics", [&metrics, &p2p_metrics](
                                                 coro_http_request& req,
                                                 coro_http_response& resp) {
        std::string metrics_str;
        metrics->serialize(metrics_str);
        p2p_metrics->serialize(metrics_str);
        resp.add_header("Content-Type", "text/plain; version=0.0.4");
        resp.set_status_and_content(status_type::ok, std::move(metrics_str));
    });

    server.set_http_handler<GET>(
        "/metrics/summary", [&metrics, &p2p_metrics](coro_http_request& req,
                                                     coro_http_response& resp) {
            std::string summary;
            summary += metrics->summary_metrics();
            summary += "\n";
            summary += p2p_metrics->summary_metrics();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    server.async_start();

    // Wait for server to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Test combined /metrics endpoint
    {
        coro_http::coro_http_client client;
        auto resp = client.get("http://127.0.0.1:" + std::to_string(test_port) +
                               "/metrics");
        EXPECT_EQ(resp.status, 200);

        // Check both transfer metrics and P2P metrics are present
        EXPECT_TRUE(resp.resp_body.find("mooncake_transfer_read_bytes") !=
                    std::string::npos)
            << "Should contain transfer read bytes";
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_get_requests_total") !=
            std::string::npos)
            << "Should contain P2P get requests";
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_put_requests_total") !=
            std::string::npos)
            << "Should contain P2P put requests";
    }

    // Test combined /metrics/summary endpoint
    {
        coro_http::coro_http_client client;
        auto resp = client.get("http://127.0.0.1:" + std::to_string(test_port) +
                               "/metrics/summary");
        EXPECT_EQ(resp.status, 200);

        // Check both transfer and P2P summaries are present
        EXPECT_TRUE(resp.resp_body.find("Transfer Metrics Summary") !=
                    std::string::npos)
            << "Should contain transfer metrics summary";
        EXPECT_TRUE(resp.resp_body.find("P2P Local Request Metrics") !=
                    std::string::npos)
            << "Should contain P2P metrics summary";
        EXPECT_TRUE(resp.resp_body.find("Total Read") != std::string::npos)
            << "Should contain transfer total read";
        EXPECT_TRUE(resp.resp_body.find("Get:") != std::string::npos)
            << "Should contain get section";
        EXPECT_TRUE(resp.resp_body.find("Put:") != std::string::npos)
            << "Should contain put section";
    }

    server.stop();
}

// Test P2P client peer_request metrics HTTP endpoints
TEST_F(ClientHttpMetricsTest, P2PClientPeerMetricsHttpEndpointsTest) {
    const uint16_t test_port = 19006;

    // Create P2PClientMetric instance
    auto p2p_metrics = P2PClientMetric::Create({{"p2p_label", "peer_test"}});
    ASSERT_NE(p2p_metrics, nullptr);

    // Add test data to both local_request and peer_request
    // Local Put metrics
    p2p_metrics->local_request.put_requests.inc(10);
    p2p_metrics->local_request.put_bytes.inc(5 * 1024 * 1024);  // 5 MB
    p2p_metrics->local_request.put_latency_success.observe(200);

    // Local Get metrics
    p2p_metrics->local_request.get_requests.inc(100);
    p2p_metrics->local_request.get_hits.inc(80);
    p2p_metrics->local_request.get_misses.inc(15);
    p2p_metrics->local_request.get_failures.inc(5);
    p2p_metrics->local_request.get_bytes.inc(20 * 1024 * 1024);  // 20 MB
    p2p_metrics->local_request.get_latency_success.observe(100);

    p2p_metrics->local_request.write_revoke_requests.inc(2);
    p2p_metrics->local_request.write_revoke_failures.inc(1);
    p2p_metrics->local_request.write_revoke_latency_success.observe(70);
    p2p_metrics->local_request.unpin_key_requests.inc(4);
    p2p_metrics->local_request.unpin_key_failures.inc(1);
    p2p_metrics->local_request.unpin_key_latency_success.observe(55);

    // Peer per-RPC metrics (write: no bytes, aligned with local put minus bytes)
    p2p_metrics->peer_request_metrics.write_remote_data.requests.inc(30);
    p2p_metrics->peer_request_metrics.write_remote_data.failures.inc(2);
    p2p_metrics->peer_request_metrics.write_remote_data.latency_success.observe(250);
    p2p_metrics->peer_request_metrics.write_remote_data.latency_failure.observe(350);

    p2p_metrics->peer_request_metrics.read_remote_data.requests.inc(200);
    p2p_metrics->peer_request_metrics.read_remote_data.hits.inc(150);
    p2p_metrics->peer_request_metrics.read_remote_data.misses.inc(40);
    p2p_metrics->peer_request_metrics.read_remote_data.failures.inc(10);
    p2p_metrics->peer_request_metrics.read_remote_data.latency_success.observe(120);
    p2p_metrics->peer_request_metrics.read_remote_data.latency_failure.observe(180);

    // Create and start HTTP server
    coro_http::coro_http_server server(1, test_port);

    using namespace coro_http;

    // Register handlers for P2P metrics
    server.set_http_handler<GET>("/metrics", [&p2p_metrics](
                                                 coro_http_request& req,
                                                 coro_http_response& resp) {
        std::string metrics_str;
        p2p_metrics->serialize(metrics_str);
        resp.add_header("Content-Type", "text/plain; version=0.0.4");
        resp.set_status_and_content(status_type::ok, std::move(metrics_str));
    });

    server.set_http_handler<GET>(
        "/metrics/summary",
        [&p2p_metrics](coro_http_request& req, coro_http_response& resp) {
            std::string summary = p2p_metrics->summary_metrics();
            resp.add_header("Content-Type", "text/plain; version=0.0.4");
            resp.set_status_and_content(status_type::ok, std::move(summary));
        });

    server.async_start();

    // Wait for server to start
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Test /metrics endpoint for P2P peer metrics
    {
        coro_http::coro_http_client client;
        auto resp = client.get("http://127.0.0.1:" + std::to_string(test_port) +
                               "/metrics");
        EXPECT_EQ(resp.status, 200)
            << "P2P peer metrics endpoint returned wrong status";

        // Check local metrics are present
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_get_requests_total") !=
            std::string::npos)
            << "Metrics should contain local get requests metric";
        EXPECT_TRUE(
            resp.resp_body.find("mooncake_p2p_local_put_requests_total") !=
            std::string::npos)
            << "Metrics should contain local put requests metric";
        EXPECT_TRUE(resp.resp_body.find(
                        "mooncake_p2p_local_write_revoke_requests_total") !=
                    std::string::npos)
            << "Metrics should contain local write_revoke rollback requests";
        EXPECT_TRUE(resp.resp_body.find(
                        "mooncake_p2p_local_unpin_key_requests_total") !=
                    std::string::npos)
            << "Metrics should contain local unpin_key rollback requests";

        // Check peer per-RPC metrics are present
        EXPECT_TRUE(resp.resp_body.find(
                        "mooncake_p2p_peer_read_remote_data_requests_total") !=
                    std::string::npos)
            << "Metrics should contain peer read_remote_data requests";
        EXPECT_TRUE(resp.resp_body.find(
                        "mooncake_p2p_peer_read_remote_data_hits_total") !=
                    std::string::npos)
            << "Metrics should contain peer read_remote_data hits";
        EXPECT_TRUE(resp.resp_body.find(
                        "mooncake_p2p_peer_read_remote_data_misses_total") !=
                    std::string::npos)
            << "Metrics should contain peer read_remote_data misses";
        EXPECT_TRUE(resp.resp_body.find(
                        "mooncake_p2p_peer_read_remote_data_failures_total") !=
                    std::string::npos)
            << "Metrics should contain peer read_remote_data failures";
        EXPECT_TRUE(resp.resp_body.find(
                        "mooncake_p2p_peer_read_remote_data_latency_success_us") !=
                    std::string::npos)
            << "Metrics should contain peer read_remote_data latency success";
        EXPECT_TRUE(resp.resp_body.find(
                        "mooncake_p2p_peer_write_remote_data_requests_total") !=
                    std::string::npos)
            << "Metrics should contain peer write_remote_data requests";
        EXPECT_TRUE(resp.resp_body.find(
                        "mooncake_p2p_peer_write_remote_data_failures_total") !=
                    std::string::npos)
            << "Metrics should contain peer write_remote_data failures";
        EXPECT_TRUE(resp.resp_body.find(
                        "mooncake_p2p_peer_write_remote_data_latency_success_us") !=
                    std::string::npos)
            << "Metrics should contain peer write_remote_data latency success";
        EXPECT_TRUE(resp.resp_body.find(
                        "mooncake_p2p_peer_write_remote_data_latency_failure_us") !=
                    std::string::npos)
            << "Metrics should contain peer write_remote_data latency failure";

        EXPECT_TRUE(
            resp.resp_body.find(
                "mooncake_p2p_peer_read_remote_data_requests_total") !=
                std::string::npos &&
            resp.resp_body.find("} 200\n") != std::string::npos)
            << "Peer read_remote_data requests should be 200";
        EXPECT_TRUE(
            resp.resp_body.find(
                "mooncake_p2p_peer_read_remote_data_hits_total") !=
                std::string::npos &&
            resp.resp_body.find("} 150\n") != std::string::npos)
            << "Peer read_remote_data hits should be 150";
        EXPECT_TRUE(
            resp.resp_body.find(
                "mooncake_p2p_peer_write_remote_data_requests_total") !=
                std::string::npos &&
            resp.resp_body.find("} 30\n") != std::string::npos)
            << "Peer write_remote_data requests should be 30";
        EXPECT_TRUE(
            resp.resp_body.find(
                "mooncake_p2p_peer_write_remote_data_failures_total") !=
                std::string::npos &&
            resp.resp_body.find("} 2\n") != std::string::npos)
            << "Peer write_remote_data failures should be 2";
    }

    // Test /metrics/summary endpoint for P2P peer metrics
    {
        coro_http::coro_http_client client;
        auto resp = client.get("http://127.0.0.1:" + std::to_string(test_port) +
                               "/metrics/summary");
        EXPECT_EQ(resp.status, 200)
            << "P2P peer metrics summary endpoint returned wrong status";

        // Check summary contains both local and peer metrics
        EXPECT_TRUE(resp.resp_body.find("P2P Local Request Metrics") !=
                    std::string::npos)
            << "Summary should contain local metrics header";
        EXPECT_TRUE(resp.resp_body.find("P2P Peer Request Metrics") !=
                    std::string::npos)
            << "Summary should contain peer metrics header";

        // Check local metrics in summary
        EXPECT_TRUE(resp.resp_body.find("100 requests") != std::string::npos)
            << "Summary should show 100 local get requests";
        EXPECT_TRUE(resp.resp_body.find("80 hits") != std::string::npos)
            << "Summary should show 80 local get hits";

        // Check peer per-RPC metrics in summary
        EXPECT_TRUE(resp.resp_body.find("ReadRemoteData: 200 requests") !=
                    std::string::npos)
            << "Summary should show peer ReadRemoteData requests";
        EXPECT_TRUE(resp.resp_body.find("150 hits") != std::string::npos)
            << "Summary should show peer ReadRemoteData hits";
        EXPECT_TRUE(resp.resp_body.find("WriteRemoteData: 30 requests") !=
                    std::string::npos)
            << "Summary should show peer WriteRemoteData requests";
        EXPECT_TRUE(resp.resp_body.find("2 failures") != std::string::npos)
            << "Summary should show peer WriteRemoteData failures";
        EXPECT_TRUE(resp.resp_body.find("WriteRevoke rollback: 2 requests") !=
                    std::string::npos)
            << "Summary should show local WriteRevoke rollback requests";
        EXPECT_TRUE(resp.resp_body.find("UnPinKey rollback: 4 requests") !=
                    std::string::npos)
            << "Summary should show local UnPinKey rollback requests";
    }

    server.stop();
}

}  // namespace mooncake::test
