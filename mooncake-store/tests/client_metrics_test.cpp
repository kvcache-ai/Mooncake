#include <glog/logging.h>
#include <gtest/gtest.h>

#include <array>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <optional>
#include <string>

#include "client_metric.h"
#include "utils.h"
#include <ylt/coro_http/coro_http_server.hpp>
#include <ylt/coro_http/coro_http_client.hpp>

namespace mooncake::test {

class ClientMetricsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ClientMetricsTest");
        FLAGS_logtostderr = true;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(ClientMetricsTest, TransferMetricsSummaryTest) {
    TransferMetric metrics;

    // Test empty metrics
    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("Total Read: 0 B") != std::string::npos);
    EXPECT_TRUE(summary.find("Total Write: 0 B") != std::string::npos);
    EXPECT_TRUE(summary.find("Get: No data") != std::string::npos);
    EXPECT_TRUE(summary.find("Put: No data") != std::string::npos);

    // Add some data
    metrics.total_read_bytes.inc(1024);              // 1KB
    metrics.total_write_bytes.inc(2 * 1024 * 1024);  // 2MB

    // Add latency observations
    metrics.get_latency_us.observe(150);  // 150 microseconds
    metrics.get_latency_us.observe(200);  // 200 microseconds
    metrics.get_latency_us.observe(300);  // 300 microseconds

    metrics.put_latency_us.observe(500);  // 500 microseconds
    metrics.put_latency_us.observe(750);  // 750 microseconds

    summary = metrics.summary_metrics();

    // Check byte formatting
    EXPECT_TRUE(summary.find("Total Read: 1.00 KB") != std::string::npos);
    EXPECT_TRUE(summary.find("Total Write: 2.00 MB") != std::string::npos);

    // Check latency summaries
    EXPECT_TRUE(summary.find("Get: count=3") != std::string::npos);
    EXPECT_TRUE(summary.find("Put: count=2") != std::string::npos);

    // Check percentiles are present
    EXPECT_TRUE(summary.find("p95<") != std::string::npos);
    EXPECT_TRUE(summary.find("max<") != std::string::npos);

    std::cout << "Transfer Metrics Summary:\n" << summary << std::endl;
}

TEST_F(ClientMetricsTest, MasterClientMetricsSummaryTest) {
    MasterClientMetric metrics;

    // Test empty metrics
    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("No RPC calls recorded") != std::string::npos);

    // Add some RPC calls
    std::array<std::string, 1> get_replica_label = {"GetReplicaList"};
    std::array<std::string, 1> mount_segment_label = {"MountSegment"};
    std::array<std::string, 1> unmount_segment_label = {"UnmountSegment"};

    // Simulate RPC calls
    metrics.rpc_count.inc(get_replica_label);
    metrics.rpc_count.inc(get_replica_label);
    metrics.rpc_count.inc(mount_segment_label);
    metrics.rpc_count.inc(unmount_segment_label);

    // Add latency observations
    metrics.rpc_latency.observe(get_replica_label, 200);  // 200 microseconds
    metrics.rpc_latency.observe(get_replica_label, 250);  // 250 microseconds
    metrics.rpc_latency.observe(mount_segment_label, 37789);   // 37.789 ms
    metrics.rpc_latency.observe(unmount_segment_label, 7536);  // 7.536 ms

    summary = metrics.summary_metrics();

    // Check that RPC calls are recorded
    EXPECT_TRUE(summary.find("GetReplicaList: count=2") != std::string::npos);
    EXPECT_TRUE(summary.find("MountSegment: count=1") != std::string::npos);
    EXPECT_TRUE(summary.find("UnmountSegment: count=1") != std::string::npos);

    // Check percentiles are present for RPCs with data
    EXPECT_TRUE(summary.find("p95<") != std::string::npos);
    EXPECT_TRUE(summary.find("max<") != std::string::npos);

    std::cout << "Master Client Metrics Summary:\n" << summary << std::endl;
}

TEST_F(ClientMetricsTest, ClientMetricsSummaryTest) {
    ClientMetric metrics;

    // Add some transfer data
    metrics.transfer_metric.total_read_bytes.inc(5 * 1024 * 1024);    // 5MB
    metrics.transfer_metric.total_write_bytes.inc(10 * 1024 * 1024);  // 10MB

    metrics.transfer_metric.batch_get_latency_us.observe(1500);  // 1.5ms
    metrics.transfer_metric.batch_put_latency_us.observe(2000);  // 2ms

    // Add some RPC data
    std::array<std::string, 1> exist_key_label = {"ExistKey"};
    metrics.master_client_metric.rpc_count.inc(exist_key_label);
    metrics.master_client_metric.rpc_latency.observe(exist_key_label, 180);

    std::string summary = metrics.summary_metrics();

    // Should contain both transfer and RPC metrics
    EXPECT_TRUE(summary.find("Transfer Metrics Summary") != std::string::npos);
    EXPECT_TRUE(summary.find("RPC Metrics Summary") != std::string::npos);
    EXPECT_TRUE(summary.find("Total Read: 5.00 MB") != std::string::npos);
    EXPECT_TRUE(summary.find("Total Write: 10.00 MB") != std::string::npos);
    EXPECT_TRUE(summary.find("ExistKey: count=1") != std::string::npos);

    std::cout << "Full Client Metrics Summary:\n" << summary << std::endl;
}

TEST_F(ClientMetricsTest, ByteFormattingTest) {
    TransferMetric metrics;

    // Test different byte sizes
    metrics.total_read_bytes.inc(512);  // 512 B
    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("512 B") != std::string::npos);

    metrics.total_read_bytes.inc(1024 - 512);  // Total 1024 B = 1 KB
    summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("1.00 KB") != std::string::npos);

    metrics.total_read_bytes.inc(1024 * 1024 - 1024);  // Total 1 MB
    summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("1.00 MB") != std::string::npos);

    metrics.total_read_bytes.inc(1024LL * 1024 * 1024 -
                                 1024 * 1024);  // Total 1 GB
    summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("1.00 GB") != std::string::npos);
}

TEST_F(ClientMetricsTest, CompareWithSerializedMetrics) {
    ClientMetric metrics;

    // Add some data
    metrics.transfer_metric.total_read_bytes.inc(1024 * 1024);
    metrics.transfer_metric.get_latency_us.observe(200);

    std::array<std::string, 1> get_replica_label = {"GetReplicaList"};
    metrics.master_client_metric.rpc_count.inc(get_replica_label);
    metrics.master_client_metric.rpc_latency.observe(get_replica_label, 250);

    // Get both summary and full serialized metrics
    std::string summary = metrics.summary_metrics();
    std::string serialized;
    metrics.serialize(serialized);

    std::cout << "\n=== Summary Metrics ===" << std::endl;
    std::cout << summary << std::endl;

    std::cout << "\n=== Full Serialized Metrics ===" << std::endl;
    std::cout << serialized << std::endl;

    // Summary should be much shorter and more readable
    EXPECT_LT(summary.length(), serialized.length());
    EXPECT_TRUE(summary.find("count=") != std::string::npos);
    EXPECT_TRUE(summary.find("p95<") != std::string::npos ||
                summary.find("No data") != std::string::npos);
    EXPECT_TRUE(summary.find("max<") != std::string::npos ||
                summary.find("No data") != std::string::npos);
}

TEST_F(ClientMetricsTest, SerializeWithDynamicLabels) {
    auto verify = [](const std::string& str) {
        EXPECT_TRUE(str.find("instance_id=\"12345\"") != std::string::npos);
        EXPECT_TRUE(str.find("cluster_id=\"cluster1\"") != std::string::npos);
        EXPECT_TRUE(str.find("replica_id=\"replica1\"") != std::string::npos);
        EXPECT_TRUE(str.find("mount_segment_id=\"mount1\"") !=
                    std::string::npos);
    };

    std::map<std::string, std::string> static_labels = {
        {"instance_id", "12345"},
        {"cluster_id", "cluster1"},
        {"replica_id", "replica1"},
        {"mount_segment_id", "mount1"}};
    std::array<std::string, 1> get_replica_label = {"GetReplicaList"};
    {
        ClientMetric metrics(0, static_labels);
        metrics.transfer_metric.total_read_bytes.inc(1024 * 1024);
        std::string serialized;
        metrics.serialize(serialized);
        verify(serialized);
    }

    {
        ClientMetric metrics(0, static_labels);
        metrics.transfer_metric.get_latency_us.observe(200);
        std::string serialized;
        metrics.serialize(serialized);
        verify(serialized);
    }

    {
        ClientMetric metrics(0, static_labels);
        metrics.master_client_metric.rpc_count.inc(get_replica_label);
        std::string serialized;
        metrics.serialize(serialized);
        verify(serialized);
    }

    {
        ClientMetric metrics(0, static_labels);
        metrics.master_client_metric.rpc_latency.observe(get_replica_label,
                                                         250);
        std::string serialized;
        metrics.serialize(serialized);
        verify(serialized);
    }
}

TEST_F(ClientMetricsTest, SerializeWithoutDynamicLabels) {
    auto verify = [](const std::string& str) {
        EXPECT_TRUE(str.find("instance_id") == std::string::npos);
        EXPECT_TRUE(str.find("cluster_id") == std::string::npos);
        EXPECT_TRUE(str.find("replica_id") == std::string::npos);
        EXPECT_TRUE(str.find("mount_segment_id") == std::string::npos);
    };

    std::array<std::string, 1> get_replica_label = {"GetReplicaList"};
    {
        ClientMetric metrics(0);
        metrics.transfer_metric.total_read_bytes.inc(1024 * 1024);
        std::string serialized;
        metrics.serialize(serialized);
        verify(serialized);
    }

    {
        ClientMetric metrics(0);
        metrics.transfer_metric.get_latency_us.observe(200);
        std::string serialized;
        metrics.serialize(serialized);
        verify(serialized);
    }

    {
        ClientMetric metrics(0);
        metrics.master_client_metric.rpc_count.inc(get_replica_label);
        std::string serialized;
        metrics.serialize(serialized);
        verify(serialized);
    }

    {
        ClientMetric metrics(0);
        metrics.master_client_metric.rpc_latency.observe(get_replica_label,
                                                         250);
        std::string serialized;
        metrics.serialize(serialized);
        verify(serialized);
    }
}

TEST_F(ClientMetricsTest, SingleSamplePreservesA00Summary) {
    TransferMetric transfer;
    transfer.get_latency_us.observe(5000);
    EXPECT_NE(
        transfer.summary_metrics().find("Get: count=1, p95<125μs, max<5000μs"),
        std::string::npos);

    MasterClientMetric rpc;
    const std::array<std::string, 1> label = {"GetReplicaList"};
    rpc.rpc_count.inc(label, 3);
    EXPECT_EQ(rpc.summary_metrics(),
              "=== RPC Metrics Summary ===\nNo RPC calls recorded\n");
    rpc.rpc_latency.observe(label, 5000);
    EXPECT_EQ(rpc.summary_metrics(),
              "=== RPC Metrics Summary ===\n"
              "GetReplicaList: count=1, p95<125μs, max<5000μs\n");

    // The centralized baseline reports only its fixed RPC name list.
    rpc.rpc_count.inc({"P2POnlyRpc"});
    rpc.rpc_latency.observe({"P2POnlyRpc"}, 5000);
    EXPECT_EQ(rpc.summary_metrics().find("P2POnlyRpc"), std::string::npos);
}

TEST_F(ClientMetricsTest, ClusterLabelsPreserveA00Precedence) {
    const std::map<std::string, std::string> labels = {
        {"cluster_id", "caller-cluster"}, {"instance", "test-instance"}};
    const auto merged = merge_labels(labels);
    const char* cluster = std::getenv("MC_STORE_CLUSTER_ID");
    EXPECT_EQ(merged.at("cluster_id"),
              cluster && *cluster ? cluster : "caller-cluster");
    EXPECT_EQ(merged.at("instance"), "test-instance");
    EXPECT_EQ(labels.at("cluster_id"), "caller-cluster");
}

class ClientMetricFactoryTest : public ClientMetricsTest {
   protected:
    void SetUp() override {
        ClientMetricsTest::SetUp();
        for (size_t i = 0; i < env_names_.size(); ++i) {
            if (const char* value = std::getenv(env_names_[i])) {
                saved_env_[i] = value;
            }
            ASSERT_EQ(unsetenv(env_names_[i]), 0);
        }
    }

    void TearDown() override {
        for (size_t i = 0; i < env_names_.size(); ++i) {
            const int result =
                saved_env_[i] ? setenv(env_names_[i], saved_env_[i]->c_str(), 1)
                              : unsetenv(env_names_[i]);
            EXPECT_EQ(result, 0);
        }
        ClientMetricsTest::TearDown();
    }

   private:
    const std::array<const char*, 2> env_names_ = {
        "MC_STORE_CLIENT_METRIC", "MC_STORE_CLIENT_METRIC_INTERVAL"};
    std::array<std::optional<std::string>, 2> saved_env_;
};

TEST_F(ClientMetricFactoryTest, DefaultsToCollectionWithoutReporting) {
    auto metrics = ClientMetric::Create({{"instance", "centralized"}});
    ASSERT_NE(metrics, nullptr);
    EXPECT_EQ(metrics->GetReportingInterval(), 0u);
    // Untouched zero-valued counters are omitted from serialized output.
    metrics->transfer_metric.total_read_bytes.inc(1);
    std::string serialized;
    metrics->serialize(serialized);
    EXPECT_NE(serialized.find("instance=\"centralized\""), std::string::npos);
    EXPECT_EQ(serialized.find("cluster_id="), std::string::npos);
}

TEST_F(ClientMetricFactoryTest, UsesA00EnableValues) {
    for (const char* value : {"1", "TRUE", "Yes", "ON", "enable"}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MC_STORE_CLIENT_METRIC", value, 1), 0);
        EXPECT_NE(ClientMetric::Create(), nullptr);
    }
    for (const char* value : {"0", "false", "off", "", "invalid"}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MC_STORE_CLIENT_METRIC", value, 1), 0);
        EXPECT_EQ(ClientMetric::Create(), nullptr);
    }
}

TEST_F(ClientMetricFactoryTest, UsesEnvironmentReportingInterval) {
    ASSERT_EQ(setenv("MC_STORE_CLIENT_METRIC_INTERVAL", "7", 1), 0);
    auto metrics = ClientMetric::Create();
    ASSERT_NE(metrics, nullptr);
    EXPECT_EQ(metrics->GetReportingInterval(), 7u);
    metrics.reset();

    for (const char* value : {"0", "invalid", "18446744073709551616"}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MC_STORE_CLIENT_METRIC_INTERVAL", value, 1), 0);
        metrics = ClientMetric::Create();
        ASSERT_NE(metrics, nullptr);
        EXPECT_EQ(metrics->GetReportingInterval(), 0u);
    }
}

TEST_F(ClientMetricsTest, HttpEndpointsTest) {
    // Create ClientMetric instance
    auto metrics = std::make_unique<ClientMetric>(
        0, std::map<std::string, std::string>{{"test_label", "test_value"}});
    ASSERT_NE(metrics, nullptr);

    // Add some test data to metrics
    metrics->transfer_metric.total_read_bytes.inc(1024);
    metrics->transfer_metric.total_write_bytes.inc(2048);
    metrics->transfer_metric.get_latency_us.observe(100);
    metrics->transfer_metric.get_latency_us.observe(200);

    // Create and start HTTP server
    coro_http::coro_http_server server(1, 0);

    using namespace coro_http;

    // Serialize centralized metrics through HTTP handlers
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

    auto started = server.async_start();
    if (started.hasResult()) {
        const auto error = started.value();
        LOG(ERROR) << "HTTP metrics test server stopped during startup: "
                   << error.message();
        FAIL() << "HTTP server startup failed: " << error.message();
    }
    const uint16_t test_port = server.port();
    ASSERT_NE(test_port, 0);

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

TEST_F(ClientMetricFactoryTest, MetricFactoryPoliciesRemainIndependent) {
    ASSERT_EQ(setenv("MC_STORE_CLIENT_METRIC", "false", 1), 0);
    ASSERT_EQ(setenv("MC_STORE_CLIENT_METRIC_INTERVAL", "7", 1), 0);
    EXPECT_EQ(ClientMetric::Create(), nullptr);

    ASSERT_EQ(setenv("MC_STORE_CLIENT_METRIC", "true", 1), 0);
    auto centralized_metrics = ClientMetric::Create();
    ASSERT_NE(centralized_metrics, nullptr);
    EXPECT_EQ(centralized_metrics->GetReportingInterval(), 7u);
}

// Each architecture exposes its own output without concatenating metric sets.
TEST_F(ClientMetricsTest, IndependentMetricsHttpEndpointsTest) {
    ClientMetric centralized_metrics;
    centralized_metrics.transfer_metric.total_read_bytes.inc(1024 * 1024);

    using namespace coro_http;
    coro_http_server server(1, 0);
    const auto register_metrics = [&server](const std::string& prefix,
                                            auto* metrics) {
        server.set_http_handler<GET>(
            prefix + "/metrics",
            [metrics](coro_http_request&, coro_http_response& resp) {
                std::string output;
                metrics->serialize(output);
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                resp.set_status_and_content(status_type::ok, std::move(output));
            });
        server.set_http_handler<GET>(
            prefix + "/metrics/summary",
            [metrics](coro_http_request&, coro_http_response& resp) {
                resp.add_header("Content-Type", "text/plain; version=0.0.4");
                resp.set_status_and_content(status_type::ok,
                                            metrics->summary_metrics());
            });
    };
    register_metrics("/centralized", &centralized_metrics);
    auto started = server.async_start();
    if (started.hasResult()) {
        const auto error = started.value();
        LOG(ERROR) << "HTTP metrics test server stopped during startup: "
                   << error.message();
        FAIL() << "HTTP server startup failed: " << error.message();
    }
    const uint16_t test_port = server.port();
    ASSERT_NE(test_port, 0);

    const std::string base = "http://127.0.0.1:" + std::to_string(test_port);
    coro_http_client client;
    auto resp = client.get(base + "/centralized/metrics");
    EXPECT_EQ(resp.status, 200);
    EXPECT_NE(resp.resp_body.find("mooncake_transfer_read_bytes 1048576"),
              std::string::npos);
    EXPECT_EQ(resp.resp_body.find("mooncake_p2p_"), std::string::npos);
    resp = client.get(base + "/centralized/metrics/summary");
    EXPECT_EQ(resp.status, 200);
    EXPECT_NE(resp.resp_body.find("Total Read: 1.00 MB"), std::string::npos);
    EXPECT_EQ(resp.resp_body.find("P2P Total"), std::string::npos);

    server.stop();
}

}  // namespace mooncake::test
