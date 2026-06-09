#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "client_metric.h"
#include "p2p_client_metric.h"

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

TEST_F(ClientMetricsTest, P2PClientMetricBasicTest) {
    P2PClientMetric metrics;

    // Test empty metrics
    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("Get: 0 requests") != std::string::npos);
    EXPECT_TRUE(summary.find("Put: 0 requests") != std::string::npos);
    EXPECT_TRUE(summary.find("WriteRevoke rollback: 0 requests") !=
                std::string::npos);
    EXPECT_TRUE(summary.find("UnPinKey rollback: 0 requests") !=
                std::string::npos);

    // Add put data
    metrics.local_request.put_requests.inc();
    metrics.local_request.put_requests.inc();
    metrics.local_request.put_failures.inc();
    metrics.local_request.put_bytes.inc(1024 * 1024);  // 1 MB
    metrics.local_request.put_latency_success.observe(200);
    metrics.local_request.put_latency_success.observe(300);
    metrics.local_request.put_latency_failure.observe(500);

    metrics.local_request.write_revoke_requests.inc(3);
    metrics.local_request.write_revoke_failures.inc(1);
    metrics.local_request.write_revoke_latency_success.observe(80);
    metrics.local_request.write_revoke_latency_failure.observe(120);

    metrics.local_request.unpin_key_requests.inc(5);
    metrics.local_request.unpin_key_failures.inc(2);
    metrics.local_request.unpin_key_latency_success.observe(60);
    metrics.local_request.unpin_key_latency_failure.observe(90);

    // Add get data
    metrics.local_request.get_requests.inc();
    metrics.local_request.get_requests.inc();
    metrics.local_request.get_requests.inc();
    metrics.local_request.get_failures.inc();
    metrics.local_request.get_misses.inc();
    metrics.local_request.get_hits.inc();
    metrics.local_request.get_bytes.inc(2 * 1024 * 1024);  // 2 MB
    metrics.local_request.get_latency_success.observe(100);
    metrics.local_request.get_latency_success.observe(150);
    metrics.local_request.get_latency_failure.observe(400);

    summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("Put: 2 requests") != std::string::npos);
    EXPECT_TRUE(summary.find("1.00 MB written") != std::string::npos);
    EXPECT_TRUE(summary.find("Get: 3 requests") != std::string::npos);
    EXPECT_TRUE(summary.find("2.00 MB read") != std::string::npos);
    EXPECT_TRUE(summary.find("1 misses") != std::string::npos);
    EXPECT_TRUE(summary.find("1 hits") != std::string::npos);
    EXPECT_TRUE(summary.find("WriteRevoke rollback: 3 requests") !=
                std::string::npos);
    EXPECT_TRUE(summary.find("UnPinKey rollback: 5 requests") !=
                std::string::npos);

    std::cout << "P2P Client Metrics Summary:\n" << summary << std::endl;
}

TEST_F(ClientMetricsTest, P2PClientMetricSerializeTest) {
    P2PClientMetric metrics;

    // Add some data
    metrics.local_request.put_requests.inc(100);
    metrics.local_request.put_bytes.inc(50 * 1024 * 1024);  // 50 MB
    metrics.local_request.get_requests.inc(500);
    metrics.local_request.get_misses.inc(20);
    metrics.local_request.get_hits.inc(480);
    metrics.local_request.get_bytes.inc(100 * 1024 * 1024);  // 100 MB

    // Add latency data to test histogram output
    metrics.local_request.put_latency_success.observe(200);
    metrics.local_request.put_latency_success.observe(300);
    metrics.local_request.put_latency_failure.observe(500);
    metrics.local_request.get_latency_success.observe(100);
    metrics.local_request.get_latency_failure.observe(400);
    metrics.local_request.write_revoke_requests.inc(4);
    metrics.local_request.unpin_key_requests.inc(6);

    std::string serialized;
    metrics.serialize(serialized);

    // Verify Prometheus format output
    EXPECT_TRUE(serialized.find("mooncake_p2p_local_put_requests_total 100") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find(
                    "mooncake_p2p_local_write_revoke_requests_total 4") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find(
                    "mooncake_p2p_local_unpin_key_requests_total 6") !=
                std::string::npos);
    EXPECT_TRUE(
        serialized.find("mooncake_p2p_local_put_bytes_total 52428800") !=
        std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_local_get_requests_total 500") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_local_get_misses_total 20") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_local_get_hits_total 480") !=
                std::string::npos);
    EXPECT_TRUE(
        serialized.find("mooncake_p2p_local_get_bytes_total 104857600") !=
        std::string::npos);

    // Verify histogram metrics are present (only output when data exists)
    EXPECT_TRUE(serialized.find("mooncake_p2p_local_put_latency_success_us") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_local_put_latency_failure_us") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_local_get_latency_success_us") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_local_get_latency_failure_us") !=
                std::string::npos);

    std::cout << "P2P Client Serialized Metrics:\n" << serialized << std::endl;
}

TEST_F(ClientMetricsTest, P2PClientMetricWithLabelsTest) {
    std::map<std::string, std::string> labels = {
        {"instance_id", "test-instance"}, {"deployment_mode", "p2p"}};

    auto metrics = P2PClientMetric::Create(labels);
    ASSERT_NE(metrics, nullptr);
    metrics->local_request.put_requests.inc();
    metrics->local_request.get_requests.inc();

    std::string serialized;
    metrics->serialize(serialized);

    // Verify labels are present in output
    EXPECT_TRUE(serialized.find("instance_id=\"test-instance\"") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("deployment_mode=\"p2p\"") !=
                std::string::npos);
}

// Test P2PClientMetric inheritance from ClientMetric
TEST_F(ClientMetricsTest, P2PClientMetricInheritanceTest) {
    auto p2p_metrics = P2PClientMetric::Create({});
    ASSERT_NE(p2p_metrics, nullptr);

    // Add data to both base class metrics and P2P-specific metrics
    p2p_metrics->transfer_metric.total_read_bytes.inc(1024 * 1024);  // 1 MB
    p2p_metrics->transfer_metric.total_write_bytes.inc(2 * 1024 *
                                                       1024);  // 2 MB
    p2p_metrics->local_request.get_requests.inc(100);
    p2p_metrics->local_request.put_requests.inc(50);

    // Test serialize includes both base and P2P metrics
    std::string serialized;
    p2p_metrics->serialize(serialized);
    EXPECT_TRUE(serialized.find("mooncake_transfer_read_bytes") !=
                std::string::npos);  // Base class metric
    EXPECT_TRUE(serialized.find("mooncake_transfer_write_bytes") !=
                std::string::npos);  // Base class metric
    EXPECT_TRUE(serialized.find("mooncake_p2p_local_get_requests_total") !=
                std::string::npos);  // P2P-specific metric
    EXPECT_TRUE(serialized.find("mooncake_p2p_local_put_requests_total") !=
                std::string::npos);  // P2P-specific metric

    // Test summary_metrics includes both base and P2P metrics
    std::string summary = p2p_metrics->summary_metrics();
    EXPECT_TRUE(summary.find("Transfer Metrics Summary") !=
                std::string::npos);  // Base class summary
    EXPECT_TRUE(summary.find("RPC Metrics Summary") !=
                std::string::npos);  // Base class summary
    EXPECT_TRUE(summary.find("P2P Local Request Metrics") !=
                std::string::npos);  // P2P-specific summary
    EXPECT_TRUE(summary.find("Get: 100 requests") != std::string::npos);
    EXPECT_TRUE(summary.find("Put: 50 requests") != std::string::npos);
}

// Test P2PClientMetric peer_request_metrics (per-RPC peer metrics)
TEST_F(ClientMetricsTest, P2PClientMetricPeerRequestTest) {
    P2PClientMetric metrics;

    metrics.peer_request_metrics.read_remote_data.requests.inc(100);
    metrics.peer_request_metrics.read_remote_data.hits.inc(80);
    metrics.peer_request_metrics.read_remote_data.misses.inc(15);
    metrics.peer_request_metrics.read_remote_data.failures.inc(5);
    metrics.peer_request_metrics.read_remote_data.latency_success.observe(120);

    metrics.peer_request_metrics.write_remote_data.requests.inc(50);
    metrics.peer_request_metrics.write_remote_data.failures.inc(2);
    metrics.peer_request_metrics.write_remote_data.latency_success.observe(300);

    metrics.peer_request_metrics.prewrite.requests.inc(20);

    std::string serialized;
    metrics.serialize(serialized);

    EXPECT_TRUE(serialized.find(
                    "mooncake_p2p_peer_read_remote_data_requests_total 100") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find(
                    "mooncake_p2p_peer_read_remote_data_hits_total 80") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find(
                    "mooncake_p2p_peer_read_remote_data_misses_total 15") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find(
                    "mooncake_p2p_peer_read_remote_data_failures_total 5") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find(
                    "mooncake_p2p_peer_read_remote_data_latency_success_us") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find(
                    "mooncake_p2p_peer_write_remote_data_requests_total 50") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find(
                    "mooncake_p2p_peer_prewrite_requests_total 20") !=
                std::string::npos);

    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("P2P Peer Request Metrics") != std::string::npos);
    EXPECT_TRUE(summary.find("ReadRemoteData: 100 requests") !=
                std::string::npos);
    EXPECT_TRUE(summary.find("80 hits") != std::string::npos);
    EXPECT_TRUE(summary.find("15 misses") != std::string::npos);
    EXPECT_TRUE(summary.find("5 failures") != std::string::npos);
    EXPECT_TRUE(summary.find("WriteRemoteData: 50 requests") !=
                std::string::npos);
    EXPECT_TRUE(summary.find("PreWrite: 20 requests") != std::string::npos);
}

// Test both local_request and peer_request_metrics together
TEST_F(ClientMetricsTest, P2PClientMetricBothLocalAndPeerTest) {
    P2PClientMetric metrics;

    metrics.local_request.get_requests.inc(1000);
    metrics.local_request.get_hits.inc(900);
    metrics.local_request.get_misses.inc(50);
    metrics.local_request.get_failures.inc(50);
    metrics.local_request.get_bytes.inc(100 * 1024 * 1024);  // 100 MB

    metrics.peer_request_metrics.pin_key.requests.inc(500);
    metrics.peer_request_metrics.pin_key.hits.inc(400);

    std::string serialized;
    metrics.serialize(serialized);

    EXPECT_TRUE(serialized.find("mooncake_p2p_local_get_requests_total 1000") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_peer_pin_key_requests_total 500") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_local_get_hits_total 900") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_peer_pin_key_hits_total 400") !=
                std::string::npos);

    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("P2P Local Request Metrics") != std::string::npos);
    EXPECT_TRUE(summary.find("P2P Peer Request Metrics") != std::string::npos);

    std::cout << "P2P Both Local and Peer Metrics Summary:\n"
              << summary << std::endl;
}

// Test ClientMetric::Create returns nullptr when disabled
TEST_F(ClientMetricsTest, ClientMetricCreateDisabledTest) {
    // Save current env and set to disable
    const char* original = std::getenv("MC_STORE_CLIENT_METRIC");

    // Set to disable
    setenv("MC_STORE_CLIENT_METRIC", "0", 1);
    auto metrics_disabled = ClientMetric::Create({});
    EXPECT_EQ(metrics_disabled, nullptr);

    // Set to enable
    setenv("MC_STORE_CLIENT_METRIC", "1", 1);
    auto metrics_enabled = ClientMetric::Create({});
    EXPECT_NE(metrics_enabled, nullptr);

    // Restore original value
    if (original) {
        setenv("MC_STORE_CLIENT_METRIC", original, 1);
    } else {
        unsetenv("MC_STORE_CLIENT_METRIC");
    }
}

// Test P2PClientMetric::Create returns nullptr when disabled
TEST_F(ClientMetricsTest, P2PClientMetricCreateDisabledTest) {
    // Save current env and set to disable
    const char* original = std::getenv("MC_STORE_CLIENT_METRIC");

    // Set to disable
    setenv("MC_STORE_CLIENT_METRIC", "false", 1);
    auto p2p_disabled = P2PClientMetric::Create({});
    EXPECT_EQ(p2p_disabled, nullptr);

    // Set to enable
    setenv("MC_STORE_CLIENT_METRIC", "true", 1);
    auto p2p_enabled = P2PClientMetric::Create({});
    EXPECT_NE(p2p_enabled, nullptr);

    // Restore original value
    if (original) {
        setenv("MC_STORE_CLIENT_METRIC", original, 1);
    } else {
        unsetenv("MC_STORE_CLIENT_METRIC");
    }
}

// Test IsEnabled and GetDefaultInterval
TEST_F(ClientMetricsTest, MetricEnvironmentVariablesTest) {
    const char* original_metric = std::getenv("MC_STORE_CLIENT_METRIC");
    const char* original_interval =
        std::getenv("MC_STORE_CLIENT_METRIC_INTERVAL");

    // Test IsEnabled with various values
    setenv("MC_STORE_CLIENT_METRIC", "1", 1);
    EXPECT_TRUE(ClientMetric::IsEnabled());

    setenv("MC_STORE_CLIENT_METRIC", "0", 1);
    EXPECT_FALSE(ClientMetric::IsEnabled());

    setenv("MC_STORE_CLIENT_METRIC", "true", 1);
    EXPECT_TRUE(ClientMetric::IsEnabled());

    setenv("MC_STORE_CLIENT_METRIC", "false", 1);
    EXPECT_FALSE(ClientMetric::IsEnabled());

    // Test GetDefaultInterval
    setenv("MC_STORE_CLIENT_METRIC_INTERVAL", "10", 1);
    EXPECT_EQ(ClientMetric::GetDefaultInterval(), 10);

    setenv("MC_STORE_CLIENT_METRIC_INTERVAL", "0", 1);
    EXPECT_EQ(ClientMetric::GetDefaultInterval(), 0);

    // Test with invalid value
    setenv("MC_STORE_CLIENT_METRIC_INTERVAL", "invalid", 1);
    EXPECT_EQ(ClientMetric::GetDefaultInterval(), 0);

    // Restore original values
    if (original_metric) {
        setenv("MC_STORE_CLIENT_METRIC", original_metric, 1);
    } else {
        unsetenv("MC_STORE_CLIENT_METRIC");
    }
    if (original_interval) {
        setenv("MC_STORE_CLIENT_METRIC_INTERVAL", original_interval, 1);
    } else {
        unsetenv("MC_STORE_CLIENT_METRIC_INTERVAL");
    }
}

}  // namespace mooncake::test
