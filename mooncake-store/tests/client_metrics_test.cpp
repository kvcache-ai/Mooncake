#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "client_metric.h"

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

TEST_F(ClientMetricsTest, LocalStorageMetricBasicTest) {
    LocalStorageMetric metrics;

    // Test empty metrics
    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("Local Put: 0 requests") != std::string::npos);
    EXPECT_TRUE(summary.find("Local Get: 0 requests") != std::string::npos);
    EXPECT_TRUE(summary.find("0 misses") != std::string::npos);

    // Add put data
    metrics.put_requests.inc();
    metrics.put_requests.inc();
    metrics.put_failures.inc();
    metrics.put_bytes.inc(1024 * 1024);  // 1 MB
    metrics.put_latency_us.observe(200);
    metrics.put_latency_us.observe(300);

    // Add get data
    metrics.get_requests.inc();
    metrics.get_requests.inc();
    metrics.get_requests.inc();
    metrics.get_failures.inc();
    metrics.get_misses.inc();
    metrics.get_bytes.inc(2 * 1024 * 1024);  // 2 MB
    metrics.get_latency_us.observe(100);
    metrics.get_latency_us.observe(150);

    summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("Local Put: 2 requests") != std::string::npos);
    EXPECT_TRUE(summary.find("1.00 MB written") != std::string::npos);
    EXPECT_TRUE(summary.find("Local Get: 3 requests") != std::string::npos);
    EXPECT_TRUE(summary.find("2.00 MB read") != std::string::npos);
    EXPECT_TRUE(summary.find("1 misses") != std::string::npos);

    std::cout << "Local Storage Metrics Summary:\n" << summary << std::endl;
}

TEST_F(ClientMetricsTest, LocalStorageMetricSerializeTest) {
    LocalStorageMetric metrics;

    // Add some data
    metrics.put_requests.inc(100);
    metrics.put_bytes.inc(50 * 1024 * 1024);  // 50 MB
    metrics.get_requests.inc(500);
    metrics.get_misses.inc(20);
    metrics.get_bytes.inc(100 * 1024 * 1024);  // 100 MB

    // Add latency data to test histogram output
    metrics.put_latency_us.observe(200);
    metrics.put_latency_us.observe(300);
    metrics.get_latency_us.observe(100);

    std::string serialized;
    metrics.serialize(serialized);

    // Verify Prometheus format output
    EXPECT_TRUE(
        serialized.find("mooncake_client_local_put_requests_total 100") !=
        std::string::npos);
    EXPECT_TRUE(
        serialized.find("mooncake_client_local_put_bytes_total 52428800") !=
        std::string::npos);
    EXPECT_TRUE(
        serialized.find("mooncake_client_local_get_requests_total 500") !=
        std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_client_local_get_misses_total 20") !=
                std::string::npos);
    EXPECT_TRUE(
        serialized.find("mooncake_client_local_get_bytes_total 104857600") !=
        std::string::npos);

    // Verify HELP and TYPE annotations for counter metrics
    EXPECT_TRUE(
        serialized.find("# HELP mooncake_client_local_put_requests_total") !=
        std::string::npos);
    EXPECT_TRUE(
        serialized.find(
            "# TYPE mooncake_client_local_put_requests_total counter") !=
        std::string::npos);

    // Verify histogram metrics are present (only output when data exists)
    EXPECT_TRUE(serialized.find("mooncake_client_local_put_latency_us") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_client_local_get_latency_us") !=
                std::string::npos);

    std::cout << "Local Storage Serialized Metrics:\n"
              << serialized << std::endl;
}

TEST_F(ClientMetricsTest, LocalStorageMetricWithLabelsTest) {
    std::map<std::string, std::string> labels = {
        {"instance_id", "test-instance"}, {"deployment_mode", "p2p"}};

    LocalStorageMetric metrics(labels);
    metrics.put_requests.inc();
    metrics.get_requests.inc();

    std::string serialized;
    metrics.serialize(serialized);

    // Verify labels are present in output
    EXPECT_TRUE(serialized.find("instance_id=\"test-instance\"") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("deployment_mode=\"p2p\"") !=
                std::string::npos);
}

TEST_F(ClientMetricsTest, ClientMetricWithLocalStorageTest) {
    ClientMetric metrics;

    // Add data to all metric types
    metrics.transfer_metric.total_read_bytes.inc(1024);
    metrics.master_client_metric.rpc_count.inc({"GetReplicaList"});
    metrics.local_storage_metric.put_requests.inc(10);
    metrics.local_storage_metric.get_requests.inc(20);
    metrics.local_storage_metric.get_misses.inc(5);

    std::string summary = metrics.summary_metrics();

    // Verify all sections are present
    EXPECT_TRUE(summary.find("Transfer Metrics Summary") != std::string::npos);
    EXPECT_TRUE(summary.find("RPC Metrics Summary") != std::string::npos);
    EXPECT_TRUE(summary.find("Local Storage Metrics Summary") !=
                std::string::npos);

    // Verify local storage data
    EXPECT_TRUE(summary.find("Local Put: 10 requests") != std::string::npos);
    EXPECT_TRUE(summary.find("Local Get: 20 requests") != std::string::npos);
    EXPECT_TRUE(summary.find("5 misses") != std::string::npos);

    std::string serialized;
    metrics.serialize(serialized);

    // Verify all metric types in serialized output
    EXPECT_TRUE(serialized.find("mooncake_transfer_read_bytes") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_client_rpc_count") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_client_local_put_requests_total") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_client_local_get_requests_total") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_client_local_get_misses_total") !=
                std::string::npos);

    std::cout << "Client Metric Summary with Local Storage:\n"
              << summary << std::endl;
}

}  // namespace mooncake::test
