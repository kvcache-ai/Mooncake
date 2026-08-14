#include <glog/logging.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdlib>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "client_metric.h"
#include "p2p_client_metric.h"
#include "tiered_cache/tiers/cache_tier.h"

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
    EXPECT_TRUE(summary.find("GetReplicaList: total=2") != std::string::npos);
    EXPECT_TRUE(summary.find("MountSegment: total=1") != std::string::npos);
    EXPECT_TRUE(summary.find("UnmountSegment: total=1") != std::string::npos);

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
    EXPECT_TRUE(summary.find("ExistKey: total=1") != std::string::npos);

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
    metrics.total_request.put_requests.inc();
    metrics.total_request.put_requests.inc();
    metrics.total_request.put_failures.inc();
    metrics.total_request.put_bytes.inc(1024 * 1024);  // 1 MB
    metrics.total_request.put_latency_success.observe(200);
    metrics.total_request.put_latency_success.observe(300);
    metrics.total_request.put_latency_failure.observe(500);

    metrics.rollback.write_revoke_requests.inc(3);
    metrics.rollback.write_revoke_failures.inc(1);
    metrics.rollback.write_revoke_latency_success.observe(80);
    metrics.rollback.write_revoke_latency_failure.observe(120);

    metrics.rollback.unpin_key_requests.inc(5);
    metrics.rollback.unpin_key_failures.inc(2);
    metrics.rollback.unpin_key_latency_success.observe(60);
    metrics.rollback.unpin_key_latency_failure.observe(90);

    // Add get data
    metrics.total_request.get_requests.inc();
    metrics.total_request.get_requests.inc();
    metrics.total_request.get_requests.inc();
    metrics.total_request.get_failures.inc();
    metrics.total_request.get_misses.inc();
    metrics.total_request.get_hits.inc();
    metrics.total_request.get_bytes.inc(2 * 1024 * 1024);  // 2 MB
    metrics.total_request.get_latency_success.observe(100);
    metrics.total_request.get_latency_success.observe(150);
    metrics.total_request.get_latency_failure.observe(400);

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
    metrics.total_request.put_requests.inc(100);
    metrics.total_request.put_bytes.inc(50 * 1024 * 1024);  // 50 MB
    metrics.total_request.get_requests.inc(500);
    metrics.total_request.get_misses.inc(20);
    metrics.total_request.get_hits.inc(480);
    metrics.total_request.get_bytes.inc(100 * 1024 * 1024);  // 100 MB

    // Add latency data to test histogram output
    metrics.total_request.put_latency_success.observe(200);
    metrics.total_request.put_latency_success.observe(300);
    metrics.total_request.put_latency_failure.observe(500);
    metrics.total_request.get_latency_success.observe(100);
    metrics.total_request.get_latency_failure.observe(400);
    metrics.rollback.write_revoke_requests.inc(4);
    metrics.rollback.unpin_key_requests.inc(6);

    std::string serialized;
    metrics.serialize(serialized);

    // Verify Prometheus format output
    EXPECT_TRUE(serialized.find("mooncake_p2p_total_put_requests_total 100") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find(
                    "mooncake_p2p_rollback_write_revoke_requests_total 4") !=
                std::string::npos);
    EXPECT_TRUE(
        serialized.find("mooncake_p2p_rollback_unpin_key_requests_total 6") !=
        std::string::npos);
    EXPECT_TRUE(
        serialized.find("mooncake_p2p_total_put_bytes_total 52428800") !=
        std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_total_get_requests_total 500") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_total_get_misses_total 20") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_total_get_hits_total 480") !=
                std::string::npos);
    EXPECT_TRUE(
        serialized.find("mooncake_p2p_total_get_bytes_total 104857600") !=
        std::string::npos);

    // Verify histogram metrics are present (only output when data exists)
    EXPECT_TRUE(serialized.find("mooncake_p2p_total_put_latency_success_us") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_total_put_latency_failure_us") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_total_get_latency_success_us") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_total_get_latency_failure_us") !=
                std::string::npos);

    std::cout << "P2P Client Serialized Metrics:\n" << serialized << std::endl;
}

TEST_F(ClientMetricsTest, P2PClientMetricWithLabelsTest) {
    std::map<std::string, std::string> labels = {
        {"instance_id", "test-instance"}, {"deployment_mode", "p2p"}};

    auto metrics = P2PClientMetric::Create(labels);
    ASSERT_NE(metrics, nullptr);
    metrics->total_request.put_requests.inc();
    metrics->total_request.get_requests.inc();

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
    p2p_metrics->total_request.get_requests.inc(100);
    p2p_metrics->total_request.put_requests.inc(50);

    // Test serialize includes both base and P2P metrics
    std::string serialized;
    p2p_metrics->serialize(serialized);
    EXPECT_TRUE(serialized.find("mooncake_transfer_read_bytes") !=
                std::string::npos);  // Base class metric
    EXPECT_TRUE(serialized.find("mooncake_transfer_write_bytes") !=
                std::string::npos);  // Base class metric
    EXPECT_TRUE(serialized.find("mooncake_p2p_total_get_requests_total") !=
                std::string::npos);  // P2P-specific metric
    EXPECT_TRUE(serialized.find("mooncake_p2p_total_put_requests_total") !=
                std::string::npos);  // P2P-specific metric

    // Test summary_metrics includes both base and P2P metrics.
    // Note: transfer metrics are not recorded in P2P mode, so the P2P
    // summary intentionally skips the transfer section.
    std::string summary = p2p_metrics->summary_metrics();
    EXPECT_TRUE(summary.find("RPC Metrics Summary") !=
                std::string::npos);  // Base class summary
    EXPECT_TRUE(summary.find("P2P Total (per-request)") !=
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
    EXPECT_TRUE(
        serialized.find("mooncake_p2p_peer_read_remote_data_hits_total 80") !=
        std::string::npos);
    EXPECT_TRUE(
        serialized.find("mooncake_p2p_peer_read_remote_data_misses_total 15") !=
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
    EXPECT_TRUE(
        serialized.find("mooncake_p2p_peer_prewrite_requests_total 20") !=
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

// Test both total_request and peer_request_metrics together
TEST_F(ClientMetricsTest, P2PClientMetricBothLocalAndPeerTest) {
    P2PClientMetric metrics;

    metrics.total_request.get_requests.inc(1000);
    metrics.total_request.get_hits.inc(900);
    metrics.total_request.get_misses.inc(50);
    metrics.total_request.get_failures.inc(50);
    metrics.total_request.get_bytes.inc(100 * 1024 * 1024);  // 100 MB

    metrics.peer_request_metrics.pin_key.requests.inc(500);
    metrics.peer_request_metrics.pin_key.hits.inc(400);

    std::string serialized;
    metrics.serialize(serialized);

    EXPECT_TRUE(serialized.find("mooncake_p2p_total_get_requests_total 1000") !=
                std::string::npos);
    EXPECT_TRUE(
        serialized.find("mooncake_p2p_peer_pin_key_requests_total 500") !=
        std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_total_get_hits_total 900") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_peer_pin_key_hits_total 400") !=
                std::string::npos);

    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("P2P Total (per-request)") != std::string::npos);
    EXPECT_TRUE(summary.find("P2P Peer Request Metrics") != std::string::npos);

    std::cout << "P2P Both Local and Peer Metrics Summary:\n"
              << summary << std::endl;
}

TEST_F(ClientMetricsTest, ClientMetricCreateReturnsInstance) {
    auto metrics = ClientMetric::Create({});
    EXPECT_NE(metrics, nullptr);
    // Created without a reporting thread; interval is set at Init.
    EXPECT_EQ(metrics->GetReportingInterval(), 0u);
}

TEST_F(ClientMetricsTest, P2PClientMetricCreateReturnsInstance) {
    auto p2p_metrics = P2PClientMetric::Create({});
    EXPECT_NE(p2p_metrics, nullptr);
    EXPECT_EQ(p2p_metrics->GetReportingInterval(), 0u);
}

TEST_F(ClientMetricsTest, DataMetricRecordGetSemanticsTest) {
    DataMetric dm("mooncake_p2p_test");

    // Success: hit + bytes + success latency sample.
    dm.RecordGet(100, ErrorCode::OK, 64);
    EXPECT_EQ(dm.get_requests.value(), 1);
    EXPECT_EQ(dm.get_hits.value(), 1);
    EXPECT_EQ(dm.get_bytes.value(), 64);

    // Miss: counted, but no latency sample.
    dm.RecordGet(120, ErrorCode::OBJECT_NOT_FOUND, 0);
    EXPECT_EQ(dm.get_requests.value(), 2);
    EXPECT_EQ(dm.get_misses.value(), 1);
    EXPECT_EQ(dm.get_failures.value(), 0);

    // Other errors: failure + failure latency sample.
    dm.RecordGet(200, ErrorCode::INTERNAL_ERROR, 0);
    EXPECT_EQ(dm.get_requests.value(), 3);
    EXPECT_EQ(dm.get_failures.value(), 1);

    std::string summary = dm.summary_metrics();
    EXPECT_TRUE(summary.find("Get: 3 requests, 1 hits, 1 misses, 1 failures") !=
                std::string::npos);
    // Only the OK sample landed in the success histogram.
    EXPECT_TRUE(summary.find("success: count=1") != std::string::npos);
}

TEST_F(ClientMetricsTest, BuildSyncSnapshotGoldenValues) {
    P2PClientMetric metrics;

    // total_request (batch granularity).
    metrics.total_request.RecordGet(100, ErrorCode::OK, 64);
    metrics.total_request.RecordGet(200, ErrorCode::OK, 128);
    metrics.total_request.RecordGet(50, ErrorCode::OBJECT_NOT_FOUND, 0);
    metrics.total_request.RecordPut(300, ErrorCode::OK, 256);

    // local/remote (per-op granularity).
    metrics.local_request.RecordGet(150, ErrorCode::OK, 64);
    metrics.remote_request.RecordGet(250, ErrorCode::OK, 64);
    metrics.remote_request.read_retries.inc();
    metrics.remote_request.write_retries.inc(2);

    auto snap = metrics.BuildSyncSnapshot();

    EXPECT_EQ(snap.total_request.get_requests, 3);
    EXPECT_EQ(snap.total_request.get_hits, 2);
    EXPECT_EQ(snap.total_request.get_misses, 1);
    EXPECT_EQ(snap.total_request.get_failures, 0);
    EXPECT_EQ(snap.total_request.get_bytes, 192);
    EXPECT_EQ(snap.total_request.put_requests, 1);
    EXPECT_EQ(snap.total_request.put_failures, 0);
    EXPECT_EQ(snap.total_request.put_bytes, 256);

    EXPECT_EQ(snap.local_request.get_requests, 1);
    EXPECT_EQ(snap.local_request.get_hits, 1);
    EXPECT_EQ(snap.local_request.get_bytes, 64);

    EXPECT_EQ(snap.remote_request.data.get_requests, 1);
    EXPECT_EQ(snap.remote_request.data.get_bytes, 64);
    EXPECT_EQ(snap.remote_request.read_retries, 1);
    EXPECT_EQ(snap.remote_request.write_retries, 2);
}

TEST_F(ClientMetricsTest, DataMetricRecordPutSemanticsTest) {
    DataMetric dm("mooncake_p2p_test");

    // Success: bytes + success latency sample.
    dm.RecordPut(100, ErrorCode::OK, 128);
    EXPECT_EQ(dm.put_requests.value(), 1);
    EXPECT_EQ(dm.put_bytes.value(), 128);
    EXPECT_EQ(dm.put_failures.value(), 0);

    // Failure: failure counter + failure latency sample, no bytes.
    dm.RecordPut(300, ErrorCode::INTERNAL_ERROR, 128);
    EXPECT_EQ(dm.put_requests.value(), 2);
    EXPECT_EQ(dm.put_failures.value(), 1);
    EXPECT_EQ(dm.put_bytes.value(), 128);

    // Already-exists errors are ignored entirely: no counters, no latency.
    const ErrorCode already_exists[] = {ErrorCode::OBJECT_ALREADY_EXISTS,
                                        ErrorCode::REPLICA_ALREADY_EXISTS,
                                        ErrorCode::REPLICA_NUM_EXCEEDED};
    for (auto err : already_exists) {
        dm.RecordPut(50, err, 10);
    }
    EXPECT_EQ(dm.put_requests.value(), 2);
    EXPECT_EQ(dm.put_failures.value(), 1);
    EXPECT_EQ(dm.put_bytes.value(), 128);

    std::string summary = dm.summary_metrics();
    EXPECT_TRUE(summary.find("Put: 2 requests, 1 failures") !=
                std::string::npos);
    EXPECT_TRUE(summary.find("written") != std::string::npos);
}

TEST_F(ClientMetricsTest, FormatLatencySummarySingleSampleTest) {
    // A single sample in a high bucket must not be reported as p95 of the
    // first bucket (p95_target rounds to at least 1).
    ylt::metric::histogram_t hist("test_single_sample_hist", "test",
                                  kLatencyBucket,
                                  std::map<std::string, std::string>{});
    hist.observe(5000);

    std::string summary = format_latency_summary(hist);
    EXPECT_TRUE(summary.find("count=1") != std::string::npos);
    EXPECT_TRUE(summary.find("p95<5000μs") != std::string::npos);
    EXPECT_TRUE(summary.find("max<5000μs") != std::string::npos);
    EXPECT_TRUE(summary.find("p95<125μs") == std::string::npos);

    // Same boundary through the labeled RPC histogram path.
    MasterClientMetric master_metrics;
    std::array<std::string, 1> label = {"GetReplicaList"};
    master_metrics.rpc_count.inc(label);
    master_metrics.rpc_latency.observe(label, 5000);

    std::string rpc_summary = master_metrics.summary_metrics();
    EXPECT_TRUE(
        rpc_summary.find("GetReplicaList: total=1, success=1, p95<5000μs") !=
        std::string::npos);
    EXPECT_TRUE(rpc_summary.find("p95<125μs") == std::string::npos);
}

TEST_F(ClientMetricsTest, P2PClientMetricRetryCountersTest) {
    P2PClientMetric metrics;
    metrics.remote_request.write_retries.inc(3);
    metrics.remote_request.read_retries.inc(2);

    std::string serialized;
    metrics.serialize(serialized);
    EXPECT_TRUE(serialized.find("mooncake_p2p_remote_write_retries_total 3") !=
                std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_p2p_remote_read_retries_total 2") !=
                std::string::npos);

    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("Retries: write=3, read=2") != std::string::npos);
}

namespace {
// Minimal CacheTier stub with fixed capacity/usage for TierMetric tests.
class StubStatsTier : public CacheTier {
   public:
    StubStatsTier(UUID id, size_t capacity, size_t usage, MemoryType type)
        : id_(id), capacity_(capacity), usage_(usage), type_(type) {}

    tl::expected<void, ErrorCode> Init(TieredBackend*,
                                       TransferEngine*) override {
        return {};
    }
    tl::expected<void, ErrorCode> Allocate(size_t, DataSource&) override {
        return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
    }
    tl::expected<void, ErrorCode> Free(DataSource) override { return {}; }
    UUID GetTierId() const override { return id_; }
    size_t GetCapacity() const override { return capacity_; }
    size_t GetUsage() const override { return usage_; }
    MemoryType GetMemoryType() const override { return type_; }
    const std::vector<std::string>& GetTags() const override { return tags_; }

    void SetUsage(size_t usage) { usage_ = usage; }

   private:
    UUID id_;
    size_t capacity_;
    size_t usage_;
    MemoryType type_;
    std::vector<std::string> tags_;
};
}  // namespace

TEST_F(ClientMetricsTest, TierMetricEmptyTest) {
    TierMetric metric;

    std::string summary = metric.summary_metrics();
    EXPECT_TRUE(summary.find("No tiers registered") != std::string::npos);

    // No tier registered -> no tier series serialized.
    std::string serialized;
    metric.serialize(serialized);
    EXPECT_TRUE(serialized.find("mooncake_p2p_tier") == std::string::npos);
}

TEST_F(ClientMetricsTest, TierMetricRegisterAndCountTest) {
    TierMetric metric;
    const UUID tier_id{1, 2};
    const std::string label = "tier_1_2";
    auto tier =
        std::make_shared<StubStatsTier>(tier_id, /*capacity=*/1024,
                                        /*usage=*/256, MemoryType::DRAM);

    metric.RegisterTier(tier_id, label, tier, /*priority=*/10);

    std::array<std::string, 1> label_array = {label};
    EXPECT_EQ(metric.capacity_bytes.value(label_array), 1024);
    // Usage is captured at registration and only refreshed on read.
    EXPECT_EQ(metric.used_bytes.value(label_array), 256);
    EXPECT_EQ(metric.key_count.value(label_array), 0);

    // Key count follows replica add/remove.
    metric.OnReplicaAdded(tier_id);
    metric.OnReplicaAdded(tier_id);
    metric.OnReplicaAdded(tier_id);
    metric.OnReplicaRemoved(tier_id);
    EXPECT_EQ(metric.key_count.value(label_array), 2);

    // Usage is refreshed from the tier only on serialize/summary reads.
    tier->SetUsage(512);
    EXPECT_EQ(metric.used_bytes.value(label_array), 256);  // stale before read
    std::string serialized;
    metric.serialize(serialized);
    EXPECT_EQ(metric.used_bytes.value(label_array), 512);
    EXPECT_TRUE(serialized.find("mooncake_p2p_tier_key_count") !=
                std::string::npos);

    // Summary contains label, memory type, priority and counters.
    std::string summary = metric.summary_metrics();
    EXPECT_TRUE(summary.find(label) != std::string::npos);
    EXPECT_TRUE(summary.find("DRAM") != std::string::npos);
    EXPECT_TRUE(summary.find("priority=10") != std::string::npos);
    EXPECT_TRUE(summary.find("keys=2") != std::string::npos);
}

TEST_F(ClientMetricsTest, TierMetricUnregisteredTierTest) {
    TierMetric metric;

    // Hooks on unknown tiers must not crash nor create series.
    metric.OnReplicaAdded(UUID{9, 9});
    metric.OnReplicaRemoved(UUID{9, 9});

    std::string serialized;
    metric.serialize(serialized);
    EXPECT_TRUE(serialized.find("mooncake_p2p_tier") == std::string::npos);
}

TEST_F(ClientMetricsTest, TierMetricExpiredTierKeepsLastUsageTest) {
    TierMetric metric;
    const UUID tier_id{3, 4};
    const std::string label = "tier_3_4";
    auto tier =
        std::make_shared<StubStatsTier>(tier_id, /*capacity=*/2048,
                                        /*usage=*/100, MemoryType::NVME);
    metric.RegisterTier(tier_id, label, tier, /*priority=*/5);

    tier->SetUsage(999);
    std::string serialized;
    metric.serialize(serialized);  // refresh while the tier is alive

    tier.reset();                  // destroy the tier
    metric.serialize(serialized);  // must not crash on the expired weak_ptr

    std::array<std::string, 1> label_array = {label};
    EXPECT_EQ(metric.used_bytes.value(label_array), 999);  // last known value
}

TEST_F(ClientMetricsTest, TierMetricDuplicateRegistrationTest) {
    TierMetric metric;
    const UUID tier_id{5, 6};
    auto tier = std::make_shared<StubStatsTier>(tier_id, /*capacity=*/1024,
                                                /*usage=*/0, MemoryType::DRAM);

    metric.RegisterTier(tier_id, "tier_5_6", tier, /*priority=*/1);
    metric.RegisterTier(tier_id, "tier_5_6_dup", tier, /*priority=*/1);

    // The duplicate registration is rejected; only the first label exists.
    std::string summary = metric.summary_metrics();
    EXPECT_TRUE(summary.find("tier_5_6") != std::string::npos);
    EXPECT_TRUE(summary.find("tier_5_6_dup") == std::string::npos);
}

TEST_F(ClientMetricsTest, P2PClientMetricTierSectionTest) {
    P2PClientMetric metrics;

    std::string summary = metrics.summary_metrics();
    EXPECT_TRUE(summary.find("=== P2P Tier Metrics ===") != std::string::npos);
    EXPECT_TRUE(summary.find("No tiers registered") != std::string::npos);

    std::string serialized;
    metrics.serialize(serialized);  // must not crash with empty tier metric
}

TEST_F(ClientMetricsTest, TierMetricMovementCountersTest) {
    TierMetric metric;
    const UUID high{1, 1};
    const UUID low{2, 2};
    auto high_tier = std::make_shared<StubStatsTier>(
        high, /*capacity=*/1024, /*usage=*/0, MemoryType::DRAM);
    auto low_tier = std::make_shared<StubStatsTier>(
        low, /*capacity=*/2048, /*usage=*/0, MemoryType::NVME);

    metric.RegisterTier(high, "tier_1_1", high_tier, /*priority=*/20);
    metric.RegisterTier(low, "tier_2_2", low_tier, /*priority=*/10);

    std::array<std::string, 1> high_label = {"tier_1_1"};
    std::array<std::string, 1> low_label = {"tier_2_2"};

    // Counters start at zero at registration.
    EXPECT_EQ(metric.evicted_keys.value(high_label), 0);
    EXPECT_EQ(metric.offloaded_keys.value(high_label), 0);
    EXPECT_EQ(metric.onboarded_keys.value(high_label), 0);

    // Evictions are counted on the tier losing the replica.
    metric.OnEvicted(high);
    metric.OnEvicted(high);
    EXPECT_EQ(metric.evicted_keys.value(high_label), 2);
    EXPECT_EQ(metric.evicted_keys.value(low_label), 0);

    // A downward movement counts as an offload on the source tier.
    metric.OnMoved(high, low);
    EXPECT_EQ(metric.offloaded_keys.value(high_label), 1);
    EXPECT_EQ(metric.onboarded_keys.value(low_label), 0);

    // An upward movement counts as an onboard on the source tier.
    metric.OnMoved(low, high);
    metric.OnMoved(low, high);
    EXPECT_EQ(metric.onboarded_keys.value(low_label), 2);
    EXPECT_EQ(metric.offloaded_keys.value(high_label), 1);

    // Same-priority movements are neither offload nor onboard.
    const UUID peer{3, 3};
    auto peer_tier = std::make_shared<StubStatsTier>(
        peer, /*capacity=*/512, /*usage=*/0, MemoryType::DRAM);
    metric.RegisterTier(peer, "tier_3_3", peer_tier, /*priority=*/20);
    metric.OnMoved(high, peer);
    std::array<std::string, 1> peer_label = {"tier_3_3"};
    EXPECT_EQ(metric.offloaded_keys.value(high_label), 1);
    EXPECT_EQ(metric.onboarded_keys.value(high_label), 0);
    EXPECT_EQ(metric.offloaded_keys.value(peer_label), 0);

    // Summary exposes the new counters.
    std::string summary = metric.summary_metrics();
    EXPECT_TRUE(summary.find("evicted_keys=2") != std::string::npos);
    EXPECT_TRUE(summary.find("offloaded_keys=1") != std::string::npos);
    EXPECT_TRUE(summary.find("onboarded_keys=2") != std::string::npos);

    // Serialized output contains the counter series.
    std::string serialized;
    metric.serialize(serialized);
    EXPECT_TRUE(serialized.find("mooncake_p2p_tier_evicted_keys_total") !=
                std::string::npos);
}

}  // namespace mooncake::test
