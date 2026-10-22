#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>

#include "client_metric.h"
#include "utils.h"

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

}  // namespace mooncake::test
