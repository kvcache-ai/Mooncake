#include <glog/logging.h>
#include <gtest/gtest.h>

// csignal must precede coro_http_client.hpp: the bundled ylt's coro_io.hpp
// calls std::signal without including <csignal> itself.
#include <csignal>
#include <cstdlib>
#include <optional>
#include <string>
#include <unordered_set>
#include <ylt/coro_http/coro_http_client.hpp>

#include "client_metric.h"
#include "real_client.h"
#include "test_server_helpers.h"
#include "utils.h"

namespace mooncake::test {
namespace {

struct HttpResponse {
    int status;
    std::string body;
};

HttpResponse FetchUrl(const std::string& url) {
    coro_http::coro_http_client client;
    auto res = client.get(url);
    return HttpResponse{res.status, std::string(res.resp_body)};
}

int GetTestPort(std::unordered_set<int>& used_ports) {
    for (int i = 0; i < 100; ++i) {
        int port = getFreeTcpPort();
        if (port > 0 && port < 65535 && !used_ports.contains(port)) {
            used_ports.insert(port);
            return port;
        }
    }
    return -1;
}

class ScopedEnv {
   public:
    explicit ScopedEnv(const char* name) : name_(name) {
        const char* value = std::getenv(name);
        if (value) old_value_ = value;
    }

    ~ScopedEnv() {
        if (old_value_) {
            setenv(name_, old_value_->c_str(), 1);
        } else {
            unsetenv(name_);
        }
    }

   private:
    const char* name_;
    std::optional<std::string> old_value_;
};

tl::expected<void, ErrorCode> SetupClientWithHttp(
    const std::shared_ptr<RealClient>& client, const std::string& client_addr,
    const std::string& master_addr, bool enable_http, int http_port) {
    return client->setup_internal(
        client_addr, "P2PHANDSHAKE", /*global_segment_size=*/0,
        /*local_buffer_size=*/0, "tcp", "", master_addr, nullptr, "",
        /*local_rpc_port=*/50052, /*enable_ssd_offload=*/false,
        /*start_offload_rpc_server=*/false, /*ssd_offload_path=*/"",
        /*tenant_id=*/"default", enable_http, http_port);
}

}  // namespace

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
    EXPECT_TRUE(summary.find("Average Read Throughput:") != std::string::npos);
    EXPECT_TRUE(summary.find("Average Write Throughput:") != std::string::npos);

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
    metrics.ObserveTransferOperation(TransferOperationKind::kRead, "get_buffer",
                                     2 * 1024, 220);
    metrics.ObserveTransferOperation(TransferOperationKind::kWrite, "put_batch",
                                     4 * 1024, 420);

    std::string summary = metrics.summary_metrics();

    // Should contain transfer, RPC, and interface metrics
    EXPECT_TRUE(summary.find("Transfer Metrics Summary") != std::string::npos);
    EXPECT_TRUE(summary.find("RPC Metrics Summary") != std::string::npos);
    EXPECT_TRUE(summary.find("Interface Operation Metrics Summary") !=
                std::string::npos);
    EXPECT_TRUE(summary.find("Total Read: 5.00 MB") != std::string::npos);
    EXPECT_TRUE(summary.find("Total Write: 10.00 MB") != std::string::npos);
    EXPECT_TRUE(summary.find("ExistKey: count=1") != std::string::npos);
    EXPECT_TRUE(summary.find("get_buffer: count=1") != std::string::npos);
    EXPECT_TRUE(summary.find("put_batch: count=1") != std::string::npos);

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

TEST_F(ClientMetricsTest, BandwidthSummaryRespectsEnvFlag) {
    setenv("MC_STORE_CLIENT_METRIC_BANDWIDTH", "0", 1);
    auto metrics = ClientMetric::Create();
    ASSERT_NE(metrics, nullptr);

    metrics->transfer_metric.total_read_bytes.inc(1024);
    std::string summary = metrics->summary_metrics();
    EXPECT_TRUE(summary.find("Average Read Throughput:") == std::string::npos);

    unsetenv("MC_STORE_CLIENT_METRIC_BANDWIDTH");
}

TEST_F(ClientMetricsTest, SummaryCanOmitMasterRpcMetrics) {
    auto metrics = ClientMetric::Create({}, false);
    ASSERT_NE(metrics, nullptr);

    metrics->ObserveTransferOperation(TransferOperationKind::kRead,
                                      "get_buffer", 1024, 200);
    std::string summary = metrics->summary_metrics();
    std::string serialized;
    metrics->serialize(serialized);

    EXPECT_TRUE(summary.find("RPC Metrics Summary") == std::string::npos);
    EXPECT_TRUE(serialized.find("mooncake_client_rpc_count") ==
                std::string::npos);
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

TEST_F(ClientMetricsTest, HttpMetricsEndpointsReturnData) {
    std::unordered_set<int> used_ports;
    int master_rpc_port = GetTestPort(used_ports);
    int master_http_port = GetTestPort(used_ports);
    int http_port = GetTestPort(used_ports);
    int client_port = GetTestPort(used_ports);
    ASSERT_GT(master_rpc_port, 0);
    ASSERT_GT(master_http_port, 0);
    ASSERT_GT(http_port, 0);
    ASSERT_GT(client_port, 0);

    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(mooncake::InProcMasterConfigBuilder()
                                 .set_rpc_port(master_rpc_port)
                                 .set_http_metrics_port(master_http_port)
                                 .set_http_metadata_port(0)
                                 .build()));

    auto client = RealClient::create();
    auto setup_result = SetupClientWithHttp(
        client, "127.0.0.1:" + std::to_string(client_port),
        master.master_address(), /*enable_http=*/true, http_port);
    ASSERT_TRUE(setup_result.has_value()) << toString(setup_result.error());

    auto metrics =
        FetchUrl("http://127.0.0.1:" + std::to_string(http_port) + "/metrics");
    EXPECT_EQ(metrics.status, 200);
    EXPECT_EQ(metrics.body.find("metrics not available"), std::string::npos);

    auto summary = FetchUrl("http://127.0.0.1:" + std::to_string(http_port) +
                            "/metrics/summary");
    EXPECT_EQ(summary.status, 200);
    EXPECT_NE(summary.body.find("Client Metrics Summary"), std::string::npos);

    EXPECT_EQ(client->tearDownAll(), 0);
}

TEST_F(ClientMetricsTest, HttpMetricsConfigParserTrimsWhitespace) {
    std::unordered_set<int> used_ports;
    int master_rpc_port = GetTestPort(used_ports);
    int master_http_port = GetTestPort(used_ports);
    int http_port = GetTestPort(used_ports);
    int client_port = GetTestPort(used_ports);
    ASSERT_GT(master_rpc_port, 0);
    ASSERT_GT(master_http_port, 0);
    ASSERT_GT(http_port, 0);
    ASSERT_GT(client_port, 0);

    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(mooncake::InProcMasterConfigBuilder()
                                 .set_rpc_port(master_rpc_port)
                                 .set_http_metrics_port(master_http_port)
                                 .set_http_metadata_port(0)
                                 .build()));

    ConfigDict config = {
        {CONFIG_KEY_LOCAL_HOSTNAME, "127.0.0.1:" + std::to_string(client_port)},
        {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
        {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "0"},
        {CONFIG_KEY_LOCAL_BUFFER_SIZE, "0"},
        {CONFIG_KEY_PROTOCOL, "tcp"},
        {CONFIG_KEY_MASTER_SERVER_ADDR, master.master_address()},
        {CONFIG_KEY_ENABLE_CLIENT_HTTP_SERVER, " true "},
        {CONFIG_KEY_CLIENT_HTTP_PORT, " " + std::to_string(http_port) + " "},
    };

    auto client = RealClient::create();
    auto setup_result = client->setup_internal(config);
    ASSERT_TRUE(setup_result.has_value()) << toString(setup_result.error());

    auto health =
        FetchUrl("http://127.0.0.1:" + std::to_string(http_port) + "/health");
    EXPECT_EQ(health.status, 200);
    EXPECT_NE(health.body.find("\"status\":\"healthy\""), std::string::npos);

    EXPECT_EQ(client->tearDownAll(), 0);
}

TEST_F(ClientMetricsTest, HttpMetricsConfigParserRejectsInvalidIntegers) {
    const char* invalid_ports[] = {
        "9300x",
        "999999999999999999999999",
    };

    for (const char* invalid_port : invalid_ports) {
        ConfigDict config = {
            {CONFIG_KEY_LOCAL_HOSTNAME, "127.0.0.1:1"},
            {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
            {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "0"},
            {CONFIG_KEY_LOCAL_BUFFER_SIZE, "0"},
            {CONFIG_KEY_PROTOCOL, "tcp"},
            {CONFIG_KEY_ENABLE_CLIENT_HTTP_SERVER, "true"},
            {CONFIG_KEY_CLIENT_HTTP_PORT, invalid_port},
        };

        auto client = RealClient::create();
        auto setup_result = client->setup_internal(config);
        ASSERT_FALSE(setup_result.has_value()) << invalid_port;
        EXPECT_EQ(setup_result.error(), ErrorCode::INVALID_PARAMS)
            << invalid_port;
    }
}

TEST_F(ClientMetricsTest, HttpMetricsEndpointReturns503WhenMetricsDisabled) {
    ScopedEnv metrics_env("MC_STORE_CLIENT_METRIC");
    setenv("MC_STORE_CLIENT_METRIC", "0", 1);

    std::unordered_set<int> used_ports;
    int master_rpc_port = GetTestPort(used_ports);
    int master_http_port = GetTestPort(used_ports);
    int http_port = GetTestPort(used_ports);
    int client_port = GetTestPort(used_ports);
    ASSERT_GT(master_rpc_port, 0);
    ASSERT_GT(master_http_port, 0);
    ASSERT_GT(http_port, 0);
    ASSERT_GT(client_port, 0);

    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(mooncake::InProcMasterConfigBuilder()
                                 .set_rpc_port(master_rpc_port)
                                 .set_http_metrics_port(master_http_port)
                                 .set_http_metadata_port(0)
                                 .build()));

    auto client = RealClient::create();
    auto setup_result = SetupClientWithHttp(
        client, "127.0.0.1:" + std::to_string(client_port),
        master.master_address(), /*enable_http=*/true, http_port);
    ASSERT_TRUE(setup_result.has_value()) << toString(setup_result.error());

    auto metrics =
        FetchUrl("http://127.0.0.1:" + std::to_string(http_port) + "/metrics");
    EXPECT_EQ(metrics.status, 503);
    EXPECT_NE(metrics.body.find("metrics not available"), std::string::npos);

    EXPECT_EQ(client->tearDownAll(), 0);
}

TEST_F(ClientMetricsTest, HttpMetricsPortConflictDoesNotFailSetup) {
    std::unordered_set<int> used_ports;
    int master_rpc_port = GetTestPort(used_ports);
    int master_http_port = GetTestPort(used_ports);
    int http_port = GetTestPort(used_ports);
    int first_client_port = GetTestPort(used_ports);
    int second_client_port = GetTestPort(used_ports);
    ASSERT_GT(master_rpc_port, 0);
    ASSERT_GT(master_http_port, 0);
    ASSERT_GT(http_port, 0);
    ASSERT_GT(first_client_port, 0);
    ASSERT_GT(second_client_port, 0);

    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(mooncake::InProcMasterConfigBuilder()
                                 .set_rpc_port(master_rpc_port)
                                 .set_http_metrics_port(master_http_port)
                                 .set_http_metadata_port(0)
                                 .build()));

    auto first_client = RealClient::create();
    auto first_setup = SetupClientWithHttp(
        first_client, "127.0.0.1:" + std::to_string(first_client_port),
        master.master_address(), /*enable_http=*/true, http_port);
    ASSERT_TRUE(first_setup.has_value()) << toString(first_setup.error());

    auto second_client = RealClient::create();
    auto second_setup = SetupClientWithHttp(
        second_client, "127.0.0.1:" + std::to_string(second_client_port),
        master.master_address(), /*enable_http=*/true, http_port);
    EXPECT_TRUE(second_setup.has_value()) << toString(second_setup.error());

    EXPECT_EQ(second_client->tearDownAll(), 0);
    EXPECT_EQ(first_client->tearDownAll(), 0);
}

}  // namespace mooncake::test
