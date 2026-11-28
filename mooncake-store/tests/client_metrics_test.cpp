#include <glog/logging.h>
#include <gtest/gtest.h>

// csignal must precede coro_http_client.hpp: the bundled ylt's coro_io.hpp
// calls std::signal without including <csignal> itself.
#include <unistd.h>

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <span>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
#include <ylt/coro_http/coro_http_client.hpp>

#include "client_metric.h"
#include "common/network.h"
#include "environment_variables.h"
#include "real_client.h"
#include "test_server_helpers.h"
#include "version.h"

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

size_t CountOccurrences(const std::string& text, const std::string& needle) {
    if (needle.empty()) return 0;

    size_t count = 0;
    size_t position = 0;
    while ((position = text.find(needle, position)) != std::string::npos) {
        ++count;
        position += needle.size();
    }
    return count;
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
    const std::string& master_addr, bool enable_http, int http_port,
    size_t global_segment_size = 0, size_t local_buffer_size = 0) {
    return client->setup_internal(
        client_addr, "P2PHANDSHAKE", global_segment_size, local_buffer_size,
        "tcp", "", master_addr, nullptr, "",
        /*local_rpc_port=*/50052, /*enable_ssd_offload=*/false,
        /*start_offload_rpc_server=*/false, /*ssd_offload_path=*/"",
        /*tenant_id=*/"default", enable_http, http_port);
}

int64_t SumMatchingMetricLines(const std::string& body,
                               const std::string& prefix,
                               const std::vector<std::string>& label_filters) {
    int64_t total = 0;
    std::istringstream lines(body);
    std::string line;
    while (std::getline(lines, line)) {
        if (!line.starts_with(prefix)) continue;
        bool matches = true;
        for (const auto& filter : label_filters) {
            if (line.find(filter) == std::string::npos) {
                matches = false;
                break;
            }
        }
        if (!matches) continue;
        const auto space_pos = line.rfind(' ');
        if (space_pos == std::string::npos) continue;
        total += std::stoll(line.substr(space_pos + 1));
    }
    return total;
}

bool WaitForDiskOnlyReplica(const std::shared_ptr<RealClient>& client,
                            const std::string& key,
                            const std::string& segment_name,
                            std::chrono::milliseconds timeout) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    bool has_disk = false;
    while (std::chrono::steady_clock::now() < deadline && !has_disk) {
        for (const auto& replica : client->get_replica_desc(key)) {
            if (replica.is_disk_replica()) {
                has_disk = true;
                break;
            }
        }
        if (!has_disk)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    if (!has_disk) return false;

    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    while (std::chrono::steady_clock::now() < deadline) {
        auto cleared = client->batch_replica_clear({key}, segment_name);
        if (cleared.size() == 1 && cleared[0] == key) {
            const auto descs = client->get_replica_desc(key);
            return descs.size() == 1 && descs.front().is_disk_replica();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return false;
}

struct StageBreakdownEnv {
    std::filesystem::path tmp_dir;
    mooncake::testing::InProcMaster master;
    std::shared_ptr<RealClient> client;
    std::string client_addr;
    int http_port = -1;

    ~StageBreakdownEnv() {
        if (client) client->tearDownAll();
        master.Stop();
        if (!tmp_dir.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(tmp_dir, ec);
        }
    }

    void Start(bool with_disk) {
        std::unordered_set<int> used_ports;
        const int master_rpc = GetTestPort(used_ports);
        const int master_http = GetTestPort(used_ports);
        http_port = GetTestPort(used_ports);
        const int client_port = GetTestPort(used_ports);
        ASSERT_GT(master_rpc, 0);
        ASSERT_GT(master_http, 0);
        ASSERT_GT(http_port, 0);
        ASSERT_GT(client_port, 0);

        auto builder = mooncake::InProcMasterConfigBuilder()
                           .set_rpc_port(master_rpc)
                           .set_http_metrics_port(master_http)
                           .set_http_metadata_port(0);
        if (with_disk) {
            const auto ts =
                std::chrono::steady_clock::now().time_since_epoch().count();
            tmp_dir = std::filesystem::temp_directory_path() /
                      ("mc_metrics_disk_" + std::to_string(::getpid()) + "_" +
                       std::to_string(ts));
            std::filesystem::create_directories(tmp_dir);
            builder.set_root_fs_dir(tmp_dir.string())
                .set_default_kv_lease_ttl(10);
        }
        ASSERT_TRUE(master.Start(builder.build()));

        client = RealClient::create();
        client_addr = "127.0.0.1:" + std::to_string(client_port);
        auto res = SetupClientWithHttp(
            client, client_addr, master.master_address(), /*enable_http=*/true,
            http_port, /*global_segment_size=*/16 * 1024 * 1024,
            /*local_buffer_size=*/4 * 1024 * 1024);
        ASSERT_TRUE(res.has_value()) << toString(res.error());
    }

    HttpResponse FetchMetrics() const {
        return FetchUrl("http://127.0.0.1:" + std::to_string(http_port) +
                        "/metrics");
    }
};

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
    // The build is named inline so the summary alone identifies the binary.
    EXPECT_TRUE(summary.find("Version: " + GetMooncakeStoreVersion()) !=
                std::string::npos);
    EXPECT_TRUE(summary.find(MOONCAKE_DISPLAY_VERSION) != std::string::npos);

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

TEST_F(ClientMetricsTest, HybridHistogramSerializesEachLabelOnce) {
    ylt::metric::hybrid_histogram_1t histogram(
        "request_latency", "Request latency", {10.0, 20.0}, {}, {"route"});
    histogram.observe({"alpha"}, 5);
    histogram.observe({"beta"}, 15);

    std::string serialized;
    histogram.serialize(serialized);

    EXPECT_EQ(
        CountOccurrences(serialized, "request_latency_bucket{route=\"alpha\","),
        3);
    EXPECT_EQ(
        CountOccurrences(serialized, "request_latency_bucket{route=\"beta\","),
        3);
}

TEST_F(ClientMetricsTest, ZeroSumHybridHistogramPreservesExistingMetrics) {
    ylt::metric::hybrid_histogram_1t histogram(
        "request_latency", "Request latency", {10.0}, {}, {"route"});
    histogram.observe({"idle"}, 0);
    std::string serialized = "existing_metric 1\n";

    histogram.serialize(serialized);

    EXPECT_EQ(serialized, "existing_metric 1\n");
}

TEST_F(ClientMetricsTest, BandwidthSummaryRespectsEnvFlag) {
    ScopedEnv bandwidth_env(
        ClientMetricEnvironmentVariables::MC_STORE_CLIENT_METRIC_BANDWIDTH
            .name);
    setenv(
        ClientMetricEnvironmentVariables::MC_STORE_CLIENT_METRIC_BANDWIDTH.name,
        "0", 1);
    auto metrics = ClientMetric::Create();
    ASSERT_NE(metrics, nullptr);

    metrics->transfer_metric.total_read_bytes.inc(1024);
    std::string summary = metrics->summary_metrics();
    EXPECT_TRUE(summary.find("Average Read Throughput:") == std::string::npos);
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

TEST_F(ClientMetricsTest, BuildInfoMetricIsSerialized) {
    ClientMetric metrics(0);

    std::string serialized;
    metrics.serialize(serialized);

    ASSERT_NE(serialized.find("mooncake_build_info{"), std::string::npos)
        << "build info metric missing from serialized output";
    // Label order inside the series is not asserted: only the presence of both
    // labels with the compiled-in values matters for scraping and grouping.
    EXPECT_NE(serialized.find("version=\"" + GetMooncakeStoreVersion() + "\""),
              std::string::npos)
        << "RPC handshake version missing from build info labels";
    EXPECT_NE(serialized.find("display_version=\"" +
                              std::string(MOONCAKE_DISPLAY_VERSION) + "\""),
              std::string::npos)
        << "display version missing from build info labels";

    // Info-style metric: the value is fixed at 1, so exactly one series is
    // emitted and its value follows the closing brace of the label set.
    EXPECT_EQ(CountOccurrences(serialized, "mooncake_build_info{"), 1u);
    const auto metric_pos = serialized.find("mooncake_build_info{");
    const auto brace_end = serialized.find('}', metric_pos);
    ASSERT_NE(brace_end, std::string::npos);
    const auto line_end = serialized.find('\n', brace_end);
    const std::string value_part =
        serialized.substr(brace_end + 1, line_end == std::string::npos
                                             ? std::string::npos
                                             : line_end - brace_end - 1);
    EXPECT_NE(value_part.find('1'), std::string::npos)
        << "build info value should be 1, got:" << value_part;
}

TEST_F(ClientMetricsTest, BuildInfoMetricKeepsCallerLabels) {
    // Caller supplied labels identify the instance; the version labels are
    // added on top of them rather than replacing them.
    std::map<std::string, std::string> static_labels = {
        {"instance_id", "12345"}, {"cluster_id", "cluster1"}};
    ClientMetric metrics(0, static_labels);

    std::string serialized;
    metrics.serialize(serialized);

    const auto metric_pos = serialized.find("mooncake_build_info{");
    ASSERT_NE(metric_pos, std::string::npos);
    const auto brace_end = serialized.find('}', metric_pos);
    ASSERT_NE(brace_end, std::string::npos);
    const std::string labels =
        serialized.substr(metric_pos, brace_end - metric_pos);

    EXPECT_NE(labels.find("instance_id=\"12345\""), std::string::npos);
    EXPECT_NE(labels.find("cluster_id=\"cluster1\""), std::string::npos);
    EXPECT_NE(labels.find("version=\"" + GetMooncakeStoreVersion() + "\""),
              std::string::npos);
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
    // A request-only client does not start the storage heartbeat.
    EXPECT_EQ(metrics.body.find("mooncake_client_master_heartbeat_status_ok"),
              std::string::npos);
    EXPECT_EQ(
        metrics.body.find(
            "mooncake_client_master_heartbeat_observation_timestamp_seconds"),
        std::string::npos);

    auto summary = FetchUrl("http://127.0.0.1:" + std::to_string(http_port) +
                            "/metrics/summary");
    EXPECT_EQ(summary.status, 200);
    EXPECT_NE(summary.body.find("Client Metrics Summary"), std::string::npos);

    EXPECT_EQ(client->tearDownAll(), 0);
}

TEST_F(ClientMetricsTest, HeartbeatObservationsUseClientMetricLabels) {
    ClientMetric metrics(
        0, {{"cluster_id", "cluster1"}, {"instance_id", "12345"}});
    auto& heartbeat = metrics.master_heartbeat_metric;
    heartbeat.EndConnection(heartbeat.BeginConnection(), true);
    heartbeat.Observe(heartbeat.BeginObservation(), false, 100.5);
    std::string serialized;
    metrics.serialize(serialized);
    const std::string labels =
        "{cluster_id=\"cluster1\",instance_id=\"12345\"}";
    EXPECT_NE(serialized.find("mooncake_client_master_heartbeat_status_ok" +
                              labels + " 0\n"),
              std::string::npos);
    EXPECT_NE(
        serialized.find(
            "mooncake_client_master_heartbeat_observation_timestamp_seconds" +
            labels + " 100.500000\n"),
        std::string::npos);
}

TEST_F(ClientMetricsTest, HttpHeartbeatMetricsRecoverAfterMasterRestart) {
    ScopedEnv timeout("MC_RPC_TIMEOUT_MS");
    setenv("MC_RPC_TIMEOUT_MS", "200", 1);
    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(mooncake::InProcMasterConfigBuilder()
                                 .set_http_metadata_port(0)
                                 .build()));
    std::unordered_set<int> used_ports;
    const int http_port = GetTestPort(used_ports);
    const int client_port = GetTestPort(used_ports);
    ASSERT_GT(http_port, 0);
    ASSERT_GT(client_port, 0);
    auto client = RealClient::create();
    auto setup_result = SetupClientWithHttp(
        client, "127.0.0.1:" + std::to_string(client_port),
        master.master_address(), true, http_port, 16 * 1024 * 1024);
    ASSERT_TRUE(setup_result.has_value()) << toString(setup_result.error());

    const std::string url =
        "http://127.0.0.1:" + std::to_string(http_port) + "/metrics";
    const std::string status_name =
        "mooncake_client_master_heartbeat_status_ok";
    const std::string timestamp_name =
        "mooncake_client_master_heartbeat_observation_timestamp_seconds";
    auto sample = [](const std::string& body, const std::string& name) {
        std::istringstream lines(body);
        std::string line;
        while (std::getline(lines, line)) {
            if (line.starts_with(name + "{") || line.starts_with(name + " "))
                return line;
        }
        return std::string{};
    };
    HttpResponse response;
    auto wait_for = [&](bool known) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(15);
        do {
            response = FetchUrl(url);
            if (response.status == 200) {
                const auto status = sample(response.body, status_name);
                const auto timestamp = sample(response.body, timestamp_name);
                if (known ? (status.ends_with(" 1") && !timestamp.empty())
                          : (status.empty() && timestamp.empty()))
                    return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        } while (std::chrono::steady_clock::now() < deadline);
        return false;
    };

    ASSERT_TRUE(wait_for(true)) << response.body;
    EXPECT_NE(sample(response.body, status_name).find("client_mode=\"real\""),
              std::string::npos);
    const auto before = std::stod(
        sample(response.body, timestamp_name)
            .substr(sample(response.body, timestamp_name).rfind(' ') + 1));
    master.Stop();
    ASSERT_TRUE(wait_for(false)) << response.body;
    ASSERT_TRUE(
        master.Start(mooncake::InProcMasterConfigBuilder()
                         .set_rpc_port(master.rpc_port())
                         .set_http_metrics_port(master.http_metrics_port())
                         .set_http_metadata_port(0)
                         .build()));
    ASSERT_TRUE(wait_for(true)) << response.body;
    const auto timestamp = sample(response.body, timestamp_name);
    EXPECT_GT(std::stod(timestamp.substr(timestamp.rfind(' ') + 1)), before);
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

    // `/version` is served without touching the metric collector, so disabling
    // metrics must not take it down along with `/metrics`.
    auto version =
        FetchUrl("http://127.0.0.1:" + std::to_string(http_port) + "/version");
    EXPECT_EQ(version.status, 200);
    EXPECT_NE(
        version.body.find("\"version\":\"" + GetMooncakeStoreVersion() + "\""),
        std::string::npos);
    EXPECT_NE(version.body.find("display_version"), std::string::npos);

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

TEST_F(ClientMetricsTest, ReadOperationStageBreakdownIsReported) {
    auto stage_count = [](const std::string& text, const std::string& stage,
                          const std::string& outcome) {
        return SumMatchingMetricLines(
            text, "mooncake_transfer_read_operation_stage_latency_count{",
            {"op_name=\"batch_get_into_multi_buffers\"",
             "stage=\"" + stage + "\"", "outcome=\"" + outcome + "\""});
    };

    ClientMetric metrics;
    EXPECT_NE(metrics.summary_metrics().find("Read Stage Breakdown:\nNo data"),
              std::string::npos);

    // 1) Single-sample ok/partial formatting + upward-rounded p95.
    metrics.ObserveReadOperationBreakdown("batch_get_into_multi_buffers",
                                          OperationOutcome::kOk, 300, 1500);
    metrics.ObserveReadOperationBreakdown(
        "batch_get_into_multi_buffers", OperationOutcome::kPartial, 250, 2000);
    std::string summary = metrics.summary_metrics();
    EXPECT_NE(summary.find("batch_get_into_multi_buffers [stage=metadata, "
                           "outcome=ok]: count=1, p95<300μs, max<300μs"),
              std::string::npos);
    EXPECT_NE(summary.find("batch_get_into_multi_buffers [stage=post_query, "
                           "outcome=ok]: count=1, p95<1500μs, max<1500μs"),
              std::string::npos);
    EXPECT_NE(summary.find("batch_get_into_multi_buffers [stage=post_query, "
                           "outcome=partial]: count=1, p95<2000μs, max<2000μs"),
              std::string::npos);

    std::string serialized;
    metrics.serialize(serialized);
    EXPECT_EQ(stage_count(serialized, "metadata", "ok"), 1);
    EXPECT_EQ(stage_count(serialized, "post_query", "ok"), 1);
    EXPECT_EQ(stage_count(serialized, "metadata", "partial"), 1);
    EXPECT_EQ(stage_count(serialized, "post_query", "partial"), 1);

    // 2) Five-sample ceil(0.95 * 5) = 5 rank + >20s overflow bucket +
    // multi-thread contention check.
    ClientMetric edge_metrics;
    for (int i = 0; i < 4; ++i) {
        edge_metrics.ObserveReadOperationBreakdown(
            "batch_get_into_multi_buffers", OperationOutcome::kOk, 100, 300);
    }
    edge_metrics.ObserveReadOperationBreakdown(
        "batch_get_into_multi_buffers", OperationOutcome::kOk, 100, 20000);
    edge_metrics.ObserveReadOperationBreakdown(
        "batch_get_into_multi_buffers", OperationOutcome::kError, 300, 300);
    edge_metrics.ObserveReadOperationBreakdown("batch_get_into_multi_buffers",
                                               OperationOutcome::kError, 300,
                                               30000000);
    summary = edge_metrics.summary_metrics();
    EXPECT_NE(summary.find("batch_get_into_multi_buffers [stage=post_query, "
                           "outcome=ok]: count=5, p95<20000μs, max<20000μs"),
              std::string::npos)
        << summary;
    EXPECT_NE(summary.find("batch_get_into_multi_buffers [stage=post_query, "
                           "outcome=error]: count=2, p95>20000000μs, "
                           "max>20000000μs"),
              std::string::npos)
        << summary;

    std::vector<std::thread> workers;
    for (int t = 0; t < 4; ++t) {
        workers.emplace_back([&]() {
            for (int i = 0; i < 500; ++i) {
                edge_metrics.ObserveReadOperationBreakdown(
                    "batch_get_into_multi_buffers", OperationOutcome::kOk, 100,
                    500);
            }
        });
    }
    for (auto& w : workers) w.join();
    serialized.clear();
    edge_metrics.serialize(serialized);
    EXPECT_EQ(stage_count(serialized, "metadata", "ok"), 2005);
    EXPECT_EQ(stage_count(serialized, "post_query", "ok"), 2005);
}

TEST_F(ClientMetricsTest, BatchGetStageBreakdownProductionPaths) {
    StageBreakdownEnv env;
    ASSERT_NO_FATAL_FAILURE(env.Start(/*with_disk=*/true));

    const std::string mem_payload(1024, 'M');
    const std::string disk_payload(2048, 'D');
    const std::string mem_key = "stage_mem_key";
    const std::string disk_key = "stage_disk_key";
    ASSERT_EQ(env.client->put(mem_key, std::span<const char>(mem_payload)), 0);
    ASSERT_EQ(env.client->put(disk_key, std::span<const char>(disk_payload)),
              0);
    ASSERT_TRUE(WaitForDiskOnlyReplica(env.client, disk_key, env.client_addr,
                                       std::chrono::seconds(10)));

    std::vector<char> mem_dst(mem_payload.size(), '\0');
    std::vector<char> disk_dst(disk_payload.size(), '\0');
    ASSERT_EQ(env.client->register_buffer(mem_dst.data(), mem_dst.size()), 0);
    ASSERT_EQ(env.client->register_buffer(disk_dst.data(), disk_dst.size()), 0);

    auto stage_count = [](const std::string& body, const std::string& stage,
                          const std::string& outcome) {
        return SumMatchingMetricLines(
            body, "mooncake_transfer_read_operation_stage_latency_count{",
            {"op_name=\"batch_get_into_multi_buffers\"",
             "stage=\"" + stage + "\"", "outcome=\"" + outcome + "\""});
    };

    // 1) No-transfer calls (missing keys + undersized buffer) are skipped.
    auto miss = env.client->batch_get_into_multi_buffers(
        {"missing_a", "missing_b"}, {{mem_dst.data()}, {mem_dst.data()}},
        {{mem_dst.size()}, {mem_dst.size()}}, false);
    EXPECT_LT(miss[0], 0);
    EXPECT_LT(miss[1], 0);
    auto small = env.client->batch_get_into_multi_buffers(
        {mem_key}, {{mem_dst.data()}}, {{1}}, false);
    EXPECT_LT(small[0], 0);

    auto m = env.FetchMetrics();
    ASSERT_EQ(m.status, 200);
    EXPECT_EQ(SumMatchingMetricLines(
                  m.body, "mooncake_transfer_read_operation_count{",
                  {"op_name=\"batch_get_into_multi_buffers\""}),
              2);
    EXPECT_EQ(
        SumMatchingMetricLines(
            m.body, "mooncake_transfer_read_operation_stage_latency_count{",
            {"op_name=\"batch_get_into_multi_buffers\""}),
        0);

    // 2) Successful MEMORY read -> +1 sample per stage.
    auto r_mem = env.client->batch_get_into_multi_buffers(
        {mem_key}, {{mem_dst.data()}}, {{mem_dst.size()}}, false);
    ASSERT_EQ(r_mem, std::vector<int>({static_cast<int>(mem_payload.size())}));
    EXPECT_EQ(std::string(mem_dst.data(), mem_dst.size()), mem_payload);
    m = env.FetchMetrics();
    EXPECT_EQ(stage_count(m.body, "metadata", "ok"), 1);
    EXPECT_EQ(stage_count(m.body, "post_query", "ok"), 1);

    // 3) Successful DISK read -> +1 sample per stage.
    auto r_disk = env.client->batch_get_into_multi_buffers(
        {disk_key}, {{disk_dst.data()}}, {{disk_dst.size()}}, false);
    ASSERT_EQ(r_disk,
              std::vector<int>({static_cast<int>(disk_payload.size())}));
    EXPECT_EQ(std::string(disk_dst.data(), disk_dst.size()), disk_payload);
    m = env.FetchMetrics();
    EXPECT_EQ(stage_count(m.body, "metadata", "ok"), 2);
    EXPECT_EQ(stage_count(m.body, "post_query", "ok"), 2);

    // 4) Mixed MEMORY + DISK batch invokes Client::BatchGet twice (+2), while
    // stage breakdown emits only +1 per stage.
    std::fill(mem_dst.begin(), mem_dst.end(), '\0');
    std::fill(disk_dst.begin(), disk_dst.end(), '\0');
    auto r_mix = env.client->batch_get_into_multi_buffers(
        {mem_key, disk_key}, {{mem_dst.data()}, {disk_dst.data()}},
        {{mem_dst.size()}, {disk_dst.size()}}, false);
    ASSERT_EQ(r_mix, std::vector<int>({static_cast<int>(mem_payload.size()),
                                       static_cast<int>(disk_payload.size())}));
    EXPECT_EQ(std::string(mem_dst.data(), mem_dst.size()), mem_payload);
    EXPECT_EQ(std::string(disk_dst.data(), disk_dst.size()), disk_payload);

    // 5) Partial batch ({hit, miss}) -> +1 under outcome="partial".
    auto r_partial = env.client->batch_get_into_multi_buffers(
        {mem_key, "missing_c"}, {{mem_dst.data()}, {disk_dst.data()}},
        {{mem_dst.size()}, {disk_dst.size()}}, false);
    EXPECT_EQ(r_partial[0], static_cast<int>(mem_payload.size()));
    EXPECT_LT(r_partial[1], 0);

    m = env.FetchMetrics();
    EXPECT_EQ(SumMatchingMetricLines(
                  m.body, "mooncake_transfer_batch_get_latency_count", {}),
              5);
    EXPECT_EQ(stage_count(m.body, "metadata", "ok"), 3);
    EXPECT_EQ(stage_count(m.body, "post_query", "ok"), 3);
    EXPECT_EQ(stage_count(m.body, "metadata", "partial"), 1);
    EXPECT_EQ(stage_count(m.body, "post_query", "partial"), 1);

    EXPECT_EQ(env.client->unregister_buffer(disk_dst.data()), 0);
    EXPECT_EQ(env.client->unregister_buffer(mem_dst.data()), 0);
}

TEST_F(ClientMetricsTest, BatchGetStageBreakdownWhenMetricsDisabled) {
    ScopedEnv metrics_env(
        ClientMetricEnvironmentVariables::MC_STORE_CLIENT_METRIC.name);
    setenv(ClientMetricEnvironmentVariables::MC_STORE_CLIENT_METRIC.name, "0",
           1);

    StageBreakdownEnv env;
    ASSERT_NO_FATAL_FAILURE(env.Start(/*with_disk=*/false));

    const std::string payload = "metrics_disabled_payload";
    ASSERT_EQ(env.client->put("disabled_key", std::span<const char>(payload)),
              0);
    std::vector<char> dst(payload.size(), '\0');
    ASSERT_EQ(env.client->register_buffer(dst.data(), dst.size()), 0);
    auto res = env.client->batch_get_into_multi_buffers(
        {"disabled_key"}, {{dst.data()}}, {{dst.size()}}, false);
    ASSERT_EQ(res, std::vector<int>({static_cast<int>(payload.size())}));
    EXPECT_EQ(std::string(dst.data(), dst.size()), payload);
    EXPECT_EQ(env.FetchMetrics().status, 503);
    EXPECT_EQ(env.client->unregister_buffer(dst.data()), 0);
}

}  // namespace mooncake::test
