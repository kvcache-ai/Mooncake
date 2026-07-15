#include <glog/logging.h>
#include <gtest/gtest.h>

// csignal must precede coro_http_client.hpp: the bundled ylt's coro_io.hpp
// calls std::signal without including <csignal> itself.
#include <algorithm>
#include <atomic>
#include <csignal>
#include <cstdlib>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>
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

TEST_F(ClientMetricsTest, HybridHistogramSerializesUniqueLabelSeries) {
    ylt::metric::hybrid_histogram_1t histogram(
        "test_multi_label_histogram", "Test multi-label histogram", {10, 20},
        {{"cluster", "cluster-a"}}, {"operation"});
    const std::array<std::string, 1> read_label = {"read"};
    const std::array<std::string, 1> write_label = {"write"};

    histogram.observe(read_label, 5);
    histogram.observe(read_label, 15);
    histogram.observe(write_label, 25);
    histogram.observe(write_label, 25);

    std::string serialized;
    histogram.serialize(serialized);

    std::vector<std::string> sample_lines;
    std::istringstream stream(serialized);
    for (std::string line; std::getline(stream, line);) {
        if (line.rfind("test_multi_label_histogram", 0) == 0) {
            sample_lines.push_back(line);
        }
    }

    std::set<std::string> unique_series;
    for (const auto& line : sample_lines) {
        const auto value_separator = line.rfind(' ');
        ASSERT_NE(value_separator, std::string::npos);
        unique_series.insert(line.substr(0, value_separator));
    }

    EXPECT_EQ(sample_lines.size(), 10);
    EXPECT_EQ(sample_lines.size(), unique_series.size());

    const auto expect_once = [&sample_lines](const std::string& sample) {
        EXPECT_EQ(std::count(sample_lines.begin(), sample_lines.end(), sample),
                  1)
            << sample;
    };
    expect_once(
        "test_multi_label_histogram_bucket{cluster=\"cluster-a\","
        "operation=\"read\",le=\"10.000000\"} 1");
    expect_once(
        "test_multi_label_histogram_bucket{cluster=\"cluster-a\","
        "operation=\"read\",le=\"20.000000\"} 2");
    expect_once(
        "test_multi_label_histogram_bucket{cluster=\"cluster-a\","
        "operation=\"read\",le=\"+Inf\"} 2");
    expect_once(
        "test_multi_label_histogram_sum{cluster=\"cluster-a\","
        "operation=\"read\"} 20");
    expect_once(
        "test_multi_label_histogram_count{cluster=\"cluster-a\","
        "operation=\"read\"} 2");
    expect_once(
        "test_multi_label_histogram_bucket{cluster=\"cluster-a\","
        "operation=\"write\",le=\"10.000000\"} 0");
    expect_once(
        "test_multi_label_histogram_bucket{cluster=\"cluster-a\","
        "operation=\"write\",le=\"20.000000\"} 0");
    expect_once(
        "test_multi_label_histogram_bucket{cluster=\"cluster-a\","
        "operation=\"write\",le=\"+Inf\"} 2");
    expect_once(
        "test_multi_label_histogram_sum{cluster=\"cluster-a\","
        "operation=\"write\"} 50");
    expect_once(
        "test_multi_label_histogram_count{cluster=\"cluster-a\","
        "operation=\"write\"} 2");

    std::string repeated;
    histogram.serialize(repeated);
    EXPECT_EQ(repeated, serialized);
}

TEST_F(ClientMetricsTest,
       ReplicaSelectionMetricsUseOnlyFixedLowCardinalityLabels) {
    ReplicaSelectionMetric metrics(
        std::map<std::string, std::string>{{"cluster", "cluster-a"}});

    ReplicaSelectionDecision agree;
    agree.mode = ReplicaSelectionMode::SHADOW;
    agree.outcome = ReplicaSelectionOutcome::SCORED_REMOTE_MEMORY;
    agree.selected = ReplicaRef{0, 10};
    agree.recommendation = ReplicaRef{0, 10};
    metrics.Observe(agree, 240);

    auto disagree = agree;
    disagree.recommendation = ReplicaRef{1, 11};
    metrics.Observe(disagree, 750);

    ReplicaSelectionDecision unavailable;
    unavailable.mode = ReplicaSelectionMode::SHADOW;
    unavailable.outcome =
        ReplicaSelectionOutcome::FALLBACK_SIGNAL_SOURCE_UNAVAILABLE;
    metrics.Observe(unavailable, 1800);

    EXPECT_EQ(
        metrics.decisions.value({"shadow", "scored_remote_memory", "agree"}),
        1);
    EXPECT_EQ(
        metrics.decisions.value({"shadow", "scored_remote_memory", "disagree"}),
        1);
    EXPECT_EQ(
        metrics.decisions.value({"shadow", "fallback_signal_source_unavailable",
                                 "neither_available"}),
        1);

    std::string serialized;
    metrics.serialize(serialized);
    EXPECT_NE(
        serialized.find("mode=\"shadow\",outcome=\"scored_remote_memory\","),
        std::string::npos);
    EXPECT_NE(serialized.find("comparison=\"agree\""), std::string::npos);
    EXPECT_NE(serialized.find("comparison=\"disagree\""), std::string::npos);
    EXPECT_NE(serialized.find("outcome=\"fallback_signal_source_unavailable\""),
              std::string::npos);
    EXPECT_EQ(serialized.find("tenant"), std::string::npos);
    EXPECT_EQ(serialized.find("endpoint"), std::string::npos);
    EXPECT_EQ(serialized.find("replica_id"), std::string::npos);
    EXPECT_EQ(serialized.find("key="), std::string::npos);
}

TEST_F(ClientMetricsTest, ReplicaSelectionMetricsAreExactUnderConcurrency) {
    ReplicaSelectionMetric metrics;
    ReplicaSelectionDecision decision;
    decision.mode = ReplicaSelectionMode::SHADOW;
    decision.outcome = ReplicaSelectionOutcome::SCORED_REMOTE_MEMORY;
    decision.selected = ReplicaRef{0, 1};
    decision.recommendation = ReplicaRef{1, 2};

    constexpr int kThreads = 32;
    constexpr int kIterations = 10000;
    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (int thread = 0; thread < kThreads; ++thread) {
        threads.emplace_back([&] {
            for (int i = 0; i < kIterations; ++i) {
                metrics.Observe(decision, 500);
            }
        });
    }
    for (auto& thread : threads) thread.join();

    EXPECT_EQ(
        metrics.decisions.value({"shadow", "scored_remote_memory", "disagree"}),
        static_cast<int64_t>(kThreads) * kIterations);

    std::string serialized;
    metrics.serialize(serialized);
    EXPECT_NE(
        serialized.find("mooncake_replica_selection_selector_latency_ns_count{"
                        "mode=\"shadow\",outcome=\"scored_remote_memory\"} " +
                        std::to_string(kThreads * kIterations)),
        std::string::npos);
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

TEST_F(ClientMetricsTest,
       ShadowSignalsRecommendDifferentRemoteReplicaWithoutChangingRead) {
    std::unordered_set<int> used_ports;
    const int master_rpc_port = GetTestPort(used_ports);
    const int master_http_port = GetTestPort(used_ports);
    const int source_a_port = GetTestPort(used_ports);
    const int source_b_port = GetTestPort(used_ports);
    const int reader_port = GetTestPort(used_ports);
    const int reader_http_port = GetTestPort(used_ports);
    ASSERT_GT(master_rpc_port, 0);
    ASSERT_GT(master_http_port, 0);
    ASSERT_GT(source_a_port, 0);
    ASSERT_GT(source_b_port, 0);
    ASSERT_GT(reader_port, 0);
    ASSERT_GT(reader_http_port, 0);

    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(mooncake::InProcMasterConfigBuilder()
                                 .set_rpc_port(master_rpc_port)
                                 .set_http_metrics_port(master_http_port)
                                 .set_http_metadata_port(0)
                                 .build()));

    const auto endpoint = [](int port) {
        return "127.0.0.1:" + std::to_string(port);
    };
    const auto source_a = RealClient::create();
    const auto source_b = RealClient::create();
    const auto reader = RealClient::create();
    const auto setup_source = [&](const std::shared_ptr<RealClient>& client,
                                  int port) {
        return client->setup_internal(
            endpoint(port), "P2PHANDSHAKE", 16 * 1024 * 1024, 16 * 1024 * 1024,
            "tcp", "", master.master_address(), nullptr, "", port, false, false,
            "", "default", false, 0);
    };

    ASSERT_TRUE(setup_source(source_a, source_a_port).has_value());
    ASSERT_TRUE(setup_source(source_b, source_b_port).has_value());
    ASSERT_TRUE(reader
                    ->setup_internal_with_options(
                        endpoint(reader_port), "P2PHANDSHAKE",
                        /*global_segment_size=*/0,
                        /*local_buffer_size=*/16 * 1024 * 1024, "tcp", "",
                        master.master_address(), nullptr, "", reader_port,
                        false, false, "", "default", true, reader_http_port,
                        ReplicaSelectionOptions{ReplicaSelectionMode::SHADOW})
                    .has_value());

    const std::string key = "shadow-remote-replica-integration";
    const std::string value(128 * 1024, 's');
    ReplicateConfig config;
    config.replica_num = 2;
    config.preferred_segments = {endpoint(source_a_port),
                                 endpoint(source_b_port)};
    ASSERT_EQ(reader->put(key, std::span<const char>(value), config), 0);

    auto query = reader->batch_query({key});
    ASSERT_EQ(query.size(), 1);
    ASSERT_TRUE(query[0].has_value());
    const auto& replicas = query[0]->replicas;
    std::vector<ReplicaEndpointProtocolKey> remote_candidates;
    for (const auto& replica : replicas) {
        if (replica.status != ReplicaStatus::COMPLETE ||
            !replica.is_memory_replica()) {
            continue;
        }
        const auto& descriptor =
            replica.get_memory_descriptor().buffer_descriptor;
        remote_candidates.push_back(
            {descriptor.transport_endpoint_, descriptor.protocol_});
    }
    ASSERT_EQ(remote_candidates.size(), 2);
    ASSERT_NE(remote_candidates[0], remote_candidates[1]);

    ReplicaSignalSnapshot snapshot;
    snapshot.generation = 1;
    snapshot.load_source = {true, std::chrono::steady_clock::now()};
    constexpr uint64_t kSaturationBytes = 1024ULL * 1024 * 1024;
    snapshot.load.emplace(
        remote_candidates[0],
        ReplicaLoadSignal{kSaturationBytes, kSaturationBytes, 1.0});
    snapshot.load.emplace(remote_candidates[1],
                          ReplicaLoadSignal{0, kSaturationBytes, 0.0});
    ASSERT_EQ(reader->publish_replica_signal_snapshot(std::move(snapshot)),
              ReplicaSignalPublishStatus::PUBLISHED);

    const auto buffer = reader->get_buffer(key);
    ASSERT_NE(buffer, nullptr);
    ASSERT_EQ(buffer->size(), value.size());
    EXPECT_EQ(
        std::string(static_cast<const char*>(buffer->ptr()), buffer->size()),
        value);

    const auto metrics = FetchUrl(
        "http://127.0.0.1:" + std::to_string(reader_http_port) + "/metrics");
    ASSERT_EQ(metrics.status, 200);
    EXPECT_NE(metrics.body.find(
                  "mooncake_replica_selection_decisions_total{client_mode=\""
                  "real\",mode=\"shadow\",outcome=\"scored_remote_memory\","
                  "comparison=\"disagree\"} 1"),
              std::string::npos)
        << metrics.body;

    EXPECT_EQ(reader->tearDownAll(), 0);
    EXPECT_EQ(source_b->tearDownAll(), 0);
    EXPECT_EQ(source_a->tearDownAll(), 0);
}

TEST_F(ClientMetricsTest,
       ShadowLiveSignalsObserveRealQueuedReadsAndPreserveData) {
    std::unordered_set<int> used_ports;
    const int master_rpc_port = GetTestPort(used_ports);
    const int master_http_port = GetTestPort(used_ports);
    const int source_a_port = GetTestPort(used_ports);
    const int source_b_port = GetTestPort(used_ports);
    const int reader_port = GetTestPort(used_ports);
    const int reader_http_port = GetTestPort(used_ports);
    ASSERT_GT(reader_http_port, 0);

    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(mooncake::InProcMasterConfigBuilder()
                                 .set_rpc_port(master_rpc_port)
                                 .set_http_metrics_port(master_http_port)
                                 .set_http_metadata_port(0)
                                 .build()));
    const auto endpoint = [](int port) {
        return "127.0.0.1:" + std::to_string(port);
    };
    const auto source_a = RealClient::create();
    const auto source_b = RealClient::create();
    const auto reader = RealClient::create();
    const auto setup_source = [&](const std::shared_ptr<RealClient>& client,
                                  int port) {
        return client->setup_internal(
            endpoint(port), "P2PHANDSHAKE", 64 * 1024 * 1024, 16 * 1024 * 1024,
            "tcp", "", master.master_address(), nullptr, "", port, false, false,
            "", "default", false, 0);
    };
    ASSERT_TRUE(setup_source(source_a, source_a_port).has_value());
    ASSERT_TRUE(setup_source(source_b, source_b_port).has_value());
    ASSERT_TRUE(
        reader
            ->setup_internal_with_options(
                endpoint(reader_port), "P2PHANDSHAKE", 0, 256 * 1024 * 1024,
                "tcp", "", master.master_address(), nullptr, "", reader_port,
                false, false, "", "default", true, reader_http_port,
                ReplicaSelectionOptions{ReplicaSelectionMode::SHADOW, true})
            .has_value());

    const std::string key = "shadow-live-real-queue-integration";
    const std::string value(8 * 1024 * 1024, 'q');
    ReplicateConfig config;
    config.replica_num = 2;
    config.preferred_segments = {endpoint(source_a_port),
                                 endpoint(source_b_port)};
    ASSERT_EQ(reader->put(key, std::span<const char>(value), config), 0);

    constexpr int kReaders = 16;
    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> successful_reads{0};
    std::vector<std::thread> workers;
    workers.reserve(kReaders);
    for (int i = 0; i < kReaders; ++i) {
        workers.emplace_back([&] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            const auto buffer = reader->get_buffer(key);
            if (buffer && buffer->size() == value.size() &&
                std::string(static_cast<const char*>(buffer->ptr()),
                            buffer->size()) == value) {
                successful_reads.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != kReaders) {
        std::this_thread::yield();
    }
    const auto read_start = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();
    const auto read_elapsed_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - read_start)
            .count();
    ASSERT_EQ(successful_reads.load(std::memory_order_relaxed), kReaders);

    const auto metrics = FetchUrl(
        "http://127.0.0.1:" + std::to_string(reader_http_port) + "/metrics");
    ASSERT_EQ(metrics.status, 200);
    const std::string prefix =
        "mooncake_replica_selection_decisions_total{client_mode=\"real\","
        "mode=\"shadow\",outcome=\"scored_remote_memory\",comparison=\""
        "disagree\"}";
    const auto metric_pos = metrics.body.find(prefix);
    ASSERT_NE(metric_pos, std::string::npos) << metrics.body;
    const auto value_pos = metric_pos + prefix.size() + 1;
    ASSERT_LT(value_pos, metrics.body.size());
    const auto disagreements = std::stoull(metrics.body.substr(value_pos));
    EXPECT_GT(disagreements, 0u) << metrics.body;
    std::cout << "SHADOW_LIVE_RESULT readers=" << kReaders
              << " bytes_per_read=" << value.size() << " successful_reads="
              << successful_reads.load(std::memory_order_relaxed)
              << " disagreements=" << disagreements
              << " elapsed_us=" << read_elapsed_us << std::endl;

    EXPECT_EQ(reader->tearDownAll(), 0);
    EXPECT_EQ(source_b->tearDownAll(), 0);
    EXPECT_EQ(source_a->tearDownAll(), 0);
}

TEST_F(ClientMetricsTest,
       ShadowLiveSignalsObserveRemoteBatchReadQueueAndPreserveData) {
    std::unordered_set<int> used_ports;
    const int master_rpc_port = GetTestPort(used_ports);
    const int master_http_port = GetTestPort(used_ports);
    const int source_a_port = GetTestPort(used_ports);
    const int source_b_port = GetTestPort(used_ports);
    const int reader_port = GetTestPort(used_ports);
    const int reader_http_port = GetTestPort(used_ports);
    ASSERT_GT(reader_http_port, 0);

    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(mooncake::InProcMasterConfigBuilder()
                                 .set_rpc_port(master_rpc_port)
                                 .set_http_metrics_port(master_http_port)
                                 .set_http_metadata_port(0)
                                 .build()));
    const auto endpoint = [](int port) {
        return "127.0.0.1:" + std::to_string(port);
    };
    const auto source_a = RealClient::create();
    const auto source_b = RealClient::create();
    const auto reader = RealClient::create();
    const auto setup_source = [&](const std::shared_ptr<RealClient>& client,
                                  int port) {
        return client->setup_internal(
            endpoint(port), "P2PHANDSHAKE", 128 * 1024 * 1024, 16 * 1024 * 1024,
            "tcp", "", master.master_address(), nullptr, "", port, false, false,
            "", "default", false, 0);
    };
    ASSERT_TRUE(setup_source(source_a, source_a_port).has_value());
    ASSERT_TRUE(setup_source(source_b, source_b_port).has_value());
    ASSERT_TRUE(
        reader
            ->setup_internal_with_options(
                endpoint(reader_port), "P2PHANDSHAKE", 0, 512 * 1024 * 1024,
                "tcp", "", master.master_address(), nullptr, "", reader_port,
                false, false, "", "default", true, reader_http_port,
                ReplicaSelectionOptions{ReplicaSelectionMode::SHADOW, true})
            .has_value());

    constexpr int kKeys = 4;
    constexpr int kReaders = 8;
    constexpr size_t kBytesPerKey = 8 * 1024 * 1024;
    std::vector<std::string> keys;
    std::vector<std::string> values;
    keys.reserve(kKeys);
    values.reserve(kKeys);
    ReplicateConfig config;
    config.replica_num = 2;
    config.preferred_segments = {endpoint(source_a_port),
                                 endpoint(source_b_port)};
    for (int i = 0; i < kKeys; ++i) {
        keys.push_back("shadow-live-batch-queue-" + std::to_string(i));
        values.emplace_back(kBytesPerKey, static_cast<char>('a' + i));
        ASSERT_EQ(reader->put(keys.back(), std::span<const char>(values.back()),
                              config),
                  0);
    }

    std::atomic<int> ready{0};
    std::atomic<bool> start{false};
    std::atomic<int> successful_batches{0};
    std::vector<std::thread> workers;
    workers.reserve(kReaders);
    for (int i = 0; i < kReaders; ++i) {
        workers.emplace_back([&] {
            ready.fetch_add(1, std::memory_order_release);
            while (!start.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            const auto buffers = reader->batch_get_buffer(keys);
            bool valid = buffers.size() == values.size();
            for (size_t j = 0; valid && j < buffers.size(); ++j) {
                valid = buffers[j] && buffers[j]->size() == values[j].size() &&
                        std::string(static_cast<const char*>(buffers[j]->ptr()),
                                    buffers[j]->size()) == values[j];
            }
            if (valid) {
                successful_batches.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    while (ready.load(std::memory_order_acquire) != kReaders) {
        std::this_thread::yield();
    }
    const auto read_start = std::chrono::steady_clock::now();
    start.store(true, std::memory_order_release);
    for (auto& worker : workers) worker.join();
    const auto read_elapsed_us =
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - read_start)
            .count();
    ASSERT_EQ(successful_batches.load(std::memory_order_relaxed), kReaders);

    const auto metrics = FetchUrl(
        "http://127.0.0.1:" + std::to_string(reader_http_port) + "/metrics");
    ASSERT_EQ(metrics.status, 200);
    const std::string disagree =
        "mooncake_replica_selection_decisions_total{client_mode=\"real\","
        "mode=\"shadow\",outcome=\"scored_remote_memory\",comparison=\""
        "disagree\"}";
    const auto metric_pos = metrics.body.find(disagree);
    ASSERT_NE(metric_pos, std::string::npos) << metrics.body;
    const auto value_pos = metric_pos + disagree.size() + 1;
    ASSERT_LT(value_pos, metrics.body.size());
    const auto disagreements = std::stoull(metrics.body.substr(value_pos));
    EXPECT_GT(disagreements, 0u) << metrics.body;
    std::cout << "SHADOW_LIVE_BATCH_RESULT readers=" << kReaders
              << " keys_per_batch=" << kKeys
              << " bytes_per_key=" << kBytesPerKey << " successful_batches="
              << successful_batches.load(std::memory_order_relaxed)
              << " disagreements=" << disagreements
              << " elapsed_us=" << read_elapsed_us << std::endl;

    EXPECT_EQ(reader->tearDownAll(), 0);
    EXPECT_EQ(source_b->tearDownAll(), 0);
    EXPECT_EQ(source_a->tearDownAll(), 0);
}

TEST_F(ClientMetricsTest,
       ShadowLiveSignalsAttributeRealTcpFailuresAndHardVetoEndpoint) {
    std::unordered_set<int> used_ports;
    const int master_rpc_port = GetTestPort(used_ports);
    const int master_http_port = GetTestPort(used_ports);
    const int source_a_port = GetTestPort(used_ports);
    const int source_b_port = GetTestPort(used_ports);
    const int reader_port = GetTestPort(used_ports);
    const int reader_http_port = GetTestPort(used_ports);
    ASSERT_GT(reader_http_port, 0);

    mooncake::testing::InProcMaster master;
    ASSERT_TRUE(master.Start(mooncake::InProcMasterConfigBuilder()
                                 .set_rpc_port(master_rpc_port)
                                 .set_http_metrics_port(master_http_port)
                                 .set_http_metadata_port(0)
                                 .build()));
    const auto endpoint = [](int port) {
        return "127.0.0.1:" + std::to_string(port);
    };
    const auto source_a = RealClient::create();
    const auto source_b = RealClient::create();
    const auto reader = RealClient::create();
    const auto setup_source = [&](const std::shared_ptr<RealClient>& client,
                                  int port) {
        return client->setup_internal(
            endpoint(port), "P2PHANDSHAKE", 16 * 1024 * 1024, 16 * 1024 * 1024,
            "tcp", "", master.master_address(), nullptr, "", port, false, false,
            "", "default", false, 0);
    };
    ASSERT_TRUE(setup_source(source_a, source_a_port).has_value());
    ASSERT_TRUE(setup_source(source_b, source_b_port).has_value());
    ASSERT_TRUE(
        reader
            ->setup_internal_with_options(
                endpoint(reader_port), "P2PHANDSHAKE", 0, 16 * 1024 * 1024,
                "tcp", "", master.master_address(), nullptr, "", reader_port,
                false, false, "", "default", true, reader_http_port,
                ReplicaSelectionOptions{ReplicaSelectionMode::SHADOW, true})
            .has_value());

    const std::string key = "shadow-live-real-failure-integration";
    const std::string value(128 * 1024, 'f');
    ReplicateConfig config;
    config.replica_num = 2;
    config.preferred_segments = {endpoint(source_a_port),
                                 endpoint(source_b_port)};
    ASSERT_EQ(reader->put(key, std::span<const char>(value), config), 0);

    auto query = reader->batch_query({key});
    ASSERT_EQ(query.size(), 1);
    ASSERT_TRUE(query[0].has_value());
    const auto first_complete =
        std::find_if(query[0]->replicas.begin(), query[0]->replicas.end(),
                     [](const Replica::Descriptor& replica) {
                         return replica.status == ReplicaStatus::COMPLETE &&
                                replica.is_memory_replica();
                     });
    ASSERT_NE(first_complete, query[0]->replicas.end());
    const auto failed_endpoint = first_complete->get_memory_descriptor()
                                     .buffer_descriptor.transport_endpoint_;
    ASSERT_FALSE(failed_endpoint.empty());

    PyClient::QueryResultCache query_cache;
    query_cache.emplace(key, std::move(query[0]));
    ASSERT_EQ(source_a->tearDownAll(), 0);
    ASSERT_EQ(source_b->tearDownAll(), 0);

    std::vector<char> destination(value.size());
    ASSERT_EQ(reader->register_buffer(destination.data(), destination.size()),
              0);
    const std::vector<void*> buffers{destination.data()};
    const std::vector<std::vector<std::string>> keys{{key}};
    const std::vector<std::vector<std::vector<size_t>>> offsets{{{0}}};
    const std::vector<std::vector<std::vector<size_t>>> sizes{{{value.size()}}};

    for (int attempt = 0; attempt < 4; ++attempt) {
        const auto results = reader->get_into_ranges(
            buffers, keys, offsets, offsets, sizes, &query_cache);
        ASSERT_EQ(results.size(), 1);
        ASSERT_EQ(results[0].size(), 1);
        ASSERT_EQ(results[0][0].size(), 1);
        EXPECT_LT(results[0][0][0], 0) << "attempt=" << attempt;
    }

    const auto metrics = FetchUrl(
        "http://127.0.0.1:" + std::to_string(reader_http_port) + "/metrics");
    ASSERT_EQ(metrics.status, 200);
    const std::string disagree =
        "mooncake_replica_selection_decisions_total{client_mode=\"real\","
        "mode=\"shadow\",outcome=\"scored_remote_memory\",comparison=\""
        "disagree\"}";
    const auto metric_pos = metrics.body.find(disagree);
    ASSERT_NE(metric_pos, std::string::npos) << metrics.body;
    const auto value_pos = metric_pos + disagree.size() + 1;
    ASSERT_LT(value_pos, metrics.body.size());
    const auto disagreements = std::stoull(metrics.body.substr(value_pos));
    EXPECT_GT(disagreements, 0u) << metrics.body;
    std::cout << "SHADOW_LIVE_FAILURE_RESULT attempts=4 failed_endpoint="
              << failed_endpoint << " disagreements=" << disagreements
              << std::endl;

    EXPECT_EQ(reader->unregister_buffer(destination.data()), 0);
    EXPECT_EQ(reader->tearDownAll(), 0);
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
