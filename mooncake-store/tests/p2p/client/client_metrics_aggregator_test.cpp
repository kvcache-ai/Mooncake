// Tests for ClientMetricsAggregator, accessed through
// P2PMasterMetricManager.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "p2p/heartbeat_type.h"
#include "master_metric_manager.h"
#include "p2p/p2p_master_metric_manager.h"

namespace mooncake {
namespace test {
namespace {

ClientMetricSnapshot MakeSnapshot() { return ClientMetricSnapshot{}; }

std::optional<int64_t> ParseMetricValue(const std::string& text,
                                        const std::string& name) {
    std::istringstream iss(text);
    std::string line;
    while (std::getline(iss, line)) {
        if (line.rfind(name + " ", 0) == 0) {
            return std::stoll(line.substr(name.size() + 1));
        }
    }
    return std::nullopt;
}

void ExpectMetricValue(const std::string& text, const std::string& name,
                       int64_t expected) {
    auto value = ParseMetricValue(text, name);
    ASSERT_TRUE(value.has_value()) << "metric not found: " << name;
    EXPECT_EQ(*value, expected);
}

}  // namespace

class ClientMetricsAggregatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ClientMetricsAggregatorTest");
        FLAGS_logtostderr = 1;
        MasterMetricManager::instance().reset_all_metrics();
        P2PMasterMetricManager::instance().ResetClientMetricsForTest();

        // Remove all known clients to subtract their contributions.
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    static std::string Serialize() {
        std::string base = MasterMetricManager::instance().serialize_metrics(); std::string client; P2PMasterMetricManager::instance().SerializeClientMetrics(client); return base + client;
    }

    static void Update(const UUID& client_id,
                       const ClientMetricSnapshot& snap) {
        P2PMasterMetricManager::instance().UpdateClientMetrics(client_id, snap);
    }

    static void Remove(const UUID& client_id) {
        P2PMasterMetricManager::instance().OnClientRemoved(client_id);
    }
};

TEST_F(ClientMetricsAggregatorTest, FirstSnapshotAddsFullValues) {
    ClientMetricSnapshot snap = MakeSnapshot();
    snap.total_request.get_requests = 10;
    snap.total_request.get_hits = 7;
    snap.remote_request.read_retries = 3;
    Update({1, 1}, snap);

    const std::string text = Serialize();
    // First join adds the client's full cumulative values (no baseline).
    ExpectMetricValue(text, "master_cluster_total_get_requests", 10);
    ExpectMetricValue(text, "master_cluster_total_get_hits", 7);
    ExpectMetricValue(text, "master_cluster_remote_read_retries", 3);
}

TEST_F(ClientMetricsAggregatorTest, DeltaAccumulation) {
    UUID client{1, 1};

    ClientMetricSnapshot first = MakeSnapshot();
    first.total_request.get_requests = 10;
    first.total_request.get_bytes = 100;
    Update(client, first);  // first join: added in full

    ClientMetricSnapshot second = MakeSnapshot();
    second.total_request.get_requests = 25;
    second.total_request.get_bytes = 300;
    Update(client, second);

    const std::string text = Serialize();
    ExpectMetricValue(text, "master_cluster_total_get_requests", 25);
    ExpectMetricValue(text, "master_cluster_total_get_bytes", 300);
}

TEST_F(ClientMetricsAggregatorTest, MultipleClientsSum) {
    UUID client_a{1, 1};
    UUID client_b{2, 2};

    ClientMetricSnapshot a1 = MakeSnapshot();
    a1.total_request.get_requests = 10;
    Update(client_a, a1);  // first join
    ClientMetricSnapshot a2 = MakeSnapshot();
    a2.total_request.get_requests = 20;
    Update(client_a, a2);

    ClientMetricSnapshot b1 = MakeSnapshot();
    b1.total_request.get_requests = 5;
    Update(client_b, b1);  // first join
    ClientMetricSnapshot b2 = MakeSnapshot();
    b2.total_request.get_requests = 12;
    Update(client_b, b2);

    ExpectMetricValue(Serialize(), "master_cluster_total_get_requests", 32);
}

TEST_F(ClientMetricsAggregatorTest, OnClientRemovedSubtractsContribution) {
    UUID client{1, 1};

    ClientMetricSnapshot first = MakeSnapshot();
    first.total_request.get_requests = 10;
    Update(client, first);
    ClientMetricSnapshot second = MakeSnapshot();
    second.total_request.get_requests = 20;
    Update(client, second);
    ExpectMetricValue(Serialize(), "master_cluster_total_get_requests", 20);

    Remove(client);
    // Removal subtracts the client's last reported values.
    ExpectMetricValue(Serialize(), "master_cluster_total_get_requests", 0);

    // A later re-registration is a fresh join: full values are added again.
    ClientMetricSnapshot third = MakeSnapshot();
    third.total_request.get_requests = 25;
    Update(client, third);
    ExpectMetricValue(Serialize(), "master_cluster_total_get_requests", 25);
}

TEST_F(ClientMetricsAggregatorTest, RetryCountersSupportNegativeDeltas) {
    UUID client{1, 1};

    ClientMetricSnapshot first = MakeSnapshot();
    first.remote_request.read_retries = 10;
    first.remote_request.write_retries = 5;
    Update(client, first);  // first join: full values added

    // Restarted client: retry counters fell back; negative deltas must pull
    // the gauges down to the new cumulative values.
    ClientMetricSnapshot second = MakeSnapshot();
    second.remote_request.read_retries = 4;
    second.remote_request.write_retries = 2;
    Update(client, second);

    const std::string text = Serialize();
    ExpectMetricValue(text, "master_cluster_remote_read_retries", 4);
    ExpectMetricValue(text, "master_cluster_remote_write_retries", 2);

    Remove(client);
    const std::string after_removal = Serialize();
    ExpectMetricValue(after_removal, "master_cluster_remote_read_retries", 0);
    ExpectMetricValue(after_removal, "master_cluster_remote_write_retries", 0);
}

TEST_F(ClientMetricsAggregatorTest, SerializeFormat) {
    UUID client{1, 1};

    ClientMetricSnapshot first = MakeSnapshot();
    first.total_request.get_requests = 1;
    Update(client, first);

    const std::string text = Serialize();
    EXPECT_NE(text.find("# TYPE master_cluster_total_get_requests gauge"),
              std::string::npos);
    EXPECT_NE(text.find("master_cluster_total_get_requests 1\n"),
              std::string::npos);
    // No latency histogram series.
    EXPECT_EQ(text.find("master_cluster_get_latency_success"),
              std::string::npos);
}

TEST_F(ClientMetricsAggregatorTest, SummaryContent) {
    UUID client{1, 1};

    ClientMetricSnapshot first = MakeSnapshot();
    first.total_request.get_requests = 100;
    first.total_request.get_hits = 80;
    first.total_request.get_misses = 15;
    first.total_request.get_failures = 5;
    first.total_request.put_requests = 40;
    first.total_request.put_failures = 2;
    first.local_request.get_requests = 60;
    first.remote_request.data.get_requests = 40;
    first.remote_request.read_retries = 3;
    first.remote_request.write_retries = 1;
    Update(client, first);  // first join: added in full

    ClientMetricSnapshot second = first;
    second.total_request.get_requests = 200;
    second.total_request.get_hits = 160;
    second.total_request.get_misses = 30;
    second.total_request.get_failures = 10;
    second.total_request.put_requests = 80;
    second.total_request.put_failures = 4;
    second.local_request.get_requests = 120;
    second.remote_request.data.get_requests = 80;
    second.remote_request.read_retries = 6;
    second.remote_request.write_retries = 2;
    Update(client, second);

    const std::string summary =
        MasterMetricManager::instance().get_summary_string() + P2PMasterMetricManager::instance().SummaryClientMetrics();
    EXPECT_NE(summary.find("Cluster Data Plane: "), std::string::npos);
    // hit_rate = 160 / (160 + 30) = 84.2%.
    EXPECT_NE(summary.find("Get(total): requests=200, hits=160, misses=30, "
                           "failures=10, bytes=0 B (hit_rate=84.2%)"),
              std::string::npos);
    EXPECT_NE(summary.find("Get(local): requests=120"), std::string::npos);
    EXPECT_NE(summary.find("Get(remote): requests=80"), std::string::npos);
    EXPECT_NE(summary.find("Put(total): requests=80, failures=4"),
              std::string::npos);
    EXPECT_NE(summary.find("Put(local): requests=0"), std::string::npos);
    EXPECT_NE(summary.find("Put(remote): requests=0"), std::string::npos);
    EXPECT_NE(summary.find("Retries: read=6, write=2"), std::string::npos);

    // Section order: Get(total/local/remote), Put(total/local/remote),
    // retries.
    const size_t pos_total = summary.find("Get(total):");
    const size_t pos_local = summary.find("Get(local):");
    const size_t pos_remote = summary.find("Get(remote):");
    const size_t pos_put_total = summary.find("Put(total):");
    const size_t pos_put_local = summary.find("Put(local):");
    const size_t pos_put_remote = summary.find("Put(remote):");
    const size_t pos_retries = summary.find("Retries: read=6, write=2");
    EXPECT_LT(pos_total, pos_local);
    EXPECT_LT(pos_local, pos_remote);
    EXPECT_LT(pos_remote, pos_put_total);
    EXPECT_LT(pos_put_total, pos_put_local);
    EXPECT_LT(pos_put_local, pos_put_remote);
    EXPECT_LT(pos_put_remote, pos_retries);
}

TEST_F(ClientMetricsAggregatorTest, SummaryNoData) {
    const std::string summary =
        MasterMetricManager::instance().get_summary_string() + P2PMasterMetricManager::instance().SummaryClientMetrics();
    // hit_rate is omitted when there is no data.
    EXPECT_NE(summary.find("Cluster Data Plane: Get(total): requests=0"),
              std::string::npos);
    EXPECT_EQ(summary.find("hit_rate="), std::string::npos);
    EXPECT_NE(summary.find("Retries: read=0, write=0"), std::string::npos);
}

// Concurrent Update()/OnClientRemoved()/Serialize(): final gauges must equal
// the sum of the last snapshots of the still-registered clients.
TEST_F(ClientMetricsAggregatorTest, ConcurrentUpdateSerializeAndRemove) {
    constexpr int kNumWriters = 4;
    constexpr int kClientsPerWriter = 2;
    constexpr int kIterations = 200;

    std::vector<std::thread> writers;
    writers.reserve(kNumWriters);
    for (int t = 0; t < kNumWriters; ++t) {
        writers.emplace_back([&, t]() {
            for (int c = 0; c < kClientsPerWriter; ++c) {
                UUID client_id{static_cast<uint64_t>(t + 1),
                               static_cast<uint64_t>(c + 1)};
                for (int i = 1; i <= kIterations; ++i) {
                    ClientMetricSnapshot snap;
                    snap.total_request.get_requests = i;
                    snap.total_request.get_bytes = i * 10;
                    snap.remote_request.data.get_requests = i;
                    Update(client_id, snap);
                }
            }
        });
    }

    std::atomic<bool> stop{false};
    std::thread reader([&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            (void)Serialize();
            (void)(MasterMetricManager::instance().get_summary_string() + P2PMasterMetricManager::instance().SummaryClientMetrics());
            std::this_thread::yield();
        }
    });

    // Churn client: exercises Update()/OnClientRemoved() concurrently.
    UUID churn_client{9999, 9999};
    std::thread remover([&]() {
        for (int i = 1; i <= kIterations; ++i) {
            ClientMetricSnapshot snap;
            snap.total_request.get_requests = i;
            Update(churn_client, snap);
            Remove(churn_client);
        }
    });

    for (auto& w : writers) w.join();
    remover.join();
    stop.store(true, std::memory_order_relaxed);
    reader.join();

    // Only the writers' clients remain; each last snapshot carries
    // get_requests = kIterations.
    const int64_t expected_requests =
        static_cast<int64_t>(kNumWriters) * kClientsPerWriter * kIterations;
    const std::string text = Serialize();
    ExpectMetricValue(text, "master_cluster_total_get_requests",
                      expected_requests);
    ExpectMetricValue(text, "master_cluster_total_get_bytes",
                      expected_requests * 10);
    ExpectMetricValue(text, "master_cluster_remote_get_requests",
                      expected_requests);

    // Removing every remaining client zeroes the gauges again.
    for (int t = 0; t < kNumWriters; ++t) {
        for (int c = 0; c < kClientsPerWriter; ++c) {
            Remove(UUID{static_cast<uint64_t>(t + 1),
                        static_cast<uint64_t>(c + 1)});
        }
    }
    const std::string empty = Serialize();
    ExpectMetricValue(empty, "master_cluster_total_get_requests", 0);
    ExpectMetricValue(empty, "master_cluster_total_get_bytes", 0);
    ExpectMetricValue(empty, "master_cluster_remote_get_requests", 0);
}

}  // namespace test
}  // namespace mooncake
