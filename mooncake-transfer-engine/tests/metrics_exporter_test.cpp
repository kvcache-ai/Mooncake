// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

#include <csignal>  // Required before ylt headers: coro_io.hpp uses SIGPIPE
#include <ylt/coro_http/coro_http_client.hpp>

#include "metrics_exporter.h"
#include "transfer_engine_metrics.h"

namespace mooncake {
namespace {

// Holds a listening loopback socket on a kernel-picked port until released.
class PortOccupier {
   public:
    PortOccupier() {
        fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        EXPECT_GE(fd_, 0);
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        EXPECT_EQ(::bind(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)),
                  0);
        EXPECT_EQ(::listen(fd_, 1), 0);

        sockaddr_in bound{};
        socklen_t len = sizeof(bound);
        EXPECT_EQ(::getsockname(fd_, reinterpret_cast<sockaddr*>(&bound), &len),
                  0);
        port_ = ntohs(bound.sin_port);
    }

    ~PortOccupier() { release(); }

    void release() {
        if (fd_ >= 0) ::close(fd_);
        fd_ = -1;
    }

    uint16_t port() const { return port_; }

   private:
    int fd_ = -1;
    uint16_t port_ = 0;
};

uint16_t getFreeTcpPort() { return PortOccupier().port(); }

struct HttpResponse {
    int http_status;
    std::string body;
};

// Retries absorb the window between async_start() and the server accepting.
HttpResponse retryGet(uint16_t port, const std::string& path,
                      int attempts = 20) {
    HttpResponse resp{0, ""};
    for (int i = 0; i < attempts; ++i) {
        coro_http::coro_http_client client;
        auto result =
            client.get("http://127.0.0.1:" + std::to_string(port) + path);
        resp = {result.status, std::string(result.resp_body)};
        if (resp.http_status == 200) return resp;
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    }
    return resp;
}

struct OwnerMetrics {
    ylt::metric::counter_t requests_total{"exporter_test_requests_total",
                                          "Requests seen by the test owner"};
    metrics::Histogram latency_us{"exporter_test_latency_us",
                                  "Latency in microseconds",
                                  std::vector<double>{100, 1000, 10000}};
};

class MetricsExporterTest : public ::testing::Test {
   protected:
    void SetUp() override {
        exporter_.addCounter(&owner_.requests_total);
        exporter_.addHistogram(&owner_.latency_us);
        exporter_.setSummaryProvider(
            []() { return std::string("owner summary line"); });
    }

    static metrics::ExporterConfig localConfig(uint16_t port) {
        metrics::ExporterConfig config;
        config.http_host = "127.0.0.1";
        config.http_port = port;
        return config;
    }

    void recordSomething() {
        owner_.requests_total.inc();
        owner_.latency_us.observe(250);
    }

    OwnerMetrics owner_;
    metrics::MetricsExporter exporter_{"Exporter Test"};
};

TEST_F(MetricsExporterTest, FromEnvReadsHttpVariables) {
    setenv("MC_TE_METRIC_TEST_HTTP_HOST", "127.0.0.1", 1);
    setenv("MC_TE_METRIC_TEST_HTTP_PORT", "19321", 1);
    setenv("MC_TE_METRIC_TEST_HTTP_THREADS", "3", 1);

    auto config = metrics::ExporterConfig::fromEnv("MC_TE_METRIC_TEST");
    EXPECT_EQ(config.http_host, "127.0.0.1");
    EXPECT_EQ(config.http_port, 19321);
    EXPECT_EQ(config.http_server_threads, 3);

    unsetenv("MC_TE_METRIC_TEST_HTTP_HOST");
    unsetenv("MC_TE_METRIC_TEST_HTTP_PORT");
    unsetenv("MC_TE_METRIC_TEST_HTTP_THREADS");
}

TEST_F(MetricsExporterTest, FromEnvKeepsDefaultsOnBadValues) {
    setenv("MC_TE_METRIC_TEST_HTTP_PORT", "not-a-port", 1);
    setenv("MC_TE_METRIC_TEST_HTTP_THREADS", "0", 1);

    auto config = metrics::ExporterConfig::fromEnv("MC_TE_METRIC_TEST");
    EXPECT_EQ(config.http_port, 0);
    EXPECT_EQ(config.http_server_threads, 1);

    setenv("MC_TE_METRIC_TEST_HTTP_PORT", "70000", 1);
    EXPECT_EQ(metrics::ExporterConfig::fromEnv("MC_TE_METRIC_TEST").http_port,
              0);

    setenv("MC_TE_METRIC_TEST_HTTP_PORT", "9100abc", 1);
    EXPECT_EQ(metrics::ExporterConfig::fromEnv("MC_TE_METRIC_TEST").http_port,
              0);

    unsetenv("MC_TE_METRIC_TEST_HTTP_PORT");
    unsetenv("MC_TE_METRIC_TEST_HTTP_THREADS");
}

TEST_F(MetricsExporterTest, ServesAllFourEndpoints) {
    const uint16_t port = getFreeTcpPort();
    ASSERT_GT(port, 0);
    exporter_.start(localConfig(port));
    ASSERT_EQ(exporter_.httpPort(), port);

    recordSomething();

    auto health = retryGet(port, "/health");
    EXPECT_EQ(health.http_status, 200);
    EXPECT_EQ(health.body, "OK");

    auto prom = retryGet(port, "/metrics");
    EXPECT_EQ(prom.http_status, 200);
    EXPECT_NE(prom.body.find("exporter_test_requests_total"),
              std::string::npos);
    EXPECT_NE(prom.body.find("exporter_test_latency_us_sum"),
              std::string::npos);
    EXPECT_NE(prom.body.find("exporter_test_latency_us_count"),
              std::string::npos);

    auto json = retryGet(port, "/metrics/json");
    EXPECT_EQ(json.http_status, 200);
    EXPECT_NE(json.body.find("exporter_test_requests_total"),
              std::string::npos);
    EXPECT_NE(json.body.find("\"exporter_test_latency_us\":{\"count\":1,"
                             "\"sum\":250,\"buckets\":{"),
              std::string::npos)
        << json.body;

    auto summary = retryGet(port, "/metrics/summary");
    EXPECT_EQ(summary.http_status, 200);
    EXPECT_EQ(summary.body, "owner summary line");

    // A scrape reflects recording done after the server started.
    recordSomething();
    auto later = retryGet(port, "/metrics");
    ASSERT_EQ(later.http_status, 200);
    EXPECT_NE(later.body.find("exporter_test_requests_total 2"),
              std::string::npos);
}

// cinatra's async_start() signals a failed bind through its future rather
// than by throwing; that must not be reported as a listening endpoint.
TEST_F(MetricsExporterTest, BusyPortDoesNotReportListening) {
    PortOccupier occupier;
    ASSERT_GT(occupier.port(), 0);

    exporter_.start(localConfig(occupier.port()));
    EXPECT_EQ(exporter_.httpPort(), 0);
}

// Several engines in one process share the exporter; only the first start()
// configures it.
TEST_F(MetricsExporterTest, RepeatStartIsANoOp) {
    const uint16_t port = getFreeTcpPort();
    ASSERT_GT(port, 0);
    exporter_.start(localConfig(port));
    ASSERT_EQ(exporter_.httpPort(), port);

    exporter_.start(localConfig(0));
    exporter_.start(localConfig(port + 1));
    EXPECT_EQ(exporter_.httpPort(), port);
}

TEST_F(MetricsExporterTest, StopClearsBoundPortAndReleasesIt) {
    const uint16_t port = getFreeTcpPort();
    ASSERT_GT(port, 0);
    exporter_.start(localConfig(port));
    ASSERT_EQ(exporter_.httpPort(), port);

    exporter_.stop();
    EXPECT_EQ(exporter_.httpPort(), 0);

    metrics::MetricsExporter restarted("Exporter Test Restarted");
    restarted.start(localConfig(port));
    EXPECT_EQ(restarted.httpPort(), port);
    restarted.stop();

    exporter_.start(localConfig(port));
    EXPECT_EQ(exporter_.httpPort(), port);
}

using Direction = TransferEngineMetrics::Direction;

class TransferEngineMetricsHttpTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto& metrics = TransferEngineMetrics::instance();
        metrics.shutdown();

        port_ = getFreeTcpPort();
        ASSERT_GT(port_, 0);
        setenv("MC_TE_METRIC_HTTP_HOST", "127.0.0.1", 1);
        setenv("MC_TE_METRIC_HTTP_PORT", std::to_string(port_).c_str(), 1);
        metrics.initializeFromEnv();

        ASSERT_EQ(metrics.httpPort(), port_);
    }

    void TearDown() override {
        TransferEngineMetrics::instance().shutdown();
        unsetenv("MC_TE_METRIC_HTTP_HOST");
        unsetenv("MC_TE_METRIC_HTTP_PORT");
    }

    uint16_t port_ = 0;
};

// The MC_TE_METRIC_HTTP_* variables bind the Classic TE exporter.
TEST_F(TransferEngineMetricsHttpTest, RecordedTransfersAreScrapable) {
    auto& metrics = TransferEngineMetrics::instance();
    const auto before =
        metrics.forDirection(Direction::Read).bytes_total.value();
    metrics.recordCompleted(Direction::Read, 65536, 1500);

    auto resp = retryGet(port_, "/metrics");
    ASSERT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("mooncake_te_read_bytes_total " +
                             std::to_string(before + 65536)),
              std::string::npos);
}

}  // namespace
}  // namespace mooncake
