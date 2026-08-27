// Copyright 2026 KVCache.AI
//
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
//
// TDD tests for the TENT metrics recording path. Drives a real
// TransferEngineImpl with a FakeTransport (pattern borrowed from
// causal_chain_test.cpp) and asserts on TentMetrics JSON/Prometheus output.
//
// All assertions are DELTA-based (snapshot before, act, snapshot after,
// compare). TentMetrics is a process-wide singleton whose counter/histogram
// values are not reset by shutdown()/initialize(), so absolute-value
// assertions would be order-dependent across tests.

#include <gtest/gtest.h>

#include <algorithm>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "tent/common/config.h"
#include "tent/common/types.h"
#include "tent/metrics/config_loader.h"
#include "tent/metrics/tent_metrics.h"
#include "tent/runtime/transfer_engine_impl.h"
#include "tent/runtime/transport.h"
#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

#if TENT_METRICS_ENABLED

#include <csignal>  // required before ylt headers (coro_io uses std::signal)
#include <ylt/coro_http/coro_http_client.hpp>

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// Bind a loopback socket on port 0, read the assigned port, close, return it.
// TOCTOU race is inherent; callers re-check via TentMetrics::httpPort().
// Adapted from mooncake-store/src/utils.cpp getFreeTcpPort().
uint16_t getFreeTcpPort() {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) return 0;
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(0);
    if (::bind(sock, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(sock);
        return 0;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(sock, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(sock);
        return 0;
    }
    int port = ntohs(addr.sin_port);
    ::close(sock);
    return static_cast<uint16_t>(port);
}

// HTTP GET helper. Adapted from
// mooncake-store/tests/master_metrics_test.cpp FetchUrl().
struct HttpResponse {
    int http_status;
    std::string body;
};

HttpResponse FetchUrl(uint16_t port, const std::string& path) {
    coro_http::coro_http_client client;
    auto result = client.get("http://127.0.0.1:" + std::to_string(port) + path);
    return {result.status, std::string(result.resp_body)};
}

// Snapshot of TentMetrics JSON output. Provides delta-based accessors so
// tests are robust to singleton state accumulated by prior tests.
class MetricsSnapshot {
   public:
    explicit MetricsSnapshot(TentMetrics& m) {
        std::string body = m.getJsonMetrics();
        prometheus_ = m.getPrometheusMetrics();
        try {
            json_ = nlohmann::json::parse(body);
        } catch (...) {
            json_ = nlohmann::json::object();
        }
    }

    // Exact Prometheus series value (0 if absent). `series` includes its full
    // label set, for example:
    // tent_transport_attempts_total{transport="rdma",operation="write"}
    double series(const std::string& series) const {
        std::istringstream input(prometheus_);
        std::string line;
        const std::string prefix = series + " ";
        while (std::getline(input, line)) {
            if (line.rfind(prefix, 0) != 0) continue;
            try {
                return std::stod(line.substr(prefix.size()));
            } catch (...) {
                return 0.0;
            }
        }
        return 0.0;
    }

    // Counter value (0 if absent).
    double counter(const std::string& name) const {
        if (!json_.contains(name)) return 0.0;
        const auto& v = json_[name];
        return v.is_number() ? v.get<double>() : 0.0;
    }

    // Histogram total sample count (0 if absent).
    int64_t histogramCount(const std::string& name) const {
        if (!json_.contains(name) || !json_[name].is_object()) return 0;
        if (!json_[name].contains("count")) return 0;
        return json_[name]["count"].get<int64_t>();
    }

    // Histogram bucket value by boundary key (e.g. "100"). 0 if absent.
    int64_t bucket(const std::string& name, const std::string& le) const {
        if (!json_.contains(name) || !json_[name].contains("buckets")) return 0;
        const auto& buckets = json_[name]["buckets"];
        if (!buckets.contains(le)) return 0;
        return buckets[le].get<int64_t>();
    }

    // All bucket keys for a histogram (sorted ascending as integers).
    std::vector<int64_t> bucketKeys(const std::string& name) const {
        std::vector<int64_t> keys;
        if (!json_.contains(name) || !json_[name].contains("buckets"))
            return keys;
        for (auto it = json_[name]["buckets"].begin();
             it != json_[name]["buckets"].end(); ++it) {
            try {
                keys.push_back(std::stoll(it.key()));
            } catch (...) {
                // skip non-numeric keys (shouldn't happen for our histograms)
            }
        }
        std::sort(keys.begin(), keys.end());
        return keys;
    }

   private:
    nlohmann::json json_;
    std::string prometheus_;
};

// ---------------------------------------------------------------------------
// FakeTransport: minimal Transport that completes every submitted task.
// Pattern borrowed from causal_chain_test.cpp.
// ---------------------------------------------------------------------------

class FakeSubBatch : public Transport::SubBatch {
   public:
    size_t size() const override { return task_count; }
    size_t task_count = 0;
    std::vector<Request> requests;
    std::vector<TransferStatus> statuses;
};

class FakeTransport : public Transport {
   public:
    explicit FakeTransport(TransportType self_type, bool force_fail = false,
                           bool force_submit_fail = false)
        : self_type_(self_type),
          force_fail_(force_fail),
          force_submit_fail_(force_submit_fail) {
        caps.dram_to_dram = true;
    }
    std::atomic<int> submit_calls{0};

    Status install(std::string&, std::shared_ptr<ControlService>,
                   std::shared_ptr<Topology>,
                   std::shared_ptr<Config>) override {
        return Status::OK();
    }

    Status allocateSubBatch(SubBatchRef& batch, size_t) override {
        batch = new FakeSubBatch();
        return Status::OK();
    }

    Status freeSubBatch(SubBatchRef& batch) override {
        delete static_cast<FakeSubBatch*>(batch);
        batch = nullptr;
        return Status::OK();
    }

    Status submitTransferTasks(SubBatchRef batch,
                               const std::vector<Request>& requests) override {
        ++submit_calls;
        // Synchronous submit failure: report the error without enqueuing any
        // physical task, exercising the engine's !status.ok() attempt-failure
        // branches (commitPreparedSubmit / resubmitTransferTask / ...).
        if (force_submit_fail_) {
            return Status::InternalError("forced submit failure" LOC_MARK);
        }
        auto* fake = static_cast<FakeSubBatch*>(batch);
        for (const auto& request : requests) {
            fake->requests.push_back(request);
            fake->statuses.push_back(
                force_fail_ ? TransferStatus{TransferStatusEnum::FAILED, 0}
                            : TransferStatus{TransferStatusEnum::COMPLETED,
                                             request.length});
            ++fake->task_count;
        }
        batch->notifyProgress();
        return Status::OK();
    }

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        auto* fake = static_cast<FakeSubBatch*>(batch);
        if (task_id < 0 || task_id >= (int)fake->statuses.size()) {
            return Status::InvalidArgument("bad task_id" LOC_MARK);
        }
        status = fake->statuses[task_id];
        return Status::OK();
    }

    Status addMemoryBuffer(BufferDesc& desc, const MemoryOptions&) override {
        desc.transports.push_back(self_type_);
        return Status::OK();
    }

    Status addMemoryBuffer(std::vector<BufferDesc>& desc_list,
                           const MemoryOptions& options) override {
        for (auto& desc : desc_list) {
            auto s = addMemoryBuffer(desc, options);
            if (!s.ok()) return s;
        }
        return Status::OK();
    }

    Status removeMemoryBuffer(BufferDesc&) override { return Status::OK(); }

    Status allocateLocalMemory(void** addr, size_t size,
                               MemoryOptions&) override {
        *addr = std::malloc(size);
        return *addr ? Status::OK()
                     : Status::InternalError("malloc failed" LOC_MARK);
    }

    Status freeLocalMemory(void* addr, size_t) override {
        std::free(addr);
        return Status::OK();
    }

    bool warmupMemory(void*, size_t) override { return false; }

    const char* getName() const override { return "<fake-metrics>"; }

   private:
    TransportType self_type_;
    bool force_fail_;
    bool force_submit_fail_;
};

std::shared_ptr<Config> makeMetricsTestConfig() {
    auto cfg = std::make_shared<Config>();
    cfg->set("metadata_type", "p2p");
    cfg->set("metadata_servers", "");
    cfg->set("rpc_server_hostname", "127.0.0.1");
    cfg->set("rpc_server_port", "0");
    cfg->set("log_level", "warning");
    cfg->set("merge_requests", false);
    cfg->set("enable_runtime_queue", false);
    cfg->set("transports/tcp/enable", false);
    cfg->set("transports/shm/enable", false);
    cfg->set("transports/rdma/enable", false);
    cfg->set("transports/io_uring/enable", false);
    cfg->set("transports/nvlink/enable", false);
    cfg->set("transports/mnnvl/enable", false);
    cfg->set("transports/gds/enable", false);
    cfg->set("transports/ascend_direct/enable", false);
    return cfg;
}

void installFakeRdma(TransferEngineImpl& engine,
                     const std::shared_ptr<FakeTransport>& fake) {
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(fake->install(seg_name, nullptr, nullptr, nullptr).ok());
    engine.swapTransportForTest(RDMA, fake);
}

void installFakeTcp(TransferEngineImpl& engine,
                    const std::shared_ptr<FakeTransport>& fake) {
    std::string seg_name = engine.getSegmentName();
    ASSERT_TRUE(fake->install(seg_name, nullptr, nullptr, nullptr).ok());
    engine.swapTransportForTest(TCP, fake);
}

Request makeLocalWrite(uint8_t* ptr, size_t length, uint64_t deadline_ns = 0) {
    Request request;
    request.opcode = Request::WRITE;
    request.source = ptr;
    request.target_id = LOCAL_SEGMENT_ID;
    request.target_offset = reinterpret_cast<uint64_t>(ptr);
    request.length = length;
    request.transport_hint = RDMA;
    request.deadline_ns = deadline_ns;
    return request;
}

// Poll a batch/task until it reaches a terminal status or timeout (1s).
TransferStatusEnum pollUntilTerminal(TransferEngineImpl& engine, BatchID batch,
                                     int task_id = 0) {
    TransferStatus ts{};
    for (int i = 0; i < 200; ++i) {
        engine.getTransferStatus(batch, task_id, ts);
        if (ts.s == TransferStatusEnum::COMPLETED ||
            ts.s == TransferStatusEnum::FAILED) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return ts.s;
}

// ---------------------------------------------------------------------------
// Test fixture: re-initialize the TentMetrics singleton per test with a free
// HTTP port and no periodic reporting thread. Counter/histogram values are
// NOT reset (singleton members), so tests use delta assertions.
// ---------------------------------------------------------------------------
class MetricsRecordingTest : public ::testing::Test {
   protected:
    void SetUp() override {
        TentMetrics::instance().shutdown();
        MetricsConfig config;
        config.enabled = true;
        config.http_host = "127.0.0.1";
        config.http_port = getFreeTcpPort();
        config.report_interval_seconds = 0;
        auto status = TentMetrics::instance().initialize(config);
        ASSERT_TRUE(status.ok()) << status.ToString();
        TentMetrics::setEnabled(true);
    }

    void TearDown() override { TentMetrics::instance().shutdown(); }
};

// ---------------------------------------------------------------------------
// Completed transfer records bytes, requests, latency. Drives the real
// recordTaskCompletionMetrics path via FakeTransport.
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, CompletedTransferRecordsBytesAndLatency) {
    auto cfg = makeMetricsTestConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xBB);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(4);
    ASSERT_NE(batch, (BatchID)0);

    auto before = MetricsSnapshot(TentMetrics::instance());
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buf.data(), kLen)}).ok());
    ASSERT_EQ(pollUntilTerminal(engine, batch, 0),
              TransferStatusEnum::COMPLETED);
    auto after = MetricsSnapshot(TentMetrics::instance());

    EXPECT_EQ(after.counter("tent_write_bytes_total") -
                  before.counter("tent_write_bytes_total"),
              kLen);
    EXPECT_EQ(after.counter("tent_write_requests_total") -
                  before.counter("tent_write_requests_total"),
              1);
    EXPECT_EQ(after.histogramCount("tent_write_latency_us") -
                  before.histogramCount("tent_write_latency_us"),
              1);
    EXPECT_EQ(after.histogramCount("tent_write_size_bytes") -
                  before.histogramCount("tent_write_size_bytes"),
              1);
    EXPECT_EQ(after.counter("tent_transport_attempts_total") -
                  before.counter("tent_transport_attempts_total"),
              1);
    EXPECT_EQ(after.counter("tent_transport_attempt_failures_total") -
                  before.counter("tent_transport_attempt_failures_total"),
              0);
    EXPECT_EQ(after.histogramCount("tent_transport_attempt_latency_us") -
                  before.histogramCount("tent_transport_attempt_latency_us"),
              1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

// ---------------------------------------------------------------------------
// A recovered RDMA->TCP failover closes the failed RDMA physical attempt and
// opens a distinct TCP attempt. Logical request metrics remain final-transport
// compatible, while attempt metrics expose RDMA reliability and per-attempt
// latency without charging the RDMA/failover interval to TCP stage latency.
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, RecoveredFailoverRecordsDistinctAttempts) {
    auto cfg = makeMetricsTestConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, /*force_fail=*/true);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP);
    installFakeRdma(engine, fake_rdma);
    installFakeTcp(engine, fake_tcp);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xBC);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(4);
    ASSERT_NE(batch, (BatchID)0);

    auto before = MetricsSnapshot(TentMetrics::instance());
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buf.data(), kLen)}).ok());
    ASSERT_EQ(pollUntilTerminal(engine, batch, 0),
              TransferStatusEnum::COMPLETED);
    auto after = MetricsSnapshot(TentMetrics::instance());

    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 1);

    EXPECT_EQ(after.counter("tent_transport_attempts_total") -
                  before.counter("tent_transport_attempts_total"),
              2);
    EXPECT_EQ(after.counter("tent_transport_attempt_failures_total") -
                  before.counter("tent_transport_attempt_failures_total"),
              1);
    EXPECT_EQ(after.histogramCount("tent_transport_attempt_latency_us") -
                  before.histogramCount("tent_transport_attempt_latency_us"),
              2);

    const std::string rdma_attempts =
        "tent_transport_attempts_total{transport=\"rdma\",operation=\"write\"}";
    const std::string tcp_attempts =
        "tent_transport_attempts_total{transport=\"tcp\",operation=\"write\"}";
    const std::string rdma_failures =
        "tent_transport_attempt_failures_total{transport=\"rdma\",operation="
        "\"write\"}";
    EXPECT_EQ(after.series(rdma_attempts) - before.series(rdma_attempts), 1);
    EXPECT_EQ(after.series(tcp_attempts) - before.series(tcp_attempts), 1);
    EXPECT_EQ(after.series(rdma_failures) - before.series(rdma_failures), 1);

    // Logical compatibility: the recovered request completes once and does not
    // become a terminal request failure.
    EXPECT_EQ(after.counter("tent_write_requests_total") -
                  before.counter("tent_write_requests_total"),
              1);
    EXPECT_EQ(after.counter("tent_write_failures_total") -
                  before.counter("tent_write_failures_total"),
              0);

    // Backward-compatible stage decomposition: the causal-chain stage metrics
    // remain attributed to the final transport (tcp) and measure the full
    // request span, exactly as before this change. The per-attempt truth
    // (which transport actually failed, and each attempt's latency) lives in
    // the additive tent_transport_attempt_* metrics asserted above.
    const std::string tcp_dispatch_count =
        "tent_stage_dispatch_us_count{transport=\"tcp\"}";
    const std::string tcp_transport_count =
        "tent_stage_transport_us_count{transport=\"tcp\"}";
    // Direct submissions have exactly zero queue wait. YLT omits dynamic
    // histogram label series whose sample sum is zero from Prometheus output,
    // but the JSON aggregate still preserves the observation count.
    EXPECT_EQ(after.histogramCount("tent_stage_queue_wait_us") -
                  before.histogramCount("tent_stage_queue_wait_us"),
              1);
    EXPECT_EQ(
        after.series(tcp_dispatch_count) - before.series(tcp_dispatch_count),
        1);
    EXPECT_EQ(
        after.series(tcp_transport_count) - before.series(tcp_transport_count),
        1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

// ---------------------------------------------------------------------------
// A synchronous submit failure on the direct path (submitTransferTasks returns
// non-ok) is closed as a failed physical attempt. The attempt-started counter
// still increments (the engine committed to the attempt before submitting),
// the attempt-failure counter increments, and the logical request terminates
// as a failure. This exercises the commitPreparedSubmit !status.ok() branch,
// which FakeTransport could not reach before force_submit_fail existed.
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, SynchronousSubmitFailureRecordsFailedAttempt) {
    auto cfg = makeMetricsTestConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake_rdma = std::make_shared<FakeTransport>(
        RDMA, /*force_fail=*/false, /*force_submit_fail=*/true);
    installFakeRdma(engine, fake_rdma);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xCD);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(4);
    ASSERT_NE(batch, (BatchID)0);

    auto before = MetricsSnapshot(TentMetrics::instance());
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buf.data(), kLen)}).ok());
    ASSERT_EQ(pollUntilTerminal(engine, batch, 0), TransferStatusEnum::FAILED);
    auto after = MetricsSnapshot(TentMetrics::instance());

    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);

    // One RDMA attempt was started and failed synchronously; no bytes moved.
    EXPECT_EQ(after.counter("tent_transport_attempts_total") -
                  before.counter("tent_transport_attempts_total"),
              1);
    EXPECT_EQ(after.counter("tent_transport_attempt_failures_total") -
                  before.counter("tent_transport_attempt_failures_total"),
              1);
    EXPECT_EQ(after.histogramCount("tent_transport_attempt_latency_us") -
                  before.histogramCount("tent_transport_attempt_latency_us"),
              1);
    const std::string rdma_attempt_failures =
        "tent_transport_attempt_failures_total{transport=\"rdma\",operation="
        "\"write\"}";
    EXPECT_EQ(after.series(rdma_attempt_failures) -
                  before.series(rdma_attempt_failures),
              1);

    // Logical request records exactly one terminal failure, no double counting
    // from recordTaskCompletionMetrics closing an already-finished attempt.
    EXPECT_EQ(after.counter("tent_write_requests_total") -
                  before.counter("tent_write_requests_total"),
              1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

// ---------------------------------------------------------------------------
// When the failover target also fails synchronously, both the original async
// failure and the resubmit's synchronous failure are recorded as distinct
// failed attempts. This exercises the resubmitTransferTask !status.ok() branch.
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, FailoverResubmitSyncFailureRecordsBothAttempts) {
    auto cfg = makeMetricsTestConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    // RDMA fails asynchronously (poll returns FAILED), triggering failover to
    // TCP, whose submitTransferTasks then fails synchronously.
    auto fake_rdma = std::make_shared<FakeTransport>(RDMA, /*force_fail=*/true);
    auto fake_tcp = std::make_shared<FakeTransport>(TCP, /*force_fail=*/false,
                                                    /*force_submit_fail=*/true);
    installFakeRdma(engine, fake_rdma);
    installFakeTcp(engine, fake_tcp);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xEF);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(4);
    ASSERT_NE(batch, (BatchID)0);

    auto before = MetricsSnapshot(TentMetrics::instance());
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buf.data(), kLen)}).ok());
    ASSERT_EQ(pollUntilTerminal(engine, batch, 0), TransferStatusEnum::FAILED);
    auto after = MetricsSnapshot(TentMetrics::instance());

    EXPECT_EQ(fake_rdma->submit_calls.load(), 1);
    EXPECT_EQ(fake_tcp->submit_calls.load(), 1);

    // Two attempts started (RDMA then TCP), both failed.
    EXPECT_EQ(after.counter("tent_transport_attempts_total") -
                  before.counter("tent_transport_attempts_total"),
              2);
    EXPECT_EQ(after.counter("tent_transport_attempt_failures_total") -
                  before.counter("tent_transport_attempt_failures_total"),
              2);

    const std::string rdma_failures =
        "tent_transport_attempt_failures_total{transport=\"rdma\",operation="
        "\"write\"}";
    const std::string tcp_failures =
        "tent_transport_attempt_failures_total{transport=\"tcp\",operation="
        "\"write\"}";
    EXPECT_EQ(after.series(rdma_failures) - before.series(rdma_failures), 1);
    EXPECT_EQ(after.series(tcp_failures) - before.series(tcp_failures), 1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

// ---------------------------------------------------------------------------
// Failed transfer increments failures and requests but NOT bytes or size
// histogram. Uses the direct API so the assertion is deterministic and does
// not depend on engine failover behavior.
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, FailedTransferCountsFailureNotBytes) {
    auto before = MetricsSnapshot(TentMetrics::instance());

    TentMetrics::instance().recordReadFailed(UNSPEC);
    TentMetrics::instance().recordWriteFailed(UNSPEC);

    auto after = MetricsSnapshot(TentMetrics::instance());

    EXPECT_EQ(after.counter("tent_read_failures_total") -
                  before.counter("tent_read_failures_total"),
              1);
    EXPECT_EQ(after.counter("tent_write_failures_total") -
                  before.counter("tent_write_failures_total"),
              1);
    EXPECT_EQ(after.counter("tent_read_requests_total") -
                  before.counter("tent_read_requests_total"),
              1);
    EXPECT_EQ(after.counter("tent_write_requests_total") -
                  before.counter("tent_write_requests_total"),
              1);
    // Bytes must NOT move on failure.
    EXPECT_EQ(after.counter("tent_read_bytes_total") -
                  before.counter("tent_read_bytes_total"),
              0);
    EXPECT_EQ(after.counter("tent_write_bytes_total") -
                  before.counter("tent_write_bytes_total"),
              0);
    // Size histograms must NOT observe failures.
    EXPECT_EQ(after.histogramCount("tent_read_size_bytes") -
                  before.histogramCount("tent_read_size_bytes"),
              0);
    EXPECT_EQ(after.histogramCount("tent_write_size_bytes") -
                  before.histogramCount("tent_write_size_bytes"),
              0);
}

// ---------------------------------------------------------------------------
// A transfer whose deadline was already in the past at submit must increment
// the dedicated tent_deadline_infeasible_total counter, and must NOT pollute
// the tent_deadline_mlu_permille histogram with a sentinel value.
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, InfeasibleDeadlineRecordsSeparateCounter) {
    auto cfg = makeMetricsTestConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xBB);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(4);
    ASSERT_NE(batch, (BatchID)0);

    auto before = MetricsSnapshot(TentMetrics::instance());
    // deadline_ns = 1 is always in the past relative to steady_clock now.
    ASSERT_TRUE(engine
                    .submitTransfer(batch, {makeLocalWrite(buf.data(), kLen,
                                                           /*deadline_ns=*/1)})
                    .ok());
    ASSERT_EQ(pollUntilTerminal(engine, batch, 0),
              TransferStatusEnum::COMPLETED);
    auto after = MetricsSnapshot(TentMetrics::instance());

    // The infeasible counter must exist and increment by exactly 1.
    EXPECT_EQ(after.counter("tent_deadline_infeasible_total") -
                  before.counter("tent_deadline_infeasible_total"),
              1)
        << "expected a dedicated tent_deadline_infeasible_total counter";
    // The MLU histogram must NOT receive a sentinel sample for the infeasible
    // case (the old code observed MLU=5.0 here).
    EXPECT_EQ(after.histogramCount("tent_deadline_mlu_permille") -
                  before.histogramCount("tent_deadline_mlu_permille"),
              0)
        << "infeasible deadline must not pollute the MLU histogram";

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

// ---------------------------------------------------------------------------
// A batch whose reclaim fails permanently is quarantined by lazyFreeBatch
// after kMaxReclaimAttempts (4096) consecutive failures, and that event is
// counted in tent_quarantined_batches_total. Drives the real sweep path.
// ---------------------------------------------------------------------------

// FakeTransport whose poll fails permanently once poisoned.
class PoisonedPollFakeTransport : public FakeTransport {
   public:
    explicit PoisonedPollFakeTransport(TransportType type)
        : FakeTransport(type) {}

    Status getTransferStatus(SubBatchRef batch, int task_id,
                             TransferStatus& status) override {
        if (poisoned.load(std::memory_order_acquire)) {
            return Status::InternalError(
                "injected permanent poll failure" LOC_MARK);
        }
        return FakeTransport::getTransferStatus(batch, task_id, status);
    }

    std::atomic<bool> poisoned{false};
};

TEST_F(MetricsRecordingTest, QuarantinedBatchIncrementsCounter) {
    constexpr size_t kMaxReclaimAttempts = 4096;  // mirrors the impl constant
    auto cfg = makeMetricsTestConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake = std::make_shared<PoisonedPollFakeTransport>(RDMA);
    installFakeRdma(engine, fake);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xAB);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(1);
    ASSERT_NE(batch, (BatchID)0);
    ASSERT_TRUE(
        engine.submitTransfer(batch, {makeLocalWrite(buf.data(), kLen)}).ok());
    fake->poisoned.store(true, std::memory_order_release);

    auto before = MetricsSnapshot(TentMetrics::instance());
    // Each freeBatch call on an already free-requested batch runs one sweep.
    ASSERT_TRUE(engine.freeBatch(batch).ok());
    for (size_t i = 0; i + 1 < kMaxReclaimAttempts; ++i) {
        (void)engine.freeBatch(batch);
    }
    auto after = MetricsSnapshot(TentMetrics::instance());

    EXPECT_EQ(after.counter("tent_quarantined_batches_total") -
                  before.counter("tent_quarantined_batches_total"),
              1)
        << "quarantining a batch must increment "
           "tent_quarantined_batches_total exactly once";
    // Prometheus endpoint carries the series too.
    EXPECT_EQ(after.series("tent_quarantined_batches_total") -
                  before.series("tent_quarantined_batches_total"),
              1);

    // Unpoison so teardown drains cleanly; the batch itself is reclaimed by
    // the engine destructor regardless.
    fake->poisoned.store(false, std::memory_order_release);
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

// ---------------------------------------------------------------------------
// A transfer whose deadline is in the future records a genuine MLU sample
// into the histogram (feasible or missed, but real).
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, FeasibleDeadlineRecordsMLU) {
    auto cfg = makeMetricsTestConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake = std::make_shared<FakeTransport>(RDMA);
    installFakeRdma(engine, fake);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xBB);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(4);
    ASSERT_NE(batch, (BatchID)0);

    auto before = MetricsSnapshot(TentMetrics::instance());
    // Deadline far in the future -> window is huge -> MLU is tiny but > 0.
    uint64_t future_ns = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now().time_since_epoch() +
            std::chrono::hours(1))
            .count());
    ASSERT_TRUE(engine
                    .submitTransfer(
                        batch, {makeLocalWrite(buf.data(), kLen, future_ns)})
                    .ok());
    ASSERT_EQ(pollUntilTerminal(engine, batch, 0),
              TransferStatusEnum::COMPLETED);
    auto after = MetricsSnapshot(TentMetrics::instance());

    EXPECT_EQ(after.histogramCount("tent_deadline_mlu_permille") -
                  before.histogramCount("tent_deadline_mlu_permille"),
              1);
    // And the infeasible counter must NOT move for a feasible deadline.
    EXPECT_EQ(after.counter("tent_deadline_infeasible_total") -
                  before.counter("tent_deadline_infeasible_total"),
              0);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

// ---------------------------------------------------------------------------
// A failed transfer whose deadline was already in the past at submit must
// also increment the infeasible counter. The infeasible-at-submit condition
// is independent of the transfer outcome, so it is recorded for both
// COMPLETED and FAILED (the MLU histogram, which needs actual latency, is
// only recorded on COMPLETED).
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, InfeasibleDeadlineRecordedOnFailedTransfer) {
    auto cfg = makeMetricsTestConfig();
    TransferEngineImpl engine(cfg);
    ASSERT_TRUE(engine.available());

    auto fake = std::make_shared<FakeTransport>(RDMA, /*force_fail=*/true);
    installFakeRdma(engine, fake);

    constexpr size_t kLen = 4096;
    std::vector<uint8_t> buf(kLen, 0xBB);
    ASSERT_TRUE(engine.registerLocalMemory(buf.data(), buf.size()).ok());

    BatchID batch = engine.allocateBatch(4);
    ASSERT_NE(batch, (BatchID)0);

    auto before = MetricsSnapshot(TentMetrics::instance());
    // deadline_ns = 1 is always in the past relative to steady_clock now.
    ASSERT_TRUE(engine
                    .submitTransfer(batch, {makeLocalWrite(buf.data(), kLen,
                                                           /*deadline_ns=*/1)})
                    .ok());
    // The failing transport reports FAILED; poll until terminal.
    ASSERT_EQ(pollUntilTerminal(engine, batch, 0), TransferStatusEnum::FAILED);
    auto after = MetricsSnapshot(TentMetrics::instance());

    // Infeasible counter must increment even though the transfer failed.
    EXPECT_EQ(after.counter("tent_deadline_infeasible_total") -
                  before.counter("tent_deadline_infeasible_total"),
              1);
    // MLU histogram must not receive a sample (no completion, no latency).
    EXPECT_EQ(after.histogramCount("tent_deadline_mlu_permille") -
                  before.histogramCount("tent_deadline_mlu_permille"),
              0);
    // And the write failure was recorded.
    EXPECT_EQ(after.counter("tent_write_failures_total") -
                  before.counter("tent_write_failures_total"),
              1);

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

// ---------------------------------------------------------------------------
// Regression guard: every histogram's JSON bucket keys must match its
// compile-time boundary vector. Locks the invariant that getJsonMetrics()
// pairs each histogram with the correct bucket boundaries.
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, HistogramJsonBucketsMatchBoundaries) {
    struct HistSpec {
        const char* name;
        std::vector<int64_t> expected_keys;
    };
    const std::vector<HistSpec> specs = {
        {"tent_read_latency_us",
         {100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000}},
        {"tent_write_latency_us",
         {100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000}},
        {"tent_read_size_bytes",
         {1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216,
          67108864, 268435456, 1073741824}},
        {"tent_write_size_bytes",
         {1024, 4096, 16384, 65536, 262144, 1048576, 4194304, 16777216,
          67108864, 268435456, 1073741824}},
        {"tent_deadline_mlu_permille",
         {100, 250, 500, 750, 900, 1000, 1250, 1500, 2000, 5000}},
        {"tent_stage_queue_wait_us",
         {10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000}},
        {"tent_stage_dispatch_us",
         {10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000}},
        {"tent_stage_transport_us",
         {10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000}},
        {"tent_transport_attempt_latency_us",
         {100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000}},
    };

    auto snap = MetricsSnapshot(TentMetrics::instance());
    for (const auto& s : specs) {
        auto keys = snap.bucketKeys(s.name);
        EXPECT_EQ(keys, s.expected_keys)
            << "histogram " << s.name << " bucket keys mismatch";
    }
}

// ---------------------------------------------------------------------------
// Histogram buckets are fixed at compile time. The runtime config knob
// (latency_buckets/size_buckets) has been removed; buckets are version-
// controlled in code for reproducible observability. This test asserts the
// latency histogram exposes exactly the kLatencyBuckets keys.
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, BucketsAreCompileTimeDefaults) {
    // Record one sample so the histogram is non-empty.
    TentMetrics::instance().recordReadCompleted(UNSPEC, 4096, 0.001);

    auto snap = MetricsSnapshot(TentMetrics::instance());
    auto keys = snap.bucketKeys("tent_read_latency_us");
    ASSERT_FALSE(keys.empty());

    // Compile-time kLatencyBuckets: 100, 500, 1000, 5000, 10000, 50000,
    // 100000, 500000, 1000000.
    std::vector<int64_t> expected = {100,   500,    1000,   5000,   10000,
                                     50000, 100000, 500000, 1000000};
    EXPECT_EQ(keys, expected)
        << "latency buckets must be the compile-time defaults";
}

// Concurrency guard for the cached-cell hot path: several threads hammering
// overlapping (transport, operation) labels must produce exactly the summed
// counts — including the first-use window where multiple threads resolve the
// same label cell concurrently. Exercises every hot-path metric family
// (per-transport counters, N=2 attempt counters/histogram, stage histograms).
TEST_F(MetricsRecordingTest, ConcurrentRecordsAcrossTransportsAreExact) {
    constexpr int kThreads = 8;
    constexpr int kIters = 2000;
    constexpr size_t kBytes = 4096;

    auto before = MetricsSnapshot(TentMetrics::instance());

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([t] {
            for (int i = 0; i < kIters; ++i) {
                // Even threads write to rdma, odd threads to nvlink, so both
                // transports' cells are resolved concurrently.
                TransportType tp = (t % 2 == 0) ? RDMA : NVLINK;
                Request::OpCode op =
                    (i % 2 == 0) ? Request::WRITE : Request::READ;
                TentMetrics::instance().recordTransportAttemptStarted(tp, op);
                TentMetrics::instance().recordTransportAttemptFinished(
                    tp, op, TransferStatusEnum::COMPLETED, 150.0);
                if (op == Request::WRITE) {
                    TentMetrics::instance().recordWriteCompleted(tp, kBytes,
                                                                 0.002);
                } else {
                    TentMetrics::instance().recordReadCompleted(tp, kBytes,
                                                                0.002);
                }
                TentMetrics::instance().recordStageLatency(
                    TentMetrics::Stage::Transport, tp, 120.0);
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    auto after = MetricsSnapshot(TentMetrics::instance());
    const int64_t total = kThreads * kIters;  // all attempts
    const int64_t writes = total / 2;         // half the iterations
    const int64_t bytes = writes * static_cast<int64_t>(kBytes);

    EXPECT_EQ(after.counter("tent_write_bytes_total") -
                  before.counter("tent_write_bytes_total"),
              bytes);
    EXPECT_EQ(after.counter("tent_read_bytes_total") -
                  before.counter("tent_read_bytes_total"),
              bytes);
    EXPECT_EQ(after.counter("tent_write_requests_total") -
                  before.counter("tent_write_requests_total"),
              writes);
    EXPECT_EQ(after.counter("tent_read_requests_total") -
                  before.counter("tent_read_requests_total"),
              writes);
    EXPECT_EQ(after.counter("tent_transport_attempts_total") -
                  before.counter("tent_transport_attempts_total"),
              total);
    EXPECT_EQ(after.histogramCount("tent_transport_attempt_latency_us") -
                  before.histogramCount("tent_transport_attempt_latency_us"),
              total);
    EXPECT_EQ(after.histogramCount("tent_write_latency_us") -
                  before.histogramCount("tent_write_latency_us"),
              writes);
    EXPECT_EQ(after.histogramCount("tent_stage_transport_us") -
                  before.histogramCount("tent_stage_transport_us"),
              total);
    // Per-label Prometheus series must also be exact: 4 threads per transport,
    // half of their iterations are writes.
    const double per_transport_bytes =
        static_cast<double>(kThreads / 2) * (kIters / 2) * kBytes;
    EXPECT_EQ(after.series("tent_write_bytes_total{transport=\"rdma\"}") -
                  before.series("tent_write_bytes_total{transport=\"rdma\"}"),
              per_transport_bytes);
    EXPECT_EQ(after.series("tent_write_bytes_total{transport=\"nvlink\"}") -
                  before.series("tent_write_bytes_total{transport=\"nvlink\"}"),
              per_transport_bytes);
    EXPECT_EQ(after.series("tent_transport_attempts_total{transport=\"nvlink\","
                           "operation=\"read\"}") -
                  before.series("tent_transport_attempts_total{transport="
                                "\"nvlink\",operation=\"read\"}"),
              static_cast<double>(kThreads / 2) * (kIters / 2));
    EXPECT_EQ(after.counter("tent_write_failures_total") -
                  before.counter("tent_write_failures_total"),
              0);
    EXPECT_EQ(after.counter("tent_read_failures_total") -
                  before.counter("tent_read_failures_total"),
              0);
}

// ---------------------------------------------------------------------------
// L2 HTTP integration: scrape the real /metrics, /metrics/json, /health
// endpoints via coro_http_client and assert on status + body. Validates the
// HTTP wiring (handlers, content, status codes) on top of the L1 recording
// assertions.
// ---------------------------------------------------------------------------
class MetricsHttpTest : public ::testing::Test {
   protected:
    void SetUp() override {
        TentMetrics::instance().shutdown();
        MetricsConfig config;
        config.enabled = true;
        config.http_host = "127.0.0.1";
        config.http_port = getFreeTcpPort();
        config.report_interval_seconds = 0;
        ASSERT_TRUE(TentMetrics::instance().initialize(config).ok());
        TentMetrics::setEnabled(true);
        port_ = TentMetrics::instance().httpPort();
        // If the port bind raced (another process grabbed it), degrade
        // gracefully rather than failing the build.
        if (port_ == 0) {
            GTEST_SKIP() << "metrics HTTP server did not bind; skipping HTTP "
                            "integration test";
        }
    }

    void TearDown() override { TentMetrics::instance().shutdown(); }

    // Retry a GET a few times to absorb the brief window between
    // async_start() returning and the server accepting connections.
    HttpResponse retryGet(const std::string& path, int attempts = 20) {
        HttpResponse resp{0, ""};
        for (int i = 0; i < attempts; ++i) {
            resp = FetchUrl(port_, path);
            if (resp.http_status == 200) return resp;
            std::this_thread::sleep_for(std::chrono::milliseconds(25));
        }
        return resp;
    }

    uint16_t port_ = 0;
};

TEST_F(MetricsHttpTest, PrometheusEndpointExposesAllCounters) {
    // Record a mix of operations so the counters are non-zero.
    TentMetrics::instance().recordReadCompleted(UNSPEC, 1024, 0.001);
    TentMetrics::instance().recordWriteCompleted(UNSPEC, 2048, 0.002);
    TentMetrics::instance().recordDeadlineInfeasible(UNSPEC);

    auto resp = retryGet("/metrics");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("tent_read_bytes_total"), std::string::npos);
    EXPECT_NE(resp.body.find("tent_write_bytes_total"), std::string::npos);
    EXPECT_NE(resp.body.find("tent_deadline_infeasible_total"),
              std::string::npos);
    EXPECT_NE(resp.body.find("tent_write_latency_us_bucket"),
              std::string::npos);
}

TEST_F(MetricsHttpTest, PrometheusLabelsDistinguishTransports) {
    // Record the same metric with different transports.
    TentMetrics::instance().recordReadCompleted(RDMA, 1024, 0.001);
    TentMetrics::instance().recordReadCompleted(TCP, 2048, 0.002);
    TentMetrics::instance().recordTransportFailover(RDMA, TCP);

    auto resp = retryGet("/metrics");
    EXPECT_EQ(resp.http_status, 200);

    // Per-transport labels must appear as separate lines.
    EXPECT_NE(resp.body.find("tent_read_bytes_total{transport=\"rdma\"}"),
              std::string::npos);
    EXPECT_NE(resp.body.find("tent_read_bytes_total{transport=\"tcp\"}"),
              std::string::npos);

    // Failover counter must carry from/to labels.
    EXPECT_NE(resp.body.find(
                  "tent_transport_failover_total{from=\"rdma\",to=\"tcp\"}"),
              std::string::npos);
}

TEST_F(MetricsHttpTest, JsonEndpointValid) {
    TentMetrics::instance().recordReadCompleted(UNSPEC, 512, 0.0005);

    auto resp = retryGet("/metrics/json");
    EXPECT_EQ(resp.http_status, 200);
    // Must be parseable JSON and contain expected keys.
    auto json =
        nlohmann::json::parse(resp.body, nullptr, /*allow_exceptions=*/false);
    ASSERT_FALSE(json.is_discarded())
        << "body was not valid JSON: " << resp.body;
    EXPECT_TRUE(json.contains("tent_read_bytes_total"));
    EXPECT_TRUE(json.contains("tent_deadline_infeasible_total"));
    EXPECT_TRUE(json.contains("tent_read_latency_us"));
}

// Regression guard for the silent-drop bug: ylt's
// basic_dynamic_histogram::serialize() clears its output string (taking the
// # HELP / # TYPE header with it) when every observed value truncates to 0
// under int64_t. Sub-microsecond queue_wait latencies trigger this in
// production — JSON reports a non-zero count but /metrics omits the metric
// entirely. The custom serializer in getPrometheusMetrics() walks bucket
// counts directly so the metric always appears when count > 0.
TEST_F(MetricsHttpTest, PrometheusExposesHistogramWhenAllSamplesAreZero) {
    // Reproduce the bug condition: observe 3 sub-microsecond latencies.
    // recordStageLatency casts to int64_t (val=0), so sum_ stays 0 — exactly
    // the case ylt's serialize() drops.
    auto& m = TentMetrics::instance();
    m.recordStageLatency(TentMetrics::Stage::QueueWait, UNSPEC, 0.3);
    m.recordStageLatency(TentMetrics::Stage::QueueWait, UNSPEC, 0.5);
    m.recordStageLatency(TentMetrics::Stage::QueueWait, UNSPEC, 0.9);

    auto resp = retryGet("/metrics");
    EXPECT_EQ(resp.http_status, 200);
    // The bug: this substring was MISSING from Prometheus output even though
    // JSON reported count=3. Must appear now.
    EXPECT_NE(resp.body.find("tent_stage_queue_wait_us_bucket"),
              std::string::npos)
        << "stage_queue_wait_us silently dropped by ylt serialize() when all "
           "samples truncate to 0 under int64_t";
    // The +Inf bucket must carry the cumulative count of 3.
    EXPECT_NE(
        resp.body.find("tent_stage_queue_wait_us_bucket{transport=\"unspec\","
                       "le=\"+Inf\"} 3"),
        std::string::npos)
        << "+Inf bucket must be cumulative (== total count)";
    // _count line must be present with value 3.
    EXPECT_NE(resp.body.find("tent_stage_queue_wait_us_count{transport=\""
                             "unspec\"} 3"),
              std::string::npos)
        << "_count must match total observations";
}

// Same bug condition as above but with transport=TCP — this is what tebench
// produces and what surfaced the original silent-drop bug in production.
// Guard against a regression that only affects a subset of transports.
TEST_F(MetricsHttpTest, PrometheusExposesZeroValuedHistogramForTcpTransport) {
    auto& m = TentMetrics::instance();

    // TentMetrics is a process-wide singleton whose histogram values are not
    // reset across tests, and other tests in this binary can complete real
    // TCP transfers (e.g. failover recovery) that record tcp stage latency.
    // So snapshot the tcp +Inf cumulative first and assert on the delta, the
    // same convention JsonAndPrometheusAgreeOnHistogramCount uses.
    auto tcp_inf_count = [&]() -> int64_t {
        std::string needle =
            "tent_stage_queue_wait_us_bucket{transport=\"tcp\",le=\"+Inf\"} ";
        auto body = retryGet("/metrics").body;
        auto pos = body.find(needle);
        if (pos == std::string::npos) return 0;
        return std::strtoll(body.c_str() + pos + needle.size(), nullptr, 10);
    };
    int64_t inf_before = tcp_inf_count();

    m.recordStageLatency(TentMetrics::Stage::QueueWait, TCP, 0.3);
    m.recordStageLatency(TentMetrics::Stage::QueueWait, TCP, 0.5);
    m.recordStageLatency(TentMetrics::Stage::QueueWait, TCP, 0.9);

    auto resp = retryGet("/metrics");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("tent_stage_queue_wait_us_bucket"),
              std::string::npos)
        << "stage_queue_wait_us (transport=tcp) silently dropped when all "
           "samples truncate to 0 under int64_t";
    // All three samples truncate to 0 (sub-microsecond), so the +Inf bucket
    // (cumulative == total count) must still advance by exactly 3 even though
    // sum_ stays 0 — the silent-drop regression this test guards against.
    EXPECT_EQ(tcp_inf_count() - inf_before, 3)
        << "+Inf bucket for transport=tcp must be cumulative (== total count)";
}

// Regression guard for Prometheus/JSON drift: both endpoints must agree on
// the histogram count for a given metric. Previously they used two completely
// different serialization paths (ylt serialize() vs. custom bucket walk),
// so any ylt behavior change would silently desync them. Both now share the
// bucket-walk code path.
TEST_F(MetricsHttpTest, JsonAndPrometheusAgreeOnHistogramCount) {
    // Snapshot before so the assertion is delta-based. TentMetrics is a
    // process-wide singleton whose counter/histogram values are not reset
    // across tests, so absolute values would be order-dependent.
    auto json_before_resp = retryGet("/metrics/json");
    ASSERT_EQ(json_before_resp.http_status, 200);
    auto json_before_obj = nlohmann::json::parse(json_before_resp.body, nullptr,
                                                 /*allow_exceptions=*/false);
    ASSERT_FALSE(json_before_obj.is_discarded());
    int64_t json_count_before =
        json_before_obj["tent_stage_transport_us"]["count"];

    // Parse the unspec _count line from Prometheus before-state too.
    auto prom_before = retryGet("/metrics");
    ASSERT_EQ(prom_before.http_status, 200);
    auto count_before = [&]() -> int64_t {
        std::string needle =
            "tent_stage_transport_us_count{transport=\"unspec\"} ";
        auto pos = prom_before.body.find(needle);
        if (pos == std::string::npos) return 0;
        return std::strtoll(prom_before.body.c_str() + pos + needle.size(),
                            nullptr, 10);
    }();

    // Record 3 observations landing in distinct buckets.
    auto& m = TentMetrics::instance();
    m.recordStageLatency(TentMetrics::Stage::Transport, UNSPEC, 750.0);
    m.recordStageLatency(TentMetrics::Stage::Transport, UNSPEC, 1200.0);
    m.recordStageLatency(TentMetrics::Stage::Transport, UNSPEC, 50.0);
    const int64_t kDelta = 3;

    auto prom = retryGet("/metrics");
    auto json_resp = retryGet("/metrics/json");
    ASSERT_EQ(prom.http_status, 200);
    ASSERT_EQ(json_resp.http_status, 200);

    // JSON aggregates across all transport labels; its count must advance by
    // exactly kDelta.
    auto json = nlohmann::json::parse(json_resp.body, nullptr,
                                      /*allow_exceptions=*/false);
    ASSERT_FALSE(json.is_discarded()) << "invalid JSON: " << json_resp.body;
    int64_t json_count_after = json["tent_stage_transport_us"]["count"];
    EXPECT_EQ(json_count_after - json_count_before, kDelta)
        << "JSON count delta must equal observations (" << kDelta
        << "); before=" << json_count_before << " after=" << json_count_after;

    // Prometheus: the unspec label's _count must have advanced by kDelta too.
    std::string expected =
        "tent_stage_transport_us_count{transport=\"unspec\"} " +
        std::to_string(count_before + kDelta);
    EXPECT_NE(prom.body.find(expected), std::string::npos)
        << "Prometheus unspec _count (" << (count_before + kDelta)
        << ") must match JSON delta. Body length=" << prom.body.size();
}

TEST_F(MetricsHttpTest, HealthEndpointOk) {
    auto resp = retryGet("/health");
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_EQ(resp.body, "OK");
}

#else  // !TENT_METRICS_ENABLED

// When metrics are disabled at compile time, the recording path is a no-op
// and there is nothing to assert on. This test confirms the stub initializes.
TEST(MetricsRecording, DisabledAtCompileTime) {
    MetricsConfig config;
    EXPECT_TRUE(TentMetrics::instance().initialize(config).ok());
    TentMetrics::instance().shutdown();
}

#endif  // TENT_METRICS_ENABLED

}  // namespace
}  // namespace tent
}  // namespace mooncake
