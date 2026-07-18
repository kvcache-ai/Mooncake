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

// Snapshot of TentMetrics JSON output. Provides delta-based accessors so
// tests are robust to singleton state accumulated by prior tests.
class MetricsSnapshot {
   public:
    explicit MetricsSnapshot(TentMetrics& m) {
        std::string body = m.getJsonMetrics();
        try {
            json_ = nlohmann::json::parse(body);
        } catch (...) {
            json_ = nlohmann::json::object();
        }
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
        if (!json_.contains(name) || !json_[name].contains("buckets")) return keys;
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
    explicit FakeTransport(TransportType self_type) : self_type_(self_type) {
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
        auto* fake = static_cast<FakeSubBatch*>(batch);
        for (const auto& request : requests) {
            fake->requests.push_back(request);
            fake->statuses.push_back(
                {TransferStatusEnum::COMPLETED, request.length});
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

Request makeLocalWrite(uint8_t* ptr, size_t length,
                       uint64_t deadline_ns = 0) {
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

    void TearDown() override {
        TentMetrics::instance().shutdown();
    }
};

// ---------------------------------------------------------------------------
// Fix A (Red -> Green): completed transfer records bytes, requests, latency.
// Drives the real recordTaskCompletionMetrics path via FakeTransport.
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
    ASSERT_EQ(pollUntilTerminal(engine, batch, 0), TransferStatusEnum::COMPLETED);
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

    EXPECT_TRUE(engine.freeBatch(batch).ok());
    EXPECT_TRUE(engine.unregisterLocalMemory(buf.data(), buf.size()).ok());
}

// ---------------------------------------------------------------------------
// Fix A (Red -> Green): failed transfer increments failures and requests but
// NOT bytes or size histogram. Uses the direct API so the assertion is
// deterministic and does not depend on engine failover behavior.
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, FailedTransferCountsFailureNotBytes) {
    auto before = MetricsSnapshot(TentMetrics::instance());

    TentMetrics::instance().recordReadFailed();
    TentMetrics::instance().recordWriteFailed();

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
// Fix B (Red): a transfer whose deadline was already in the past at submit
// must increment a SEPARATE tent_deadline_infeasible_total counter, and must
// NOT pollute the tent_deadline_mlu_permille histogram. Today there is no
// such counter and the sentinel (5.0) lands in the histogram, so this fails.
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
                    .submitTransfer(batch,
                                    {makeLocalWrite(buf.data(), kLen, /*deadline_ns=*/1)})
                    .ok());
    ASSERT_EQ(pollUntilTerminal(engine, batch, 0), TransferStatusEnum::COMPLETED);
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
// Fix B (companion): a transfer whose deadline is in the future records a
// genuine MLU sample into the histogram (feasible or missed, but real).
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
            std::chrono::steady_clock::now().time_since_epoch() + std::chrono::hours(1))
            .count());
    ASSERT_TRUE(engine
                    .submitTransfer(batch,
                                    {makeLocalWrite(buf.data(), kLen, future_ns)})
                    .ok());
    ASSERT_EQ(pollUntilTerminal(engine, batch, 0), TransferStatusEnum::COMPLETED);
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
// Fix D (regression guard): every histogram's JSON bucket keys must match its
// compile-time boundary vector. Passes before AND after the parallel-vector
// -> struct refactor; locks the invariant the refactor must preserve.
//
// Runs BEFORE BucketsAreCompileTimeDefaultsIgnoringConfig because that test
// destructively reassigns the singleton's histogram members (pre-Fix-C), which
// would pollute the bucket structure observed here. After Fix C removes the
// reassignment, the ordering constraint is moot.
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
    };

    auto snap = MetricsSnapshot(TentMetrics::instance());
    for (const auto& s : specs) {
        auto keys = snap.bucketKeys(s.name);
        EXPECT_EQ(keys, s.expected_keys)
            << "histogram " << s.name << " bucket keys mismatch";
    }
}

// ---------------------------------------------------------------------------
// Fix C (Green): histogram buckets are fixed at compile time. The runtime
// config knob (latency_buckets/size_buckets) has been removed; buckets are
// version-controlled in code for reproducible observability. This test
// asserts the latency histogram exposes exactly the kLatencyBuckets keys.
// ---------------------------------------------------------------------------
TEST_F(MetricsRecordingTest, BucketsAreCompileTimeDefaults) {
    // Record one sample so the histogram is non-empty.
    TentMetrics::instance().recordReadCompleted(4096, 0.001);

    auto snap = MetricsSnapshot(TentMetrics::instance());
    auto keys = snap.bucketKeys("tent_read_latency_us");
    ASSERT_FALSE(keys.empty());

    // Compile-time kLatencyBuckets: 100, 500, 1000, 5000, 10000, 50000,
    // 100000, 500000, 1000000.
    std::vector<int64_t> expected = {100,  500,   1000,   5000,   10000,
                                     50000, 100000, 500000, 1000000};
    EXPECT_EQ(keys, expected)
        << "latency buckets must be the compile-time defaults";
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
