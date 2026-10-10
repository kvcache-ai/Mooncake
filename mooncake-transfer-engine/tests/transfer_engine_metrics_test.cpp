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

#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "transfer_engine_impl.h"
#include "transfer_engine_metrics.h"
#include "transport/transport.h"

namespace mooncake {

// Builds batches and tasks without RDMA or TCP plumbing.
class TransferEngineImplTestPeer {
   public:
    static void installTransport(TransferEngineImpl& engine,
                                 std::shared_ptr<Transport> transport) {
        engine.multi_transports_->transport_map_.clear();
        engine.multi_transports_->transport_map_.emplace("fake",
                                                         std::move(transport));
    }

    static BatchID allocateBatch(TransferEngineImpl& engine, size_t size) {
        return engine.multi_transports_->allocateBatchID(size);
    }

    // Append a task stamped as submitTransfer() would, with one slice.
    static void addSubmittedTask(BatchID batch_id, Transport* transport,
                                 const Transport::TransferRequest& request) {
        auto& batch = Transport::toBatchDesc(batch_id);
        batch.task_list.emplace_back();
        auto& task = batch.task_list.back();
        task.batch_id = batch_id;
        task.transport_ = transport;
        task.request = &request;
        MultiTransport::recordTaskStart(task, request);
        task.slice_count = 1;
    }

    static void markAllFinished(BatchID batch_id) {
        auto& batch = Transport::toBatchDesc(batch_id);
        for (auto& task : batch.task_list) {
            task.is_finished = true;
        }
    }

    static Status freeBatch(TransferEngineImpl& engine, BatchID batch_id) {
        return engine.multi_transports_->freeBatchID(batch_id);
    }
};

namespace {

// Reports successful completion for every task.
class FakeTransport : public Transport {
   public:
    explicit FakeTransport(size_t transferred_bytes)
        : transferred_bytes_(transferred_bytes) {}

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }

    Status submitTransferTask(
        const std::vector<TransferTask*>& tasks) override {
        for (auto* task : tasks) __sync_fetch_and_add(&task->slice_count, 1);
        return Status::OK();
    }

    Status getTransferStatus(BatchID batch_id, size_t task_id,
                             TransferStatus& status) override {
        status.s = TransferStatusEnum::COMPLETED;
        status.transferred_bytes = transferred_bytes_;
        __atomic_store_n(&toBatchDesc(batch_id).task_list[task_id].is_finished,
                         true, __ATOMIC_RELEASE);
        return Status::OK();
    }

   private:
    int registerLocalMemory(void*, size_t, const std::string&, bool,
                            bool) override {
        return 0;
    }
    int unregisterLocalMemory(void*, bool) override { return 0; }
    int registerLocalMemoryBatch(const std::vector<BufferEntry>&,
                                 const std::string&) override {
        return 0;
    }
    int unregisterLocalMemoryBatch(const std::vector<void*>&) override {
        return 0;
    }
    const char* getName() const override { return "fake"; }

    size_t transferred_bytes_;
};

using Direction = TransferEngineMetrics::Direction;

TransferEngineMetrics::DirectionMetrics& seriesFor(Direction direction) {
    return TransferEngineMetrics::instance().forDirection(direction);
}
int64_t bytesTotal(Direction d) { return seriesFor(d).bytes_total.value(); }
int64_t requestsTotal(Direction d) {
    return seriesFor(d).requests_total.value();
}
int64_t failuresTotal(Direction d) {
    return seriesFor(d).failures_total.value();
}
int64_t timeoutsTotal(Direction d) {
    return seriesFor(d).timeouts_total.value();
}
int64_t latencyCount(Direction d) { return seriesFor(d).latency_us.count(); }
int64_t sizeCount(Direction d) { return seriesFor(d).size_bytes.count(); }

Transport::TransferRequest makeRequest(
    Transport::TransferRequest::OpCode opcode, size_t length) {
    Transport::TransferRequest request{};
    request.opcode = opcode;
    request.length = length;
    return request;
}

// Collection is process-global and cannot be disabled after enable(). Run in
// a fresh process so this check is independent of test order and repetition.
TEST(EngineMetricsDeathTest, DisabledCollectionDoesNotRecordTransfers) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    ASSERT_EXIT(
        {
            unsetenv("MC_TE_METRIC");
            unsetenv("MC_TE_METRIC_HTTP_PORT");
            auto check = ([] {
                ASSERT_FALSE(TransferEngineMetrics::isEnabled());
                TransferEngineImpl engine(false);
                ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:0"), 0);
                auto transport = std::make_shared<FakeTransport>(8192);
                TransferEngineImplTestPeer::installTransport(engine, transport);
                auto desc = std::make_shared<TransferMetadata::SegmentDesc>();
                desc->name = "fake-segment";
                desc->protocol = "fake";
                engine.getMetadata()->addLocalSegment(42, "fake-segment",
                                                      std::move(desc));
                auto request = makeRequest(TransferRequest::READ, 8192);
                request.target_id = 42;
                std::vector<TransferRequest> entries{request};
                auto batch =
                    TransferEngineImplTestPeer::allocateBatch(engine, 1);
                EXPECT_TRUE(engine.submitTransfer(batch, entries).ok());
                TransferStatus status;
                EXPECT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
                EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
                EXPECT_TRUE(engine.getBatchTransferStatus(batch, status).ok());
                EXPECT_TRUE(
                    TransferEngineImplTestPeer::freeBatch(engine, batch).ok());
                EXPECT_FALSE(TransferEngineMetrics::isEnabled());
                for (auto direction : {Direction::Read, Direction::Write}) {
                    EXPECT_EQ(requestsTotal(direction), 0);
                    EXPECT_EQ(bytesTotal(direction), 0);
                    EXPECT_EQ(failuresTotal(direction), 0);
                    EXPECT_EQ(timeoutsTotal(direction), 0);
                    EXPECT_EQ(latencyCount(direction), 0);
                    EXPECT_EQ(sizeCount(direction), 0);
                }
            });
            check();
            std::exit(::testing::Test::HasFailure() ? 1 : 0);
        },
        ::testing::ExitedWithCode(0), "");
}

class MetricsTestBase : public ::testing::Test {
   protected:
    void SetUp() override {
        unsetenv("MC_TE_METRIC_HTTP_PORT");
        auto& metrics = TransferEngineMetrics::instance();
        // Reset only in this fixture, while no engine or HTTP server is active.
        for (auto direction : {Direction::Read, Direction::Write}) {
            auto& dir = metrics.forDirection(direction);
            for (auto* counter : {&dir.bytes_total, &dir.requests_total,
                                  &dir.failures_total, &dir.timeouts_total}) {
                counter->reset();
            }
            for (auto* histogram : {&dir.latency_us, &dir.size_bytes}) {
                histogram->histogram = ylt::metric::histogram_t(
                    histogram->histogram.str_name(),
                    std::string(histogram->histogram.help()),
                    histogram->boundaries);
                histogram->sum.store(0, std::memory_order_relaxed);
            }
        }
    }

    TransferEngineMetrics& metrics() {
        return TransferEngineMetrics::instance();
    }
};

TEST_F(MetricsTestBase, ReadAndWriteAreAccountedSeparately) {
    auto& m = metrics();
    m.recordCompleted(Direction::Read, 4096, 2000);
    m.recordCompleted(Direction::Read, 128, 0);  // Zero latency still counts.
    m.recordFailed(Direction::Write);

    EXPECT_EQ(requestsTotal(Direction::Read), 2);
    EXPECT_EQ(bytesTotal(Direction::Read), 4224);
    EXPECT_EQ(failuresTotal(Direction::Read), 0);
    EXPECT_EQ(latencyCount(Direction::Read), 2);
    EXPECT_EQ(sizeCount(Direction::Read), 2);

    EXPECT_EQ(requestsTotal(Direction::Write), 1);
    EXPECT_EQ(bytesTotal(Direction::Write), 0);
    EXPECT_EQ(failuresTotal(Direction::Write), 1);
    EXPECT_EQ(latencyCount(Direction::Write), 0);
}

TEST_F(MetricsTestBase, TimeoutsAreSeparateFromFailuresAndExported) {
    auto& m = metrics();
    for (auto direction : {Direction::Read, Direction::Write}) {
        m.recordTimeout(direction);
        EXPECT_EQ(requestsTotal(direction), 1);
        EXPECT_EQ(timeoutsTotal(direction), 1);
        EXPECT_EQ(failuresTotal(direction), 0);
        EXPECT_EQ(bytesTotal(direction), 0);
        EXPECT_EQ(latencyCount(direction), 0);
        EXPECT_EQ(sizeCount(direction), 0);
    }
    EXPECT_NE(m.prometheusText().find("mooncake_te_read_timeouts_total 1"),
              std::string::npos);
    EXPECT_NE(m.prometheusText().find("mooncake_te_write_timeouts_total 1"),
              std::string::npos);
    EXPECT_NE(m.jsonText().find("\"mooncake_te_read_timeouts_total\":1"),
              std::string::npos);
    EXPECT_NE(m.summaryText().find("1 timeouts"), std::string::npos);
}

TEST_F(MetricsTestBase, PrometheusTextCarriesPrefixedSeries) {
    auto& m = metrics();
    m.recordCompleted(Direction::Read, 1048576, 12345);

    const std::string text = m.prometheusText();
    EXPECT_NE(text.find("mooncake_te_read_bytes_total"), std::string::npos);
    EXPECT_NE(text.find("mooncake_te_read_requests_total"), std::string::npos);
    EXPECT_NE(text.find("mooncake_te_read_latency_us_bucket"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_te_read_latency_us_sum"), std::string::npos);
    EXPECT_NE(text.find("mooncake_te_read_latency_us_count"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_te_read_size_bytes_bucket"),
              std::string::npos);
    EXPECT_EQ(text.find("mooncake_te_read_latency_us_sum 0"),
              std::string::npos);
    // Untouched counters are still exported as 0.
    EXPECT_NE(text.find("mooncake_te_read_failures_total 0"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_te_write_requests_total 0"),
              std::string::npos);
}

TEST_F(MetricsTestBase, JsonTextCarriesHistogramSums) {
    auto& m = metrics();
    m.recordCompleted(Direction::Read, 1048576, 2000000);
    m.recordCompleted(Direction::Read, 1024, 500000);

    const std::string text = m.jsonText();
    EXPECT_NE(text.find("\"mooncake_te_read_latency_us\":{\"count\":2,"
                        "\"sum\":2500000,\"buckets\":{"),
              std::string::npos)
        << text;
    EXPECT_NE(text.find("\"mooncake_te_read_size_bytes\":{\"count\":2,"
                        "\"sum\":1049600,\"buckets\":{"),
              std::string::npos)
        << text;
    EXPECT_NE(text.find("\"mooncake_te_write_latency_us\":{\"count\":0,"
                        "\"sum\":0,\"buckets\":{"),
              std::string::npos)
        << text;
}

class EngineMetricsTest : public MetricsTestBase {
   protected:
    void SetUp() override {
        setenv("MC_TE_METRIC", "1", /*overwrite=*/1);
        MetricsTestBase::SetUp();
        // The engine reads MC_TE_METRIC on construction.
        engine_ = std::make_unique<TransferEngineImpl>(false);
    }

    void TearDown() override {
        if (batch_) {
            TransferEngineImplTestPeer::markAllFinished(batch_);
            EXPECT_TRUE(
                TransferEngineImplTestPeer::freeBatch(*engine_, batch_).ok());
        }
    }

    // Engine with `transport` and a batch of `task_count` submitted tasks.
    void buildBatch(const char* listen_addr,
                    std::shared_ptr<Transport> transport,
                    Transport::TransferRequest::OpCode opcode,
                    size_t task_count) {
        ASSERT_EQ(engine_->init(P2PHANDSHAKE, listen_addr), 0);
        TransferEngineImplTestPeer::installTransport(*engine_, transport);
        request_ = makeRequest(opcode, 8192);
        batch_ =
            TransferEngineImplTestPeer::allocateBatch(*engine_, task_count);
        for (size_t i = 0; i < task_count; ++i) {
            TransferEngineImplTestPeer::addSubmittedTask(
                batch_, transport.get(), request_);
        }
    }

    std::unique_ptr<TransferEngineImpl> engine_;
    Transport::TransferRequest request_{};
    BatchID batch_ = 0;
};

TEST_F(EngineMetricsTest, ConcurrentPollsRecordOnce) {
    auto transport = std::make_shared<FakeTransport>(4096);
    constexpr size_t kTasks = 64;
    buildBatch("127.0.0.1:12396", transport, Transport::TransferRequest::READ,
               kTasks);

    std::vector<std::thread> pollers;
    for (int t = 0; t < 4; ++t) {
        pollers.emplace_back([&] {
            TransferStatus status;
            for (size_t i = 0; i < kTasks; ++i)
                engine_->getTransferStatus(batch_, i, status);
        });
    }
    for (auto& p : pollers) p.join();

    EXPECT_EQ(requestsTotal(Direction::Read), kTasks);
    EXPECT_EQ(bytesTotal(Direction::Read), kTasks * 4096);
    EXPECT_EQ(latencyCount(Direction::Read), kTasks);
}

}  // namespace
}  // namespace mooncake
