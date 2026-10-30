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

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <map>
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

    static Status tryFreeBatch(TransferEngineImpl& engine, BatchID batch_id) {
        return engine.multi_transports_->tryFreeBatchID(batch_id, {});
    }
};

namespace {

// Reports `task_status[task_id]` if set, else `status` (COMPLETED by default),
// and marks the task finished on COMPLETED or FAILED as real transports do.
class FakeTransport : public Transport {
   public:
    explicit FakeTransport(size_t transferred_bytes)
        : transferred_bytes_(transferred_bytes) {}

    TransferStatusEnum status = TransferStatusEnum::COMPLETED;
    std::map<size_t, TransferStatusEnum> task_status;
    std::atomic<size_t> polls{0};

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
        polls.fetch_add(1, std::memory_order_relaxed);
        auto it = task_status.find(task_id);
        status.s = it == task_status.end() ? this->status : it->second;
        status.transferred_bytes = transferred_bytes_;
        if (status.s == TransferStatusEnum::COMPLETED ||
            status.s == TransferStatusEnum::FAILED) {
            __atomic_store_n(
                &toBatchDesc(batch_id).task_list[task_id].is_finished, true,
                __ATOMIC_RELEASE);
        }
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
    // Untouched counters are still exported as 0.
    EXPECT_NE(text.find("mooncake_te_read_failures_total 0"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_te_write_requests_total 0"),
              std::string::npos);
}

// ylt's own serializer drops a histogram whose sum is 0.
TEST_F(MetricsTestBase, ZeroLatencyHistogramIsExported) {
    metrics().recordCompleted(Direction::Read, 64, 0);

    const std::string text = metrics().prometheusText();
    EXPECT_NE(text.find("mooncake_te_read_latency_us_sum 0\n"),
              std::string::npos);
    EXPECT_NE(text.find("mooncake_te_read_latency_us_count 1\n"),
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

    // Frees the batch without forcing its tasks finished.
    Status tryFree() {
        Status s = TransferEngineImplTestPeer::tryFreeBatch(*engine_, batch_);
        if (s.ok()) batch_ = 0;
        return s;
    }

    // Finish a task through its slice counters alone, without a poll.
    static void finishOk(Transport::TransferTask& task, size_t bytes) {
        task.success_slice_count = 1;
        task.transferred_bytes = bytes;
        task.is_finished = true;
    }
    static void finishFailed(Transport::TransferTask& task) {
        task.failed_slice_count = 1;
        task.is_finished = true;
    }

    std::unique_ptr<TransferEngineImpl> engine_;
    Transport::TransferRequest request_{};
    BatchID batch_ = 0;
};

TEST_F(EngineMetricsTest, ConcurrentPollsRecordOnce) {
    auto transport = std::make_shared<FakeTransport>(4096);
    constexpr size_t kTasks = 64;
    buildBatch("127.0.0.1:0", transport, Transport::TransferRequest::READ,
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

// A batch poll records every task even when has_failure is already set, and
// a repeated poll records nothing new.
TEST_F(EngineMetricsTest, BatchPollRecordsTasksAfterFailure) {
    auto transport = std::make_shared<FakeTransport>(4096);
    buildBatch("127.0.0.1:0", transport, Transport::TransferRequest::WRITE, 3);
    transport->task_status[0] = TransferStatusEnum::FAILED;
    transport->task_status[1] = TransferStatusEnum::TIMEOUT;
    Transport::toBatchDesc(batch_).has_failure.store(true);

    TransferStatus status;
    engine_->getBatchTransferStatus(batch_, status);

    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_EQ(transport->polls.load(), 3u);
    EXPECT_EQ(requestsTotal(Direction::Write), 3);
    EXPECT_EQ(failuresTotal(Direction::Write), 1);
    EXPECT_EQ(timeoutsTotal(Direction::Write), 1);
    EXPECT_EQ(bytesTotal(Direction::Write), 4096);

    engine_->getBatchTransferStatus(batch_, status);
    EXPECT_EQ(requestsTotal(Direction::Write), 3);
    // Only the failed task is polled again; recorded tasks are skipped.
    EXPECT_EQ(transport->polls.load(), 4u);
}

// A timed-out batch poll must not flag the batch as failed: the tasks can
// still complete, and mooncake-store reads has_failure for the result.
TEST_F(EngineMetricsTest, TimeoutDoesNotSetHasFailure) {
    auto transport = std::make_shared<FakeTransport>(4096);
    buildBatch("127.0.0.1:0", transport, Transport::TransferRequest::WRITE, 2);
    auto& batch = Transport::toBatchDesc(batch_);

    TransferStatus status;
    transport->status = TransferStatusEnum::TIMEOUT;
    engine_->getBatchTransferStatus(batch_, status);
    EXPECT_EQ(status.s, TransferStatusEnum::FAILED);
    EXPECT_FALSE(batch.has_failure.load());

    transport->status = TransferStatusEnum::COMPLETED;
    engine_->getBatchTransferStatus(batch_, status);
    EXPECT_EQ(status.s, TransferStatusEnum::COMPLETED);
    EXPECT_EQ(status.transferred_bytes, 2 * 4096u);
    EXPECT_FALSE(batch.has_failure.load());
    EXPECT_EQ(timeoutsTotal(Direction::Write), 2);
}

// A busy task does not free the batch, but tasks behind it that never got a
// poll are still queried and recorded.
TEST_F(EngineMetricsTest, FreeBatchRecordsTasksBehindBusyTask) {
    auto transport = std::make_shared<FakeTransport>(4096);
    buildBatch("127.0.0.1:0", transport, Transport::TransferRequest::WRITE, 3);
    transport->task_status[0] = TransferStatusEnum::TIMEOUT;
    transport->task_status[2] = TransferStatusEnum::FAILED;

    EXPECT_TRUE(tryFree().IsBatchBusy());
    EXPECT_EQ(transport->polls.load(), 3u);
    EXPECT_EQ(requestsTotal(Direction::Write), 3);
    EXPECT_EQ(timeoutsTotal(Direction::Write), 1);
    EXPECT_EQ(failuresTotal(Direction::Write), 1);
    EXPECT_EQ(bytesTotal(Direction::Write), 4096);

    // Already-recorded tasks are not counted again on the next attempt.
    EXPECT_TRUE(tryFree().IsBatchBusy());
    EXPECT_EQ(requestsTotal(Direction::Write), 3);
}

// A task can be finished without any poll or completion callback, as the
// scatter drain does once its slice counters are complete. Freeing the batch
// records such tasks from their counters.
TEST_F(EngineMetricsTest, FreeBatchRecordsTasksFinishedWithoutPoll) {
    auto transport = std::make_shared<FakeTransport>(4096);
    buildBatch("127.0.0.1:0", transport, Transport::TransferRequest::READ, 3);
    auto& tasks = Transport::toBatchDesc(batch_).task_list;
    finishOk(tasks[0], 4096);
    finishFailed(tasks[1]);
    // Counters still incomplete: not a known outcome, so not recorded.
    tasks[2].slice_count = 2;
    tasks[2].success_slice_count = 1;
    tasks[2].is_finished = true;

    ASSERT_TRUE(tryFree().ok());
    EXPECT_EQ(requestsTotal(Direction::Read), 2);
    EXPECT_EQ(failuresTotal(Direction::Read), 1);
    EXPECT_EQ(timeoutsTotal(Direction::Read), 0);
    EXPECT_EQ(bytesTotal(Direction::Read), 4096);
    EXPECT_EQ(latencyCount(Direction::Read), 1);
}

#ifdef USE_EVENT_DRIVEN_COMPLETION
// Latency from submission to the completion time the callback stamped.
int64_t latencyUs(const Transport::TransferTask& task) {
    using Clock = std::chrono::steady_clock;
    const auto end = Clock::time_point(Clock::duration(task.end_ticks));
    return std::chrono::duration_cast<std::chrono::microseconds>(
               end - task.start_time)
        .count();
}

// The completion callback finishes tasks that nobody polls; freeing the batch
// records them, and a task that was already polled is not counted again.
TEST_F(EngineMetricsTest, FreeBatchRecordsEventDrivenCompletions) {
    auto transport = std::make_shared<FakeTransport>(4096);
    buildBatch("127.0.0.1:0", transport, Transport::TransferRequest::WRITE, 3);
    // Only task 0 is polled.
    transport->status = TransferStatusEnum::WAITING;
    transport->task_status[0] = TransferStatusEnum::COMPLETED;
    auto& tasks = Transport::toBatchDesc(batch_).task_list;
    std::vector<Transport::Slice> slices(tasks.size());
    for (size_t i = 0; i < tasks.size(); ++i) {
        slices[i].task = &tasks[i];
        slices[i].length = 4096;
    }

    slices[0].markSuccess();
    TransferStatus status;
    engine_->getTransferStatus(batch_, 0, status);
    ASSERT_FALSE(tasks[1].is_finished);
    ASSERT_FALSE(tasks[2].is_finished);
    slices[1].markSuccess();
    slices[2].markFailed();
    ASSERT_TRUE(Transport::toBatchDesc(batch_).is_finished.load());
    const int64_t expected_latency_us =
        latencyUs(tasks[0]) + latencyUs(tasks[1]);

    // Latency ends at the callback, so this delay before the free is excluded.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    ASSERT_TRUE(tryFree().ok());
    EXPECT_EQ(requestsTotal(Direction::Write), 3);
    EXPECT_EQ(failuresTotal(Direction::Write), 1);
    EXPECT_EQ(timeoutsTotal(Direction::Write), 0);
    EXPECT_EQ(bytesTotal(Direction::Write), 2 * 4096);
    EXPECT_EQ(latencyCount(Direction::Write), 2);
    EXPECT_EQ(seriesFor(Direction::Write).latency_us.sum.load(),
              expected_latency_us);
}
#endif

class EngineMetricsOutcomeTest
    : public EngineMetricsTest,
      public ::testing::WithParamInterface<TransferStatusEnum> {};

// The first terminal status decides the outcome; later COMPLETED polls of
// the same tasks must not record again.
TEST_P(EngineMetricsOutcomeTest, FirstTerminalStatusIsRecordedOnce) {
    auto transport = std::make_shared<FakeTransport>(4096);
    buildBatch("127.0.0.1:0", transport, Transport::TransferRequest::WRITE, 2);

    TransferStatus status;
    transport->status = GetParam();
    for (size_t task = 0; task < 2; ++task)
        engine_->getTransferStatus(batch_, task, status);
    transport->status = TransferStatusEnum::COMPLETED;
    engine_->getBatchTransferStatus(batch_, status);
    for (size_t task = 0; task < 2; ++task)
        engine_->getTransferStatus(batch_, task, status);

    const bool timed_out = GetParam() == TransferStatusEnum::TIMEOUT;
    EXPECT_EQ(requestsTotal(Direction::Write), 2);
    EXPECT_EQ(failuresTotal(Direction::Write), timed_out ? 0 : 2);
    EXPECT_EQ(timeoutsTotal(Direction::Write), timed_out ? 2 : 0);
    EXPECT_EQ(bytesTotal(Direction::Write), 0);
    EXPECT_EQ(latencyCount(Direction::Write), 0);
}

// The reverse order: tasks recorded as COMPLETED must keep that outcome
// when a later poll reports a terminal failure status.
TEST_P(EngineMetricsOutcomeTest, CompletionBlocksLaterTerminalStatuses) {
    auto transport = std::make_shared<FakeTransport>(4096);
    buildBatch("127.0.0.1:0", transport, Transport::TransferRequest::WRITE, 2);

    TransferStatus status;
    for (size_t task = 0; task < 2; ++task)
        engine_->getTransferStatus(batch_, task, status);
    transport->status = GetParam();
    for (size_t task = 0; task < 2; ++task)
        engine_->getTransferStatus(batch_, task, status);

    EXPECT_EQ(requestsTotal(Direction::Write), 2);
    EXPECT_EQ(failuresTotal(Direction::Write), 0);
    EXPECT_EQ(timeoutsTotal(Direction::Write), 0);
    EXPECT_EQ(bytesTotal(Direction::Write), 2 * 4096);
    EXPECT_EQ(latencyCount(Direction::Write), 2);
}

INSTANTIATE_TEST_SUITE_P(TerminalStatuses, EngineMetricsOutcomeTest,
                         ::testing::Values(TransferStatusEnum::FAILED,
                                           TransferStatusEnum::CANCELED,
                                           TransferStatusEnum::TIMEOUT));

}  // namespace
}  // namespace mooncake
