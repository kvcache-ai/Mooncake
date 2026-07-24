// Copyright 2024 KVCache.AI
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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "transfer_engine_impl.h"
#include "transfer_engine_metrics.h"
#include "transport/transport.h"

using namespace mooncake;

namespace mooncake {

// Test peer that reaches into TransferEngineImpl internals to build batches and
// tasks without going through the full submit/segment plumbing. This keeps the
// metrics test hermetic (no RDMA/TCP setup required).
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

    // Append one task backed by `transport` and run it through the real
    // MultiTransport submission-metrics path (stamps start_time and calls
    // recordSubmitted), exactly as submitTransfer() does before posting.
    static void addSubmittedTask(BatchID batch_id, Transport* transport) {
        auto& batch = Transport::toBatchDesc(batch_id);
        batch.task_list.emplace_back();
        auto& task = batch.task_list.back();
        task.batch_id = batch_id;
        task.transport_ = transport;
        markTaskSubmitted(task);
    }

    static void markAllFinished(BatchID batch_id) {
        auto& batch = Transport::toBatchDesc(batch_id);
        for (auto& task : batch.task_list) {
            task.is_finished = true;
        }
    }

    static void freeBatch(TransferEngineImpl& engine, BatchID batch_id) {
        engine.multi_transports_->freeBatchID(batch_id);
    }

    // Expose MultiTransport's private submission-metrics helpers for testing.
    static void markTaskSubmitted(Transport::TransferTask& task) {
        MultiTransport::markTaskSubmitted(task);
    }
    static void rollbackTaskSubmission(Transport::TransferTask& task) {
        MultiTransport::rollbackTaskSubmission(task);
    }
};

namespace {

// Fake transport that reports a fixed terminal status for every task.
class FakeTransport : public Transport {
   public:
    FakeTransport(TransferStatusEnum status, size_t transferred_bytes)
        : status_(status), transferred_bytes_(transferred_bytes) {}

    Status submitTransfer(BatchID,
                          const std::vector<TransferRequest>&) override {
        return Status::OK();
    }

    Status getTransferStatus(BatchID, size_t, TransferStatus& status) override {
        status.s = status_;
        status.transferred_bytes = transferred_bytes_;
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

    TransferStatusEnum status_;
    size_t transferred_bytes_;
};

#ifdef WITH_METRICS

class TransferEngineMetricsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto& m = TransferEngineMetrics::instance();
        // Initialize without an HTTP server (port 0) so tests never bind a
        // socket, then start from a clean slate.
        m.initialize(TransferEngineMetrics::Config{});
        m.resetForTesting();
    }

    TransferEngineMetrics& metrics() {
        return TransferEngineMetrics::instance();
    }
};

// --- Direct API tests ------------------------------------------------------

TEST_F(TransferEngineMetricsTest, SuccessfulTransferRecordsBytesAndLatency) {
    auto& m = metrics();
    m.recordSubmitted();
    m.recordCompleted(4096, 0.002);  // 4KB in 2ms

    EXPECT_EQ(m.requestsTotal(), 1);
    EXPECT_EQ(m.bytesTotal(), 4096);
    EXPECT_EQ(m.failuresTotal(), 0);
    EXPECT_EQ(m.latencySampleCount(), 1);  // latency recorded
    EXPECT_EQ(m.sizeSampleCount(), 1);     // bytes recorded
    EXPECT_EQ(m.inflightTransfers(), 0);   // inflight back to zero
}

TEST_F(TransferEngineMetricsTest, FailedTransferRecordsFailure) {
    auto& m = metrics();
    m.recordSubmitted();
    m.recordFailed();

    EXPECT_EQ(m.requestsTotal(), 1);
    EXPECT_EQ(m.failuresTotal(), 1);
    EXPECT_EQ(m.bytesTotal(), 0);
    EXPECT_EQ(m.latencySampleCount(), 0);  // no latency for failures
    EXPECT_EQ(m.inflightTransfers(), 0);   // inflight back to zero
}

TEST_F(TransferEngineMetricsTest, InflightTracksConcurrentTransfers) {
    auto& m = metrics();
    m.recordSubmitted();
    m.recordSubmitted();
    m.recordSubmitted();
    EXPECT_EQ(m.inflightTransfers(), 3);

    m.recordCompleted(1024, 0.001);
    EXPECT_EQ(m.inflightTransfers(), 2);

    m.recordFailed();
    EXPECT_EQ(m.inflightTransfers(), 1);

    m.recordCompleted(2048, 0.001);
    EXPECT_EQ(m.inflightTransfers(), 0);
}

TEST_F(TransferEngineMetricsTest, PrometheusOutputContainsStandardMetrics) {
    auto& m = metrics();
    m.recordSubmitted();
    m.recordCompleted(1048576, 0.01);

    std::string prom = m.getPrometheusMetrics();
    EXPECT_NE(prom.find("transfer_requests_total"), std::string::npos);
    EXPECT_NE(prom.find("transfer_bytes_total"), std::string::npos);
    EXPECT_NE(prom.find("inflight_transfers"), std::string::npos);
    EXPECT_NE(prom.find("transfer_latency_seconds"), std::string::npos);
    EXPECT_NE(prom.find("transfer_size_bytes"), std::string::npos);
}

TEST_F(TransferEngineMetricsTest, DisabledAtRuntimeSkipsRecording) {
    auto& m = metrics();
    TransferEngineMetrics::setEnabled(false);
    m.recordSubmitted();
    m.recordCompleted(4096, 0.002);
    m.recordFailed();
    EXPECT_EQ(m.requestsTotal(), 0);
    EXPECT_EQ(m.bytesTotal(), 0);
    EXPECT_EQ(m.failuresTotal(), 0);
    EXPECT_EQ(m.inflightTransfers(), 0);
    TransferEngineMetrics::setEnabled(true);
}

// --- End-to-end dedup test through TransferEngineImpl -----------------------

// Drives the real TransferEngineImpl::getTransferStatus() to verify that
// repeated polling of a terminal task records metrics exactly once.
class TransferEngineImplMetricsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        setenv("MC_TE_METRIC", "1", /*overwrite=*/1);
        auto& m = TransferEngineMetrics::instance();
        m.initialize(TransferEngineMetrics::Config{});
        m.resetForTesting();
    }
};

TEST_F(TransferEngineImplMetricsTest,
       DuplicateCompletionPollDoesNotDoubleCount) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12399"), 0);

    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 8192);
    TransferEngineImplTestPeer::installTransport(engine, transport);

    BatchID batch = TransferEngineImplTestPeer::allocateBatch(engine, 1);
    // addSubmittedTask runs the real submission-metrics path (recordSubmitted).
    TransferEngineImplTestPeer::addSubmittedTask(batch, transport.get());

    auto& m = TransferEngineMetrics::instance();
    EXPECT_EQ(m.requestsTotal(), 1);
    EXPECT_EQ(m.inflightTransfers(), 1);

    // Poll several times: metrics must be recorded exactly once.
    TransferStatus status;
    for (int i = 0; i < 5; ++i) {
        ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
        EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);
    }

    EXPECT_EQ(m.bytesTotal(), 8192);
    EXPECT_EQ(m.latencySampleCount(), 1);
    EXPECT_EQ(m.inflightTransfers(), 0);

    TransferEngineImplTestPeer::markAllFinished(batch);
    TransferEngineImplTestPeer::freeBatch(engine, batch);
}

TEST_F(TransferEngineImplMetricsTest, FailedTaskPollRecordsSingleFailure) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12398"), 0);

    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::FAILED, 0);
    TransferEngineImplTestPeer::installTransport(engine, transport);

    BatchID batch = TransferEngineImplTestPeer::allocateBatch(engine, 1);
    TransferEngineImplTestPeer::addSubmittedTask(batch, transport.get());

    auto& m = TransferEngineMetrics::instance();
    EXPECT_EQ(m.inflightTransfers(), 1);

    TransferStatus status;
    for (int i = 0; i < 3; ++i) {
        ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
        EXPECT_EQ(status.s, Transport::TransferStatusEnum::FAILED);
    }

    EXPECT_EQ(m.failuresTotal(), 1);
    EXPECT_EQ(m.inflightTransfers(), 0);

    TransferEngineImplTestPeer::markAllFinished(batch);
    TransferEngineImplTestPeer::freeBatch(engine, batch);
}

// Reproduces the async-completion scenario: the task is already terminal by the
// time the caller polls it exactly once. Because start_time is stamped at
// submit (before posting), the single poll still records completion and clears
// the inflight gauge — there is no permanent inflight leak.
TEST_F(TransferEngineImplMetricsTest,
       SinglePollOnAlreadyTerminalTaskIsRecorded) {
    TransferEngineImpl engine(false);
    ASSERT_EQ(engine.init(P2PHANDSHAKE, "127.0.0.1:12397"), 0);

    auto transport = std::make_shared<FakeTransport>(
        Transport::TransferStatusEnum::COMPLETED, 4096);
    TransferEngineImplTestPeer::installTransport(engine, transport);

    BatchID batch = TransferEngineImplTestPeer::allocateBatch(engine, 1);
    TransferEngineImplTestPeer::addSubmittedTask(batch, transport.get());

    auto& m = TransferEngineMetrics::instance();
    ASSERT_EQ(m.inflightTransfers(), 1);

    TransferStatus status;
    ASSERT_TRUE(engine.getTransferStatus(batch, 0, status).ok());
    EXPECT_EQ(status.s, Transport::TransferStatusEnum::COMPLETED);

    EXPECT_EQ(m.requestsTotal(), 1);
    EXPECT_EQ(m.bytesTotal(), 4096);
    EXPECT_EQ(m.inflightTransfers(), 0);  // no leak

    TransferEngineImplTestPeer::markAllFinished(batch);
    TransferEngineImplTestPeer::freeBatch(engine, batch);
}

// A task that is submission-recorded but then rolled back (transport failed to
// post it) must not leak the inflight gauge, and is counted as a failure.
TEST_F(TransferEngineMetricsTest, SubmissionRollbackBalancesInflight) {
    auto& m = metrics();

    Transport::BatchDesc batch;
    batch.task_list.emplace_back();
    auto& task = batch.task_list.back();

    TransferEngineImplTestPeer::markTaskSubmitted(task);
    EXPECT_EQ(m.requestsTotal(), 1);
    EXPECT_EQ(m.inflightTransfers(), 1);

    TransferEngineImplTestPeer::rollbackTaskSubmission(task);
    EXPECT_EQ(m.failuresTotal(), 1);
    EXPECT_EQ(m.inflightTransfers(), 0);  // balanced
}

#endif  // WITH_METRICS

}  // namespace
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
