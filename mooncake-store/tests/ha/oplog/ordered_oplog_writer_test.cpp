#include "master/ha/oplog/ordered_oplog_writer.h"

#include <gtest/gtest.h>
#include <xxhash.h>

#include <async_simple/coro/FutureAwaiter.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/executors/SimpleExecutor.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "master/ha_metric_manager.h"

namespace mooncake::test {
namespace {

class FakeBatchWriter {
   public:
    using Clock = std::chrono::steady_clock;

    ErrorCode Write(const OpLogBatchRecord& batch,
                    const DurablePrefix& expected_prefix) {
        std::unique_lock<std::mutex> lock(mutex_);
        attempt_times_.push_back(Clock::now());
        attempt_cv_.notify_all();
        while (blocked_) {
            blocked_write_active_ = true;
            blocked_cv_.notify_all();
            blocked_cv_.wait(lock);
        }
        blocked_write_active_ = false;
        if (next_error_ != ErrorCode::OK) {
            ErrorCode err = next_error_;
            next_error_ = ErrorCode::OK;
            return err;
        }
        if (failures_remaining_ > 0) {
            --failures_remaining_;
            return repeated_error_;
        }
        writes_.push_back({.batch = batch, .expected_prefix = expected_prefix});
        if (writes_.size() == block_after_writes_) blocked_ = true;
        cv_.notify_all();
        return ErrorCode::OK;
    }

    std::vector<OpLogBatchRecord> Batches() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<OpLogBatchRecord> batches;
        batches.reserve(writes_.size());
        for (const auto& write : writes_) {
            batches.push_back(write.batch);
        }
        return batches;
    }

    std::vector<DurablePrefix> ExpectedPrefixes() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<DurablePrefix> prefixes;
        prefixes.reserve(writes_.size());
        for (const auto& write : writes_) {
            prefixes.push_back(write.expected_prefix);
        }
        return prefixes;
    }

    bool WaitForWrites(size_t count, std::chrono::milliseconds timeout =
                                         std::chrono::milliseconds(1000)) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout,
                            [&] { return writes_.size() >= count; });
    }

    bool WaitForAttempts(size_t count, std::chrono::milliseconds timeout =
                                           std::chrono::milliseconds(5000)) {
        std::unique_lock<std::mutex> lock(mutex_);
        return attempt_cv_.wait_for(
            lock, timeout, [&] { return attempt_times_.size() >= count; });
    }

    std::vector<Clock::time_point> AttemptTimes() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return attempt_times_;
    }

    void FailNextWrite(ErrorCode err) {
        std::lock_guard<std::mutex> lock(mutex_);
        next_error_ = err;
    }
    void FailNextWrites(size_t count, ErrorCode err) {
        std::lock_guard<std::mutex> lock(mutex_);
        failures_remaining_ = count;
        repeated_error_ = err;
    }
    void AllowWrites() {
        std::lock_guard<std::mutex> lock(mutex_);
        next_error_ = ErrorCode::OK;
        failures_remaining_ = 0;
    }
    void BlockAfterWrites(size_t count) {
        std::lock_guard<std::mutex> lock(mutex_);
        block_after_writes_ = count;
    }
    void BlockWrites() {
        std::lock_guard<std::mutex> lock(mutex_);
        blocked_ = true;
    }
    void UnblockWrites() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            blocked_ = false;
        }
        blocked_cv_.notify_all();
    }
    bool WaitForBlockedWrite(
        std::chrono::milliseconds timeout = std::chrono::milliseconds(1000)) {
        std::unique_lock<std::mutex> lock(mutex_);
        return blocked_cv_.wait_for(lock, timeout,
                                    [&] { return blocked_write_active_; });
    }

   private:
    struct WriteRecord {
        OpLogBatchRecord batch;
        DurablePrefix expected_prefix;
    };

    mutable std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable attempt_cv_;
    std::condition_variable blocked_cv_;
    std::vector<WriteRecord> writes_;
    std::vector<Clock::time_point> attempt_times_;
    ErrorCode next_error_{ErrorCode::OK};
    ErrorCode repeated_error_{ErrorCode::OK};
    size_t failures_remaining_{0};
    size_t block_after_writes_{0};
    bool blocked_{false};
    bool blocked_write_active_{false};
};

OpLogEntry MakeEntry(std::string key = "key", std::string payload = "value") {
    OpLogEntry entry;
    entry.timestamp_ms = 1234567890;
    entry.op_type = OpType::PUT_END;
    entry.tenant_id = "tenant";
    entry.object_key = std::move(key);
    entry.payload = std::move(payload);
    entry.checksum = static_cast<uint32_t>(
        XXH32(entry.payload.data(), entry.payload.size(), 0));
    entry.prefix_hash = static_cast<uint32_t>(
        XXH32(entry.object_key.data(), entry.object_key.size(), 0));
    return entry;
}

#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
bool WaitForMetric(const std::function<bool()>& predicate) {
    for (int i = 0; i < 100; ++i) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return false;
}
#endif

}  // namespace

namespace {

async_simple::coro::Lazy<ErrorCode> AwaitOnExecutor(
    OrderedOpLogWriter& writer, uint64_t sequence,
    std::promise<std::thread::id>& registered,
    std::promise<std::thread::id>& resumed) {
    auto future = writer.AwaitDurable(sequence);
    registered.set_value(std::this_thread::get_id());
    auto error = co_await std::move(future);
    resumed.set_value(std::this_thread::get_id());
    co_return error;
}

class OrderedOpLogWriterAwaitTest : public ::testing::Test {
   protected:
    void CreateWriter(OrderedOpLogWriterConfig config = {},
                      OrderedOpLogWriter::TerminalCallback callback = {}) {
        writer_ = std::make_unique<OrderedOpLogWriter>(
            config,
            [this](const OpLogBatchRecord& batch, const DurablePrefix& prefix) {
                return storage_.Write(batch, prefix);
            },
            std::move(callback));
    }

    async_simple::Future<ErrorCode>& Await(uint64_t sequence) {
        waiters_.push_back(writer_->AwaitDurable(sequence));
        return waiters_.back();
    }

    void ExpectPending(async_simple::Future<ErrorCode>& waiter) {
        // Storage is gated, so readiness can be checked without a timeout.
        EXPECT_FALSE(waiter.hasResult());
    }

    void ExpectResult(async_simple::Future<ErrorCode>& waiter,
                      ErrorCode expected) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!waiter.hasResult() &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        ASSERT_TRUE(waiter.hasResult());
        EXPECT_EQ(std::move(waiter).get(), expected);
    }

    void ExpectReadyResult(async_simple::Future<ErrorCode>& waiter,
                           ErrorCode expected) {
        ASSERT_TRUE(waiter.hasResult());
        EXPECT_EQ(std::move(waiter).get(), expected);
    }

    void ReenterFromContinuation(async_simple::Future<ErrorCode>& waiter) {
        waiter = std::move(waiter).thenValue([this](ErrorCode error) {
            // These methods all acquire the writer mutex. Inline completion
            // must not hold it, including terminal failure and Stop().
            writer_->IsAccepting();
            writer_->LastError();
            writer_->GetTerminalState();
            EXPECT_TRUE(writer_->AwaitDurable(0).hasResult());
            return error;
        });
    }

    void BlockCallback() {
        std::unique_lock<std::mutex> lock(callback_mutex_);
        callback_entered_ = true;
        callback_cv_.notify_all();
        callback_cv_.wait(lock, [this] { return callback_released_; });
    }

    bool WaitForCallback() {
        std::unique_lock<std::mutex> lock(callback_mutex_);
        return callback_cv_.wait_for(lock, std::chrono::seconds(5),
                                     [this] { return callback_entered_; });
    }

    void ReleaseCallback() {
        {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            callback_released_ = true;
        }
        callback_cv_.notify_all();
    }

    void CheckTerminal(ErrorCode error, OrderedOpLogWriterTerminalReason reason,
                       std::chrono::milliseconds retry_timeout = {}) {
        storage_.BlockWrites();
        storage_.FailNextWrites(100000, error);
        CreateWriter({.initial_durable_prefix = {.batch_id = 1, .last_seq = 10},
                      .retry_timeout = retry_timeout},
                     [this](const OrderedOpLogWriterTerminalState&) {
                         ++terminal_callbacks_;
                         BlockCallback();
                     });
        auto reservation = writer_->Reserve();
        ASSERT_TRUE(reservation.has_value());
        auto pending =
            writer_->Commit(std::move(*reservation), MakeEntry(), {});
        ASSERT_TRUE(pending.has_value());
        auto& first = Await(pending->sequence_id());
        auto& later = Await(pending->sequence_id() + 1);
        ReenterFromContinuation(first);
        writer_->Start();
        ASSERT_TRUE(storage_.WaitForBlockedWrite());
        ExpectPending(first);
        ExpectPending(later);
        storage_.UnblockWrites();
        ASSERT_TRUE(WaitForCallback());

        // Awaiters must wake before the terminal callback is allowed to finish.
        ExpectResult(first, error);
        ExpectResult(later, error);
        auto state = writer_->GetTerminalState();
        ASSERT_TRUE(state.has_value());
        EXPECT_EQ(state->reason, reason);
        EXPECT_EQ(state->error, error);
        EXPECT_EQ(state->durable_prefix.last_seq, 10);
        EXPECT_EQ(terminal_callbacks_.load(), 1);
        ExpectReadyResult(Await(pending->sequence_id()), error);
        ExpectReadyResult(Await(10), ErrorCode::OK);
        ReleaseCallback();
        writer_->Stop();
        ExpectReadyResult(Await(pending->sequence_id()), error);
    }

    void TearDown() override {
        storage_.UnblockWrites();
        ReleaseCallback();
        if (writer_) writer_->Stop();
    }

    FakeBatchWriter storage_;
    std::unique_ptr<OrderedOpLogWriter> writer_;
    std::deque<async_simple::Future<ErrorCode>> waiters_;
    std::atomic<int> terminal_callbacks_{0};
    std::atomic<bool> callback_finished_{false};
    std::mutex callback_mutex_;
    std::condition_variable callback_cv_;
    bool callback_entered_{false};
    bool callback_released_{false};
};

}  // namespace

TEST_F(OrderedOpLogWriterAwaitTest,
       RestoredPrefixSucceedsBeforeStartAndAfterStop) {
    CreateWriter({.initial_durable_prefix = {.batch_id = 2, .last_seq = 10}});
    ExpectReadyResult(Await(0), ErrorCode::OK);
    ExpectReadyResult(Await(1), ErrorCode::OK);
    ExpectReadyResult(Await(10), ErrorCode::OK);
    writer_->Stop();
    ExpectReadyResult(Await(10), ErrorCode::OK);
    ExpectReadyResult(Await(11), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST_F(OrderedOpLogWriterAwaitTest, CommittedSequenceWaitsForStorageSuccess) {
    storage_.BlockWrites();
    CreateWriter();
    writer_->Start();
    auto reservation = writer_->Reserve();
    ASSERT_TRUE(reservation.has_value());
    auto pending = writer_->Commit(std::move(*reservation), MakeEntry(), {});
    ASSERT_TRUE(pending.has_value());
    ASSERT_TRUE(storage_.WaitForBlockedWrite());
    auto& waiter = Await(pending->sequence_id());
    ExpectPending(waiter);
    storage_.UnblockWrites();
    ExpectResult(waiter, ErrorCode::OK);
    // Registration after advancement cannot miss an earlier notification.
    ExpectReadyResult(Await(pending->sequence_id()), ErrorCode::OK);
}

TEST_F(OrderedOpLogWriterAwaitTest, PrefixAdvancesInStages) {
    storage_.BlockWrites();
    storage_.BlockAfterWrites(1);
    CreateWriter({.max_entries_per_batch = 3});
    for (int i = 0; i < 3; ++i) {
        auto reservation = writer_->Reserve();
        ASSERT_TRUE(reservation.has_value());
        ASSERT_TRUE(writer_->Commit(std::move(*reservation), MakeEntry(), {})
                        .has_value());
    }
    auto& third = Await(3);
    auto& second = Await(2);
    auto& first = Await(1);
    writer_->Start();
    ASSERT_TRUE(storage_.WaitForBlockedWrite());
    ExpectPending(first);
    storage_.UnblockWrites();
    ExpectResult(first, ErrorCode::OK);
    ASSERT_TRUE(storage_.WaitForBlockedWrite());
    ExpectPending(second);
    ExpectPending(third);
    storage_.UnblockWrites();
    ExpectResult(second, ErrorCode::OK);
    ExpectResult(third, ErrorCode::OK);
}

TEST_F(OrderedOpLogWriterAwaitTest,
       NonRetryableFailureWakesBeforeTerminalCallback) {
    CheckTerminal(ErrorCode::INVALID_PARAMS,
                  OrderedOpLogWriterTerminalReason::kNonRetryableWriteError);
}

TEST_F(OrderedOpLogWriterAwaitTest, FencingFailurePreservesTerminalError) {
    CheckTerminal(ErrorCode::ETCD_TRANSACTION_FAIL,
                  OrderedOpLogWriterTerminalReason::kFenced);
}

TEST_F(OrderedOpLogWriterAwaitTest, RetryTimeoutWakesAllOutstandingWaiters) {
    CheckTerminal(ErrorCode::PERSISTENT_FAIL,
                  OrderedOpLogWriterTerminalReason::kRetryTimeout,
                  std::chrono::milliseconds(5));
}

TEST_F(OrderedOpLogWriterAwaitTest, StopWakesUncoveredWaitersBeforeStart) {
    CreateWriter();
    auto& waiter = Await(1);
    ReenterFromContinuation(waiter);
    ExpectPending(waiter);
    writer_->Stop();
    ExpectResult(waiter, ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST_F(OrderedOpLogWriterAwaitTest, StopWakesUncoveredWaitersAfterStart) {
    CreateWriter();
    writer_->Start();
    auto& waiter = Await(1);
    ReenterFromContinuation(waiter);
    ExpectPending(waiter);
    writer_->Stop();
    ExpectResult(waiter, ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST_F(OrderedOpLogWriterAwaitTest, DurableCallbackCompletionIsNotRequired) {
    CreateWriter();
    auto reservation = writer_->Reserve();
    ASSERT_TRUE(reservation.has_value());
    auto pending = writer_->Commit(std::move(*reservation), MakeEntry(),
                                   [this](const OpLogEntry&) {
                                       BlockCallback();
                                       callback_finished_ = true;
                                   });
    ASSERT_TRUE(pending.has_value());
    auto& waiter = Await(pending->sequence_id());
    writer_->Start();
    ASSERT_TRUE(WaitForCallback());
    ExpectResult(waiter, ErrorCode::OK);
    EXPECT_FALSE(callback_finished_.load());
    ReleaseCallback();
    writer_->Stop();
    EXPECT_TRUE(callback_finished_.load());
    ExpectResult(Await(pending->sequence_id()), ErrorCode::OK);
}

TEST_F(OrderedOpLogWriterAwaitTest, AllWaitersForSameSequenceWake) {
    storage_.BlockWrites();
    CreateWriter();
    auto reservation = writer_->Reserve();
    ASSERT_TRUE(reservation.has_value());
    auto pending = writer_->Commit(std::move(*reservation), MakeEntry(), {});
    ASSERT_TRUE(pending.has_value());
    for (int i = 0; i < 8; ++i) Await(pending->sequence_id());
    writer_->Start();
    ASSERT_TRUE(storage_.WaitForBlockedWrite());
    for (auto& waiter : waiters_) ExpectPending(waiter);
    storage_.UnblockWrites();
    for (auto& waiter : waiters_) ExpectResult(waiter, ErrorCode::OK);
}

TEST_F(OrderedOpLogWriterAwaitTest, DurableContinuationCanReenterWriter) {
    storage_.BlockWrites();
    CreateWriter();
    auto reservation = writer_->Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(
        writer_->Commit(std::move(*reservation), MakeEntry(), {}).has_value());
    auto& waiter = Await(1);
    ReenterFromContinuation(waiter);
    writer_->Start();
    ASSERT_TRUE(storage_.WaitForBlockedWrite());
    ExpectPending(waiter);
    storage_.UnblockWrites();
    ExpectResult(waiter, ErrorCode::OK);
}

TEST_F(OrderedOpLogWriterAwaitTest, CompletesWaitersInSequenceOrder) {
    storage_.BlockWrites();
    CreateWriter({.max_entries_per_batch = 3});
    for (int i = 0; i < 3; ++i) {
        auto reservation = writer_->Reserve();
        ASSERT_TRUE(reservation.has_value());
        ASSERT_TRUE(writer_->Commit(std::move(*reservation), MakeEntry(), {})
                        .has_value());
    }
    std::mutex mutex;
    std::vector<uint64_t> completed;
    for (uint64_t sequence : {3, 1, 2}) {
        auto& waiter = Await(sequence);
        waiter = std::move(waiter).thenValue([&, sequence](ErrorCode error) {
            std::lock_guard<std::mutex> lock(mutex);
            completed.push_back(sequence);
            return error;
        });
    }
    writer_->Start();
    EXPECT_TRUE(storage_.WaitForBlockedWrite());
    storage_.UnblockWrites();
    for (auto& waiter : waiters_) ExpectResult(waiter, ErrorCode::OK);
    writer_->Stop();
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_EQ(completed, (std::vector<uint64_t>{1, 2, 3}));
}

TEST_F(OrderedOpLogWriterAwaitTest,
       OutstandingWaitAllowsOtherWorkOnSameExecutorThread) {
    storage_.BlockWrites();
    CreateWriter();
    auto reservation = writer_->Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(
        writer_->Commit(std::move(*reservation), MakeEntry(), {}).has_value());
    writer_->Start();
    ASSERT_TRUE(storage_.WaitForBlockedWrite());

    std::promise<std::thread::id> registered;
    std::promise<std::thread::id> resumed;
    std::promise<std::thread::id> other_work;
    std::promise<ErrorCode> completed;
    auto registered_future = registered.get_future();
    auto resumed_future = resumed.get_future();
    auto work_future = other_work.get_future();
    auto completed_future = completed.get_future();
    async_simple::executors::SimpleExecutor executor(1);
    AwaitOnExecutor(*writer_, 1, registered, resumed)
        .via(&executor)
        .start([&](async_simple::Try<ErrorCode>&& result) {
            if (result.hasError()) {
                completed.set_exception(result.getException());
            } else {
                completed.set_value(result.value());
            }
        });
    const auto registered_status =
        registered_future.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(registered_status, std::future_status::ready);
    EXPECT_TRUE(executor.schedule(
        [&] { other_work.set_value(std::this_thread::get_id()); }));
    const auto work_status = work_future.wait_for(std::chrono::seconds(5));
    EXPECT_EQ(work_status, std::future_status::ready);
    EXPECT_EQ(completed_future.wait_for(std::chrono::milliseconds(0)),
              std::future_status::timeout);

    // Always release storage and drain the writer before destroying executor,
    // even if a non-blocking/scheduling assertion above failed.
    storage_.UnblockWrites();
    const auto completed_status =
        completed_future.wait_for(std::chrono::seconds(5));
    writer_->Stop();
    EXPECT_EQ(completed_status, std::future_status::ready);
    if (completed_status == std::future_status::ready) {
        EXPECT_EQ(completed_future.get(), ErrorCode::OK);
    }
    if (registered_status == std::future_status::ready &&
        work_status == std::future_status::ready) {
        const auto executor_thread = registered_future.get();
        EXPECT_EQ(work_future.get(), executor_thread);
        if (resumed_future.wait_for(std::chrono::milliseconds(0)) ==
            std::future_status::ready) {
            EXPECT_EQ(resumed_future.get(), executor_thread);
        } else {
            ADD_FAILURE() << "durability coroutine did not resume";
        }
    }
}

TEST(OrderedOpLogWriterMetricsTest, RuntimeSnapshotTracksAdmission) {
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [](const OpLogBatchRecord&, const DurablePrefix&) {
            return ErrorCode::OK;
        });
    writer.ActivateRuntimeMetrics();
    auto snapshot = HAMetricManager::instance().get_writer_runtime();
    EXPECT_TRUE(snapshot.accepting);
    EXPECT_EQ(snapshot.waiting_slots, 0);

    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    snapshot = HAMetricManager::instance().get_writer_runtime();
    EXPECT_EQ(snapshot.waiting_slots, 1);
    writer.Abort(std::move(*reservation));
    snapshot = HAMetricManager::instance().get_writer_runtime();
    EXPECT_EQ(snapshot.waiting_slots, 0);
}

TEST(OrderedOpLogWriterMetricsTest,
     CandidateAndOldDestructorCannotOverwriteOwner) {
    auto write = [](const OpLogBatchRecord&, const DurablePrefix&) {
        return ErrorCode::OK;
    };
    auto old = std::make_unique<OrderedOpLogWriter>(
        OrderedOpLogWriterConfig{
            .initial_durable_prefix = {.batch_id = 1, .last_seq = 10}},
        write);
    old->ActivateRuntimeMetrics();
    auto& metrics = HAMetricManager::instance();
    {
        OrderedOpLogWriter rejected({}, write);
        EXPECT_EQ(metrics.get_writer_runtime().durable_sequence, 10);
    }
    EXPECT_TRUE(metrics.get_writer_runtime().accepting);
    OrderedOpLogWriter replacement(
        OrderedOpLogWriterConfig{
            .initial_durable_prefix = {.batch_id = 2, .last_seq = 20}},
        write);
    EXPECT_EQ(metrics.get_writer_runtime().durable_sequence, 10);
    replacement.ActivateRuntimeMetrics();
    old->Stop();
    old.reset();
    EXPECT_TRUE(metrics.get_writer_runtime().accepting);
    EXPECT_EQ(metrics.get_writer_runtime().durable_sequence, 20);
    replacement.Stop();
    EXPECT_FALSE(metrics.get_writer_runtime().accepting);
}

TEST(OrderedOpLogWriterAdmissionTest, AbortLeavesNoSequenceGap) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    auto first = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    writer.Abort(std::move(*first));

    auto second = writer.Reserve();
    ASSERT_TRUE(second.has_value());
    auto pending = writer.Commit(std::move(*second), MakeEntry(),
                                 [](const OpLogEntry&) {});
    ASSERT_TRUE(pending.has_value());

    EXPECT_EQ(1u, pending->sequence_id());
}

TEST(OrderedOpLogWriterAdmissionTest, CommitAssignsContiguousSequences) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 3},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    auto first = writer.Reserve();
    auto second = writer.Reserve();
    auto third = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(third.has_value());

    auto p1 =
        writer.Commit(std::move(*first), MakeEntry("k1"), [](const auto&) {});
    auto p2 =
        writer.Commit(std::move(*second), MakeEntry("k2"), [](const auto&) {});
    auto p3 =
        writer.Commit(std::move(*third), MakeEntry("k3"), [](const auto&) {});

    ASSERT_TRUE(p1.has_value());
    ASSERT_TRUE(p2.has_value());
    ASSERT_TRUE(p3.has_value());
    EXPECT_EQ(1u, p1->sequence_id());
    EXPECT_EQ(2u, p2->sequence_id());
    EXPECT_EQ(3u, p3->sequence_id());
}

#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
TEST(OrderedOpLogWriterMetricTest, RecordsDurabilityQueuesAndRetry) {
    FakeBatchWriter storage;
    storage.FailNextWrites(1, ErrorCode::PERSISTENT_FAIL);
    auto& metrics = HAMetricManager::instance();
    const auto batches_before =
        metrics.get_batch_record_durable_batches_total();
    const auto entries_before =
        metrics.get_batch_record_durable_entries_total();
    const auto retries_before = metrics.get_batch_record_retries_total();

    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    std::atomic<bool> callback_done{false};
    ASSERT_TRUE(writer
                    .Commit(std::move(*reservation), MakeEntry(),
                            [&](const OpLogEntry&) { callback_done = true; })
                    .has_value());

    ASSERT_TRUE(WaitForMetric([&] { return callback_done.load(); }));
    writer.Stop();

    EXPECT_EQ(batches_before + 1,
              metrics.get_batch_record_durable_batches_total());
    EXPECT_EQ(entries_before + 1,
              metrics.get_batch_record_durable_entries_total());
    EXPECT_EQ(retries_before + 1, metrics.get_batch_record_retries_total());
    EXPECT_EQ(1, metrics.get_batch_record_last_batch_id());
    EXPECT_EQ(1, metrics.get_batch_record_durable_sequence());
    EXPECT_EQ(0, metrics.get_batch_record_committed_queue_depth());
    EXPECT_EQ(0, metrics.get_batch_record_callback_queue_depth());
}
#endif

TEST(OrderedOpLogWriterAdmissionTest,
     InvalidEntryDoesNotConsumeSequenceOrInvokeCallback) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    auto invalid_reservation = writer.Reserve();
    ASSERT_TRUE(invalid_reservation.has_value());
    auto invalid = MakeEntry("bad");
    invalid.tenant_id = "_reserved";
    bool callback_called = false;
    auto rejected =
        writer.Commit(std::move(*invalid_reservation), std::move(invalid),
                      [&](const OpLogEntry&) { callback_called = true; });
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, rejected.error());
    EXPECT_FALSE(callback_called);

    auto valid_reservation = writer.Reserve();
    ASSERT_TRUE(valid_reservation.has_value());
    auto accepted = writer.Commit(std::move(*valid_reservation), MakeEntry(),
                                  [](const OpLogEntry&) {});
    ASSERT_TRUE(accepted.has_value());
    EXPECT_EQ(1u, accepted->sequence_id());
}

TEST(OrderedOpLogWriterAdmissionTest, RejectsOpTypeOutsideEnumRange) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 1},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    auto invalid = MakeEntry();
    invalid.op_type = static_cast<OpType>(255);
    auto rejected = writer.Commit(std::move(*reservation), std::move(invalid),
                                  [](const OpLogEntry&) {});

    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, rejected.error());
}

TEST(OrderedOpLogWriterAdmissionTest,
     ReserveFailsWhenOpenWaitingSlotsReachMax) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    auto first = writer.Reserve();
    auto second = writer.Reserve();
    EXPECT_TRUE(first.has_value());
    EXPECT_TRUE(second.has_value());
    auto third = writer.Reserve();

    ASSERT_FALSE(third.has_value());
    EXPECT_EQ(ErrorCode::TASK_PENDING_LIMIT_EXCEEDED, third.error());
}

TEST(OrderedOpLogWriterAdmissionTest,
     ReserveFailsWhenInitialDurablePrefixCannotAdvance) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{
            .max_entries_per_batch = 2,
            .initial_durable_prefix = {.batch_id = 1, .last_seq = UINT64_MAX}},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    auto reservation = writer.Reserve();

    ASSERT_FALSE(reservation.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, reservation.error());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, writer.LastError());
}

TEST(OrderedOpLogWriterAdmissionTest, MoveAssigningReservationReleasesOldSlot) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    auto first = writer.Reserve();
    auto second = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());

    *first = std::move(*second);
    EXPECT_TRUE(writer.Reserve().has_value());
}

TEST(OrderedOpLogWriterAdmissionTest, DestroyingReservationReleasesSlot) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 1},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    {
        auto reservation = writer.Reserve();
        ASSERT_TRUE(reservation.has_value());
    }

    EXPECT_TRUE(writer.Reserve().has_value());
}

TEST(OrderedOpLogWriterAdmissionTest,
     SealingCommittedEntriesFreesWaitingSlots) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 1},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    auto first = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*first), MakeEntry("k1"), [](const auto&) {})
            .has_value());
    ASSERT_TRUE(storage.WaitForWrites(1));

    auto second = writer.Reserve();
    EXPECT_TRUE(second.has_value());
    writer.Stop();
}

TEST(OrderedOpLogWriterAdmissionTest,
     FirstCommitFreesOpenSlotBeforeWriterThreadRuns) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 1},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    auto first = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*first), MakeEntry("k1"), [](const auto&) {})
            .has_value());

    auto second = writer.Reserve();
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*second), MakeEntry("k2"), [](const auto&) {})
            .has_value());

    writer.Start();
    ASSERT_TRUE(storage.WaitForWrites(2));
    writer.Stop();
}

TEST(OrderedOpLogWriterAdmissionTest, StopClosesAdmission) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 1},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    writer.Start();
    writer.Stop();

    EXPECT_FALSE(writer.IsAccepting());
    auto reservation = writer.Reserve();
    ASSERT_FALSE(reservation.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, reservation.error());
}

TEST(OrderedOpLogWriterAdmissionTest, StopRejectsOutstandingReservation) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 1},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    writer.Start();
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    writer.Stop();

    auto pending =
        writer.Commit(std::move(*reservation), MakeEntry(), [](const auto&) {});
    ASSERT_FALSE(pending.has_value());
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, pending.error());
    EXPECT_TRUE(storage.Batches().empty());
}

TEST(OrderedOpLogWriterAdmissionTest,
     OpenBatchCapacityRecoversAfterInflightWrite) {
    FakeBatchWriter storage;
    storage.BlockWrites();
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    auto first = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*first), MakeEntry("k1"), [](const auto&) {})
            .has_value());
    ASSERT_TRUE(storage.WaitForBlockedWrite());

    auto second = writer.Reserve();
    auto third = writer.Reserve();
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(third.has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*second), MakeEntry("k2"), [](const auto&) {})
            .has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*third), MakeEntry("k3"), [](const auto&) {})
            .has_value());

    auto full = writer.Reserve();
    ASSERT_FALSE(full.has_value());
    EXPECT_EQ(ErrorCode::TASK_PENDING_LIMIT_EXCEEDED, full.error());

    storage.UnblockWrites();
    ASSERT_TRUE(storage.WaitForWrites(2));
    EXPECT_TRUE(writer.Reserve().has_value());
    writer.Stop();
}

TEST(OrderedOpLogWriterAdmissionTest,
     ExistingReservationCanCommitAfterAcceptingFalse) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    auto existing = writer.Reserve();
    ASSERT_TRUE(existing.has_value());
    auto failing = writer.Reserve();
    ASSERT_TRUE(failing.has_value());
    storage.FailNextWrites(100000, ErrorCode::PERSISTENT_FAIL);
    ASSERT_TRUE(
        writer
            .Commit(std::move(*failing), MakeEntry("fail"), [](const auto&) {})
            .has_value());

    for (int i = 0; i < 100 && writer.IsAccepting(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_FALSE(writer.IsAccepting());

    auto pending = writer.Commit(std::move(*existing), MakeEntry("existing"),
                                 [](const auto&) {});
    EXPECT_TRUE(pending.has_value());
    writer.Stop();
}

TEST(OrderedOpLogWriterLoopTest,
     WritesSingleEntryWithoutWaitingForMaxBatchSize) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 1024},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(writer
                    .Commit(std::move(*reservation), MakeEntry("k1"),
                            [](const auto&) {})
                    .has_value());

    ASSERT_TRUE(storage.WaitForWrites(1));
    auto batches = storage.Batches();
    ASSERT_EQ(1u, batches.size());
    ASSERT_EQ(1u, batches[0].entries.size());
    EXPECT_EQ(1u, batches[0].first_seq);
    EXPECT_EQ(1u, batches[0].last_seq);
    writer.Stop();
}

TEST(OrderedOpLogWriterLoopTest, ContinuesFromInitialDurablePrefix) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{
            .max_entries_per_batch = 4,
            .initial_durable_prefix = {.batch_id = 7, .last_seq = 42}},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    auto pending = writer.Commit(std::move(*reservation), MakeEntry("k1"),
                                 [](const auto&) {});
    ASSERT_TRUE(pending.has_value());
    EXPECT_EQ(43u, pending->sequence_id());

    ASSERT_TRUE(storage.WaitForWrites(1));
    auto batches = storage.Batches();
    ASSERT_EQ(1u, batches.size());
    EXPECT_EQ(8u, batches[0].batch_id);
    EXPECT_EQ(43u, batches[0].first_seq);
    EXPECT_EQ(43u, batches[0].last_seq);
    writer.Stop();
}

TEST(OrderedOpLogWriterLoopTest, CommitWhileReadyBatchExistsFormsNextBatch) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 4},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    auto first = writer.Reserve();
    auto second = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*first), MakeEntry("k1"), [](const auto&) {})
            .has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*second), MakeEntry("k2"), [](const auto&) {})
            .has_value());
    writer.Start();

    ASSERT_TRUE(storage.WaitForWrites(2));
    auto batches = storage.Batches();
    ASSERT_EQ(2u, batches.size());
    ASSERT_EQ(1u, batches[0].entries.size());
    EXPECT_EQ(1u, batches[0].first_seq);
    EXPECT_EQ(1u, batches[0].last_seq);
    ASSERT_EQ(1u, batches[1].entries.size());
    EXPECT_EQ(2u, batches[1].first_seq);
    EXPECT_EQ(2u, batches[1].last_seq);
    writer.Stop();
}

TEST(OrderedOpLogWriterLoopTest, CommitsDuringInflightWriteFormNextBatch) {
    FakeBatchWriter storage;
    storage.BlockWrites();
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 4},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    auto first = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*first), MakeEntry("k1"), [](const auto&) {})
            .has_value());
    ASSERT_TRUE(storage.WaitForBlockedWrite());

    auto second = writer.Reserve();
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*second), MakeEntry("k2"), [](const auto&) {})
            .has_value());

    storage.UnblockWrites();
    ASSERT_TRUE(storage.WaitForWrites(2));
    auto batches = storage.Batches();
    ASSERT_EQ(2u, batches.size());
    EXPECT_EQ(1u, batches[0].entries.size());
    EXPECT_EQ(1u, batches[1].entries.size());
    EXPECT_EQ(1u, batches[0].first_seq);
    EXPECT_EQ(2u, batches[1].first_seq);
    writer.Stop();
}

TEST(OrderedOpLogWriterLoopTest,
     DoesNotWaitForUncommittedReservationsBeforeDraining) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 4},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    auto uncommitted = writer.Reserve();
    auto committed = writer.Reserve();
    ASSERT_TRUE(uncommitted.has_value());
    ASSERT_TRUE(committed.has_value());
    ASSERT_TRUE(
        writer
            .Commit(std::move(*committed), MakeEntry("k1"), [](const auto&) {})
            .has_value());

    ASSERT_TRUE(storage.WaitForWrites(1));
    auto batches = storage.Batches();
    ASSERT_EQ(1u, batches.size());
    EXPECT_EQ(1u, batches[0].entries.size());
    writer.Abort(std::move(*uncommitted));
    writer.Stop();
}

TEST(OrderedOpLogWriterFailureTest, FirstStorageFailureStopsNewReservations) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    storage.FailNextWrites(100000, ErrorCode::PERSISTENT_FAIL);
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(writer
                    .Commit(std::move(*reservation), MakeEntry("k1"),
                            [](const auto&) {})
                    .has_value());

    for (int i = 0; i < 100 && writer.IsAccepting(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    EXPECT_FALSE(writer.IsAccepting());
    EXPECT_EQ(ErrorCode::PERSISTENT_FAIL, writer.LastError());
    EXPECT_FALSE(writer.Reserve().has_value());
    writer.Stop();
}

TEST(OrderedOpLogWriterFailureTest, NonRetryableFailureLatchesTerminalState) {
    FakeBatchWriter storage;
    std::mutex mutex;
    std::condition_variable cv;
    int terminal_callbacks = 0;
    std::optional<OrderedOpLogWriterTerminalState> callback_state;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        },
        [&](const OrderedOpLogWriterTerminalState& state) {
            std::lock_guard<std::mutex> lock(mutex);
            ++terminal_callbacks;
            callback_state = state;
            cv.notify_all();
        });
    writer.Start();

    auto held = writer.Reserve();
    auto failing = writer.Reserve();
    ASSERT_TRUE(held.has_value());
    ASSERT_TRUE(failing.has_value());
    storage.FailNextWrite(ErrorCode::INVALID_PARAMS);
    ASSERT_TRUE(
        writer
            .Commit(std::move(*failing), MakeEntry("fail"), [](const auto&) {})
            .has_value());

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(1),
                                [&] { return terminal_callbacks == 1; }));
    }
    const auto state = writer.GetTerminalState();
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, state->error);
    EXPECT_EQ(OrderedOpLogWriterTerminalReason::kNonRetryableWriteError,
              state->reason);
    EXPECT_EQ(0u, state->durable_prefix.batch_id);
    EXPECT_EQ(0u, state->durable_prefix.last_seq);
    EXPECT_NE(0u, state->occurred_at_ms);
    ASSERT_TRUE(callback_state.has_value());
    EXPECT_EQ(state->error, callback_state->error);
    EXPECT_FALSE(writer.IsAccepting());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, writer.LastError());
    auto reserve_after_terminal = writer.Reserve();
    ASSERT_FALSE(reserve_after_terminal.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, reserve_after_terminal.error());

    auto committed =
        writer.Commit(std::move(*held), MakeEntry(), [](const auto&) {});
    ASSERT_FALSE(committed.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, committed.error());
    auto reused =
        writer.Commit(std::move(*held), MakeEntry(), [](const auto&) {});
    ASSERT_FALSE(reused.has_value());
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, reused.error());
    writer.Stop();
    EXPECT_EQ(ErrorCode::INVALID_PARAMS, writer.LastError());
    const auto state_after_stop = writer.GetTerminalState();
    ASSERT_TRUE(state_after_stop.has_value());
    EXPECT_EQ(state->error, state_after_stop->error);
    EXPECT_EQ(state->reason, state_after_stop->reason);
    EXPECT_EQ(state->durable_prefix.batch_id,
              state_after_stop->durable_prefix.batch_id);
    EXPECT_EQ(state->durable_prefix.last_seq,
              state_after_stop->durable_prefix.last_seq);
    EXPECT_EQ(state->occurred_at_ms, state_after_stop->occurred_at_ms);
    EXPECT_EQ(1, terminal_callbacks);
}

TEST(OrderedOpLogWriterFailureTest, TransactionConflictBecomesFencedTerminal) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 1},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    storage.FailNextWrite(ErrorCode::ETCD_TRANSACTION_FAIL);
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*reservation), MakeEntry(), [](const auto&) {})
            .has_value());
    ASSERT_TRUE(storage.WaitForAttempts(1));
    for (int i = 0; i < 100 && !writer.GetTerminalState().has_value(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    const auto state = writer.GetTerminalState();
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(ErrorCode::ETCD_TRANSACTION_FAIL, state->error);
    EXPECT_EQ(OrderedOpLogWriterTerminalReason::kFenced, state->reason);
    EXPECT_EQ(1u, storage.AttemptTimes().size());
    writer.Stop();
}

TEST(OrderedOpLogWriterFailureTest,
     DoesNotInvokeCallbacksWhileBatchIsUndurable) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    storage.FailNextWrites(100000, ErrorCode::PERSISTENT_FAIL);
    std::atomic<int> callbacks{0};
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(writer
                    .Commit(std::move(*reservation), MakeEntry("k1"),
                            [&](const auto&) { ++callbacks; })
                    .has_value());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    EXPECT_EQ(0, callbacks.load());
    writer.Stop();
}

TEST(OrderedOpLogWriterFailureTest, RetryTimeoutStopsFurtherAttempts) {
    using namespace std::chrono_literals;

    FakeBatchWriter storage;
    std::atomic<int> terminal_callbacks{0};
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 1,
                                 .retry_timeout = 50ms},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        },
        [&](const OrderedOpLogWriterTerminalState&) { ++terminal_callbacks; });
    writer.Start();

    storage.FailNextWrites(100000, ErrorCode::PERSISTENT_FAIL);
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(writer
                    .Commit(std::move(*reservation), MakeEntry("k1"),
                            [](const auto&) {})
                    .has_value());

    for (int i = 0; i < 200 && !writer.GetTerminalState().has_value(); ++i) {
        std::this_thread::sleep_for(10ms);
    }
    const auto state = writer.GetTerminalState();
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(ErrorCode::PERSISTENT_FAIL, state->error);
    EXPECT_EQ(OrderedOpLogWriterTerminalReason::kRetryTimeout, state->reason);
    EXPECT_NE(0u, state->occurred_at_ms);
    EXPECT_EQ(ErrorCode::PERSISTENT_FAIL, writer.LastError());
    EXPECT_EQ(0u,
              HAMetricManager::instance().get_writer_runtime().retry_delay_ms);
    for (int i = 0; i < 100 && terminal_callbacks.load() != 1; ++i) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_EQ(1, terminal_callbacks.load());

    const size_t attempts_after_terminal = storage.AttemptTimes().size();
    std::this_thread::sleep_for(100ms);
    EXPECT_EQ(attempts_after_terminal, storage.AttemptTimes().size());
    writer.Stop();
}

TEST(OrderedOpLogWriterFailureTest,
     SuccessBeforeRetryTimeoutDoesNotBecomeTerminal) {
    using namespace std::chrono_literals;

    FakeBatchWriter storage;
    std::atomic<int> terminal_callbacks{0};
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 1,
                                 .retry_timeout = 2s},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        },
        [&](const OrderedOpLogWriterTerminalState&) { ++terminal_callbacks; });
    writer.Start();

    storage.FailNextWrites(1, ErrorCode::PERSISTENT_FAIL);
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(writer
                    .Commit(std::move(*reservation), MakeEntry("k1"),
                            [](const auto&) {})
                    .has_value());

    ASSERT_TRUE(storage.WaitForWrites(1));
    for (int i = 0; i < 100 && !writer.IsAccepting(); ++i) {
        std::this_thread::sleep_for(10ms);
    }
    EXPECT_FALSE(writer.GetTerminalState().has_value());
    EXPECT_EQ(0, terminal_callbacks.load());
    EXPECT_TRUE(writer.IsAccepting());
    EXPECT_EQ(ErrorCode::OK, writer.LastError());
    writer.Stop();
}

TEST(OrderedOpLogWriterFailureTest, RetriesSameBatchUntilSuccess) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    storage.FailNextWrites(2, ErrorCode::PERSISTENT_FAIL);
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(writer
                    .Commit(std::move(*reservation), MakeEntry("k1"),
                            [](const auto&) {})
                    .has_value());

    ASSERT_TRUE(storage.WaitForWrites(1));
    auto batches = storage.Batches();
    ASSERT_EQ(1u, batches.size());
    EXPECT_EQ(1u, batches[0].batch_id);
    EXPECT_EQ(1u, batches[0].first_seq);
    EXPECT_EQ(1u, batches[0].last_seq);
    writer.Stop();
}

TEST(OrderedOpLogWriterFailureTest, RetryBackoffGrowsAndCaps) {
    using namespace std::chrono_literals;

    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    storage.FailNextWrites(12, ErrorCode::PERSISTENT_FAIL);
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(writer
                    .Commit(std::move(*reservation), MakeEntry("k1"),
                            [](const auto&) {})
                    .has_value());

    ASSERT_TRUE(storage.WaitForAttempts(13));
    const auto attempts = storage.AttemptTimes();
    ASSERT_GE(attempts.size(), 13u);
    EXPECT_GE(attempts[9] - attempts[0], 400ms);
    EXPECT_GE(attempts[11] - attempts[10], 800ms);
    EXPECT_LT(attempts[12] - attempts[11], 1500ms);
    writer.Stop();
}

TEST(OrderedOpLogWriterFailureTest, RetryBackoffResetsAfterSuccess) {
    using namespace std::chrono_literals;

    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    auto first = writer.Reserve();
    auto second = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());

    storage.FailNextWrites(10, ErrorCode::PERSISTENT_FAIL);
    ASSERT_TRUE(
        writer.Commit(std::move(*first), MakeEntry("k1"), [](const auto&) {})
            .has_value());
    ASSERT_TRUE(storage.WaitForWrites(1, 5s));
    const auto first_batch_attempts = storage.AttemptTimes();
    ASSERT_GE(first_batch_attempts.size(), 11u);
    EXPECT_GE(first_batch_attempts[9] - first_batch_attempts[0], 400ms);

    storage.FailNextWrites(1, ErrorCode::PERSISTENT_FAIL);
    ASSERT_TRUE(
        writer.Commit(std::move(*second), MakeEntry("k2"), [](const auto&) {})
            .has_value());
    ASSERT_TRUE(storage.WaitForAttempts(13));

    const auto attempts = storage.AttemptTimes();
    ASSERT_GE(attempts.size(), 13u);
    EXPECT_LT(attempts[12] - attempts[11], 250ms);
    writer.Stop();
}

TEST(OrderedOpLogWriterFailureTest, StopInterruptsRetryBackoff) {
    using namespace std::chrono_literals;

    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 1},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    storage.FailNextWrites(100000, ErrorCode::PERSISTENT_FAIL);
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(writer
                    .Commit(std::move(*reservation), MakeEntry("k1"),
                            [](const auto&) {})
                    .has_value());
    ASSERT_TRUE(storage.WaitForAttempts(11));
    const auto attempts = storage.AttemptTimes();
    ASSERT_GE(attempts.size(), 11u);
    EXPECT_GE(attempts[10] - attempts[0], 800ms);

    const auto started_at = FakeBatchWriter::Clock::now();
    writer.Stop();
    EXPECT_LT(FakeBatchWriter::Clock::now() - started_at, 250ms);
    EXPECT_FALSE(writer.GetTerminalState().has_value());
    EXPECT_EQ(0u,
              HAMetricManager::instance().get_writer_runtime().retry_delay_ms);
}

TEST(OrderedOpLogWriterFailureTest, SuccessAfterRetryRestoresAccepting) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    storage.FailNextWrites(1, ErrorCode::PERSISTENT_FAIL);
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(writer
                    .Commit(std::move(*reservation), MakeEntry("k1"),
                            [](const auto&) {})
                    .has_value());

    ASSERT_TRUE(storage.WaitForWrites(1));
    EXPECT_TRUE(writer.IsAccepting());
    EXPECT_TRUE(writer.Reserve().has_value());
    writer.Stop();
}

TEST(OrderedOpLogWriterFailureTest, LaterBatchDoesNotOvertakeStuckBatch) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 2},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    auto first = writer.Reserve();
    auto second = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());

    storage.FailNextWrites(100000, ErrorCode::PERSISTENT_FAIL);
    ASSERT_TRUE(
        writer.Commit(std::move(*first), MakeEntry("k1"), [](const auto&) {})
            .has_value());
    for (int i = 0; i < 100 && writer.IsAccepting(); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_FALSE(writer.IsAccepting());

    ASSERT_TRUE(
        writer.Commit(std::move(*second), MakeEntry("k2"), [](const auto&) {})
            .has_value());
    storage.AllowWrites();

    ASSERT_TRUE(storage.WaitForWrites(2));
    auto batches = storage.Batches();
    ASSERT_EQ(2u, batches.size());
    EXPECT_EQ(1u, batches[0].first_seq);
    EXPECT_EQ(1u, batches[0].last_seq);
    EXPECT_EQ(2u, batches[1].first_seq);
    EXPECT_EQ(2u, batches[1].last_seq);
    writer.Stop();
}

TEST(OrderedOpLogWriterCallbackTest, DispatchesCallbacksInBatchSequenceOrder) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 4},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    std::mutex mutex;
    std::condition_variable cv;
    std::vector<uint64_t> callback_sequences;
    auto callback = [&](const OpLogEntry& entry) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            callback_sequences.push_back(entry.sequence_id);
        }
        cv.notify_all();
    };

    auto first = writer.Reserve();
    auto second = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(writer.Commit(std::move(*first), MakeEntry("k1"), callback)
                    .has_value());
    ASSERT_TRUE(writer.Commit(std::move(*second), MakeEntry("k2"), callback)
                    .has_value());
    writer.Start();

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(1), [&] {
            return callback_sequences.size() == 2;
        }));
        EXPECT_EQ((std::vector<uint64_t>{1, 2}), callback_sequences);
    }
    writer.Stop();
}

TEST(OrderedOpLogWriterCallbackTest,
     DispatchesCallbacksAcrossBatchesInGlobalOrder) {
    FakeBatchWriter storage;
    storage.BlockWrites();
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 4},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    std::mutex mutex;
    std::condition_variable cv;
    std::vector<uint64_t> callback_sequences;
    auto callback = [&](const OpLogEntry& entry) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            callback_sequences.push_back(entry.sequence_id);
        }
        cv.notify_all();
    };
    writer.Start();

    auto first = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(writer.Commit(std::move(*first), MakeEntry("k1"), callback)
                    .has_value());
    ASSERT_TRUE(storage.WaitForBlockedWrite());

    auto second = writer.Reserve();
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(writer.Commit(std::move(*second), MakeEntry("k2"), callback)
                    .has_value());
    storage.UnblockWrites();

    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(cv.wait_for(lock, std::chrono::seconds(1), [&] {
            return callback_sequences.size() == 2;
        }));
        EXPECT_EQ((std::vector<uint64_t>{1, 2}), callback_sequences);
    }
    writer.Stop();
}

TEST(OrderedOpLogWriterCallbackTest,
     StopDrainsCallbacksFromInflightSuccessfulWrite) {
    FakeBatchWriter storage;
    storage.BlockWrites();
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 4},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });
    writer.Start();

    std::atomic<int> callbacks{0};
    auto reservation = writer.Reserve();
    ASSERT_TRUE(reservation.has_value());
    ASSERT_TRUE(writer
                    .Commit(std::move(*reservation), MakeEntry("k1"),
                            [&](const auto&) { ++callbacks; })
                    .has_value());
    ASSERT_TRUE(storage.WaitForBlockedWrite());

    std::thread stopper([&] { writer.Stop(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    storage.UnblockWrites();
    stopper.join();

    EXPECT_EQ(1, callbacks.load());
}

TEST(OrderedOpLogWriterCallbackTest, SlowCallbackDoesNotPreventNextBatchWrite) {
    FakeBatchWriter storage;
    OrderedOpLogWriter writer(
        OrderedOpLogWriterConfig{.max_entries_per_batch = 4},
        [&](const OpLogBatchRecord& batch,
            const DurablePrefix& expected_prefix) {
            return storage.Write(batch, expected_prefix);
        });

    std::mutex mutex;
    std::condition_variable callback_started_cv;
    std::condition_variable release_callback_cv;
    bool callback_started = false;
    bool release_callback = false;
    writer.Start();

    auto first = writer.Reserve();
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(writer
                    .Commit(std::move(*first), MakeEntry("k1"),
                            [&](const auto&) {
                                std::unique_lock<std::mutex> lock(mutex);
                                callback_started = true;
                                callback_started_cv.notify_all();
                                release_callback_cv.wait(
                                    lock, [&] { return release_callback; });
                            })
                    .has_value());
    {
        std::unique_lock<std::mutex> lock(mutex);
        ASSERT_TRUE(callback_started_cv.wait_for(
            lock, std::chrono::seconds(1), [&] { return callback_started; }));
    }

    auto second = writer.Reserve();
    ASSERT_TRUE(second.has_value());
    ASSERT_TRUE(
        writer.Commit(std::move(*second), MakeEntry("k2"), [](const auto&) {})
            .has_value());

    EXPECT_TRUE(storage.WaitForWrites(2, std::chrono::milliseconds(100)));
    {
        std::lock_guard<std::mutex> lock(mutex);
        release_callback = true;
    }
    release_callback_cv.notify_all();
    writer.Stop();
}

}  // namespace mooncake::test
