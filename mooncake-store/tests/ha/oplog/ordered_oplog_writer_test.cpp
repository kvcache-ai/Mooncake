#include "ha/oplog/ordered_oplog_writer.h"

#include <gtest/gtest.h>
#include <xxhash.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace mooncake::test {
namespace {

class FakeBatchWriter {
   public:
    ErrorCode Write(const OpLogBatchRecord& batch,
                    const DurablePrefix& expected_prefix) {
        std::unique_lock<std::mutex> lock(mutex_);
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

    bool WaitForWrites(size_t count, std::chrono::milliseconds timeout =
                                         std::chrono::milliseconds(1000)) {
        std::unique_lock<std::mutex> lock(mutex_);
        return cv_.wait_for(lock, timeout,
                            [&] { return writes_.size() >= count; });
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
    std::condition_variable blocked_cv_;
    std::vector<WriteRecord> writes_;
    ErrorCode next_error_{ErrorCode::OK};
    ErrorCode repeated_error_{ErrorCode::OK};
    size_t failures_remaining_{0};
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

}  // namespace

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

TEST(OrderedOpLogWriterLoopTest,
     GroupsEntriesCommittedBeforeDrainIntoOneBatch) {
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

    ASSERT_TRUE(storage.WaitForWrites(1));
    auto batches = storage.Batches();
    ASSERT_EQ(1u, batches.size());
    ASSERT_EQ(2u, batches[0].entries.size());
    EXPECT_EQ(1u, batches[0].first_seq);
    EXPECT_EQ(2u, batches[0].last_seq);
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
