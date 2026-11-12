#include "ha/oplog/batch_oplog_pruning_worker.h"

#include <gtest/gtest.h>

#include <condition_variable>
#include <future>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "ha_metric_manager.h"

namespace mooncake::test {
namespace {

class FakeHaKvBackend : public HaKvBackend {
   public:
    ErrorCode Get(std::string_view key, std::string& value) override {
        std::lock_guard lock(mutex_);
        const auto calls = ++get_calls_[std::string(key)];
        if (key == fail_get_key_ && calls == fail_get_call_) {
            return fail_get_error_;
        }
        const auto it = values_.find(std::string(key));
        if (it == values_.end()) return ErrorCode::ETCD_KEY_NOT_EXIST;
        value = it->second;
        return ErrorCode::OK;
    }
    ErrorCode Put(std::string_view key, std::string_view value) override {
        std::lock_guard lock(mutex_);
        values_[std::string(key)] = std::string(value);
        return ErrorCode::OK;
    }
    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override {
        std::lock_guard lock(mutex_);
        ++range_calls_;
        kvs.clear();
        for (const auto& [key, value] : values_) {
            if (key < begin_key || key >= end_key) continue;
            kvs.push_back({.key = key, .value = value});
            if (limit != 0 && kvs.size() >= limit) break;
        }
        return ErrorCode::OK;
    }
    ErrorCode DeleteRange(std::string_view begin_key,
                          std::string_view end_key) override {
        std::lock_guard lock(mutex_);
        ++delete_calls_;
        if (delete_error_ != ErrorCode::OK) return delete_error_;
        for (auto it = values_.lower_bound(std::string(begin_key));
             it != values_.end() && std::string_view(it->first) < end_key;) {
            it = values_.erase(it);
        }
        return ErrorCode::OK;
    }
    bool SupportsTxn() const override { return true; }
    ErrorCode Txn(const KvTxn&) override { return ErrorCode::OK; }
    bool Has(std::string_view key) const {
        std::lock_guard lock(mutex_);
        return values_.contains(std::string(key));
    }
    size_t delete_calls() const {
        std::lock_guard lock(mutex_);
        return delete_calls_;
    }
    size_t range_calls() const {
        std::lock_guard lock(mutex_);
        return range_calls_;
    }
    size_t get_calls(std::string_view key) const {
        std::lock_guard lock(mutex_);
        const auto it = get_calls_.find(std::string(key));
        return it == get_calls_.end() ? 0 : it->second;
    }
    void FailDelete(ErrorCode error) {
        std::lock_guard lock(mutex_);
        delete_error_ = error;
    }
    void FailGetOnCall(std::string key, size_t call, ErrorCode error) {
        std::lock_guard lock(mutex_);
        fail_get_key_ = std::move(key);
        fail_get_call_ = call;
        fail_get_error_ = error;
    }

   private:
    mutable std::mutex mutex_;
    std::map<std::string, std::string> values_;
    std::map<std::string, size_t> get_calls_;
    std::string fail_get_key_;
    size_t fail_get_call_{0};
    ErrorCode fail_get_error_{ErrorCode::OK};
    ErrorCode delete_error_{ErrorCode::OK};
    size_t delete_calls_{0};
    size_t range_calls_{0};
};

class BlockingFloorReadHaKvBackend : public FakeHaKvBackend {
   public:
    void BlockFloorRead(std::string key) {
        std::lock_guard lock(mutex_);
        blocked_key_ = std::move(key);
        blocked_ = true;
        read_entered_ = false;
    }

    bool WaitForFloorRead(
        std::chrono::milliseconds timeout = std::chrono::milliseconds(1000)) {
        std::unique_lock lock(mutex_);
        return condition_.wait_for(lock, timeout,
                                   [&] { return read_entered_; });
    }

    void AllowFloorRead() {
        {
            std::lock_guard lock(mutex_);
            blocked_ = false;
        }
        condition_.notify_all();
    }

    ErrorCode Get(std::string_view key, std::string& value) override {
        if (key == blocked_key_) {
            std::unique_lock lock(mutex_);
            read_entered_ = true;
            condition_.notify_all();
            condition_.wait(lock, [&] { return !blocked_; });
        }
        return FakeHaKvBackend::Get(key, value);
    }

   private:
    std::mutex mutex_;
    std::condition_variable condition_;
    std::string blocked_key_;
    bool blocked_{false};
    bool read_entered_{false};
};

class BlockingRangeHaKvBackend : public FakeHaKvBackend {
   public:
    void BlockRange() {
        std::lock_guard lock(mutex_);
        blocked_ = true;
        range_entered_ = false;
    }

    bool WaitForRange(
        std::chrono::milliseconds timeout = std::chrono::milliseconds(1000)) {
        std::unique_lock lock(mutex_);
        return condition_.wait_for(lock, timeout,
                                   [&] { return range_entered_; });
    }

    void AllowRange() {
        {
            std::lock_guard lock(mutex_);
            blocked_ = false;
        }
        condition_.notify_all();
    }

    ErrorCode Range(std::string_view begin_key, std::string_view end_key,
                    size_t limit, std::vector<KvPair>& kvs) override {
        {
            std::unique_lock lock(mutex_);
            range_entered_ = true;
            condition_.notify_all();
            condition_.wait(lock, [&] { return !blocked_; });
        }
        return FakeHaKvBackend::Range(begin_key, end_key, limit, kvs);
    }

   private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool blocked_{false};
    bool range_entered_{false};
};

class BlockingDeleteRangeHaKvBackend : public FakeHaKvBackend {
   public:
    void BlockDelete() {
        std::lock_guard lock(mutex_);
        blocked_ = true;
        delete_entered_ = false;
    }

    bool WaitForDelete(
        std::chrono::milliseconds timeout = std::chrono::milliseconds(1000)) {
        std::unique_lock lock(mutex_);
        return condition_.wait_for(lock, timeout,
                                   [&] { return delete_entered_; });
    }

    void AllowDelete() {
        {
            std::lock_guard lock(mutex_);
            blocked_ = false;
        }
        condition_.notify_all();
    }

    ErrorCode DeleteRange(std::string_view begin_key,
                          std::string_view end_key) override {
        {
            std::unique_lock lock(mutex_);
            delete_entered_ = true;
            condition_.notify_all();
            condition_.wait(lock, [&] { return !blocked_; });
        }
        return FakeHaKvBackend::DeleteRange(begin_key, end_key);
    }

   private:
    std::mutex mutex_;
    std::condition_variable condition_;
    bool blocked_{false};
    bool delete_entered_{false};
};

TEST(BatchOpLogPruningWorkerTest,
     DeletesOnlyBoundedChunksThroughPublishedFloor) {
    FakeHaKvBackend backend;
    const std::string cluster = "clusterA";
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/snapshot/compaction_floor", "15"));
    for (uint64_t batch_id = 1; batch_id <= 15; ++batch_id) {
        ASSERT_EQ(
            ErrorCode::OK,
            backend.Put(BuildBatchRecordKey(cluster, batch_id), "record"));
    }

    OpLogBatchStorage storage(cluster, backend);
    BatchOpLogPruningWorker worker(storage, /*chunk_batch_count=*/10,
                                   /*max_batches_per_second=*/50);
    bool deleted_chunk = false;
    ASSERT_EQ(ErrorCode::OK, worker.RunOneChunk(deleted_chunk));
    ASSERT_TRUE(deleted_chunk);
    EXPECT_FALSE(backend.Has(BuildBatchRecordKey(cluster, 1)));
    EXPECT_FALSE(backend.Has(BuildBatchRecordKey(cluster, 10)));
    EXPECT_TRUE(backend.Has(BuildBatchRecordKey(cluster, 11)));
    EXPECT_TRUE(backend.Has(BuildBatchRecordKey(cluster, 15)));
    EXPECT_EQ(1u, backend.delete_calls());

    ASSERT_EQ(ErrorCode::OK, worker.RunOneChunk(deleted_chunk));
    ASSERT_TRUE(deleted_chunk);
    EXPECT_FALSE(backend.Has(BuildBatchRecordKey(cluster, 11)));
    EXPECT_FALSE(backend.Has(BuildBatchRecordKey(cluster, 15)));
    EXPECT_EQ(2u, backend.delete_calls());
}

TEST(BatchOpLogPruningWorkerTest,
     StopDuringFloorReadDoesNotStartCursorRecovery) {
    BlockingFloorReadHaKvBackend backend;
    const std::string cluster = "cluster-stop-during-floor-read";
    const std::string floor_key =
        "/oplog/cluster-stop-during-floor-read/snapshot/compaction_floor";
    ASSERT_EQ(ErrorCode::OK, backend.Put(floor_key, "15"));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey(cluster, 1), "record"));

    OpLogBatchStorage storage(cluster, backend);
    BatchOpLogPruningWorker worker(storage, /*chunk_batch_count=*/10,
                                   /*max_batches_per_second=*/50);
    backend.BlockFloorRead(floor_key);
    ASSERT_EQ(ErrorCode::OK, worker.Start());

    const bool read_started = backend.WaitForFloorRead();
    if (read_started) worker.RequestStop();
    backend.AllowFloorRead();
    ASSERT_TRUE(read_started);
    worker.Stop();

    EXPECT_EQ(0u, backend.range_calls());
    EXPECT_EQ(0u, backend.delete_calls());
}

TEST(BatchOpLogPruningWorkerTest, StopDuringCursorRecoveryDoesNotStartDelete) {
    BlockingRangeHaKvBackend backend;
    const std::string cluster = "cluster-stop-during-recovery";
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/cluster-stop-during-recovery/snapshot/"
                          "compaction_floor",
                          "15"));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey(cluster, 1), "record"));

    OpLogBatchStorage storage(cluster, backend);
    BatchOpLogPruningWorker worker(storage, /*chunk_batch_count=*/10,
                                   /*max_batches_per_second=*/50);
    backend.BlockRange();
    bool deleted_chunk = false;
    auto run = std::async(std::launch::async,
                          [&] { return worker.RunOneChunk(deleted_chunk); });

    const bool range_started = backend.WaitForRange();
    if (range_started) worker.RequestStop();
    backend.AllowRange();
    ASSERT_TRUE(range_started);

    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS, run.get());
    EXPECT_FALSE(deleted_chunk);
    EXPECT_EQ(0u, backend.delete_calls());
}

TEST(BatchOpLogPruningWorkerTest,
     StopDuringDeleteSkipsFollowUpFloorReadAfterSuccessfulDelete) {
    BlockingDeleteRangeHaKvBackend backend;
    const std::string cluster = "cluster-stop-during-delete";
    const std::string floor_key =
        "/oplog/cluster-stop-during-delete/snapshot/compaction_floor";
    ASSERT_EQ(ErrorCode::OK, backend.Put(floor_key, "1"));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey(cluster, 1), "record"));

    OpLogBatchStorage storage(cluster, backend);
    BatchOpLogPruningWorker worker(storage, /*chunk_batch_count=*/10,
                                   /*max_batches_per_second=*/50);
    auto& metrics = HAMetricManager::instance();
    metrics.reset_batch_oplog_pruner_runtime();
    backend.BlockDelete();
    bool deleted_chunk = false;
    auto run = std::async(std::launch::async,
                          [&] { return worker.RunOneChunk(deleted_chunk); });

    const bool delete_started = backend.WaitForDelete();
    if (delete_started) worker.RequestStop();
    backend.AllowDelete();
    ASSERT_TRUE(delete_started);

    EXPECT_EQ(ErrorCode::OK, run.get());
    EXPECT_TRUE(deleted_chunk);
    EXPECT_EQ(1u, backend.delete_calls());
    EXPECT_FALSE(backend.Has(BuildBatchRecordKey(cluster, 1)));
    EXPECT_EQ(1u, backend.get_calls(floor_key));
    const auto serialized = metrics.serialize_metrics();
    EXPECT_NE(
        std::string::npos,
        serialized.find("mooncake_ha_batch_oplog_pruner_cursor_batch_id 1\n"));
    EXPECT_NE(std::string::npos,
              serialized.find(
                  "mooncake_ha_batch_oplog_pruner_chunks_deleted_total 1\n"));
    EXPECT_NE(
        std::string::npos,
        serialized.find(
            "mooncake_ha_batch_oplog_pruner_batch_ids_advanced_total 1\n"));
}

TEST(BatchOpLogPruningWorkerTest, CountsDeleteRangeFailure) {
    const std::string cluster = "clusterA";
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/snapshot/compaction_floor", "1"));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey(cluster, 1), "record"));
    backend.FailDelete(ErrorCode::ETCD_OPERATION_ERROR);

    auto& metrics = HAMetricManager::instance();
    metrics.reset_batch_oplog_pruner_runtime();
    OpLogBatchStorage storage(cluster, backend);
    BatchOpLogPruningWorker worker(storage, 10, 50);
    bool deleted_chunk = false;

    EXPECT_EQ(ErrorCode::ETCD_OPERATION_ERROR,
              worker.RunOneChunk(deleted_chunk));
    EXPECT_FALSE(deleted_chunk);

    const auto serialized = metrics.serialize_metrics();
    const std::string metric_name =
        "mooncake_ha_batch_oplog_pruner_errors_total ";
    const auto position = serialized.find("\n" + metric_name);
    ASSERT_NE(std::string::npos, position);
    std::istringstream value(
        serialized.substr(position + 1 + metric_name.size()));
    uint64_t errors = 0;
    ASSERT_TRUE(value >> errors);
    EXPECT_EQ(1u, errors);
}

TEST(BatchOpLogPruningWorkerTest,
     CountsFloorRefreshFailureOnceAfterSuccessfulChunkDelete) {
    const std::string cluster = "clusterA";
    const std::string floor_key = "/oplog/clusterA/snapshot/compaction_floor";
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK, backend.Put(floor_key, "1"));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey(cluster, 1), "record"));
    backend.FailGetOnCall(floor_key, 2, ErrorCode::ETCD_OPERATION_ERROR);

    auto& metrics = HAMetricManager::instance();
    metrics.reset_batch_oplog_pruner_runtime();
    OpLogBatchStorage storage(cluster, backend);
    BatchOpLogPruningWorker worker(storage, 10, 50);
    ASSERT_EQ(ErrorCode::OK, worker.Start());

    const std::string metric_name =
        "mooncake_ha_batch_oplog_pruner_errors_total ";
    uint64_t errors = 0;
    for (int i = 0; i < 1000 && errors == 0; ++i) {
        const auto serialized = metrics.serialize_metrics();
        const auto position = serialized.find("\n" + metric_name);
        ASSERT_NE(std::string::npos, position);
        std::istringstream value(
            serialized.substr(position + 1 + metric_name.size()));
        ASSERT_TRUE(value >> errors);
        if (errors == 0)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    const auto serialized = metrics.serialize_metrics();
    const auto position = serialized.find("\n" + metric_name);
    ASSERT_NE(std::string::npos, position);
    std::istringstream value(
        serialized.substr(position + 1 + metric_name.size()));
    ASSERT_TRUE(value >> errors);
    worker.Stop();

    EXPECT_EQ(1u, errors);
}

TEST(BatchOpLogPruningWorkerTest, StartsAndRetriesWhenInitialFloorReadFails) {
    const std::string cluster = "clusterA";
    const std::string floor_key = "/oplog/clusterA/snapshot/compaction_floor";
    FakeHaKvBackend backend;
    ASSERT_EQ(ErrorCode::OK, backend.Put(floor_key, "1"));
    ASSERT_EQ(ErrorCode::OK,
              backend.Put(BuildBatchRecordKey(cluster, 1), "record"));
    backend.FailGetOnCall(floor_key, 1, ErrorCode::ETCD_OPERATION_ERROR);

    auto& metrics = HAMetricManager::instance();
    metrics.reset_batch_oplog_pruner_runtime();
    OpLogBatchStorage storage(cluster, backend);
    BatchOpLogPruningWorker worker(storage, 10, 50);
    ASSERT_EQ(ErrorCode::OK, worker.Start());

    for (int i = 0; i < 3000 && backend.delete_calls() == 0; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    worker.Stop();

    EXPECT_EQ(1u, backend.delete_calls());
    EXPECT_FALSE(backend.Has(BuildBatchRecordKey(cluster, 1)));
    const auto serialized = metrics.serialize_metrics();
    const std::string metric_name =
        "mooncake_ha_batch_oplog_pruner_errors_total ";
    const auto position = serialized.find("\n" + metric_name);
    ASSERT_NE(std::string::npos, position);
    std::istringstream value(
        serialized.substr(position + 1 + metric_name.size()));
    uint64_t errors = 0;
    ASSERT_TRUE(value >> errors);
    EXPECT_EQ(1u, errors);
}

}  // namespace
}  // namespace mooncake::test
