#include "ha/oplog/batch_oplog_pruning_worker.h"

#include <gtest/gtest.h>

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
             it != values_.end() &&
             std::string_view(it->first) < end_key;) {
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
};

TEST(BatchOpLogPruningWorkerTest, DeletesOnlyBoundedChunksThroughPublishedFloor) {
    FakeHaKvBackend backend;
    const std::string cluster = "clusterA";
    ASSERT_EQ(ErrorCode::OK,
              backend.Put("/oplog/clusterA/snapshot/compaction_floor", "15"));
    for (uint64_t batch_id = 1; batch_id <= 15; ++batch_id) {
        ASSERT_EQ(ErrorCode::OK,
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
    const std::string floor_key =
        "/oplog/clusterA/snapshot/compaction_floor";
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
        if (errors == 0) std::this_thread::sleep_for(std::chrono::milliseconds(1));
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

TEST(BatchOpLogPruningWorkerTest,
     StartsAndRetriesWhenInitialFloorReadFails) {
    const std::string cluster = "clusterA";
    const std::string floor_key =
        "/oplog/clusterA/snapshot/compaction_floor";
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
