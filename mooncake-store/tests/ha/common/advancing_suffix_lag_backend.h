#pragma once

#include <algorithm>
#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include "ha/kv/ha_kv_backend.h"
#include "ha/oplog/oplog_batch_codec.h"

namespace mooncake::test {

// Simulates continuous writes and a replica that always hides the latest
// batches. Every prefix read publishes another complete batch, so serializable
// polls make progress without catching up. A linearizable Range sees the full
// suffix.
class AdvancingSuffixLagBackend final : public HaKvBackend {
   public:
    AdvancingSuffixLagBackend(std::string cluster_id,
                              std::shared_ptr<HaKvBackend> backend)
        : cluster_id_(std::move(cluster_id)), backend_(std::move(backend)) {}

    void StartLagging(
        uint64_t first_batch,
        std::chrono::milliseconds fail_after = std::chrono::seconds(5),
        uint64_t hidden_batches = 1) {
        std::lock_guard<std::mutex> lock(mutex_);
        hidden_batches_ = hidden_batches;
        StartLaggingLocked(first_batch, fail_after);
    }

    // Start after the provider's last page, to exercise the separate
    // post-restore catch-up loop in RebootstrapBatchOpLog.
    void LagAfterRangeContaining(std::string batch_key, uint64_t first_batch,
                                 uint64_t hidden_batches = 1) {
        std::lock_guard<std::mutex> lock(mutex_);
        trigger_batch_key_ = std::move(batch_key);
        trigger_first_batch_ = first_batch;
        hidden_batches_ = hidden_batches;
    }

    ErrorCode Get(std::string_view key, std::string& value) override {
        std::lock_guard<std::mutex> lock(mutex_);
        if (lagging_ && key == BuildDurablePrefixKey(cluster_id_)) {
            // Bound a broken synchronous restore/promotion instead of hanging
            // the test process forever.
            if (std::chrono::steady_clock::now() >= deadline_) {
                return ErrorCode::PERSISTENT_FAIL;
            }
            PublishLocked(++last_batch_);
        }
        return backend_->Get(key, value);
    }

    ErrorCode Put(std::string_view key, std::string_view value) override {
        return backend_->Put(key, value);
    }

    ErrorCode Range(std::string_view begin, std::string_view end, size_t limit,
                    std::vector<KvPair>& output) override {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto error = backend_->Range(begin, end, limit, output);
        if (error == ErrorCode::OK && IsBatchRange(end) &&
            std::any_of(output.begin(), output.end(), [&](const auto& pair) {
                return pair.key ==
                       BuildBatchRecordKey(cluster_id_, last_batch_);
            })) {
            lagging_ = false;
        }
        return error;
    }

    ErrorCode RangeSerializable(std::string_view begin, std::string_view end,
                                size_t limit,
                                std::vector<KvPair>& output) override {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto error =
            lagging_ ? backend_->Range(begin, end, limit, output)
                     : backend_->RangeSerializable(begin, end, limit, output);
        if (error != ErrorCode::OK || !IsBatchRange(end)) return error;
        if (lagging_) {
            const auto hidden_key = BuildBatchRecordKey(
                cluster_id_, last_batch_ >= hidden_batches_
                                 ? last_batch_ - hidden_batches_ + 1
                                 : 1);
            output.erase(std::remove_if(output.begin(), output.end(),
                                        [&](const auto& pair) {
                                            return pair.key >= hidden_key;
                                        }),
                         output.end());
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        } else if (!trigger_batch_key_.empty() &&
                   std::any_of(output.begin(), output.end(),
                               [&](const auto& pair) {
                                   return pair.key == trigger_batch_key_;
                               })) {
            trigger_batch_key_.clear();
            StartLaggingLocked(trigger_first_batch_, std::chrono::seconds(5));
        }
        return error;
    }

    ErrorCode DeleteRange(std::string_view begin,
                          std::string_view end) override {
        return backend_->DeleteRange(begin, end);
    }
    bool SupportsTxn() const override { return backend_->SupportsTxn(); }
    ErrorCode Txn(const KvTxn& txn) override { return backend_->Txn(txn); }

   private:
    bool IsBatchRange(std::string_view end) const {
        return end == BuildBatchRecordRange(cluster_id_, 0).end_key;
    }

    void StartLaggingLocked(uint64_t first_batch,
                            std::chrono::milliseconds fail_after) {
        last_batch_ = first_batch;
        PublishLocked(last_batch_);
        deadline_ = std::chrono::steady_clock::now() + fail_after;
        lagging_ = true;
    }

    void PublishLocked(uint64_t id) {
        OpLogEntry entry;
        entry.sequence_id = id;
        entry.op_type = OpType::REMOVE;
        entry.object_key = "absent";
        OpLogBatchRecord batch;
        batch.batch_id = batch.first_seq = batch.last_seq = id;
        batch.entries = {entry};
        KvTxn txn;
        txn.puts = {
            {.key = BuildBatchRecordKey(cluster_id_, id),
             .value = EncodeOpLogBatchRecord(batch)},
            {.key = BuildDurablePrefixKey(cluster_id_),
             .value = EncodeDurablePrefix({.batch_id = id, .last_seq = id})}};
        backend_->Txn(txn);
    }

    std::string cluster_id_;
    std::shared_ptr<HaKvBackend> backend_;
    std::mutex mutex_;
    bool lagging_{false};
    uint64_t last_batch_{0};
    uint64_t hidden_batches_{1};
    std::chrono::steady_clock::time_point deadline_;
    std::string trigger_batch_key_;
    uint64_t trigger_first_batch_{0};
};

}  // namespace mooncake::test
