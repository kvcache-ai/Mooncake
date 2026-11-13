#pragma once

#ifdef STORE_USE_REDIS

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include <hiredis/hiredis.h>

#include "p2p/ha/oplog/oplog_store.h"
#include "p2p/ha/oplog/p2p_oplog_types.h"
#include "p2p/ha/oplog/redis_oplog_limits.h"
#include "mutex.h"
#include "p2p/ha/redis_util.h"

namespace mooncake {

enum class OpLogAsyncQueueOverflowMode { REJECT, BYPASS };

class RedisOpLogStore : public OpLogStore {
   public:
    RedisOpLogStore(
        const std::string& cluster_id, const std::string& redis_endpoint,
        bool enable_write, int poll_interval_ms = 1000,
        const std::string& password = "", const std::string& username = "",
        int db_index = 0, size_t async_queue_max_entries = 100000,
        OpLogAsyncQueueOverflowMode async_queue_overflow_mode =
            OpLogAsyncQueueOverflowMode::REJECT,
        size_t best_effort_max_retries = 3,
        size_t async_write_batch_size = kDefaultRedisOpLogWriteBatchSize);
    ~RedisOpLogStore() override;

    RedisOpLogStore(const RedisOpLogStore&) = delete;
    RedisOpLogStore& operator=(const RedisOpLogStore&) = delete;

    ErrorCode Init() override;
    ErrorCode WriteOpLog(const OpLogEntry& entry, bool sync = true) override;
    // Signal cancellation without waiting for the queue lock or Redis I/O.
    // Commands already in progress may finish.
    void RequestStopWriting() override;
    // Once this returns, all workers have exited and no async writes remain.
    void StopWriting() override;
    OpLogDrainResult DrainWriting(uint32_t timeout_ms) override;
    ErrorCode ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) override;
    ErrorCode ReadOpLogSince(uint64_t start_sequence_id, size_t limit,
                             std::vector<OpLogEntry>& entries) override;
    ErrorCode ReadOpLogSinceWithProgress(uint64_t start_sequence_id,
                                         size_t limit,
                                         std::vector<OpLogEntry>& entries,
                                         OpLogReadProgress& progress) override;
    ErrorCode GetLatestSequenceId(uint64_t& sequence_id) override;
    ErrorCode GetLocalCommittedSequenceId(uint64_t& sequence_id) override;
    ErrorCode GetMaxSequenceId(uint64_t& sequence_id) override;
    ErrorCode GetTrimmedSequenceId(uint64_t& sequence_id) override;
    ErrorCode UpdateLatestSequenceId(uint64_t sequence_id) override;
    ErrorCode SetLocalCommittedSequenceId(uint64_t sequence_id) override;
    ErrorCode RecordSnapshotSequenceId(const std::string& snapshot_id,
                                       uint64_t sequence_id) override;
    ErrorCode GetSnapshotSequenceId(const std::string& snapshot_id,
                                    uint64_t& sequence_id) override;
    ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id) override;
    std::unique_ptr<OpLogChangeNotifier> CreateChangeNotifier(
        const std::string& cluster_id, size_t poll_batch_size = 1000) override;

   private:
    struct PendingWrite {
        explicit PendingWrite(OpLogEntry entry, bool sync)
            : entry(std::move(entry)), sync(sync) {}

        OpLogEntry entry;
        bool sync{true};
        bool done{false};
        bool write_failed{false};  // Protected by async_mutex_.
        ErrorCode result{ErrorCode::OK};
        size_t attempts{0};
    };

    struct WriteResult {
        ErrorCode error{ErrorCode::INTERNAL_ERROR};
        int64_t latency_us{0};
    };

    redisContext* CreateConnection() const;
    ErrorCode EnsureConnectedUnlocked();
    ErrorCode ReadOpLogUnlocked(uint64_t sequence_id, OpLogEntry& entry);
    bool IsValidSnapshotId(const std::string& snapshot_id) const;
    ErrorCode EnqueueWrite(const OpLogEntry& entry, bool sync);
    void StartAsyncWorkers();
    bool WaitForPendingWrites(
        std::vector<std::shared_ptr<PendingWrite>>& batch);
    void AsyncWriteLoop(size_t worker_id);
    bool FinishOrRetryWrite(redisContext*& ctx,
                            const std::shared_ptr<PendingWrite>& pending,
                            ErrorCode err, size_t worker_id,
                            int64_t write_latency_us);
    std::vector<WriteResult> PersistEntriesNoLatest(
        redisContext* ctx,
        const std::vector<std::shared_ptr<PendingWrite>>& batch);
    void SetWriteFailedUnlocked(PendingWrite& pending, bool failed);
    uint64_t MaxKnownSequenceUnlocked() const;
    bool HasCommittableLatestUnlocked() const;
    ErrorCode AdvanceCommittedLatestUnlocked(redisContext* ctx);
    void CompleteWrite(const std::shared_ptr<PendingWrite>& pending,
                       ErrorCode result);
    void UpdateWriteProgressUnlocked();

    std::string EntryKey(uint64_t sequence_id) const;
    std::string SnapshotKey(const std::string& snapshot_id) const;

    std::string cluster_id_;
    std::string redis_endpoint_;
    std::string username_;
    std::string password_;
    int db_index_;
    bool enable_write_;
    int poll_interval_ms_;

    // P2P metadata is memory-first and is not transactionally coupled with its
    // OpLog. Redis persistence runs on background workers. The queue is
    // strictly bounded: overflow follows the configured reject/bypass
    // degradation policy. Once admitted, required entries retry indefinitely,
    // while best-effort entries use bounded retries.
    //
    // Redis oplog key model:
    // - latest: committed progress watermark; dropped entries may bridge holes
    // - trimmed: highest sequence already removed by cleanup
    // - entry:<seq>: serialized oplog payload
    // OpLog keys intentionally avoid Redis Cluster hash tags so
    // high-cardinality entry keys can spread across slots.
    std::string key_prefix_;
    std::string latest_key_;
    std::string trimmed_key_;
    std::string snapshot_prefix_;
    mutable std::mutex mutex_;
    redisContext* ctx_ GUARDED_BY(mutex_) = nullptr;

    mutable std::mutex async_mutex_;
    std::condition_variable_any async_cv_;
    std::condition_variable_any sync_cv_;
    std::stop_source async_stop_source_;
    std::deque<std::shared_ptr<PendingWrite>> pending_writes_
        GUARDED_BY(async_mutex_);
    std::map<uint64_t, std::shared_ptr<PendingWrite>> inflight_writes_
        GUARDED_BY(async_mutex_);
    std::set<uint64_t> persisted_sequences_ GUARDED_BY(async_mutex_);
    std::set<uint64_t> dropped_sequences_ GUARDED_BY(async_mutex_);
    uint64_t committed_sequence_id_ GUARDED_BY(async_mutex_) = 0;
    size_t failed_write_count_ GUARDED_BY(async_mutex_) = 0;
    // Per-writer sampling, serialized by the existing queue lock.
    std::chrono::steady_clock::time_point next_queue_warning_at_
        GUARDED_BY(async_mutex_){};
    uint64_t queue_overflows_since_last_log_ GUARDED_BY(async_mutex_) = 0;
    std::atomic<bool> async_running_{false};
    std::atomic<int64_t> consecutive_write_failures_{0};
    std::vector<std::thread> async_workers_;
    std::mutex async_stop_mutex_;
    size_t async_queue_max_entries_;
    OpLogAsyncQueueOverflowMode async_queue_overflow_mode_;
    size_t best_effort_max_retries_;
    size_t async_write_batch_size_;

    static constexpr size_t kAsyncWorkerCount = 4;
    static constexpr size_t kCleanupBatchSize = 1000;
    static constexpr int kAsyncRetryDelayMs = 100;
    static constexpr int kSyncWaitTimeoutMs = 30000;
};

}  // namespace mooncake

#endif  // STORE_USE_REDIS
