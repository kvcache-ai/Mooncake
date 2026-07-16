#pragma once

#ifdef STORE_USE_REDIS

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include <hiredis/hiredis.h>

#include "ha/oplog/oplog_store.h"
#include "redis_util.h"

namespace mooncake {

class RedisOpLogStore : public OpLogStore {
   public:
    RedisOpLogStore(const std::string& cluster_id,
                    const std::string& redis_endpoint, bool enable_write,
                    int poll_interval_ms = 1000,
                    const std::string& password = "",
                    const std::string& username = "", int db_index = 0);
    ~RedisOpLogStore() override;

    RedisOpLogStore(const RedisOpLogStore&) = delete;
    RedisOpLogStore& operator=(const RedisOpLogStore&) = delete;

    ErrorCode Init() override;
    ErrorCode WriteOpLog(const OpLogEntry& entry, bool sync = true) override;
    ErrorCode ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) override;
    ErrorCode ReadOpLogSince(uint64_t start_sequence_id, size_t limit,
                             std::vector<OpLogEntry>& entries) override;
    ErrorCode GetLatestSequenceId(uint64_t& sequence_id) override;
    ErrorCode GetMaxSequenceId(uint64_t& sequence_id) override;
    ErrorCode UpdateLatestSequenceId(uint64_t sequence_id) override;
    ErrorCode RecordSnapshotSequenceId(const std::string& snapshot_id,
                                       uint64_t sequence_id) override;
    ErrorCode GetSnapshotSequenceId(const std::string& snapshot_id,
                                    uint64_t& sequence_id) override;
    ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id) override;
    std::unique_ptr<OpLogChangeNotifier> CreateChangeNotifier(
        const std::string& cluster_id) override;

   private:
    struct PendingWrite {
        explicit PendingWrite(OpLogEntry entry, bool sync)
            : entry(std::move(entry)), sync(sync) {}

        OpLogEntry entry;
        bool sync{true};
        bool done{false};
        ErrorCode result{ErrorCode::OK};
        size_t attempts{0};
    };

    redisContext* CreateConnection() const;
    ErrorCode EnsureConnectedUnlocked();
    ErrorCode ReadOpLogUnlocked(uint64_t sequence_id, OpLogEntry& entry);
    bool IsValidSnapshotId(const std::string& snapshot_id) const;
    ErrorCode EnqueueWrite(const OpLogEntry& entry, bool sync);
    void StartAsyncWorkers();
    void StopAsyncWorkers();
    void AsyncWriteLoop(size_t worker_id);
    ErrorCode PersistEntryNoLatest(redisContext* ctx, const OpLogEntry& entry);
    ErrorCode AdvanceCommittedLatestUnlocked(redisContext* ctx);
    void CompleteWrite(redisContext* ctx,
                       const std::shared_ptr<PendingWrite>& pending,
                       ErrorCode result);

    std::string EntryKey(uint64_t sequence_id) const;
    std::string SnapshotKey(const std::string& snapshot_id) const;

    std::string cluster_id_;
    std::string redis_endpoint_;
    std::string username_;
    std::string password_;
    int db_index_;
    bool enable_write_;
    int poll_interval_ms_;

    // Redis oplog key model:
    // - latest: committed contiguous sequence watermark, safe for standby
    // replay
    // - trimmed: highest sequence already removed by cleanup
    // - entry:<seq>: serialized oplog payload
    std::string key_tag_;
    std::string latest_key_;
    std::string trimmed_key_;
    std::string snapshot_prefix_;
    redisContext* ctx_{nullptr};
    mutable std::mutex mutex_;

    mutable std::mutex async_mutex_;
    std::condition_variable async_cv_;
    std::condition_variable sync_cv_;
    std::deque<std::shared_ptr<PendingWrite>> pending_writes_;
    std::map<uint64_t, std::shared_ptr<PendingWrite>> inflight_writes_;
    std::set<uint64_t> persisted_sequences_;
    uint64_t committed_sequence_id_{0};
    std::atomic<bool> async_running_{false};
    std::vector<std::thread> async_workers_;

    static constexpr size_t kAsyncWorkerCount = 4;
    static constexpr size_t kMaxAsyncQueueSize = 100000;
    static constexpr size_t kCleanupBatchSize = 1000;
    static constexpr int kAsyncRetryDelayMs = 100;
    static constexpr int kSyncWaitTimeoutMs = 30000;
};

}  // namespace mooncake

#endif  // STORE_USE_REDIS
