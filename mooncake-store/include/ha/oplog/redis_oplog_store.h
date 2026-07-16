#pragma once

#ifdef STORE_USE_REDIS

#include <string>
#include <vector>

#include <hiredis/hiredis.h>
#include <mutex>

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
    redisContext* CreateConnection() const;
    ErrorCode EnsureConnectedUnlocked();
    ErrorCode ReadOpLogUnlocked(uint64_t sequence_id, OpLogEntry& entry);
    bool IsValidSnapshotId(const std::string& snapshot_id) const;

    std::string EntryKey(uint64_t sequence_id) const;
    std::string SnapshotKey(const std::string& snapshot_id) const;

    std::string cluster_id_;
    std::string redis_endpoint_;
    std::string username_;
    std::string password_;
    int db_index_;
    bool enable_write_;
    int poll_interval_ms_;
    std::string key_tag_;
    std::string index_key_;
    std::string latest_key_;
    std::string snapshot_prefix_;
    redisContext* ctx_{nullptr};
    mutable std::mutex mutex_;
};

}  // namespace mooncake

#endif  // STORE_USE_REDIS
