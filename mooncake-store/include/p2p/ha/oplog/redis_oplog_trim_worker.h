#pragma once

#ifdef STORE_USE_REDIS

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "p2p/ha/oplog/p2p_standby_snapshot_service.h"
#include "types.h"

struct redisContext;

namespace mooncake {

struct RedisOpLogTrimWorkerConfig {
    std::string cluster_id;
    std::string self_instance_id;
    std::string redis_endpoint;
    std::string redis_username;
    std::string redis_password;
    int redis_db_index{0};
    std::chrono::seconds registry_ttl{10};
    std::chrono::seconds interval{10};
    uint64_t retention_entries{100000};
    uint64_t delete_batch_size{10000};
    uint32_t min_snapshot_ready_standby_count{1};
};

uint64_t ComputeRedisOpLogTrimTarget(
    uint64_t latest_sequence_id, uint64_t current_trimmed_sequence_id,
    uint64_t retention_entries, uint32_t min_snapshot_ready_standby_count,
    const std::vector<RedisMasterRegistryEntry>& registry_entries,
    const std::string& self_instance_id = "");

class RedisOpLogTrimWorker {
   public:
    explicit RedisOpLogTrimWorker(RedisOpLogTrimWorkerConfig config);
    ~RedisOpLogTrimWorker();

    RedisOpLogTrimWorker(const RedisOpLogTrimWorker&) = delete;
    RedisOpLogTrimWorker& operator=(const RedisOpLogTrimWorker&) = delete;

    ErrorCode Start();
    void Stop();

   private:
    ErrorCode ValidateConfig() const;
    ErrorCode TrimOnce();
    void Run();

    std::string KeyPrefix() const;
    ErrorCode ReadSequenceKey(redisContext* ctx, const std::string& key,
                              uint64_t& sequence_id) const;
    ErrorCode AdvanceTrimmed(redisContext* ctx, uint64_t target,
                             uint64_t& sequence_id) const;
    ErrorCode CollectGarbage(redisContext* ctx, uint64_t latest,
                             uint64_t trimmed, uint64_t& deleted_to) const;

    RedisOpLogTrimWorkerConfig config_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread thread_;
    std::atomic<bool> running_{false};
    bool stopping_{false};
};

}  // namespace mooncake

#endif  // STORE_USE_REDIS
