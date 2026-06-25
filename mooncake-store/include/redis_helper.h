#pragma once

#ifdef STORE_USE_REDIS

#include <atomic>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

#include <hiredis/hiredis.h>
#include "types.h"

namespace mooncake {

/**
 * @brief Redis helper for leader election, mirroring EtcdHelper's role.
 *
 * Uses SET NX EX for atomic election, Lua scripts for lease renewal,
 * and Pub/Sub + fallback polling for watching leader key expiration.
 *
 * Only uses standard Redis commands + Lua scripts.
 *
 * Thread safety: election_ctx_ is shared between KeepLeader (background
 * thread) and GetMasterView (called from any thread). All accesses to
 * election_ctx_ are serialized via election_mutex_. The subscribe and
 * polling connections are used only from single threads and do not need
 * mutual exclusion.
 */
class RedisHelper {
   public:
    RedisHelper(const std::string& cluster_id,
                const std::string& redis_endpoint,
                const std::string& password = "", int db_index = 0,
                int ttl_sec = 5, int heartbeat_interval_sec = 2);
    ~RedisHelper();

    RedisHelper(const RedisHelper&) = delete;
    RedisHelper& operator=(const RedisHelper&) = delete;

    /**
     * @brief Connect to Redis. Must be called before ElectLeader.
     * @return ErrorCode::OK on success.
     */
    ErrorCode Connect();

    /**
     * @brief Elect self as leader. Blocks until this node wins election.
     *        Same semantics as MasterViewHelper::ElectLeader.
     * @param master_address The address to register as leader.
     * @param version Output: epoch (monotonic version) from INCR.
     * @param lease_id Output: opaque lease identifier (local counter).
     */
    void ElectLeader(const std::string& master_address, ViewVersionId& version,
                     int& lease_id);

    /**
     * @brief Keep leadership by periodically renewing TTL. Blocks until
     *        leadership is lost (key no longer ours, or connection error).
     * @param lease_id The lease ID returned by ElectLeader.
     */
    void KeepLeader(int lease_id);

    /**
     * @brief Cancel the keep-alive loop. For graceful shutdown.
     */
    void CancelKeepAlive();

    /**
     * @brief Get current leader's address and version from Redis.
     * @param master_address Output: leader address.
     * @param version Output: leader epoch.
     * @return ErrorCode::OK on success, error if key not found or Redis error.
     */
    ErrorCode GetMasterView(std::string& master_address,
                            ViewVersionId& version);

   private:
    // === Internal helpers ===

    /**
     * @brief Try to elect self once. Returns true if we won.
     *        Caller must hold election_mutex_.
     */
    bool TryElectOnce(const std::string& master_address,
                      ViewVersionId& out_epoch);

    /**
     * @brief Watch leader key until it expires (replaces etcd
     * WatchUntilDeleted). Uses SUBSCRIBE for fast notification + fallback
     * polling.
     */
    void WatchLeader();

    /**
     * @brief Publish a leader event on the notification channel.
     *        Caller must hold election_mutex_.
     */
    void PublishLeaderEvent(const std::string& event);

    /**
     * @brief Reconnect a broken redisContext.
     *        Caller must hold election_mutex_ if reconnecting election_ctx_.
     */
    bool Reconnect(redisContext*& ctx);

    /**
     * @brief Create a new authenticated connection to Redis.
     *        Used internally by Connect and for the polling connection.
     */
    redisContext* CreateConnection();

    /**
     * @brief Serialize leader value as JSON.
     */
    static std::string SerializeLeaderValue(const std::string& address,
                                            ViewVersionId epoch, int ttl_sec);

    /**
     * @brief Parse leader value from JSON. Returns false on parse failure.
     */
    static bool ParseLeaderValue(const std::string& json,
                                 std::string& out_address,
                                 ViewVersionId& out_epoch);

    // === Redis key naming ===

    std::string master_view_key_;       // mooncake:{cid}:master_view
    std::string master_epoch_key_;      // mooncake:{cid}:master_epoch
    std::string leader_event_channel_;  // mooncake:{cid}:leader_event

    // === Connection state ===

    std::string redis_endpoint_;
    std::string password_;
    int db_index_;
    redisContext* election_ctx_ =
        nullptr;  // For election + keepalive + GetMasterView
    redisContext* subscribe_ctx_ =
        nullptr;  // Dedicated connection for SUBSCRIBE (single-threaded)
    mutable std::mutex election_mutex_;  // Protects election_ctx_ access

    // === Configuration ===

    int ttl_sec_ = 5;                 // redis_master_view_ttl_sec
    int heartbeat_interval_sec_ = 2;  // TTL/3, rounded up
    int connect_timeout_ms_ = 5000;
    int command_timeout_ms_ = 3000;

    // === Runtime state ===

    std::string our_value_;  // JSON value we wrote, for Lua lease renewal
    std::string cluster_id_;
    std::atomic<bool> keep_alive_running_{false};
    std::atomic<bool> cancel_requested_{false};
    std::atomic<int> next_lease_id_{
        1};  // Local monotonic counter for lease IDs
};

}  // namespace mooncake

#endif  // STORE_USE_REDIS