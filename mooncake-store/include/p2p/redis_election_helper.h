#pragma once

#ifdef STORE_USE_REDIS

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

#include <hiredis/hiredis.h>
#include "p2p/redis_util.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Redis helper for leader election, mirroring EtcdHelper's election
 * role.
 *
 * Uses Lua scripts for atomic election and lease renewal, and Pub/Sub with
 * fallback polling for watching leader key expiration.
 *
 * Only uses standard Redis commands + Lua scripts.
 *
 * Thread safety: election_ctx_ is shared between KeepLeader (background
 * thread) and GetMasterView (called from any thread). All accesses to
 * election_ctx_ are serialized via election_mutex_. subscribe_ctx_ is
 * serialized via subscribe_mutex_; polling connections are thread-local.
 */
class RedisElectionHelper {
   public:
    RedisElectionHelper(const std::string& cluster_id,
                        const std::string& redis_endpoint,
                        const std::string& password = "", int db_index = 0,
                        int ttl_sec = 5, int heartbeat_interval_sec = 2,
                        const std::string& username = "");
    ~RedisElectionHelper();

    RedisElectionHelper(const RedisElectionHelper&) = delete;
    RedisElectionHelper& operator=(const RedisElectionHelper&) = delete;

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
     * @brief Cancel any in-progress ElectLeader/WatchLeader call.
     */
    void CancelElection();

    /** Cancel blocking operations and wait until they have exited. */
    void Shutdown();

    /**
     * @brief Get current leader's address and version from Redis.
     * @param master_address Output: leader address.
     * @param version Output: leader epoch.
     * @return ErrorCode::OK on success, error if key not found or Redis error.
     */
    ErrorCode GetMasterView(std::string& master_address,
                            ViewVersionId& version);

    // === Serialization (public for testability) ===

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

   private:
    // === Internal helpers ===

    enum class ElectionAttemptResult { ELECTED, CONTENDED, ERROR };

    /**
     * @brief Atomically try to elect self once and return the outcome.
     *        Caller must hold election_mutex_.
     */
    ElectionAttemptResult TryElectOnce(const std::string& master_address,
                                       ViewVersionId& out_epoch,
                                       std::string& existing_value);

    /**
     * @brief Watch leader key until it expires (replaces etcd
     * WatchUntilDeleted). Uses SUBSCRIBE for fast notification + fallback
     * polling.
     */
    void WatchLeader();

    /**
     * @brief Fast-path watch: SUBSCRIBE to leader event channel, with a
     *        polling thread as backup. Returns true if subscribe succeeded
     *        and the watch completed, false if subscribe failed (caller
     *        should fall back to pure polling).
     */
    bool WatchLeaderSubscribe();

    /**
     * @brief Slow-path watch: pure polling via a separate connection.
     *        Blocks until the leader key expires or cancel is requested.
     */
    void WatchLeaderPolling();

    /** Publish a best-effort wake-up after a successful election. */
    void PublishLeaderEvent(const std::string& master_address,
                            ViewVersionId epoch);

    /**
     * @brief Reconnect a broken redisContext.
     *        Caller must hold election_mutex_ if reconnecting election_ctx_.
     */
    bool Reconnect(redisContext*& ctx);

    /** Caller must hold subscribe_mutex_. */
    bool ReconnectSubscribeLocked(bool record_metric = true);

    bool BeginBlockingOperation();
    bool BeginKeepAliveOperation();
    void EndBlockingOperation();
    void EndKeepAliveOperation();
    bool WaitForElectionCancellation(std::chrono::milliseconds timeout);
    bool WaitForKeepAliveCancellation(std::chrono::milliseconds timeout);

    /**
     * @brief Create a new authenticated connection to Redis.
     *        Used internally by Connect and for the polling connection.
     */
    redisContext* CreateConnection();

    // === Redis key naming ===

    std::string master_view_key_;       // mooncake:{cid}:master_view
    std::string master_epoch_key_;      // mooncake:{cid}:master_epoch
    std::string leader_event_channel_;  // mooncake:{cid}:leader_event

    // === Connection state ===

    std::string redis_endpoint_;
    std::string username_;
    std::string password_;
    int db_index_;
    redisContext* election_ctx_ =
        nullptr;  // For election + keepalive + GetMasterView
    redisContext* subscribe_ctx_ =
        nullptr;  // Dedicated connection for SUBSCRIBE (single-threaded)

    // Locking policy:
    // - Keep the Redis context locks separate because SUBSCRIBE may block for
    //   a long time and must not stall election commands.
    // - operation_mutex_ coordinates Connect/Shutdown with blocking-operation
    //   lifetimes. When locks are nested, acquire it before a context lock.
    // - cancel_mutex_ is only for cancellation waits and is never nested with
    //   a context lock.
    // These control-plane paths are low frequency; revisit the split only if
    // profiling shows contention or the connection ownership model changes.
    mutable std::mutex election_mutex_;   // Protects election_ctx_ access
    mutable std::mutex subscribe_mutex_;  // Protects subscribe_ctx_ access

    // === Configuration ===

    int ttl_sec_ = 5;                 // redis_master_view_ttl_sec
    int heartbeat_interval_sec_ = 2;  // TTL/3, rounded up
    int connect_timeout_ms_ = 5000;
    int command_timeout_ms_ = 3000;

    // === Runtime state ===

    std::string our_value_;  // JSON value we wrote, for Lua lease renewal
    std::string cluster_id_;
    std::atomic<bool> keep_alive_running_{false};
    std::atomic<bool> cancel_keep_alive_{false};  // Cancel KeepLeader only
    std::atomic<bool> cancel_election_{
        false};  // Cancel ElectLeader/WatchLeader
    std::atomic<int> next_lease_id_{
        1};  // Local monotonic counter for lease IDs

    std::mutex cancel_mutex_;
    std::condition_variable cancel_cv_;
    std::mutex operation_mutex_;
    std::condition_variable operation_cv_;
    size_t active_blocking_operations_{0};
    bool keep_alive_operation_active_{false};
    bool shutting_down_{false};
};

}  // namespace mooncake

#endif  // STORE_USE_REDIS
