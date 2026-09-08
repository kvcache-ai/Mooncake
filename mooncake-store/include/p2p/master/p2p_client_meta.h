#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/client/heartbeat_type.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/master/p2p_segment_manager.h"
#include "types.h"

namespace mooncake {

struct P2PClientHealthState {
    P2PClientStatus status = P2PClientStatus::UNDEFINED;
    std::chrono::steady_clock::time_point last_heartbeat;
};

/**
 * @brief Records a P2P client's health state, endpoint and mounted tiers.
 */
class P2PClientMeta final {
   public:
    P2PClientMeta(const UUID& client_id, const std::string& ip_address,
                  uint16_t rpc_port);
    ~P2PClientMeta();

    auto MountSegment(const P2PSegment& segment)
        -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id)
        -> tl::expected<void, ErrorCode>;
    auto GetSegments() -> tl::expected<std::vector<P2PSegment>, ErrorCode>;
    auto QuerySegments(const std::string& segment_name)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QuerySegment(const UUID& segment_id)
        -> tl::expected<P2PSegment, ErrorCode>;
    auto QueryIp() -> tl::expected<std::vector<std::string>, ErrorCode>;

    using SegmentRemovalCallback = std::function<void(const UUID& segment_id)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

    static void SetTimeouts(int64_t disconnect_sec, int64_t crash_sec);

    /**
     * @brief Update heartbeat timestamp and health status.
     * Attention: if client is CRASHED, the heartbeat will not be updated.
     * @return std::pair<P2PClientStatus, P2PClientStatus> {old_status,
     * new_status}
     */
    std::pair<P2PClientStatus, P2PClientStatus> Heartbeat();

    /**
     * @brief Update health status based on the last heartbeat timestamp.
     * @return std::pair<P2PClientStatus, P2PClientStatus> {old_status,
     * new_status}
     */
    std::pair<P2PClientStatus, P2PClientStatus> CheckHealth();
    void RecycleMeta();

    UUID get_client_id() const { return client_id_; }
    P2PClientHealthState get_health_state() const;
    bool is_health() const;

    auto UpdateSegmentUsages(const std::vector<TierUsageInfo>& usages)
        -> SyncSegmentMetaResult;
    size_t GetAvailableCapacity() const;

    const std::string& get_ip_address() const { return ip_address_; }
    uint16_t get_rpc_port() const { return rpc_port_; }

    void MarkRegistered() { registered_ = true; }

    /**
     * @brief Evaluate this client as a write-route candidate.
     *
     * Performs health check and capacity filtering (tag_filters /
     * priority_limit / top_tier_only) internally and, on success, returns a
     * P2PWriteCandidate whose `score` is the raw free ratio (free/total over
     * eligible tiers).
     *
     * @return A populated P2PWriteCandidate when this client is routable;
     *         std::nullopt when it is not a candidate (unhealthy, no eligible
     *         tier, or insufficient free capacity).
     */
    std::optional<P2PWriteCandidate> GetWriteRouteCandidate(
        const P2PGetWriteRouteRequest& req);

    void SetSyncing(bool syncing) {
        is_syncing_.store(syncing, std::memory_order_release);
    }
    bool IsSyncing() const {
        return is_syncing_.load(std::memory_order_acquire);
    }

   private:
    auto InnerStatusCheck() const -> tl::expected<void, ErrorCode>;
    void InnerUpdateHeartbeat();
    std::pair<P2PClientStatus, P2PClientStatus> InnerUpdateHealthStatus();
    void ApplyHealthTransition(P2PClientStatus old_status,
                               P2PClientStatus new_status);
    std::string HealthToString(P2PClientStatus status) const;
    /// Free/total capacity over the segments eligible for write-route scoring.
    struct CapacityStat {
        size_t free = 0;
        size_t total = 0;
    };

    /**
     * @brief Aggregate free/total over the eligible segments for write-route
     *        scoring. A segment is eligible when it carries no tag in
     *        `tag_filters` and its priority is >= `priority_limit`. When
     *        `top_tier_only` is true, only the highest-priority eligible
     *        segment(s) contribute (a client may not spill to lower tiers under
     *        memory pressure). Returns {0,0} when no segment is eligible.
     */
    CapacityStat GetWriteScoreCapacity(
        const std::vector<std::string>& tag_filters, int priority_limit,
        bool top_tier_only) const;

   private:
    static int64_t disconnect_timeout_sec_;
    static int64_t crash_timeout_sec_;

    mutable SharedMutex client_mutex_;
    UUID client_id_;
    P2PClientHealthState health_state_ GUARDED_BY(client_mutex_);
    std::atomic<bool> recycled_{false};
    // A temporary meta may be destroyed after losing a registration race. It
    // must not remove metric series owned by the registered meta with the same
    // client ID.
    bool registered_{false};
    std::string ip_address_;
    uint16_t rpc_port_ = 0;
    P2PSegmentManager segment_manager_;
    SegmentRemovalCallback segment_removal_cb_;
    std::atomic<bool> is_syncing_{false};
};

}  // namespace mooncake
