#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

#include "client_meta.h"
#include "p2p_segment_manager.h"
#include "p2p_rpc_types.h"
#include "heartbeat_type.h"

namespace mooncake {
class P2PClientMeta final : public ClientMeta {
   public:
    P2PClientMeta(const UUID& client_id, const std::string& ip_address,
                  uint16_t rpc_port);

    std::shared_ptr<SegmentManager> GetSegmentManager() override;
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode> override;

    auto UpdateSegmentUsages(const std::vector<TierUsageInfo>& usages)
        -> SyncSegmentMetaResult;

    size_t GetAvailableCapacity() const;

    const std::string& get_ip_address() const { return ip_address_; }
    uint16_t get_rpc_port() const { return rpc_port_; }

   public:
    /**
     * @brief Evaluate this client as a write-route candidate.
     *
     * Performs health check and capacity filtering (tag_filters /
     * priority_limit / top_tier_only) internally and, on success, returns a
     * WriteCandidate whose `score` is the raw free ratio (free/total over
     * eligible tiers).
     *
     * @return A populated WriteCandidate when this client is routable;
     *         std::nullopt when it is not a candidate (unhealthy, no eligible
     *         tier, or insufficient free capacity).
     */
    std::optional<WriteCandidate> GetWriteRouteCandidate(
        const WriteRouteRequest& req);

   public:
    void DoOnDisconnected() override {}
    void DoOnRecovered() override {}

    // HA sync tracking
    void SetSyncing(bool syncing) {
        is_syncing_.store(syncing, std::memory_order_release);
    }
    bool IsSyncing() const {
        return is_syncing_.load(std::memory_order_acquire);
    }

   private:
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
    std::string ip_address_;
    uint16_t rpc_port_ = 0;
    std::shared_ptr<P2PSegmentManager> segment_manager_;

    mutable SpinRWLock capacity_mutex_;
    size_t client_capacity_ GUARDED_BY(capacity_mutex_) = 0;
    size_t client_usage_ GUARDED_BY(capacity_mutex_) = 0;

    std::atomic<bool> is_syncing_{false};
};

}  // namespace mooncake
