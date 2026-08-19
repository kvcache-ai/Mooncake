#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>

#include "p2p/client_meta.h"
#include "p2p/p2p_segment_manager.h"
#include "p2p/p2p_rpc_types.h"
#include "p2p/heartbeat_type.h"

namespace mooncake {

class P2PClientMeta final : public ClientMeta {
   public:
    P2PClientMeta(const UUID& client_id, const std::string& ip_address,
                  uint16_t rpc_port);

    std::shared_ptr<P2PSegmentManager> GetSegmentManager() override;
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode> override;

    auto UpdateSegmentUsages(const std::vector<TierUsageInfo>& usages)
        -> SyncSegmentMetaResult;

    size_t GetAvailableCapacity() const;

    const std::string& get_ip_address() const { return ip_address_; }
    uint16_t get_rpc_port() const { return rpc_port_; }

   public:
    std::optional<WriteCandidate> GetWriteRouteCandidate(
        const WriteRouteRequest& req);

   public:
    void DoOnDisconnected() override {}
    void DoOnRecovered() override {}

    void SetSyncing(bool syncing) {
        is_syncing_.store(syncing, std::memory_order_release);
    }
    bool IsSyncing() const {
        return is_syncing_.load(std::memory_order_acquire);
    }

   private:
    struct CapacityStat {
        size_t free = 0;
        size_t total = 0;
    };
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