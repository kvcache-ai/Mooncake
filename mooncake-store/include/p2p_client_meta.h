#pragma once

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

    void UpdateSegmentUsages(const std::vector<TierUsageInfo>& usages);

    size_t GetAvailableCapacity() const;

    const std::string& get_ip_address() const { return ip_address_; }
    uint16_t get_rpc_port() const { return rpc_port_; }

   public:
    /**
     * @brief A wrapper function of collecting write route candidates from this
     *        client in ForEachClient
     * @param req The write route request containing filter config
     * @param candidates Output: collected candidates
     * @return On error, returns an unexpected ErrorCode.
     *         Otherwise, returns bool value to indicate whether collected
     *         candidates are enough. If return `true`, ForEachClient will stop
     */
    auto CollectWriteRouteCandidates(const WriteRouteRequest& req,
                                     std::vector<WriteCandidate>& candidates)
        -> tl::expected<bool, ErrorCode>;

   public:
    void DoOnDisconnected() override {}
    void DoOnRecovered() override {}

   private:
    static constexpr size_t INF_PRIORITY = 10000;

    std::string ip_address_;
    uint16_t rpc_port_ = 0;
    std::shared_ptr<P2PSegmentManager> segment_manager_;

    mutable SpinRWLock capacity_mutex_;
    size_t client_capacity_ GUARDED_BY(capacity_mutex_) = 0;
    size_t client_usage_ GUARDED_BY(capacity_mutex_) = 0;
};

}  // namespace mooncake
