#pragma once

#include "client_manager.h"
#include "p2p_segment_manager.h"
namespace mooncake {
// TODO: this class is a tmp placeholder. it will be implemented later
class P2PClientManager final : public ClientManager {
   public:
    P2PClientManager(const int64_t client_live_ttl_sec);

    virtual ErrorCode UnmountSegment(const UUID& segment_id,
                                     const UUID& client_id) override;

   protected:
    virtual ErrorCode InnerMountSegment(
        const Segment& segment, const UUID& client_id,
        std::function<ErrorCode()>& pre_func) override;
    virtual ErrorCode InnerReMountSegment(
        const std::vector<Segment>& segments, const UUID& client_id,
        std::function<ErrorCode()>& pre_func) override;
    virtual void ClientMonitorFunc() override;

   protected:
    std::shared_ptr<P2PSegmentManager> segment_manager_;
};

}  // namespace mooncake
