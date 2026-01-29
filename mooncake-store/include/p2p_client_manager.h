#pragma once

#include "client_manager.h"
#include "p2p_segment_manager.h"
namespace mooncake {
// TODO: this class is a tmp placeholder. it will be implemented later
class P2PClientManager final : public ClientManager {
   public:
    P2PClientManager(const int64_t client_live_ttl_sec);

    virtual tl::expected<void, ErrorCode> UnmountSegment(
        const UUID& segment_id, const UUID& client_id) override;
    virtual tl::expected<std::vector<std::string>, ErrorCode> GetAllSegments()
        override;
    virtual tl::expected<std::pair<size_t, size_t>, ErrorCode> QuerySegments(
        const std::string& segment) override;
    virtual tl::expected<std::vector<std::string>, ErrorCode> QueryIp(
        const UUID& client_id) override;
    virtual tl::expected<ClientStatus, ErrorCode> Ping(
        const UUID& client_id) override;

   protected:
    virtual tl::expected<void, ErrorCode> InnerMountSegment(
        const Segment& segment, const UUID& client_id,
        std::function<ErrorCode()>& pre_func) override;
    virtual tl::expected<void, ErrorCode> InnerReMountSegment(
        const std::vector<Segment>& segments, const UUID& client_id,
        std::function<ErrorCode()>& pre_func) override;
    virtual void ClientMonitorFunc() override;

   protected:
    std::shared_ptr<P2PSegmentManager> segment_manager_;
};

}  // namespace mooncake
