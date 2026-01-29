#include "p2p_client_manager.h"
#include <glog/logging.h>

namespace mooncake {

P2PClientManager::P2PClientManager(const int64_t client_live_ttl_sec)
    : ClientManager(client_live_ttl_sec) {}

auto P2PClientManager::UnmountSegment(const UUID& segment_id,
                                      const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    // TODO
    return {};
}

void P2PClientManager::ClientMonitorFunc() {
    while (client_monitor_running_) {
        // TODO
    }
}

auto P2PClientManager::InnerMountSegment(const Segment& segment,
                                         const UUID& client_id,
                                         std::function<ErrorCode()>& pre_func)
    -> tl::expected<void, ErrorCode> {
    // TODO
    return {};
}

auto P2PClientManager::InnerReMountSegment(const std::vector<Segment>& segments,
                                           const UUID& client_id,
                                           std::function<ErrorCode()>& pre_func)
    -> tl::expected<void, ErrorCode> {
    // TODO
    return {};
}

auto P2PClientManager::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    // TODO
    return {};
}

auto P2PClientManager::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    // TODO
    return {};
}

auto P2PClientManager::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    // TODO
    return {};
}

auto P2PClientManager::Ping(const UUID& client_id)
    -> tl::expected<ClientStatus, ErrorCode> {
    // TODO
    return ClientStatus::OK;
}

}  // namespace mooncake
