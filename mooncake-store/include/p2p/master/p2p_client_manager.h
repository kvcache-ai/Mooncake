#pragma once

#include <boost/functional/hash.hpp>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/client/heartbeat_type.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/master/p2p_client_meta.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Manages P2P clients' lifecycle and heartbeat state.
 */
class P2PClientManager final {
   public:
    P2PClientManager(int64_t disconnect_timeout_sec, int64_t crash_timeout_sec,
                     ViewVersionId view_version);
    ~P2PClientManager();

    void Start();
    void Stop();

    void StartClientMonitor();
    void StopClientMonitor();

    auto RegisterClient(const P2PRegisterClientRequest& req)
        -> tl::expected<ViewVersionId, ErrorCode>;
    auto UnregisterClient(const UUID& client_id)
        -> tl::expected<ViewVersionId, ErrorCode>;
    auto Heartbeat(const P2PHeartbeatRequest& req)
        -> tl::expected<P2PHeartbeatResponse, ErrorCode>;
    auto QueryClientStatus(const UUID& client_id)
        -> tl::expected<P2PClientStatus, ErrorCode>;

    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetClientSegments(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QuerySegment(const UUID& client_id, const UUID& segment_id)
        -> tl::expected<P2PSegment, ErrorCode>;
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    auto GetClient(const UUID& client_id) -> std::shared_ptr<P2PClientMeta>;
    auto GetAllClients() -> std::vector<std::shared_ptr<P2PClientMeta>>;
    // Return owning client handles in strategy order, with the map unlocked.
    auto GetClientSnapshot(P2PClientSelectionStrategy strategy) const
        -> tl::expected<std::vector<std::shared_ptr<P2PClientMeta>>, ErrorCode>;

    auto GetClientIdBySegmentName(const std::string& segment_name)
        -> tl::expected<UUID, ErrorCode>;

    using ClientVisitor = std::function<tl::expected<bool, ErrorCode>(
        const std::shared_ptr<P2PClientMeta>& client)>;
    auto ForEachClient(P2PClientSelectionStrategy strategy,
                       const ClientVisitor& visitor)
        -> tl::expected<void, ErrorCode>;

    using SegmentRemovalCallback =
        std::function<void(const P2PRouteLocation& location)>;
    void SetSegmentRemovalCallback(SegmentRemovalCallback cb);

   private:
    static constexpr uint64_t kClientMonitorSleepMs = 1000;

    void ClientMonitorFunc();
    HeartbeatTaskResult ProcessTask(
        const std::shared_ptr<P2PClientMeta>& client,
        const HeartbeatTask& task);
    auto BuildClientList(P2PClientSelectionStrategy strategy) const
        -> std::optional<std::vector<std::shared_ptr<P2PClientMeta>>>;

    mutable SharedMutex clients_mutex_;
    std::unordered_map<UUID, std::shared_ptr<P2PClientMeta>, boost::hash<UUID>>
        client_metas_ GUARDED_BY(clients_mutex_);
    std::jthread client_monitor_thread_;
    const ViewVersionId view_version_;
    SegmentRemovalCallback segment_removal_cb_;
};

}  // namespace mooncake
