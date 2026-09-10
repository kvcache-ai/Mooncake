#pragma once

#include <array>
#include <cstddef>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <boost/functional/hash.hpp>
#include <ylt/util/tl/expected.hpp>

#include "mutex.h"
#include "p2p/common/p2p_master_config.h"
#include "p2p/common/p2p_rpc_types.h"
#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/ha/oplog/p2p_standby_metadata_store.h"
#include "p2p/master/p2p_client_manager.h"
#include "p2p/master/p2p_route_table.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Standalone P2P master service.
 *
 * P2PMasterService owns P2P route metadata and delegates client and segment
 * metadata to P2PClientManager.
 *
 * Lock order:
 * 1. P2PMasterService route shard mutex
 * 2. P2PClientManager::clients_mutex_
 * 3. P2PClientMeta::client_mutex_
 * 4. P2PSegmentManager::segment_mutex_
 */
class P2PMasterService final {
   public:
    explicit P2PMasterService(const P2PMasterConfig& config,
                              ViewVersionId view_version = 0);
    ~P2PMasterService() = default;

    P2PClientManager& GetClientManager() { return *client_manager_; }
    const P2PClientManager& GetClientManager() const {
        return *client_manager_;
    }

    auto RegisterClient(const P2PRegisterClientRequest& req)
        -> tl::expected<ViewVersionId, ErrorCode>;
    auto UnregisterClient(const UUID& client_id)
        -> tl::expected<ViewVersionId, ErrorCode>;
    auto Heartbeat(const P2PHeartbeatRequest& req)
        -> tl::expected<P2PHeartbeatResponse, ErrorCode>;
    auto QueryClientStatus(const UUID& client_id)
        -> tl::expected<P2PClientStatus, ErrorCode>;

    auto MountSegment(const P2PSegment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;
    auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    auto ExistKey(std::string_view key) -> tl::expected<bool, ErrorCode>;
    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string_view>& keys);
    auto GetAllKeys() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto GetClientSegments(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;
    auto BatchQueryIp(const std::vector<UUID>& client_ids) -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>;

    auto GetReadRouteByRegex(std::string_view regex_pattern) -> tl::expected<
        std::unordered_map<std::string, std::vector<P2PRouteDescriptor>>,
        ErrorCode>;
    auto GetReadRoute(std::string_view key,
                      const P2PReadRouteConfig& config = P2PReadRouteConfig())
        -> tl::expected<std::vector<P2PRouteDescriptor>, ErrorCode>;

    auto Remove(std::string_view key) -> tl::expected<void, ErrorCode>;
    auto RemoveByRegex(std::string_view regex_pattern)
        -> tl::expected<long, ErrorCode>;
    long RemoveAll();
    size_t GetKeyCount() const;

    OpLogManager* GetOpLogManager() const { return oplog_manager_.get(); }

    auto GetWriteRoute(const P2PGetWriteRouteRequest& req)
        -> tl::expected<std::vector<P2PWriteCandidate>, ErrorCode>;

    /**
     * @brief Batch get write routes for multiple keys.
     *        Reuses capacity snapshots while checking owners and health per key.
     */
    auto BatchGetWriteRoute(const P2PBatchGetWriteRouteRequest& req)
        -> P2PBatchGetWriteRouteResponse;

    /**
     * @brief Publish a route location to master
     */
    auto PublishRoute(const P2PPublishRouteRequest& req)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Withdraw a route location from master
     */
    auto WithdrawRoute(const P2PWithdrawRouteRequest& req)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Withdraw route locations from multiple segments in one call
     */
    auto BatchWithdrawRoute(const P2PBatchWithdrawRouteRequest& req)
        -> std::vector<tl::expected<void, ErrorCode>>;

    /**
     * @brief Batch sync routes with mixed publish and withdraw operations.
     */
    auto BatchSyncRoutes(const P2PBatchSyncRoutesRequest& request)
        -> P2PBatchSyncRoutesResponse;

    /**
     * @brief Client notifies Master that metadata sync is complete
     */
    auto CompleteRouteSync(UUID client_id) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Restore P2P metadata exported by P2PHotStandbyService promotion.
     *
     * The target service must be empty. Restore registers clients/segments and
     * rebuilds route metadata plus location reverse indexes without recording
     * new OpLog entries. If last_applied_sequence_id is provided, the target
     * OpLogManager starts future writes after that sequence.
     */
    ErrorCode RestoreFromStandbyMetadata(
        const P2PStandbyMetadataStore::ExportedMetadata& metadata,
        uint64_t last_applied_sequence_id = 0);

    ErrorCode RecordOplog(OpType type, const std::string& key,
                          const std::string& payload = std::string());

   private:
    using OwnerClientSet = std::unordered_set<UUID, boost::hash<UUID>>;

    void InitializeClientManager();
    void OnSegmentRemoved(const P2PRouteLocation& location);
    static OwnerClientSet CollectRouteOwnerClients(const P2PRouteEntry& route);

    // Shared single/batch selection: snapshot owners, then visit each client
    // once and assign its candidate only to keys still needing candidates.
    P2PBatchGetWriteRouteResponse SelectWriteRoutes(
        std::span<const std::string_view> keys,
        std::span<const uint64_t> object_sizes, const UUID& requester_id,
        const P2PWriteRouteConfig& config) const;

    auto BuildRouteDescriptor(const P2PRouteLocation& location,
                              uint64_t object_size) const
        -> tl::expected<P2PRouteDescriptor, ErrorCode>;

    std::vector<P2PRouteDescriptor> FilterRoutes(
        const P2PReadRouteConfig& config, const P2PRouteEntry& route) const;

    auto InnerPublishRoute(std::string_view key, const UUID& client_id,
                           const UUID& segment_id, size_t size,
                           const std::shared_ptr<P2PClientMeta>& client)
        -> tl::expected<void, ErrorCode>;
    auto InnerWithdrawRoute(std::string_view key, const UUID& client_id,
                            const UUID& segment_id)
        -> tl::expected<void, ErrorCode>;

    auto ApplyPublishLocked(P2PRouteTable& table, std::string_view key,
                            const UUID& client_id, const UUID& segment_id,
                            size_t size,
                            const std::shared_ptr<P2PClientMeta>& client)
        -> tl::expected<void, ErrorCode> NO_THREAD_SAFETY_ANALYSIS;
    auto ApplyWithdrawLocked(P2PRouteTable& table, std::string_view key,
                             const UUID& client_id, const UUID& segment_id)
        -> tl::expected<void, ErrorCode> NO_THREAD_SAFETY_ANALYSIS;

   private:
    static constexpr size_t kRouteShardCount = 1024;

    struct RouteShard {
        mutable SharedMutex mutex;
        P2PRouteTable table GUARDED_BY(mutex);
    };

    size_t GetRouteShardIndex(std::string_view key) const {
        return std::hash<std::string_view>{}(key) % kRouteShardCount;
    }
    std::optional<P2PRouteEntry> GetRouteSnapshot(std::string_view key) const;
    std::vector<std::string> ListRouteKeys() const;

   private:
    std::array<RouteShard, kRouteShardCount> route_shards_;
    const uint64_t max_client_per_key_;
    bool enable_async_oplog_write_{false};
    ViewVersionId view_version_;
    std::unique_ptr<OpLogManager> oplog_manager_;
    // Declared last so the monitor and its callbacks stop before route state.
    std::shared_ptr<P2PClientManager> client_manager_;
};

}  // namespace mooncake
