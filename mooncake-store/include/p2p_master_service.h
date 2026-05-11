#pragma once

#include <unordered_set>

#include "master_service.h"
#include "p2p_client_manager.h"
#include "p2p_rpc_types.h"

namespace mooncake {

class P2PMasterService : public MasterService {
   public:
    explicit P2PMasterService(const MasterServiceConfig& config);
    ~P2PMasterService() override = default;

    ClientManager& GetClientManager() override { return *client_manager_; }
    const ClientManager& GetClientManager() const override {
        return *client_manager_;
    }

    /**
     * @brief Get write route based on the config in the request
     */
    auto GetWriteRoute(const WriteRouteRequest& req)
        -> tl::expected<WriteRouteResponse, ErrorCode>;

    /**
     * @brief Batch get write routes for multiple keys.
     *        Reuses GetWriteRoute logic per key.
     */
    auto BatchGetWriteRoute(const BatchGetWriteRouteRequest& req)
        -> BatchGetWriteRouteResponse;

    /**
     * @brief Add a route replica to master
     */
    auto AddReplica(const AddReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Remove a route replica from master
     */
    auto RemoveReplica(const RemoveReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Remove replicas from multiple segments in one call
     */
    auto BatchRemoveReplica(const BatchRemoveReplicaRequest& req)
        -> std::vector<tl::expected<void, ErrorCode>>;

    /**
     * @brief Batch sync replicas with mixed ADD and REMOVE ops
     */
    auto BatchSyncReplica(const BatchSyncReplicaRequest& req)
        -> BatchSyncReplicaResponse;

    /**
     * @brief Client notifies Master that metadata sync is complete
     */
    auto SetSyncCompleted(UUID client_id) -> tl::expected<void, ErrorCode>;

    std::vector<Replica::Descriptor> FilterReplicas(
        const GetReplicaListRequestConfig& config,
        const ObjectMetadata& metadata) override;

   protected:
    struct P2PGroupReplicaMeta {
        UUID client_id;
        std::string group_id;
        std::unordered_set<UUID, boost::hash<UUID>> resident_segments;
    };

    struct P2PObjectMetadata final : public ObjectMetadata {
        P2PObjectMetadata(size_t value_length, std::vector<Replica>&& reps,
                          std::vector<P2PGroupReplicaMeta>&& group_replicas)
            : ObjectMetadata(value_length, std::move(reps)),
              group_replicas_(std::move(group_replicas)) {}

        std::vector<P2PGroupReplicaMeta> group_replicas_;
    };

    typedef MetadataShard P2PMetadataShard;
    MetadataShard& GetShard(size_t idx) override {
        return metadata_shards_[idx];
    }
    const MetadataShard& GetShard(size_t idx) const override {
        return metadata_shards_[idx];
    }

    P2PMetadataShard& GetP2PShard(size_t idx) { return metadata_shards_[idx]; }
    const P2PMetadataShard& GetP2PShard(size_t idx) const {
        return metadata_shards_[idx];
    }
    static constexpr size_t kNumShards = 1024;  // Number of metadata shards
    // Helper to get shard index from key
    size_t GetShardIndex(std::string_view key) const override {
        return std::hash<std::string_view>{}(key) % kNumShards;
    }
    size_t GetShardCount() const override { return kNumShards; }

   protected:
    // Hooks
    void OnObjectAccessed(ObjectMetadata& metadata) override;
    void OnObjectHit(const ObjectMetadata& metadata) override;
    void OnReplicaRemoved(const Replica& replica) override;
    void OnReplicaAdded(const Replica& replica) override;
    void OnSegmentRemoved(const UUID& segment_id) override;

   private:
    static P2PObjectMetadata& AsP2PObjectMetadata(ObjectMetadata& metadata);
    static const P2PObjectMetadata& AsP2PObjectMetadata(
        const ObjectMetadata& metadata);

    auto QueryP2PClient(const UUID& client_id) const
        -> tl::expected<std::shared_ptr<P2PClientMeta>, ErrorCode>;
    std::optional<size_t> FindGroupReplicaIndex(
        const P2PObjectMetadata& metadata, const UUID& client_id,
        const std::string& group_id) const;
    auto SelectBestResidentSegment(const std::shared_ptr<P2PClientMeta>& client,
                                   const P2PGroupReplicaMeta& group_meta,
                                   const P2PGetReplicaListConfigExtra& config)
        const -> std::shared_ptr<Segment>;
    static Replica::Descriptor MakeP2PDescriptor(
        const std::shared_ptr<P2PClientMeta>& client,
        const std::shared_ptr<Segment>& segment, size_t object_size);

    tl::expected<void, ErrorCode> InnerAddReplica(
        MetadataShard& shard, std::string_view key, const UUID& client_id,
        const UUID& segment_id, size_t size,
        const std::shared_ptr<P2PClientMeta>& client) NO_THREAD_SAFETY_ANALYSIS;
    tl::expected<void, ErrorCode> InnerRemoveReplica(
        MetadataShard& shard, std::string_view key, const UUID& client_id,
        const UUID& segment_id) NO_THREAD_SAFETY_ANALYSIS;

    std::shared_ptr<P2PClientManager> client_manager_;
    std::array<P2PMetadataShard, kNumShards> metadata_shards_;
    // for the number of replicas of a key:
    // 1. max_replicas_per_key_ == 0 means no limitation
    // 2. max_replicas_per_key_ > 0 means the max replica number of a key
    uint64_t max_replicas_per_key_;
};

}  // namespace mooncake
