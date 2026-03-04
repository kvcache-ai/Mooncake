#pragma once

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
     * @brief Add a route replica to master
     */
    auto AddReplica(const AddReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Remove a route replica from master
     */
    auto RemoveReplica(const RemoveReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;

    std::vector<Replica::Descriptor> FilterReplicas(
        const GetReplicaListRequestConfig& config,
        const ObjectMetadata& metadata) override;

   protected:
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
    size_t GetShardIndex(const std::string& key) const override {
        return std::hash<std::string>{}(key) % kNumShards;
    }
    size_t GetShardCount() const override { return kNumShards; }

   protected:
    // Hooks
    void OnObjectAccessed(ObjectMetadata& metadata) override;
    void OnObjectHit(const ObjectMetadata& metadata) override;
    void OnReplicaRemoved(const Replica& replica) override;
    void OnReplicaAdded(const Replica& replica) override;

   private:
    std::shared_ptr<P2PClientManager> client_manager_;
    std::array<P2PMetadataShard, kNumShards> metadata_shards_;
    // for the number of replicas of a key:
    // 1. max_replicas_per_key_ == 0 means no limitation
    // 2. max_replicas_per_key_ > 0 means the max replica number of a key
    uint64_t max_replicas_per_key_;
};

}  // namespace mooncake
