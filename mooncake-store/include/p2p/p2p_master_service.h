#pragma once

#include <algorithm>
#include <array>
#include <boost/functional/hash.hpp>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "p2p/p2p_ha_metric_manager.h"
#include "p2p/ha/oplog/oplog_manager.h"
#include "p2p/ha/oplog/oplog_store_factory.h"
#include "p2p/ha/oplog/p2p_oplog_types.h"
#include "p2p/ha/oplog/p2p_standby_metadata_store.h"  // for ExportedMetadata
#include "master_config.h"
#include "master_metric_manager.h"
#include "mutex.h"
#include "p2p/p2p_client_manager.h"
#include "p2p/p2p_client_meta.h"
#include "p2p/p2p_rpc_types.h"
#include "replica.h"
#include "rpc_types.h"
#include "types.h"
#include "utils.h"

namespace mooncake {

class P2PMasterService {
   public:
    explicit P2PMasterService(const MasterServiceConfig& config);
    ~P2PMasterService() = default;

    P2PClientManager& GetClientManager() { return *client_manager_; }
    const P2PClientManager& GetClientManager() const {
        return *client_manager_;
    }

    auto RegisterClient(const RegisterClientRequest& req)
        -> tl::expected<RegisterClientResponse, ErrorCode>;
    auto UnregisterClient(const UnregisterClientRequest& req)
        -> tl::expected<UnregisterClientResponse, ErrorCode>;
    auto Heartbeat(const HeartbeatRequest& req)
        -> tl::expected<HeartbeatResponse, ErrorCode>;
    auto QueryClientStatus(const QueryClientStatusRequest& req)
        -> tl::expected<QueryClientStatusResponse, ErrorCode>;

    auto MountSegment(const Segment& segment, const UUID& client_id)
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
    auto GetReplicaListByRegex(const std::string& regex_pattern)
        -> tl::expected<
            std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
            ErrorCode>;
    auto GetReplicaList(std::string_view key,
                        const GetReplicaListRequestConfig& config =
                            GetReplicaListRequestConfig())
        -> tl::expected<GetReplicaListResponse, ErrorCode>;
    auto Remove(std::string_view key, bool force = false)
        -> tl::expected<void, ErrorCode>;
    auto RemoveByRegex(std::string_view str, bool force = false)
        -> tl::expected<long, ErrorCode>;
    long RemoveAll(bool force = false);
    size_t GetKeyCount() const;

    auto GetWriteRoute(const WriteRouteRequest& req)
        -> tl::expected<WriteRouteResponse, ErrorCode>;
    auto BatchGetWriteRoute(const BatchGetWriteRouteRequest& req)
        -> BatchGetWriteRouteResponse;
    auto AddReplica(const AddReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;
    auto RemoveReplica(const RemoveReplicaRequest& req)
        -> tl::expected<void, ErrorCode>;
    auto BatchRemoveReplica(const BatchRemoveReplicaRequest& req)
        -> std::vector<tl::expected<void, ErrorCode>>;
    auto BatchSyncReplica(const BatchSyncReplicaRequest& req)
        -> BatchSyncReplicaResponse;
    auto SetSyncCompleted(UUID client_id) -> tl::expected<void, ErrorCode>;

    // Restore P2P metadata exported by P2PHotStandbyService promotion.
    ErrorCode RestoreFromStandbyMetadata(
        const P2PStandbyMetadataStore::ExportedMetadata& metadata,
        uint64_t last_applied_sequence_id = 0);

    ErrorCode RecordOplog(OpType type, const std::string& key,
                          const std::string& payload = std::string());

    OpLogManager* GetOpLogManager() const { return oplog_manager_.get(); }

    void InitializeClientManager();

   private:
    struct ObjectMetadata {
       public:
        ~ObjectMetadata();

        ObjectMetadata(size_t value_length, std::vector<Replica>&& reps);
        ObjectMetadata() = delete;

        ObjectMetadata(const ObjectMetadata&) = delete;
        ObjectMetadata& operator=(const ObjectMetadata&) = delete;
        ObjectMetadata(ObjectMetadata&&) = delete;
        ObjectMetadata& operator=(ObjectMetadata&&) = delete;

        bool IsValid() const {
            return CountReplicas() > 0 && size_ > 0;
        }

        void AddReplicas(std::vector<Replica>&& replicas) {
            replicas_.insert(replicas_.end(),
                             std::move_iterator(replicas.begin()),
                             std::move_iterator(replicas.end()));
        }

        std::vector<Replica> PopReplicas(
            const std::function<bool(const Replica&)>& pred_fn) {
            auto partition_point =
                std::partition(replicas_.begin(), replicas_.end(),
                               [&pred_fn](const Replica& replica) {
                                   return !pred_fn(replica);
                               });

            std::vector<Replica> popped_replicas;
            if (partition_point != replicas_.end()) {
                popped_replicas.reserve(
                    std::distance(partition_point, replicas_.end()));
                std::move(partition_point, replicas_.end(),
                          std::back_inserter(popped_replicas));
                replicas_.erase(partition_point, replicas_.end());
            }
            return popped_replicas;
        }

        std::vector<Replica> PopReplicas() { return std::move(replicas_); }

        size_t EraseReplicas(
            const std::function<bool(const Replica&)>& pred_fn) {
            auto erased_replicas = PopReplicas(pred_fn);
            return erased_replicas.size();
        }

        size_t EraseReplicas() {
            auto erased_replicas = PopReplicas();
            return erased_replicas.size();
        }

        size_t VisitReplicas(const std::function<bool(const Replica&)>& pred_fn,
                             const std::function<void(Replica&)>& visit_fn) {
            size_t num_visited = 0;
            for (auto& replica : replicas_) {
                if (pred_fn(replica)) {
                    visit_fn(replica);
                    ++num_visited;
                }
            }
            return num_visited;
        }

        size_t VisitReplicas(
            const std::function<bool(const Replica&)>& pred_fn,
            const std::function<void(const Replica&)>& visit_fn) const {
            size_t num_visited = 0;
            for (const auto& replica : replicas_) {
                if (pred_fn(replica)) {
                    visit_fn(replica);
                    ++num_visited;
                }
            }
            return num_visited;
        }

        bool HasReplica(
            const std::function<bool(const Replica&)>& pred_fn) const {
            return std::any_of(replicas_.begin(), replicas_.end(), pred_fn);
        }

        bool AllReplicas(
            const std::function<bool(const Replica&)>& pred_fn) const {
            return std::all_of(replicas_.begin(), replicas_.end(), pred_fn);
        }

        size_t CountReplicas(
            const std::function<bool(const Replica&)>& pred_fn) const {
            return std::count_if(replicas_.begin(), replicas_.end(), pred_fn);
        }

        size_t CountReplicas() const { return replicas_.size(); }

        Replica* GetFirstReplica(
            const std::function<bool(const Replica&)>& pred_fn) {
            const auto it =
                std::find_if(replicas_.begin(), replicas_.end(), pred_fn);
            return it != replicas_.end() ? &(*it) : nullptr;
        }

        Replica* GetReplicaByID(const ReplicaID& id) {
            return GetFirstReplica(
                [&id](const Replica& replica) { return replica.id() == id; });
        }

        bool EraseReplicaByID(const ReplicaID& id) {
            auto num_erased = EraseReplicas(
                [&id](const Replica& replica) { return replica.id() == id; });
            return num_erased > 0;
        }

        Replica* GetReplicaBySegmentName(const std::string& segment_name) {
            return GetFirstReplica([&segment_name](const Replica& replica) {
                auto names = replica.get_segment_names();
                for (auto& name_opt : names) {
                    if (name_opt == segment_name) {
                        return true;
                    }
                }
                return false;
            });
        }

        bool IsObjectAccessible() const {
            return HasReplica(&Replica::fn_is_completed);
        }

        tl::expected<void, ErrorCode> IsObjectRemovable(
            bool force = false) const {
            return {};
        }

        bool IsReplicaAccessible(const Replica& replica) const {
            return true;
        }

        tl::expected<void, ErrorCode> IsReplicaRemovable(
            const Replica& replica) const {
            return {};
        }

       public:
        std::vector<Replica> replicas_;
        size_t size_;
    };

    struct MetadataShard {
        mutable SharedMutex mutex;
        std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                           StringHash, std::equal_to<>>
            metadata GUARDED_BY(mutex);
        std::unordered_map<UUID, std::unordered_map<std::string_view, size_t>,
                           boost::hash<UUID>>
            segment_key_index GUARDED_BY(mutex);
    };

    class MetadataShardAccessorRW {
       public:
        MetadataShardAccessorRW(P2PMasterService* master_service,
                                size_t shard_index)
            : shard_(master_service->GetShard(shard_index)),
              lock_(&shard_.mutex) {}

        MetadataShard* operator->() { return &shard_; }
        const MetadataShard* operator->() const { return &shard_; }
        MetadataShard& GetRef() NO_THREAD_SAFETY_ANALYSIS { return shard_; }

       private:
        MetadataShard& shard_;
        SharedMutexLocker lock_;
    };

    class MetadataShardAccessorRO {
       public:
        MetadataShardAccessorRO(const P2PMasterService* master_service,
                                size_t shard_index)
            : shard_(master_service->GetShard(shard_index)),
              lock_(&shard_.mutex, shared_lock) {}

        const MetadataShard* operator->() const { return &shard_; }

       private:
        const MetadataShard& shard_;
        SharedMutexLocker lock_;
    };

    MetadataShard& GetShard(size_t idx) { return metadata_shards_[idx]; }
    const MetadataShard& GetShard(size_t idx) const {
        return metadata_shards_[idx];
    }
    static constexpr size_t kNumShards = 1024;
    size_t GetShardIndex(std::string_view key) const {
        return std::hash<std::string_view>{}(key) % kNumShards;
    }
    size_t GetShardCount() const { return kNumShards; }

    void AddReplicaToSegmentIndex(MetadataShard& shard, const std::string& key,
                                  const Replica& replica)
        NO_THREAD_SAFETY_ANALYSIS;
    void RemoveReplicaFromSegmentIndex(
        MetadataShard& shard, const std::string& key,
        const std::vector<Replica>& replicas) NO_THREAD_SAFETY_ANALYSIS;
    void RemoveReplicaFromSegmentIndex(
        MetadataShard& shard, const std::string& key,
        const Replica& replica) NO_THREAD_SAFETY_ANALYSIS;

    class MetadataAccessorRW {
       public:
        MetadataAccessorRW(P2PMasterService* service, std::string_view key)
            : service_(service),
              shard_idx_(service_->GetShardIndex(key)),
              shard_guard_(service_, shard_idx_),
              it_(shard_guard_->metadata.find(key)) {}

        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_guard_->metadata.end() &&
                   it_->second->IsValid();
        }

        const std::string& GetKey() const NO_THREAD_SAFETY_ANALYSIS {
            return it_->first;
        }

        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return *it_->second; }

        MetadataShardAccessorRW& GetShard() NO_THREAD_SAFETY_ANALYSIS {
            return shard_guard_;
        }

        void Erase() NO_THREAD_SAFETY_ANALYSIS {
            if (it_ != shard_guard_->metadata.end()) {
                service_->RemoveReplicaFromSegmentIndex(
                    shard_guard_.GetRef(), it_->first, it_->second->replicas_);
                shard_guard_->metadata.erase(it_);
                it_ = shard_guard_->metadata.end();
            }
        }

       private:
        P2PMasterService* service_;
        size_t shard_idx_;
        MetadataShardAccessorRW shard_guard_;
        using MetadataMap =
            std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                               StringHash, std::equal_to<>>;
        MetadataMap::iterator it_;
    };

    class MetadataAccessorRO {
       public:
        MetadataAccessorRO(const P2PMasterService* service,
                           std::string_view key)
            : service_(service),
              shard_idx_(service_->GetShardIndex(key)),
              shard_guard_(service_, shard_idx_),
              it_(shard_guard_->metadata.find(key)) {}

        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_guard_->metadata.end() &&
                   it_->second->IsValid();
        }

        const ObjectMetadata& Get() const NO_THREAD_SAFETY_ANALYSIS {
            return *it_->second;
        }

        const std::string& GetKey() const NO_THREAD_SAFETY_ANALYSIS {
            return it_->first;
        }

       private:
        const P2PMasterService* service_;
        const size_t shard_idx_;
        MetadataShardAccessorRO shard_guard_;
        using MetadataMap =
            std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                               StringHash, std::equal_to<>>;
        MetadataMap::const_iterator it_;
    };

   private:
    std::vector<Replica::Descriptor> FilterReplicas(
        const GetReplicaListRequestConfig& config,
        const ObjectMetadata& metadata);

    void OnObjectAccessed(const ObjectMetadata& metadata);
    void OnObjectRemoved(ObjectMetadata& metadata);
    void OnObjectHit(const ObjectMetadata& metadata);
    void OnReplicaRemoved(const Replica& replica);
    void OnReplicaAdded(const Replica& replica);
    void OnSegmentRemoved(const UUID& segment_id);

    using OwnerClientSet = std::unordered_set<UUID, boost::hash<UUID>>;
    static auto CollectReplicaOwnerClients(const ObjectMetadata& metadata,
                                           std::string_view key)
        -> tl::expected<OwnerClientSet, ErrorCode>;

    tl::expected<void, ErrorCode> InnerAddReplica(
        MetadataShard& shard, std::string_view key, const UUID& client_id,
        const UUID& segment_id, size_t size,
        const std::shared_ptr<P2PClientMeta>& client) NO_THREAD_SAFETY_ANALYSIS;
    tl::expected<void, ErrorCode> InnerRemoveReplica(
        MetadataShard& shard, std::string_view key, const UUID& client_id,
        const UUID& segment_id) NO_THREAD_SAFETY_ANALYSIS;

   private:
    std::shared_ptr<P2PClientManager> client_manager_;
    std::array<MetadataShard, kNumShards> metadata_shards_;
    uint64_t max_client_per_key_;
    bool enable_ha_;
    ViewVersionId view_version_;
    bool enable_async_oplog_write_{false};
    std::unique_ptr<OpLogManager> oplog_manager_;

    friend class MetadataAccessorRW;
    friend class MetadataAccessorRO;
};

}  // namespace mooncake