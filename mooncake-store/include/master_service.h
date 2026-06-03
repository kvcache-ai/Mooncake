#pragma once

#include <algorithm>
#include <boost/functional/hash.hpp>
#include <functional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "types.h"
#include "rpc_types.h"
#include "replica.h"
#include "master_config.h"
#include "utils.h"

namespace mooncake {
class ClientManager;

/**
 * @brief
 * 1. MasterService is a abstract base class for master server.
 *    This class defines common rpc interfaces that correspond to
 *    WrappedMasterService.
 *
 * 2. The multiple metadata of Master are hierarchical, MasterService manages
 *    metadata of key (ObjectMeta), and its ClientManager manages metadata of
 *    client (ClientMeta), each ClientMeta uses SegmentManager to manage its
 *    segments. The relationship between metadata:
 *    a. Client (1) —— (0..*) Segment
 *    b. Key (1) —— (1..*) Replica
 *    c. Replica (1) —— (1) Segment
 *
 * 3. The lock order of MasterService is:
 *    a. MetadataShard's mutex
 *    b. ClientManager's client_mutex_
 *    c. SegmentManager's segment_mutex_
 *    For avoiding deadlock, each metadata managers should follow this order.
 */
class MasterService {
   public:
    MasterService(const MasterServiceConfig& config);
    virtual ~MasterService() = default;
    virtual void InitializeClientManager();

    /**
     * @brief Register a client with its segments.
     */
    auto RegisterClient(const RegisterClientRequest& req)
        -> tl::expected<RegisterClientResponse, ErrorCode>;

    /**
     * @brief heartbeat interface for client to sync its status
     * @param req HeartbeatRequest containing client_id and tasks
     * @return HeartbeatResponse containing client status, view_version,
     *         and task results
     */
    auto Heartbeat(const HeartbeatRequest& req)
        -> tl::expected<HeartbeatResponse, ErrorCode>;

    /**
     * @brief Queries the status of a client
     */
    auto QueryClientStatus(const QueryClientStatusRequest& req)
        -> tl::expected<QueryClientStatusResponse, ErrorCode>;

    /**
     * @brief Mount a memory segment.
     * @return ErrorCode::SEGMENT_ALREADY_EXISTS if it is already mounted.
     *         ErrorCode::CLIENT_UNHEALTHY if the client is unhealthy.
     */
    auto MountSegment(const Segment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Unmount a memory segment.
     * @return ErrorCode::OK on success,
     *         ErrorCode::SEGMENT_NOT_FOUND if the segment doesn't exist
     *         ErrorCode::CLIENT_UNHEALTHY if the client is unhealthy
     */
    auto UnmountSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Check if an object exists
     * @return ErrorCode::OK if exists, otherwise return other ErrorCode
     */
    auto ExistKey(std::string_view key) -> tl::expected<bool, ErrorCode>;

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string_view>& keys);

    /**
     * @brief Fetch all keys
     * @return ErrorCode::OK if exists
     */
    auto GetAllKeys() -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Fetch all segments, each node has a unique real client with fixed
     * segment name : segment name, preferred format : {ip}:{port}, bad format :
     * localhost:{port}
     * @return ErrorCode::OK if exists
     */
    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Get all segments belonging to a specific client.
     * @param client_id The UUID of the client.
     * @return An expected object containing a vector of segment names on
     * success, or ErrorCode on failure.
     */
    auto GetClientSegments(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Query a segment's capacity and used size in bytes.
     * Conductor should use these information to schedule new requests.
     * @return ErrorCode::OK if exists
     */
    auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode>;

    /**
     * @brief Query IP addresses for a given client ID.
     * @param client_id The UUID of the client to query.
     * @return An expected object containing a vector of IP addresses on success
     * (empty vector if client has no IPs), or ErrorCode::CLIENT_NOT_FOUND if
     * the client doesn't exist, or another ErrorCode on other failures.
     */
    auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Batch query IP addresses for multiple client IDs.
     * @param client_ids Vector of client UUIDs to query.
     * @return An expected object containing a map from client_id to their IP
     * address lists on success, or an ErrorCode on failure. Non-existent
     * clients are omitted from the result map. Clients that exist but have no
     * IPs are included with empty vectors.
     */
    auto BatchQueryIp(const std::vector<UUID>& client_ids) -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode>;

    /**
     * @brief Retrieves replica lists for object keys that match a regex
     * pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing a map from object keys to their
     * replica descriptors on success, or an ErrorCode on failure.
     */
    auto GetReplicaListByRegex(const std::string& regex_pattern)
        -> tl::expected<
            std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
            ErrorCode>;

    /**
     * @brief Get list of replicas for an object
     * @param key The key of the object
     * @param config The filter configuration for the replica list
     * @return An expected object containing the replica list on success, or an
     * ErrorCode on failure.
     */
    virtual auto GetReplicaList(std::string_view key,
                                const GetReplicaListRequestConfig& config =
                                    GetReplicaListRequestConfig())
        -> tl::expected<GetReplicaListResponse, ErrorCode>;

    /**
     * @brief Remove an object and its replicas
     * @param key The key to remove.
     * @param force If true, skip lease and replication task checks.
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found
     */
    auto Remove(std::string_view key, bool force = false)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Removes objects from the master whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @param force If true, skip lease and replication task checks.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    auto RemoveByRegex(std::string_view str, bool force = false)
        -> tl::expected<long, ErrorCode>;

    /**
     * @brief Remove all objects and their replicas
     * @param force If true, skip lease and replication task checks.
     * @return return the number of objects removed
     */
    long RemoveAll(bool force = false);

    /**
     * @brief Get the count of keys
     * @return The count of keys
     */
    size_t GetKeyCount() const;

   protected:
    struct ObjectMetadata {
       public:
        virtual ~ObjectMetadata();

        ObjectMetadata(size_t value_length, std::vector<Replica>&& reps);
        ObjectMetadata() = delete;

        ObjectMetadata(const ObjectMetadata&) = delete;
        ObjectMetadata& operator=(const ObjectMetadata&) = delete;
        ObjectMetadata(ObjectMetadata&&) = delete;
        ObjectMetadata& operator=(ObjectMetadata&&) = delete;

        // Check if the metadata is valid
        // Valid means it has at least one replica and size is greater than 0
        virtual bool IsValid() const {
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

       public:
        // Attention:
        // The MasterService instance will call hook functions based on
        // following status functions. Each subclass of ObjectMetadata should
        // define its own status by overriding these functions.

        /**
         * @brief Whether the object is readable
         * @return true if the object is readable, false otherwise
         */
        virtual bool IsObjectAccessible() const {
            return HasReplica(&Replica::fn_is_completed);
        }

        /**
         * @brief Whether the object is removable
         * @return ErrorCode::OK if removable, otherwise return error specific
         * to the reason
         */
        virtual tl::expected<void, ErrorCode> IsObjectRemovable(
            bool force = false) const {
            return {};
        }

        /**
         * @brief Whether the replica is readable
         * @return true if the replica is readable, false otherwise
         */
        virtual bool IsReplicaAccessible(const Replica& replica) const {
            return true;
        };

        /**
         * @brief Whether the replica is removable
         * @return ErrorCode::OK if removable, otherwise return error specific
         * to the reason
         */
        virtual tl::expected<void, ErrorCode> IsReplicaRemovable(
            const Replica& replica) const {
            return {};
        }

       public:
        std::vector<Replica> replicas_;
        size_t size_;
    };

   protected:
    // Sharded metadata maps and their mutexes.
    // Attention:
    // 1. Each subclass of MasterService should define its own shard and provide
    //    the accessor functions.
    // 2. `segment_key_index` is a reverse index for `metadata` and segment.
    //    Due to the object key in `segment_key_index` is a string_view acquired
    //    from `metadata`, when removing an entry from `metadata`, you MUST
    //    first remove the corresponding key from `segment_key_index`.
    struct MetadataShard {
        mutable SharedMutex mutex;
        std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                           StringHash, std::equal_to<>>
            metadata GUARDED_BY(mutex);

        // segment_id -> { key -> replica_reference_count }.
        std::unordered_map<UUID, std::unordered_map<std::string_view, size_t>,
                           boost::hash<UUID>>
            segment_key_index GUARDED_BY(mutex);
    };

    // For accessing a metadata shard with exclusive (read-write) lock
    class MetadataShardAccessorRW {
       public:
        MetadataShardAccessorRW(MasterService* master_service,
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

    // For accessing a metadata shard with shared (read-only) lock
    class MetadataShardAccessorRO {
       public:
        MetadataShardAccessorRO(const MasterService* master_service,
                                size_t shard_index)
            : shard_(master_service->GetShard(shard_index)),
              lock_(&shard_.mutex, shared_lock) {}

        const MetadataShard* operator->() const { return &shard_; }

       private:
        const MetadataShard& shard_;
        SharedMutexLocker lock_;
    };

    // Virtual function to access shards
    virtual MetadataShard& GetShard(size_t idx) = 0;
    virtual const MetadataShard& GetShard(size_t idx) const = 0;
    virtual size_t GetShardIndex(std::string_view key) const = 0;
    virtual size_t GetShardCount() const = 0;

    // Helpers for maintaining per-shard segment_key_index.
    // 1. Must be called while holding shard.mutex.
    // 2. When add or remove a replica, must call the following functions to
    //    update the segment_key_index.
    void AddReplicaToSegmentIndex(MetadataShard& shard, const std::string& key,
                                  const Replica& replica)
        NO_THREAD_SAFETY_ANALYSIS;
    void RemoveReplicaFromSegmentIndex(
        MetadataShard& shard, const std::string& key,
        const std::vector<Replica>& replicas) NO_THREAD_SAFETY_ANALYSIS;
    void RemoveReplicaFromSegmentIndex(
        MetadataShard& shard, const std::string& key,
        const Replica& replica) NO_THREAD_SAFETY_ANALYSIS;

   protected:
    // Helper class for accessing metadata with automatic locking
    class MetadataAccessorRW {
       public:
        MetadataAccessorRW(MasterService* service, std::string_view key)
            : service_(service),
              shard_idx_(service_->GetShardIndex(key)),
              shard_guard_(service_, shard_idx_),
              it_(shard_guard_->metadata.find(key)) {}

        virtual ~MetadataAccessorRW() = default;

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_guard_->metadata.end() &&
                   it_->second->IsValid();
        }

        const std::string& GetKey() const NO_THREAD_SAFETY_ANALYSIS {
            return it_->first;
        }

        // Get metadata (only call when Exists() is true)
        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return *it_->second; }

        MetadataShardAccessorRW& GetShard() NO_THREAD_SAFETY_ANALYSIS {
            return shard_guard_;
        }

        // Delete current metadata.
        // To prevent dangling string_views in segment_key_index, segment index
        // should be cleaned up before erasing the metadata entry.
        void Erase() NO_THREAD_SAFETY_ANALYSIS {
            if (it_ != shard_guard_->metadata.end()) {
                service_->RemoveReplicaFromSegmentIndex(
                    shard_guard_.GetRef(), it_->first, it_->second->replicas_);
                shard_guard_->metadata.erase(it_);
                it_ = shard_guard_->metadata.end();
            }
        }

       protected:
        MasterService* service_;
        size_t shard_idx_;
        MetadataShardAccessorRW shard_guard_;
        using MetadataMap =
            std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                               StringHash, std::equal_to<>>;
        MetadataMap::iterator it_;
    };

    // Key-level read-only accessor
    class MetadataAccessorRO {
       public:
        MetadataAccessorRO(const MasterService* service, std::string_view key)
            : service_(service),
              shard_idx_(service_->GetShardIndex(key)),
              shard_guard_(service_, shard_idx_),
              it_(shard_guard_->metadata.find(key)) {}

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_guard_->metadata.end() &&
                   it_->second->IsValid();
        }

        // Get metadata (only call when Exists() is true)
        const ObjectMetadata& Get() const NO_THREAD_SAFETY_ANALYSIS {
            return *it_->second;
        }

        const std::string& GetKey() const NO_THREAD_SAFETY_ANALYSIS {
            return it_->first;
        }

       private:
        const MasterService* service_;
        const size_t shard_idx_;
        MetadataShardAccessorRO shard_guard_;
        using MetadataMap =
            std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>,
                               StringHash, std::equal_to<>>;
        MetadataMap::const_iterator it_;
    };

   protected:
    virtual ClientManager& GetClientManager() = 0;
    virtual const ClientManager& GetClientManager() const = 0;

   protected:
    virtual std::vector<Replica::Descriptor> FilterReplicas(
        const GetReplicaListRequestConfig& config,
        const ObjectMetadata& metadata) = 0;

    // The following methods are hooks function to handle special events

    // Triggered when the metadata of an object is accessed (e.g. Get or Exist)
    virtual void OnObjectAccessed(const ObjectMetadata& metadata) = 0;

    // Triggered when the object is removed
    virtual void OnObjectRemoved(ObjectMetadata& metadata);

    // Triggered when the object is hit (e.g. Get)
    virtual void OnObjectHit(const ObjectMetadata& metadata) = 0;

    // Triggered when the replica is removed
    virtual void OnReplicaRemoved(const Replica& replica) = 0;

    // Triggered when the replica is added
    virtual void OnReplicaAdded(const Replica& replica) = 0;

    // Callback for segment removal (triggered by ClientManager via
    // SegmentManager)
    virtual void OnSegmentRemoved(const UUID& segment_id);

   protected:
    // if high availability features enabled
    const bool enable_ha_;
    ViewVersionId view_version_;

    friend class MetadataAccessorRW;
    friend class MetadataAccessorRO;
};

}  // namespace mooncake