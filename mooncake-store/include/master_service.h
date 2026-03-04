#pragma once

#include <boost/functional/hash.hpp>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "types.h"
#include "rpc_types.h"
#include "replica.h"
#include "master_config.h"

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
    auto ExistKey(const std::string& key) -> tl::expected<bool, ErrorCode>;

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys);

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
    virtual auto GetReplicaList(const std::string& key,
                                const GetReplicaListRequestConfig& config =
                                    GetReplicaListRequestConfig())
        -> tl::expected<GetReplicaListResponse, ErrorCode>;

    /**
     * @brief Remove an object and its replicas
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found
     */
    auto Remove(const std::string& key) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Removes objects from the master whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    auto RemoveByRegex(const std::string& str) -> tl::expected<long, ErrorCode>;

    /**
     * @brief Remove all objects and their replicas
     * @return return the number of objects removed
     */
    long RemoveAll();

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
        bool IsValid() const { return !replicas_.empty() && size_ > 0; }

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
            for (const auto& replica : replicas_) {
                if (IsReplicaAccessible(replica)) {
                    return true;
                }
            }
            return false;
        }

        /**
         * @brief Whether the object is removable
         * @return ErrorCode::OK if removable, otherwise return error specific
         * to the reason
         */
        virtual tl::expected<void, ErrorCode> IsObjectRemovable() const {
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
        mutable Mutex mutex;
        std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>>
            metadata GUARDED_BY(mutex);

        // segment_id -> { key -> replica_reference_count }.
        std::unordered_map<UUID, std::unordered_map<std::string_view, size_t>,
                           boost::hash<UUID>>
            segment_key_index GUARDED_BY(mutex);
    };
    // Virtual function to access shards
    virtual MetadataShard& GetShard(size_t idx) = 0;
    virtual const MetadataShard& GetShard(size_t idx) const = 0;
    virtual size_t GetShardIndex(const std::string& key) const = 0;
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
    class MetadataAccessor {
       public:
        MetadataAccessor(MasterService* service, const std::string& key)
            : service_(service),
              key_(key),
              shard_idx_(service_->GetShardIndex(key)),
              shard_(service_->GetShard(shard_idx_)),
              lock_(&shard_.mutex),
              it_(shard_.metadata.find(key)) {}

        virtual ~MetadataAccessor() = default;

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_.metadata.end();
        }

        MetadataShard& GetShard() NO_THREAD_SAFETY_ANALYSIS { return shard_; }

        const std::string& GetKey() const NO_THREAD_SAFETY_ANALYSIS {
            return it_->first;
        }

        // Get metadata (only call when Exists() is true)
        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return *it_->second; }

        // Delete current metadata.
        // To prevent dangling string_views in segment_key_index, segment index
        // should be cleaned up before erasing the metadata entry.
        void Erase() NO_THREAD_SAFETY_ANALYSIS {
            if (it_ != shard_.metadata.end()) {
                service_->RemoveReplicaFromSegmentIndex(shard_, it_->first,
                                                        it_->second->replicas_);
                shard_.metadata.erase(it_);
                it_ = shard_.metadata.end();
            }
        }

       protected:
        MasterService* service_;
        std::string key_;
        size_t shard_idx_;
        MetadataShard& shard_;
        MutexLocker lock_;
        std::unordered_map<std::string,
                           std::unique_ptr<ObjectMetadata>>::iterator it_;
    };

    virtual std::unique_ptr<MetadataAccessor> GetMetadataAccessor(
        const std::string& key) {
        return std::make_unique<MetadataAccessor>(this, key);
    }

   protected:
    virtual ClientManager& GetClientManager() = 0;
    virtual const ClientManager& GetClientManager() const = 0;

   protected:
    virtual std::vector<Replica::Descriptor> FilterReplicas(
        const GetReplicaListRequestConfig& config,
        const ObjectMetadata& metadata) = 0;

    // The following methods are hooks function to handle special events

    // Triggered when the metadata of an object is accessed (e.g. Get or Exist)
    virtual void OnObjectAccessed(ObjectMetadata& metadata) = 0;

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

    friend class MetadataAccessor;
};

}  // namespace mooncake