#pragma once

#include <boost/functional/hash.hpp>
#include <string>
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
 * @brief MasterService is a abstract base class for master server.
 * This class defines common rpc interfaces that correspond to
 * WrappedMasterService.
 */
class MasterService {
   public:
    MasterService(const MasterServiceConfig& config);
    virtual ~MasterService() = default;
    /**
     * @brief Unmount a memory segment. This function is idempotent.
     * @return ErrorCode::OK on success,
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment is
     *         currently unmounting.
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
     * @brief Batch clear KV cache replicas for specified object keys.
     * @param object_keys Vector of object key strings to clear.
     * @param client_id The UUID of the client that owns the object keys.
     * @param segment_name The name of the segment (storage device) to clear
     * from. If empty, clears replicas from all segments for the given
     * client_id.
     * @return An expected object containing a vector of successfully cleared
     * keys on success, or an ErrorCode on failure. Only successfully
     * cleared keys are included in the result.
     */
    auto BatchReplicaClear(const std::vector<std::string>& object_keys,
                           const UUID& client_id,
                           const std::string& segment_name)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

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
     * @param[out] replica_list Vector to store replica information
     * @return ErrorCode::OK on success, ErrorCode::REPLICA_IS_NOT_READY if not
     * ready
     */
    auto GetReplicaList(std::string_view key)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

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

    /**
     * @brief Heartbeat from client
     * @param client_id The uuid of the client
     * @return PingResponse containing view version and client status
     * @return ErrorCode::OK on success, ErrorCode::INTERNAL_ERROR if the client
     *         ping queue is full
     */
    auto Ping(const UUID& client_id) -> tl::expected<PingResponse, ErrorCode>;

   protected:
    struct ObjectMetadata {
       public:
        virtual ~ObjectMetadata();

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

       protected:
        ObjectMetadata(const UUID& client_id, size_t value_length,
                       std::vector<Replica>&& reps);

       public:
        const UUID client_id_;
        std::vector<Replica> replicas_;
        size_t size_;
    };

   protected:
    // Attention:
    // Sharded metadata maps and their mutexes.
    // Each subclass of MasterService should define its own shard and provide
    // the accessor functions.
    struct MetadataShard {
        mutable Mutex mutex;
        std::unordered_map<std::string, std::unique_ptr<ObjectMetadata>>
            metadata GUARDED_BY(mutex);

       protected:
        MetadataShard() = default;
    };
    // Virtual function to access shards
    virtual MetadataShard& GetShard(size_t idx) = 0;
    virtual const MetadataShard& GetShard(size_t idx) const = 0;
    virtual size_t getShardIndex(const std::string& key) const = 0;
    virtual size_t GetShardCount() const = 0;

   protected:
    // Helper class for accessing metadata with automatic locking
    class MetadataAccessor {
       public:
        MetadataAccessor(MasterService* service, const std::string& key)
            : service_(service),
              key_(key),
              shard_idx_(service_->getShardIndex(key)),
              shard_(service_->GetShard(shard_idx_)),
              lock_(&shard_.mutex),
              it_(shard_.metadata.find(key)) {}

        virtual ~MetadataAccessor() = default;

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_.metadata.end();
        }

        // Get metadata (only call when Exists() is true)
        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return *it_->second; }

        // Delete current metadata (for PutRevoke or Remove operations)
        void Erase() NO_THREAD_SAFETY_ANALYSIS {
            shard_.metadata.erase(it_);
            it_ = shard_.metadata.end();
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
    // The following methods are hooks function to handle special events

    // Triggered when the metadata of an object is accessed (e.g. Get or Exist)
    virtual void OnObjectAccessed(ObjectMetadata& metadata) = 0;

    // Triggered when the object is removed
    virtual void OnObjectRemoved(ObjectMetadata& metadata) = 0;

    // Triggered when the object is hit (e.g. Get)
    virtual void OnObjectHit(const ObjectMetadata& metadata) = 0;

    // Triggered when the replica is removed
    virtual void OnReplicaRemoved(const Replica& replica) = 0;

   protected:
    // if high availability features enabled
    const bool enable_ha_;
    ViewVersionId view_version_;

    friend class MetadataAccessor;
};

}  // namespace mooncake