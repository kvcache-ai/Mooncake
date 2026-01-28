#pragma once

#include <boost/functional/hash.hpp>
#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <ylt/util/tl/expected.hpp>

#include "types.h"
#include "rpc_types.h"
#include "replica.h"
#include "client_manager.h"
#include "master_config.h"

namespace mooncake {

/**
 * @brief MasterService is the abstract base class for the master server.
 * This class defines the common rpc interface that corresponds to
 * WrappedMasterService.
 */
class MasterService {
   public:
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

    /**
     * @brief Heartbeat from client
     * @param client_id The uuid of the client
     * @return PingResponse containing view version and client status
     * @return ErrorCode::OK on success, ErrorCode::INTERNAL_ERROR if the client
     *         ping queue is full
     */
    auto Ping(const UUID& client_id) -> tl::expected<PingResponse, ErrorCode>;

   protected:
    MasterService(const MasterServiceConfig& config);

    // Internal data structures
    struct ObjectMetadata {
        // RAII-style metric management
        ~ObjectMetadata() {
            MasterMetricManager::instance().dec_key_count(1);
            if (soft_pin_timeout) {
                MasterMetricManager::instance().dec_soft_pin_key_count(1);
            }
        }

        ObjectMetadata() = delete;

        ObjectMetadata(
            const UUID& client_id_,
            const std::chrono::steady_clock::time_point put_start_time_,
            size_t value_length, std::vector<Replica>&& reps,
            bool enable_soft_pin)
            : client_id(client_id_),
              put_start_time(put_start_time_),
              replicas(std::move(reps)),
              size(value_length),
              lease_timeout(),
              soft_pin_timeout(std::nullopt) {
            MasterMetricManager::instance().inc_key_count(1);
            if (enable_soft_pin) {
                soft_pin_timeout.emplace();
                MasterMetricManager::instance().inc_soft_pin_key_count(1);
            }
            MasterMetricManager::instance().observe_value_size(value_length);
        }

        ObjectMetadata(const ObjectMetadata&) = delete;
        ObjectMetadata& operator=(const ObjectMetadata&) = delete;
        ObjectMetadata(ObjectMetadata&&) = delete;
        ObjectMetadata& operator=(ObjectMetadata&&) = delete;

        const UUID client_id;
        const std::chrono::steady_clock::time_point put_start_time;

        std::vector<Replica> replicas;
        size_t size;
        // Default constructor, creates a time_point representing
        // the Clock's epoch (i.e., time_since_epoch() is zero).
        std::chrono::steady_clock::time_point lease_timeout;  // hard lease
        std::optional<std::chrono::steady_clock::time_point>
            soft_pin_timeout;  // optional soft pin, only set for vip objects

        // Check if there are some replicas with a different status than the
        // given value. If there are, return the status of the first replica
        // that is not equal to the given value. Otherwise, return false.
        std::optional<ReplicaStatus> HasDiffRepStatus(
            ReplicaStatus status, ReplicaType replica_type) const {
            for (const auto& replica : replicas) {
                if (replica.status() != status &&
                    replica.type() == replica_type) {
                    return replica.status();
                }
            }
            return {};
        }

        // Grant a lease with timeout as now() + ttl, only update if the new
        // timeout is larger
        void GrantLease(const uint64_t ttl, const uint64_t soft_ttl) {
            std::chrono::steady_clock::time_point now =
                std::chrono::steady_clock::now();
            lease_timeout =
                std::max(lease_timeout, now + std::chrono::milliseconds(ttl));
            if (soft_pin_timeout) {
                soft_pin_timeout =
                    std::max(*soft_pin_timeout,
                             now + std::chrono::milliseconds(soft_ttl));
            }
        }

        // Erase all replicas of the given type
        void EraseReplica(ReplicaType replica_type) {
            replicas.erase(
                std::remove_if(replicas.begin(), replicas.end(),
                               [replica_type](const Replica& replica) {
                                   return replica.type() == replica_type;
                               }),
                replicas.end());
        }

        // Check if there is a memory replica
        bool HasMemReplica() const {
            return std::any_of(replicas.begin(), replicas.end(),
                               [](const Replica& replica) {
                                   return replica.type() == ReplicaType::MEMORY;
                               });
        }

        // Get the count of memory replicas
        int GetMemReplicaCount() const {
            return std::count_if(
                replicas.begin(), replicas.end(), [](const Replica& replica) {
                    return replica.type() == ReplicaType::MEMORY;
                });
        }

        // Check if the lease has expired
        bool IsLeaseExpired() const {
            return std::chrono::steady_clock::now() >= lease_timeout;
        }

        // Check if the lease has expired
        bool IsLeaseExpired(std::chrono::steady_clock::time_point& now) const {
            return now >= lease_timeout;
        }

        // Check if is in soft pin status
        bool IsSoftPinned() const {
            return soft_pin_timeout &&
                   std::chrono::steady_clock::now() < *soft_pin_timeout;
        }

        // Check if is in soft pin status
        bool IsSoftPinned(std::chrono::steady_clock::time_point& now) const {
            return soft_pin_timeout && now < *soft_pin_timeout;
        }

        // Check if the metadata is valid
        // Valid means it has at least one replica and size is greater than 0
        bool IsValid() const { return !replicas.empty() && size > 0; }

        bool IsAllReplicasComplete() const {
            return std::all_of(
                replicas.begin(), replicas.end(), [](const Replica& replica) {
                    return replica.status() == ReplicaStatus::COMPLETE;
                });
        }

        bool HasCompletedReplicas() const {
            return std::any_of(
                replicas.begin(), replicas.end(), [](const Replica& replica) {
                    return replica.status() == ReplicaStatus::COMPLETE;
                });
        }

        std::vector<Replica> DiscardProcessingReplicas() {
            auto partition_point = std::partition(
                replicas.begin(), replicas.end(), [](const Replica& replica) {
                    return replica.status() != ReplicaStatus::PROCESSING;
                });

            std::vector<Replica> discarded_replicas;
            if (partition_point != replicas.end()) {
                discarded_replicas.reserve(
                    std::distance(partition_point, replicas.end()));
                std::move(partition_point, replicas.end(),
                          std::back_inserter(discarded_replicas));
                replicas.erase(partition_point, replicas.end());
            }

            return discarded_replicas;
        }
    };

    // Sharded metadata maps and their mutexes
    struct MetadataShard {
        mutable Mutex mutex;
        std::unordered_map<std::string, ObjectMetadata> metadata
            GUARDED_BY(mutex);
        std::unordered_set<std::string> processing_keys GUARDED_BY(mutex);
    };

   protected:
    virtual ClientManager& GetClientManager() = 0;
    virtual const ClientManager& GetClientManager() const = 0;

    // Helper to clean up stale handles pointing to unmounted segments
    virtual bool CleanupStaleHandles(MasterService::ObjectMetadata& metadata) = 0;

    // Helper class for accessing metadata with automatic locking and cleanup
    class MetadataAccessor {
       public:
        MetadataAccessor(MasterService* service, const std::string& key)
            : service_(service),
              key_(key),
              shard_idx_(service_->getShardIndex(key)),
              shard_(service_->metadata_shards_[shard_idx_]),
              lock_(&shard_.mutex),
              it_(shard_.metadata.find(key)),
              processing_it_(shard_.processing_keys.find(key)) {
            // Automatically clean up invalid handles
            if (it_ != shard_.metadata.end()) {
                if (service_->CleanupStaleHandles(it_->second)) {
                    this->Erase();

                    if (processing_it_ != shard_.processing_keys.end()) {
                        this->EraseFromProcessing();
                    }
                }
            }
        }

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_.metadata.end();
        }

        bool InProcessing() const NO_THREAD_SAFETY_ANALYSIS {
            return processing_it_ != shard_.processing_keys.end();
        }

        // Get metadata (only call when Exists() is true)
        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return it_->second; }

        // Delete current metadata (for PutRevoke or Remove operations)
        void Erase() NO_THREAD_SAFETY_ANALYSIS {
            shard_.metadata.erase(it_);
            it_ = shard_.metadata.end();
        }

        void EraseFromProcessing() NO_THREAD_SAFETY_ANALYSIS {
            shard_.processing_keys.erase(processing_it_);
            processing_it_ = shard_.processing_keys.end();
        }

       private:
        MasterService* service_;
        std::string key_;
        size_t shard_idx_;
        MetadataShard& shard_;
        MutexLocker lock_;
        std::unordered_map<std::string, ObjectMetadata>::iterator it_;
        std::unordered_set<std::string>::iterator processing_it_;
    };

   protected:
    static constexpr size_t kNumShards = 1024;  // Number of metadata shards
    // Helper to get shard index from key
    size_t getShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % kNumShards;
    }

    std::array<MetadataShard, kNumShards> metadata_shards_;


    ViewVersionId view_version_;

    // Lease related members
    uint64_t default_kv_lease_ttl_;     // in milliseconds
    uint64_t default_kv_soft_pin_ttl_;  // in milliseconds

    // if high availability features enabled
    const bool enable_ha_;

    friend class MetadataAccessor;
};

}  // namespace mooncake