#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "allocation_strategy.h"
#include "master_metric_manager.h"
#include "mutex.h"
#include "segment.h"
#include "types.h"
#include "master_config.h"
#include "rpc_types.h"
#include "replica.h"
#include "oplog_manager.h"
#include "metadata_store.h"

namespace mooncake {
// Forward declarations
class AllocationStrategy;
class EvictionStrategy;
class BufferAllocatorBase;
struct StandbyObjectMetadata;
// ReplicationService forward declaration removed - using etcd-based OpLog sync instead

/*
 * @brief MasterService is the main class for the master server.
 * Lock order: To avoid deadlocks, the following lock order should be followed:
 * 1. client_mutex_
 * 2. metadata_shards_[shard_idx_].mutex
 * 3. segment_mutex_
 */
class MasterService {
   public:
    MasterService();
    MasterService(const MasterServiceConfig& config);
    ~MasterService();

    /**
     * @brief Mount a memory segment for buffer allocation. This function is
     * idempotent.
     * @return ErrorCode::OK on success,
     *         ErrorCode::INVALID_PARAMS on invalid parameters,
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment cannot
     *         be mounted temporarily,
     *         ErrorCode::INTERNAL_ERROR on internal errors.
     */
    auto MountSegment(const Segment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Re-mount segments, invoked when the client is the first time to
     * connect to the master or the client Ping TTL is expired and need
     * to remount. This function is idempotent. Client should retry if the
     * return code is not ErrorCode::OK.
     * @return ErrorCode::OK means either all segments are remounted
     * successfully or the fail is not solvable by a new remount request.
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment cannot
     *         be mounted temporarily.
     *         ErrorCode::INTERNAL_ERROR if something temporary error happens.
     */
    auto ReMountSegment(const std::vector<Segment>& segments,
                        const UUID& client_id) -> tl::expected<void, ErrorCode>;

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

    // Restore metadata from a Standby snapshot (fast failover).
    // NOTE: This is used only on the node that was running HotStandbyService
    // right before it was promoted to leader.
    void RestoreFromStandbySnapshot(
        const std::vector<std::pair<std::string, StandbyObjectMetadata>>& snapshot,
        uint64_t initial_oplog_sequence_id);

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
     * @brief Start a put operation for an object
     * @param[out] replica_list Vector to store replica information for the
     * slice
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if exists,
     *         ErrorCode::NO_AVAILABLE_HANDLE if allocation fails,
     *         ErrorCode::INVALID_PARAMS if slice size is invalid
     */
    auto PutStart(const UUID& client_id, const std::string& key,
                  const uint64_t slice_length, const ReplicateConfig& config)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    /**
     * @brief Complete a put operation, replica_type indicates the type of
     * replica to complete (memory or disk)
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    auto PutEnd(const UUID& client_id, const std::string& key,
                ReplicaType replica_type) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Adds a replica instance associated with the given client and key.
     */
    auto AddReplica(const UUID& client_id, const std::string& key,
                    Replica& replica) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Revoke a put operation, replica_type indicates the type of
     * replica to revoke (memory or disk)
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    auto PutRevoke(const UUID& client_id, const std::string& key,
                   ReplicaType replica_type) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Complete a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const UUID& client_id, const std::vector<std::string>& keys);

    /**
     * @brief Revoke a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const UUID& client_id, const std::vector<std::string>& keys);

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

    /**
     * @brief Get the master service cluster ID to use as subdirectory name
     * @return ErrorCode::OK on success, ErrorCode::INTERNAL_ERROR if cluster ID
     * is not set
     */
    tl::expected<std::string, ErrorCode> GetFsdir() const;

    /**
     * @brief Get storage backend configuration including eviction settings
     * @return GetStorageConfigResponse containing fsdir, enable_disk_eviction,
     * and quota_bytes
     */
    tl::expected<GetStorageConfigResponse, ErrorCode> GetStorageConfig() const;

    /**
     * @brief Get OpLogManager reference for external access
     * @return Reference to the OpLogManager instance
     */
    OpLogManager& GetOpLogManager();

    // SetReplicationService removed - using etcd-based OpLog sync instead

    /**
     * @brief Mounts a file storage segment into the master.
     * @param enable_offloading If true, enables offloading (write-to-file).
     */
    auto MountLocalDiskSegment(const UUID& client_id, bool enable_offloading)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Heartbeat call to collect object-level statistics and retrieve the
     * set of non-offloaded objects.
     * @param enable_offloading Indicates whether offloading is enabled for this
     * segment.
     */
    auto OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading)
        -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode>;

    /**
     * @brief Notifies the master that offloading of specified objects has
     * succeeded.
     * @param keys         A list of object keys (names) that were successfully
     * offloaded.
     * @param metadatas    The corresponding metadata for each offloaded object,
     * including size, storage location, etc.
     */
    auto NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::vector<StorageObjectMetadata>& metadatas)
        -> tl::expected<void, ErrorCode>;

   private:
    /**
     * @brief Helper function to append OpLog entry
     * @param type Operation type
     * @param key Object key
     * @param payload Optional payload data
     */
    void AppendOpLogAndNotify(OpType type, const std::string& key,
                             const std::string& payload = std::string());

    // Resolve the key to a sanitized format for storage
    std::string SanitizeKey(const std::string& key) const;
    std::string ResolvePath(const std::string& key) const;

    // BatchEvict evicts objects in a near-LRU way, i.e., prioritizes to evict
    // object with smaller lease timeout. It has two passes. The first pass only
    // evicts objects without soft pin. The second pass prioritizes objects
    // without soft pin, but also allows to evict soft pinned objects if
    // allow_evict_soft_pinned_objects_ is true. The first pass tries fulfill
    // evict ratio target. If the actual evicted ratio is less than
    // evict_ratio_lowerbound, the second pass will be triggered and try to
    // fulfill evict ratio lowerbound.
    void BatchEvict(double evict_ratio_target, double evict_ratio_lowerbound);

    // Clear invalid handles in all shards
    void ClearInvalidHandles();

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

    /**
     * @brief Serialize ObjectMetadata to JSON string for OpLog payload
     * @param metadata The metadata to serialize
     * @return JSON string containing the serialized metadata
     */
    std::string SerializeMetadataForOpLog(const ObjectMetadata& metadata) const;

    static constexpr size_t kNumShards = 1024;  // Number of metadata shards

    // Sharded metadata maps and their mutexes
    struct MetadataShard {
        mutable Mutex mutex;
        std::unordered_map<std::string, ObjectMetadata> metadata
            GUARDED_BY(mutex);
        std::unordered_set<std::string> processing_keys GUARDED_BY(mutex);
    };
    std::array<MetadataShard, kNumShards> metadata_shards_;

    // Helper to get shard index from key
    size_t getShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % kNumShards;
    }

    // Helper to clean up stale handles pointing to unmounted segments
    bool CleanupStaleHandles(ObjectMetadata& metadata);

    /**
     * @brief Helper to discard expired processing keys.
     */
    void DiscardExpiredProcessingKeys(
        MetadataShard& shard, const std::chrono::steady_clock::time_point& now);

    /**
     * @brief Helper to release space of expired discarded replicas.
     * @return Number of released objects that have memory replicas
     */
    uint64_t ReleaseExpiredDiscardedReplicas(
        const std::chrono::steady_clock::time_point& now);

    // Eviction thread function
    void EvictionThreadFunc();

    tl::expected<void, ErrorCode> PushOffloadingQueue(const std::string& key,
                                                      const Replica& replica);

    // Lease related members
    const uint64_t default_kv_lease_ttl_;     // in milliseconds
    const uint64_t default_kv_soft_pin_ttl_;  // in milliseconds
    const bool allow_evict_soft_pinned_objects_;

    // Eviction related members
    std::atomic<bool> need_eviction_{
        false};  // Set to trigger eviction when not enough space left
    const double eviction_ratio_;                 // in range [0.0, 1.0]
    const double eviction_high_watermark_ratio_;  // in range [0.0, 1.0]

    // Eviction thread related members
    std::thread eviction_thread_;
    std::atomic<bool> eviction_running_{false};
    static constexpr uint64_t kEvictionThreadSleepMs =
        10;  // 10 ms sleep between eviction checks

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

    friend class MetadataAccessor;

    ViewVersionId view_version_;

    // Client related members
    mutable std::shared_mutex client_mutex_;
    std::unordered_set<UUID, boost::hash<UUID>>
        ok_client_;  // client with ok status
    void ClientMonitorFunc();
    std::thread client_monitor_thread_;
    std::atomic<bool> client_monitor_running_{false};
    static constexpr uint64_t kClientMonitorSleepMs =
        1000;  // 1000 ms sleep between client monitor checks
    // boost lockfree queue requires trivial assignment operator
    struct PodUUID {
        uint64_t first;
        uint64_t second;
    };
    static constexpr size_t kClientPingQueueSize =
        128 * 1024;  // Size of the client ping queue
    boost::lockfree::queue<PodUUID> client_ping_queue_{kClientPingQueueSize};
    const int64_t client_live_ttl_sec_;

    // if high availability features enabled
    const bool enable_ha_;

    const bool enable_offload_;

    // cluster id for persistent sub directory
    const std::string cluster_id_;
    // root filesystem directory for persistent storage
    const std::string root_fs_dir_;
    // global 3fs/nfs segment size
    int64_t global_file_segment_size_;
    // storage backend eviction configuration
    const bool enable_disk_eviction_;
    const uint64_t quota_bytes_;

    bool use_disk_replica_{false};

    // Segment management
    SegmentManager segment_manager_;
    BufferAllocatorType memory_allocator_type_;

    // Keep dummy allocators alive for memory replicas restored from standby.
    // AllocatedBuffer stores allocator as weak_ptr; without an owning shared_ptr,
    // the allocator would expire immediately and transport_endpoint_ would be lost
    // when re-serializing Replica descriptors.
    std::unordered_map<std::string, std::shared_ptr<BufferAllocatorBase>>
        standby_allocator_keepalive_;

    // Operation log manager for hot-standby replication. It records
    // state-changing operations so that a standby master can replay them.
    OpLogManager oplog_manager_;
    
    // ReplicationService removed - using etcd-based OpLog sync instead
    
    std::shared_ptr<AllocationStrategy> allocation_strategy_;

    // Discarded replicas management
    const std::chrono::seconds put_start_discard_timeout_sec_;
    const std::chrono::seconds put_start_release_timeout_sec_;
    class DiscardedReplicas {
       public:
        DiscardedReplicas() = delete;

        DiscardedReplicas(std::vector<Replica>&& replicas,
                          std::chrono::steady_clock::time_point ttl)
            : replicas_(std::move(replicas)), ttl_(ttl), mem_size_(0) {
            for (auto& replica : replicas_) {
                mem_size_ += replica.get_memory_buffer_size();
            }
            MasterMetricManager::instance().inc_put_start_discard_cnt(
                1, mem_size_);
        }

        ~DiscardedReplicas() {
            MasterMetricManager::instance().inc_put_start_release_cnt(
                1, mem_size_);
        }

        uint64_t memSize() const { return mem_size_; }

        bool isExpired(const std::chrono::steady_clock::time_point& now) const {
            return ttl_ <= now;
        }

       private:
        std::vector<Replica> replicas_;
        std::chrono::steady_clock::time_point ttl_;
        uint64_t mem_size_;
    };
    std::mutex discarded_replicas_mutex_;
    std::list<DiscardedReplicas> discarded_replicas_
        GUARDED_BY(discarded_replicas_mutex_);
    size_t offloading_queue_limit_ = 50000;
};

}  // namespace mooncake