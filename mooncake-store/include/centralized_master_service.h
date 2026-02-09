#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <cstdint>
#include <list>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "master_metric_manager.h"
#include "master_service.h"
#include "mutex.h"
#include "centralized_client_manager.h"
#include "types.h"
#include "rpc_types.h"
#include "replica.h"

namespace mooncake {

/**
 * @brief The CentralizedMasterService is centralized service implementation for
 * master node. It managers all meta of cluster. The main duty of this master
 * is:
 * 1. Key metadata management (such as routing, allocation, eviction in cluster)
 * 2. Cluster state management (such as segment, client, replica)
 */
class CentralizedMasterService final : public MasterService {
    struct CentralizedMetadataShard;

   public:
    CentralizedMasterService();
    explicit CentralizedMasterService(const MasterServiceConfig& config);
    ~CentralizedMasterService() override;

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
     * @brief Complete a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    auto BatchPutEnd(const UUID& client_id,
                     const std::vector<std::string>& keys)
        -> std::vector<tl::expected<void, ErrorCode>>;

    /**
     * @brief Revoke a put operation, replica_type indicates the type of
     * replica to revoke (memory or disk)
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    auto PutRevoke(const UUID& client_id, const std::string& key,
                   ReplicaType replica_type) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Revoke a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    auto BatchPutRevoke(const UUID& client_id,
                        const std::vector<std::string>& keys)
        -> std::vector<tl::expected<void, ErrorCode>>;

    /**
     * @brief Adds a replica instance associated with the given client and key.
     */
    auto AddReplica(const UUID& client_id, const std::string& key,
                    Replica& replica) -> tl::expected<void, ErrorCode>;

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

   public:
    ClientManager& GetClientManager() override { return client_manager_; }

    const ClientManager& GetClientManager() const override {
        return client_manager_;
    }

   private:
    // Hooks implementation
    void OnObjectAccessed(ObjectMetadata& metadata) override;
    void OnObjectRemoved(ObjectMetadata& metadata) override;
    void OnObjectHit(const ObjectMetadata& metadata) override;
    void OnReplicaRemoved(const Replica& replica) override;

   private:
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

    // Helper to clean up stale handles pointing to unmounted segments
    bool CleanupStaleHandles(ObjectMetadata& metadata);

    /**
     * @brief Helper to discard expired processing keys.
     */
    void DiscardExpiredProcessingKeys(
        CentralizedMetadataShard& shard,
        const std::chrono::steady_clock::time_point& now);

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

   private:
    /**
     * @brief CentralizedObjectMetadata extends ObjectMetadata with lease and
     * ReplicaStatus management for centralized master service.
     */
    struct CentralizedObjectMetadata final : public ObjectMetadata {
       public:
        CentralizedObjectMetadata(
            const UUID& client_id,
            const std::chrono::steady_clock::time_point put_start_time,
            size_t value_length, std::vector<Replica>&& reps,
            bool enable_soft_pin);

        ~CentralizedObjectMetadata() override;

        CentralizedObjectMetadata(const CentralizedObjectMetadata&) = delete;
        CentralizedObjectMetadata& operator=(const CentralizedObjectMetadata&) =
            delete;
        CentralizedObjectMetadata(CentralizedObjectMetadata&&) = delete;
        CentralizedObjectMetadata& operator=(CentralizedObjectMetadata&&) =
            delete;

        // Check if there are some replicas with a different status than the
        // given value. If there are, return the status of the first replica
        // that is not equal to the given value. Otherwise, return std::nullopt.
        std::optional<ReplicaStatus> HasDiffRepStatus(
            ReplicaStatus status, ReplicaType replica_type) const;

        // Grant a lease with timeout as now() + ttl, only update if the new
        // timeout is larger
        void GrantLease(const uint64_t ttl, const uint64_t soft_ttl);

        // Erase all replicas of the given type
        void EraseReplica(ReplicaType type);

        // Check if there is a memory replica
        bool HasMemReplica() const;

        // Get the count of memory replicas
        int GetMemReplicaCount() const;

        // Check if the lease has expired
        bool IsLeaseExpired() const;

        // Check if the lease has expired
        bool IsLeaseExpired(
            const std::chrono::steady_clock::time_point& now) const;

        // Check if is in soft pin status
        bool IsSoftPinned() const;

        // Check if is in soft pin status
        bool IsSoftPinned(
            const std::chrono::steady_clock::time_point& now) const;

        // Check if all replicas are complete
        bool IsAllReplicasComplete() const;

        // Check if has any completed replicas
        bool HasCompletedReplicas() const;

        // Discard all processing replicas and return them
        std::vector<Replica> DiscardProcessingReplicas();

       public:
        // Hook functions
        tl::expected<void, ErrorCode> IsObjectRemovable() const override;
        bool IsReplicaAccessible(const Replica& replica) const override;
        tl::expected<void, ErrorCode> IsReplicaRemovable(
            const Replica& replica) const override;

       public:
        const std::chrono::steady_clock::time_point put_start_time_;
        std::chrono::steady_clock::time_point lease_timeout_;
        std::optional<std::chrono::steady_clock::time_point> soft_pin_timeout_;
    };

   private:
    // Extended MetadataShard with processing_keys for centralized service
    struct CentralizedMetadataShard : public MetadataShard {
        // Keys currently being written (PutStart called, but not yet PutEnd)
        // Protected by the inherited mutex from MetadataShard
        std::unordered_set<std::string> processing_keys GUARDED_BY(mutex);
    };

    // Override GetShard to return our extended shard type
    MetadataShard& GetShard(size_t idx) override {
        return metadata_shards_[idx];
    }
    const MetadataShard& GetShard(size_t idx) const override {
        return metadata_shards_[idx];
    }

    // Helper to get the extended shard with processing_keys
    CentralizedMetadataShard& GetCentralizedShard(size_t idx) {
        return metadata_shards_[idx];
    }
    const CentralizedMetadataShard& GetCentralizedShard(size_t idx) const {
        return metadata_shards_[idx];
    }
    static constexpr size_t kNumShards = 1024;  // Number of metadata shards
    // Helper to get shard index from key
    size_t getShardIndex(const std::string& key) const override {
        return std::hash<std::string>{}(key) % kNumShards;
    }
    size_t GetShardCount() const override { return kNumShards; }

   private:
    class CentralizedMetadataAccessor final
        : public MasterService::MetadataAccessor {
       public:
        CentralizedMetadataAccessor(CentralizedMasterService* service,
                                    const std::string& key)
            : MasterService::MetadataAccessor(service, key),
              c_shard_(static_cast<CentralizedMetadataShard&>(shard_)),
              processing_it_(c_shard_.processing_keys.find(key)) {
            // Automatically clean up invalid handles
            if (Exists()) {
                if (service->CleanupStaleHandles(Get())) {
                    Erase();
                    if (InProcessing()) {
                        EraseFromProcessing();
                    }
                }
            }
        }

        CentralizedObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS {
            return static_cast<CentralizedObjectMetadata&>(
                MasterService::MetadataAccessor::Get());
        }

        bool InProcessing() const NO_THREAD_SAFETY_ANALYSIS {
            return processing_it_ != c_shard_.processing_keys.end();
        }

        void EraseFromProcessing() NO_THREAD_SAFETY_ANALYSIS {
            c_shard_.processing_keys.erase(processing_it_);
            processing_it_ = c_shard_.processing_keys.end();
        }

        // Access the extended shard directly (for inserting processing keys)
        CentralizedMetadataShard& GetCentralizedShard() { return c_shard_; }

       private:
        CentralizedMetadataShard& c_shard_;
        std::unordered_set<std::string>::iterator processing_it_;
    };

    std::unique_ptr<MetadataAccessor> GetMetadataAccessor(
        const std::string& key) override {
        return std::make_unique<CentralizedMetadataAccessor>(this, key);
    }

   private:
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
    Mutex discarded_replicas_mutex_;
    std::list<DiscardedReplicas> discarded_replicas_
        GUARDED_BY(discarded_replicas_mutex_);

   private:
    std::array<CentralizedMetadataShard, kNumShards> metadata_shards_;

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
    CentralizedClientManager client_manager_;
    BufferAllocatorType memory_allocator_type_;

    // Discarded replicas management
    const std::chrono::seconds put_start_discard_timeout_sec_;
    const std::chrono::seconds put_start_release_timeout_sec_;
    friend class CentralizedMetadataAccessor;
};

}  // namespace mooncake
