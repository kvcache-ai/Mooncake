#pragma once

#include <atomic>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "allocation_strategy.h"
#include "allocator.h"
#ifdef USE_KV_EVENT
#include "kv_cache_publisher.h"
#endif
#include "object_metadata.h"
#include "types.h"

namespace mooncake {
// Forward declarations
class AllocationStrategy;
class EvictionStrategy;

// Structure to store garbage collection tasks
struct GCTask {
    std::string key;
    std::chrono::steady_clock::time_point deletion_time;

    GCTask() = default;

    GCTask(const std::string& k, std::chrono::milliseconds delay)
        : key(k), deletion_time(std::chrono::steady_clock::now() + delay) {}

    bool is_ready() const {
        return std::chrono::steady_clock::now() >= deletion_time;
    }
};

class BufferAllocatorManager {
   public:
    BufferAllocatorManager() = default;
    ~BufferAllocatorManager() = default;

    /**
     * @brief Register a new buffer for allocation
     * @param segment_name Name of the memory segment
     * @param base Base address of the memory segment
     * @param size Size of the memory segment
     * @param location_type Type of storage location for the segment
     * @param instance_id Optional LMCache instance ID for notifications
     * @param worker_id Optional LMCache worker ID for notifications
     * @return ErrorCode::OK on success, ErrorCode::INVALID_PARAMS if segment
     * exists
     */
    ErrorCode AddSegment(
        const std::string& segment_name, uint64_t base, uint64_t size,
        LocationType location_type,
        const std::optional<std::string>& instance_id = std::nullopt,
        const std::optional<std::string>& worker_id = std::nullopt);

    /**
     * @brief Unregister a buffer
     * @return ErrorCode::OK on success, ErrorCode::INVALID_PARAMS if segment
     * not found
     */
    ErrorCode RemoveSegment(const std::string& segment_name);

    /**
     * @brief Get the map of buffer allocators
     * @note Caller must hold the mutex while accessing the map
     */
    const std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>&
    GetAllocators() const {
        return buf_allocators_;
    }

    /**
     * @brief Get the mutex for thread-safe access
     */
    std::shared_mutex& GetMutex() const { return allocator_mutex_; }

   private:
    // Protects the buffer allocator map (BufferAllocator is thread-safe by
    // itself)
    mutable std::shared_mutex allocator_mutex_;
    std::unordered_map<std::string, std::shared_ptr<BufferAllocator>>
        buf_allocators_;
};

class MasterService {
   private:
    // Comparator for GC tasks priority queue
    struct GCTaskComparator {
        bool operator()(GCTask* a, GCTask* b) const {
            return a->deletion_time > b->deletion_time;
        }
    };

   public:
    MasterService(bool enable_gc = true,
                  uint64_t default_kv_lease_ttl = DEFAULT_DEFAULT_KV_LEASE_TTL,
                  double eviction_ratio = DEFAULT_EVICTION_RATIO,
                  double eviction_high_watermark_ratio =
                      DEFAULT_EVICTION_HIGH_WATERMARK_RATIO,
                  const std::string& lmcache_controller_url = "");
    ~MasterService();

    /**
     * @brief Mount a memory segment for buffer allocation
     * @param buffer Memory buffer address
     * @param size Size of the buffer
     * @param segment_name Name of the segment
     * @param instance_id Optional LMCache instance ID
     * @param worker_id Optional LMCache worker ID
     * @return ErrorCode::OK on success, ErrorCode::INVALID_PARAMS if segment
     * exists or params invalid, ErrorCode::INTERNAL_ERROR if allocation fails
     */
    ErrorCode MountSegment(
        uint64_t buffer, uint64_t size, const std::string& segment_name,
        const std::optional<std::string>& instance_id = std::nullopt,
        const std::optional<std::string>& worker_id = std::nullopt);

    /**
     * @brief Unmount a memory segment
     * @return ErrorCode::OK on success, ErrorCode::INVALID_PARAMS if segment
     * not found
     */
    ErrorCode UnmountSegment(const std::string& segment_name);

    /**
     * @brief Check if an object exists
     * @return ErrorCode::OK if exists, otherwise return other ErrorCode
     */
    ErrorCode ExistKey(const std::string& key);

    /**
     * @brief Fetch all keys
     * @return ErrorCode::OK if exists
     */
    ErrorCode GetAllKeys(std::vector<std::string>& all_keys);

    /**
     * @brief Fetch all segments, each node has a unique real client with fixed
     * segment name : segment name, preferred format : {ip}:{port}, bad format :
     * localhost:{port}
     * @return ErrorCode::OK if exists
     */
    ErrorCode GetAllSegments(std::vector<std::string>& all_segments);

    /**
     * @brief Query a segment's capacity and used size in bytes.
     * Conductor should use these information to schedule new requests.
     * @return ErrorCode::OK if exists
     */
    ErrorCode QuerySegments(const std::string& segment, size_t& used,
                            size_t& capacity);

    /**
     * @brief Get list of replicas for an object
     * @param[out] replica_list Vector to store replica information
     * @return ErrorCode::OK on success, ErrorCode::REPLICA_IS_NOT_READY if not
     * ready
     */
    ErrorCode GetReplicaList(const std::string& key,
                             std::vector<Replica::Descriptor>& replica_list);

    /**
     * @brief Get list of replicas for a batch of objects
     * @param[out] batch_replica_list Vector to store replicas information for
     * slices
     */
    ErrorCode BatchGetReplicaList(
        const std::vector<std::string>& keys,
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>&
            batch_replica_list);

    /**
     * @brief Mark a key for garbage collection after specified delay
     * @param key The key to be garbage collected
     * @param delay_ms Delay in milliseconds before removing the key
     * @return ErrorCode::OK on success
     */
    ErrorCode MarkForGC(const std::string& key, uint64_t delay_ms);

    /**
     * @brief Start a put operation for an object
     * @param[out] replica_list Vector to store replica information for slices
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if exists,
     *         ErrorCode::NO_AVAILABLE_HANDLE if allocation fails,
     *         ErrorCode::INVALID_PARAMS if slice size is invalid
     */
    ErrorCode PutStart(const std::string& key, uint64_t value_length,
                       const std::vector<uint64_t>& slice_lengths,
                       const ReplicateConfig& config,
                       std::vector<Replica::Descriptor>& replica_list);

    /**
     * @brief Complete a put operation
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    ErrorCode PutEnd(const std::string& key);

    /**
     * @brief Revoke a put operation
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    ErrorCode PutRevoke(const std::string& key);

#ifdef USE_KV_EVENT
    // Helper to send LMCache notifications
    void send_lmcache_notification_internal_(
        const std::string& key, const ObjectMetadata& metadata,
        LMCacheNotifier::NotificationEventType event_type);

    // Helper to send evict notifications
    void SendEvictNotification(const std::string& key,
                               const ObjectMetadata& metadata);
#endif

    /**
     * @brief Start a batch of put operations for N objects
     * @param[out] replica_list Vector to store replica information for slices
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if exists,
     *         ErrorCode::NO_AVAILABLE_HANDLE if allocation fails,
     *         ErrorCode::INVALID_PARAMS if slice size is invalid
     */
    ErrorCode BatchPutStart(
        const std::vector<std::string>& keys,
        const std::unordered_map<std::string, uint64_t>& value_lengths,
        const std::unordered_map<std::string, std::vector<uint64_t>>&
            slice_lengths,
        const ReplicateConfig& config,
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>&
            batch_replica_list);

    /**
     * @brief Complete a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    ErrorCode BatchPutEnd(const std::vector<std::string>& keys);

    /**
     * @brief Revoke a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    ErrorCode BatchPutRevoke(const std::vector<std::string>& keys);

    /**
     * @brief Remove an object and its replicas
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found
     */
    ErrorCode Remove(const std::string& key);

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

   private:
    // GC thread function
    void GCThreadFunc();

    // Check all shards and try to evict some keys
    void BatchEvict(double eviction_ratio);

    // Internal data structures

    // Buffer allocator management
    std::shared_ptr<BufferAllocatorManager> buffer_allocator_manager_;
    std::shared_ptr<AllocationStrategy> allocation_strategy_;

    static constexpr size_t kNumShards = 1024;  // Number of metadata shards

    // Sharded metadata maps and their mutexes
    struct MetadataShard {
        mutable std::mutex mutex;
        std::unordered_map<std::string, ObjectMetadata> metadata;
    };
    std::array<MetadataShard, kNumShards> metadata_shards_;

    // Helper to get shard index from key
    size_t getShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % kNumShards;
    }

    // Helper to clean up stale handles pointing to unmounted segments
    bool CleanupStaleHandles(ObjectMetadata& metadata);

    // GC related members
    static constexpr size_t kGCQueueSize = 10 * 1024;  // Size of the GC queue
    boost::lockfree::queue<GCTask*> gc_queue_{kGCQueueSize};
    std::thread gc_thread_;
    std::atomic<bool> gc_running_{false};
    bool enable_gc_{true};  // Flag to enable/disable garbage collection
    static constexpr uint64_t kGCThreadSleepMs =
        10;  // 10 ms sleep between GC and eviction checks

    // Lease related members
    const uint64_t default_kv_lease_ttl_;  // in milliseconds

    // LMCache notification
#ifdef USE_KV_EVENT
    std::optional<LMCacheNotifier> lmcache_notifier_;
#endif

    // Eviction related members
    std::atomic<bool> need_eviction_{
        false};  // Set to trigger eviction when not enough space left
    const double eviction_ratio_;                 // in range [0.0, 1.0]
    const double eviction_high_watermark_ratio_;  // in range [0.0, 1.0]

    // Helper class for accessing metadata with automatic locking and cleanup
    class MetadataAccessor {
       public:
        MetadataAccessor(MasterService* service, const std::string& key)
            : service_(service),
              key_(key),
              shard_idx_(service_->getShardIndex(key)),
              lock_(service_->metadata_shards_[shard_idx_].mutex),
              it_(service_->metadata_shards_[shard_idx_].metadata.find(key)) {
            // Automatically clean up invalid handles
            if (it_ != service_->metadata_shards_[shard_idx_].metadata.end()) {
                if (service_->CleanupStaleHandles(it_->second)) {
                    service_->metadata_shards_[shard_idx_].metadata.erase(it_);
                    it_ = service_->metadata_shards_[shard_idx_].metadata.end();
                }
            }
        }

        // Check if metadata exists
        bool Exists() const {
            return it_ != service_->metadata_shards_[shard_idx_].metadata.end();
        }

        // Get metadata (only call when Exists() is true)
        ObjectMetadata& Get() { return it_->second; }

        // Delete current metadata (for PutRevoke or Remove operations)
        void Erase() {
            service_->metadata_shards_[shard_idx_].metadata.erase(it_);
            it_ = service_->metadata_shards_[shard_idx_].metadata.end();
        }

        // Create new metadata with replicas and size (only call when !Exists())
        void Create(std::vector<Replica> replicas, size_t size) {
            service_->metadata_shards_[shard_idx_].metadata.try_emplace(
                key_, std::move(replicas), size);
        }

       private:
        MasterService* service_;
        std::string key_;
        size_t shard_idx_;
        std::unique_lock<std::mutex> lock_;
        std::unordered_map<std::string, ObjectMetadata>::iterator it_;
    };

    friend class MetadataAccessor;
};

}  // namespace mooncake
