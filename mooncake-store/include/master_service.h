#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "allocation_strategy.h"
#include "mutex.h"
#include "segment.h"
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

/*
 * @brief MasterService is the main class for the master server.
 * Lock order: To avoid deadlocks, the following lock order should be followed:
 * 1. client_mutex_
 * 2. metadata_shards_[shard_idx_].mutex
 * 3. segment_mutex_
 */
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
                  ViewVersionId view_version = 0,
                  int64_t client_live_ttl_sec = DEFAULT_CLIENT_LIVE_TTL_SEC,
                  bool enable_ha = false,
                  const std::string& cluster_id = DEFAULT_CLUSTER_ID);
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
     * @brief Get list of replicas for an object
     * @param[out] replica_list Vector to store replica information
     * @return ErrorCode::OK on success, ErrorCode::REPLICA_IS_NOT_READY if not
     * ready
     */
    auto GetReplicaList(std::string_view key)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    /**
     * @brief Get list of replicas for a batch of objects
     * @param[out] batch_replica_list Vector to store replicas information for
     * slices
     */
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys);

    /**
     * @brief Mark a key for garbage collection after specified delay
     * @param key The key to be garbage collected
     * @param delay_ms Delay in milliseconds before removing the key
     * @return ErrorCode::OK on success
     */
    auto MarkForGC(const std::string& key, uint64_t delay_ms)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Start a put operation for an object
     * @param[out] replica_list Vector to store replica information for slices
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if exists,
     *         ErrorCode::NO_AVAILABLE_HANDLE if allocation fails,
     *         ErrorCode::INVALID_PARAMS if slice size is invalid
     */
    auto PutStart(const std::string& key, uint64_t value_length,
                  const std::vector<uint64_t>& slice_lengths,
                  const ReplicateConfig& config)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    /**
     * @brief Complete a put operation
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    auto PutEnd(const std::string& key) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Revoke a put operation
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    auto PutRevoke(const std::string& key) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Start a batch of put operations for N objects
     * @param[out] replica_list Vector to store replica information for slices
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if exists,
     *         ErrorCode::NO_AVAILABLE_HANDLE if allocation fails,
     *         ErrorCode::INVALID_PARAMS if slice size is invalid
     */
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const std::vector<std::string>& keys,
                  const std::vector<uint64_t>& value_lengths,
                  const std::vector<std::vector<uint64_t>>& slice_lengths,
                  const ReplicateConfig& config);

    /**
     * @brief Complete a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const std::vector<std::string>& keys);

    /**
     * @brief Revoke a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const std::vector<std::string>& keys);

    /**
     * @brief Remove an object and its replicas
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found
     */
    auto Remove(const std::string& key) -> tl::expected<void, ErrorCode>;

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
     * @param[out] view_version The view version of the master
     * @param[out] client_status The status of the client from the master
     * @return ErrorCode::OK on success, ErrorCode::INTERNAL_ERROR if the client
     *         ping queue is full
     */
    auto Ping(const UUID& client_id)
        -> tl::expected<std::pair<ViewVersionId, ClientStatus>, ErrorCode>;

    /**
     * @brief Get the master service cluster ID to use as subdirectory name
     * @return ErrorCode::OK on success, ErrorCode::INTERNAL_ERROR if cluster ID
     * is not set
     */
    tl::expected<std::string, ErrorCode> GetFsdir() const;

   private:
    // GC thread function
    void GCThreadFunc();

    // Check all shards and try to evict some keys
    void BatchEvict(double eviction_ratio);

    // Clear invalid handles in all shards
    void ClearInvalidHandles();

    // Internal data structures
    struct ObjectMetadata {
        // RAII-style metric management
        ~ObjectMetadata() { MasterMetricManager::instance().dec_key_count(1); }

        ObjectMetadata() = delete;

        ObjectMetadata(size_t value_length, std::vector<Replica>&& reps)
            : replicas(std::move(reps)),
              size(value_length),
              lease_timeout(std::chrono::steady_clock::now()) {
            MasterMetricManager::instance().inc_key_count(1);
        }

        ObjectMetadata(const ObjectMetadata&) = delete;
        ObjectMetadata& operator=(const ObjectMetadata&) = delete;
        ObjectMetadata(ObjectMetadata&&) = delete;
        ObjectMetadata& operator=(ObjectMetadata&&) = delete;

        std::vector<Replica> replicas;
        size_t size;
        std::chrono::steady_clock::time_point lease_timeout;

        // Check if there is some replica with a different status than the given
        // value. If there is, return the status of the first replica that is
        // not equal to the given value. Otherwise, return false.
        std::optional<ReplicaStatus> HasDiffRepStatus(
            ReplicaStatus status) const {
            for (const auto& replica : replicas) {
                if (replica.status() != status) {
                    return replica.status();
                }
            }
            return {};
        }

        // Grant a lease with timeout as now() + ttl, only update if the new
        // timeout is larger
        void GrantLease(const uint64_t ttl) {
            lease_timeout =
                std::max(lease_timeout, std::chrono::steady_clock::now() +
                                            std::chrono::milliseconds(ttl));
        }

        // Check if the lease has expired
        bool IsLeaseExpired() const {
            return std::chrono::steady_clock::now() >= lease_timeout;
        }

        // Check if the lease has expired
        bool IsLeaseExpired(std::chrono::steady_clock::time_point& now) const {
            return now >= lease_timeout;
        }
    };

    // Segment management
    SegmentManager segment_manager_;
    std::shared_ptr<AllocationStrategy> allocation_strategy_;

    static constexpr size_t kNumShards = 1024;  // Number of metadata shards

    // Sharded metadata maps and their mutexes
    struct MetadataShard {
        mutable Mutex mutex;
        std::unordered_map<std::string, ObjectMetadata> metadata
            GUARDED_BY(mutex);
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
              lock_(&service_->metadata_shards_[shard_idx_].mutex),
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
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != service_->metadata_shards_[shard_idx_].metadata.end();
        }

        // Get metadata (only call when Exists() is true)
        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return it_->second; }

        // Delete current metadata (for PutRevoke or Remove operations)
        void Erase() NO_THREAD_SAFETY_ANALYSIS {
            service_->metadata_shards_[shard_idx_].metadata.erase(it_);
            it_ = service_->metadata_shards_[shard_idx_].metadata.end();
        }

       private:
        MasterService* service_;
        std::string key_;
        size_t shard_idx_;
        MutexLocker lock_;
        std::unordered_map<std::string, ObjectMetadata>::iterator it_;
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

    // cluster id for persistent sub directory
    const std::string cluster_id_;
};

}  // namespace mooncake
