#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <list>
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
#include "master_metric_manager.h"
#include "mutex.h"
#include "segment.h"
#include "types.h"
#include "master_config.h"
#include "rpc_types.h"
#include "replica.h"
#include "serialize/serializer_backend.h"
#include "task_manager.h"

namespace mooncake {
// Forward declarations
class AllocationStrategy;
class EvictionStrategy;

// Forward declarations for test classes
namespace test {
class MasterServiceSnapshotTestBase;
}  // namespace test

/*
 * @brief MasterService is the main class for the master server.
 * Lock order: To avoid deadlocks, the following lock order should be followed:
 * 1. client_mutex_
 * 2. metadata_shards_[shard_idx_].mutex
 * 3. segment_mutex_
 */
class MasterService {
    // Test friend class for snapshot/restore testing
    friend class test::MasterServiceSnapshotTestBase;

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
    auto GetReplicaList(const std::string& key)
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
     * @brief Start a copy operation
     *
     * This will allocate replica buffers to copy to.
     *
     * @param client_id the client that submit the CopyStart request
     * @param key key of the object
     * @param src_segment source segment name of the replica to copy from
     * @param tgt_segments target segment names of the replicas to copy to
     *
     * @return allocated replicas on success, or ErrorCode indicating the
     * failure reason
     */
    tl::expected<CopyStartResponse, ErrorCode> CopyStart(
        const UUID& client_id, const std::string& key,
        const std::string& src_segment,
        const std::vector<std::string>& tgt_segments);

    tl::expected<void, ErrorCode> CopyEnd(const UUID& client_id,
                                          const std::string& key);

    tl::expected<void, ErrorCode> CopyRevoke(const UUID& client_id,
                                             const std::string& key);

    /**
     * @brief Start a move operation
     *
     * This will allocate replica buffer to move to
     *
     * @param client_id the client that submit the MoveStart request
     * @param key key of the object
     * @param src_segment source segment name of the replica to move from
     * @param tgt_segment target segment name of the replica to move to
     *
     * @return allocated replica on success, or ErrorCode indicating the
     * failure reason
     */
    tl::expected<MoveStartResponse, ErrorCode> MoveStart(
        const UUID& client_id, const std::string& key,
        const std::string& src_segment, const std::string& tgt_segment);

    tl::expected<void, ErrorCode> MoveEnd(const UUID& client_id,
                                          const std::string& key);

    tl::expected<void, ErrorCode> MoveRevoke(const UUID& client_id,
                                             const std::string& key);

    /**
     * @brief Remove an object and its replicas
     * @param key The key to remove.
     * @param force If true, skip lease and replication task checks.
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found
     */
    auto Remove(const std::string& key, bool force = false)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Removes objects from the master whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @param force If true, skip lease and replication task checks.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    auto RemoveByRegex(const std::string& str, bool force = false)
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

    /**
     * @brief Create a copy task to copy an object's replicas to target segments
     * @return Copy task ID on success, ErrorCode on failure
     */
    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::vector<std::string>& targets);

    /**
     * @brief Create a move task to move an object's replica from source segment
     * to target segment
     * @return Move task ID on success, ErrorCode on failure
     */
    tl::expected<UUID, ErrorCode> CreateMoveTask(const std::string& key,
                                                 const std::string& source,
                                                 const std::string& target);

    /**
     * @brief Query the status of a task
     * @return Task basic info
     */
    tl::expected<QueryTaskResponse, ErrorCode> QueryTask(const UUID& task_id);

    /**
     * @brief fetch tasks assigned to a client
     * @return list of tasks
     */
    tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        const UUID& client_id, size_t batch_size);

    /**
     * @brief Mark the task as complete
     * @param client_id Client ID
     * @param request Task complete request
     * @return ErrorCode::OK on success, ErrorCode on failure
     */
    tl::expected<void, ErrorCode> MarkTaskToComplete(
        const UUID& client_id, const TaskCompleteRequest& request);

   private:
    // Resolve the key to a sanitized format for storage
    std::string SanitizeKey(const std::string& key) const;
    std::string ResolvePath(const std::string& key) const;

    void SnapshotThreadFunc();

    // Persist master state
    tl::expected<void, SerializationError> PersistState(
        const std::string& snapshot_id);

    tl::expected<void, SerializationError> UploadSnapshotFile(
        const std::vector<uint8_t>& data, const std::string& path,
        const std::string& local_filename, const std::string& snapshot_id);

    void CleanupOldSnapshot(int keep_count, const std::string& snapshot_id);

    // Restore master state
    void RestoreState();

    void WaitForSnapshotChild(pid_t pid, const std::string& snapshot_id,
                              int log_pipe_fd);

    void HandleChildTimeout(pid_t pid, const std::string& snapshot_id);
    void HandleChildExit(pid_t pid, int status, const std::string& snapshot_id);

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

    std::string FormatTimestamp(
        const std::chrono::system_clock::time_point& tp);
    // We need to clean up finished tasks periodically to avoid memory leak
    // And also we can add some task ttl mechanism in the future
    void TaskCleanupThreadFunc();

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
            const std::chrono::system_clock::time_point put_start_time_,
            size_t value_length, std::vector<Replica>&& reps,
            bool enable_soft_pin)
            : client_id(client_id_),
              put_start_time(put_start_time_),
              size(value_length),
              lease_timeout(),
              soft_pin_timeout(std::nullopt),
              replicas_(std::move(reps)) {
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
        const std::chrono::system_clock::time_point put_start_time;
        const size_t size;

        mutable SpinLock lock;
        // Default constructor, creates a time_point representing
        // the Clock's epoch (i.e., time_since_epoch() is zero).
        mutable std::chrono::system_clock::time_point lease_timeout
            GUARDED_BY(lock);  // hard lease
        mutable std::optional<std::chrono::system_clock::time_point>
            soft_pin_timeout GUARDED_BY(lock);  // optional soft pin, only
                                                // set for vip objects

        void AddReplicas(std::vector<Replica>&& replicas) {
            replicas_.insert(replicas_.end(),
                             std::move_iterator(replicas.begin()),
                             std::move_iterator(replicas.end()));
        }

        std::vector<Replica> PopReplicas(
            const std::function<bool(const Replica&)>& pred_fn) {
            auto partition_point =
                std::partition(replicas_.begin(), replicas_.end(),
                               [pred_fn](const Replica& replica) {
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
                    num_visited++;
                }
            }

            return num_visited;
        }

        size_t VisitReplicas(
            const std::function<bool(const Replica&)>& pred_fn,
            const std::function<void(const Replica&)>& visit_fn) const {
            size_t num_visited = 0;

            for (auto& replica : replicas_) {
                if (pred_fn(replica)) {
                    visit_fn(replica);
                    num_visited++;
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

        const std::vector<Replica>& GetAllReplicas() const { return replicas_; }

        std::optional<ReplicaStatus> HasDiffRepStatus(
            ReplicaStatus status) const {
            for (const auto& replica : replicas_) {
                if (replica.status() != status) {
                    return replica.status();
                }
            }
            return {};
        }

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

        // Grant a lease with timeout as now() + ttl, only update if the new
        // timeout is larger
        void GrantLease(const uint64_t ttl, const uint64_t soft_ttl) const {
            SpinLocker locker(&lock);
            std::chrono::system_clock::time_point now =
                std::chrono::system_clock::now();
            lease_timeout =
                std::max(lease_timeout, now + std::chrono::milliseconds(ttl));
            if (soft_pin_timeout) {
                soft_pin_timeout =
                    std::max(*soft_pin_timeout,
                             now + std::chrono::milliseconds(soft_ttl));
            }
        }

        // Check if the lease has expired
        bool IsLeaseExpired() const {
            SpinLocker locker(&lock);
            return std::chrono::system_clock::now() >= lease_timeout;
        }

        // Check if the lease has expired
        bool IsLeaseExpired(std::chrono::system_clock::time_point& now) const {
            SpinLocker locker(&lock);
            return now >= lease_timeout;
        }

        // Check if is in soft pin status
        bool IsSoftPinned() const {
            SpinLocker locker(&lock);
            return soft_pin_timeout &&
                   std::chrono::system_clock::now() < *soft_pin_timeout;
        }

        // Check if is in soft pin status
        bool IsSoftPinned(std::chrono::system_clock::time_point& now) const {
            SpinLocker locker(&lock);
            return soft_pin_timeout && now < *soft_pin_timeout;
        }

        // Check if the metadata is valid
        // Valid means it has at least one valid replica and size is greater
        // than 0
        bool IsValid() const {
            return size > 0 && HasReplica([](const Replica& replica) {
                       return !replica.is_memory_replica() ||
                              !replica.has_invalid_mem_handle();
                   });
        }

        std::vector<std::string> GetReplicaSegmentNames() const {
            std::vector<std::string> segment_names;
            for (const auto& replica : replicas_) {
                const auto& segment_name_options = replica.get_segment_names();
                for (const auto& segment_name_opt : segment_name_options) {
                    if (segment_name_opt.has_value()) {
                        segment_names.push_back(segment_name_opt.value());
                    }
                }
            }
            return segment_names;
        }

       private:
        // Use the accessors to visit and modify the replicas.
        std::vector<Replica> replicas_;
    };

    struct ReplicationTask {
        UUID client_id;
        std::chrono::system_clock::time_point start_time;
        enum class Type {
            COPY,
            MOVE,
        } type;
        ReplicaID source_id;
        std::vector<ReplicaID> replica_ids;
    };

    static constexpr size_t kNumShards = 1024;  // Number of metadata shards

    // Sharded metadata maps and their mutexes
    struct MetadataShard {
        mutable SharedMutex mutex;
        std::unordered_map<std::string, ObjectMetadata> metadata
            GUARDED_BY(mutex);
        std::unordered_set<std::string> processing_keys GUARDED_BY(mutex);
        std::unordered_map<std::string, const ReplicationTask> replication_tasks
            GUARDED_BY(mutex);
    };
    std::array<MetadataShard, kNumShards> metadata_shards_;

    // For accessing a metadata shard with read-write permission
    class MetadataShardAccessorRW {
       public:
        MetadataShardAccessorRW(MasterService* master_service,
                                size_t shard_index)
            : shard_(master_service->metadata_shards_[shard_index]),
              lock_(&shard_.mutex) {}

        MetadataShard* operator->() { return &shard_; }

        const MetadataShard* operator->() const { return &shard_; }

       private:
        MetadataShard& shard_;
        SharedMutexLocker lock_;
    };

    // For accessing a metadata shard with read-only permission
    class MetadataShardAccessorRO {
       public:
        MetadataShardAccessorRO(const MasterService* master_service,
                                size_t shard_index)
            : shard_(master_service->metadata_shards_[shard_index]),
              lock_(&shard_.mutex, shared_lock) {}

        const MetadataShard* operator->() const { return &shard_; }

       private:
        const MetadataShard& shard_;
        SharedMutexLocker lock_;
    };

    // Helper to get shard index from key
    size_t getShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % kNumShards;
    }

    // Helper to clean up stale handles pointing to unmounted segments
    bool CleanupStaleHandles(ObjectMetadata& metadata);

    /**
     * @brief Helper to discard expired processing keys.
     */
    void DiscardExpiredProcessingReplicas(
        MetadataShardAccessorRW& shard,
        const std::chrono::system_clock::time_point& now);

    /**
     * @brief Helper to release space of expired discarded replicas.
     * @return Number of released objects that have memory replicas
     */
    uint64_t ReleaseExpiredDiscardedReplicas(
        const std::chrono::system_clock::time_point& now);

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

    std::thread snapshot_thread_;
    std::atomic<bool> snapshot_running_{false};
    // Task cleanup thread related members
    std::thread task_cleanup_thread_;
    std::atomic<bool> task_cleanup_running_{false};
    static constexpr uint64_t kTaskCleanupThreadSleepMs =
        30000;  // 30000 ms sleep between task cleanup checks

    // Used to wake task cleanup thread immediately during shutdown.
    std::mutex task_cleanup_mutex_;
    std::condition_variable task_cleanup_cv_;

    // Helper class for accessing metadata with automatic locking and cleanup
    class MetadataAccessorRW {
       public:
        MetadataAccessorRW(MasterService* service, const std::string& key)
            : service_(service),
              key_(key),
              shard_idx_(service_->getShardIndex(key)),
              shard_guard_(service_, shard_idx_),
              it_(shard_guard_->metadata.find(key)),
              processing_it_(shard_guard_->processing_keys.find(key)),
              replication_task_it_(shard_guard_->replication_tasks.find(key)) {
            // Automatically clean up invalid handles
            if (it_ != shard_guard_->metadata.end()) {
                if (service_->CleanupStaleHandles(it_->second)) {
                    this->Erase();

                    if (processing_it_ != shard_guard_->processing_keys.end()) {
                        this->EraseFromProcessing();
                    }
                }
            }
        }

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_guard_->metadata.end() && it_->second.IsValid();
        }

        bool InProcessing() const NO_THREAD_SAFETY_ANALYSIS {
            return processing_it_ != shard_guard_->processing_keys.end();
        }

        bool HasReplicationTask() const NO_THREAD_SAFETY_ANALYSIS {
            return replication_task_it_ !=
                   shard_guard_->replication_tasks.end();
        }

        MetadataShardAccessorRW& GetShard() NO_THREAD_SAFETY_ANALYSIS {
            return shard_guard_;
        }

        // Get metadata (only call when Exists() is true)
        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return it_->second; }

        const ReplicationTask& GetReplicationTask() NO_THREAD_SAFETY_ANALYSIS {
            return replication_task_it_->second;
        }

        // Delete current metadata (for PutRevoke or Remove operations)
        void Erase() NO_THREAD_SAFETY_ANALYSIS {
            shard_guard_->metadata.erase(it_);
            it_ = shard_guard_->metadata.end();
        }

        void EraseFromProcessing() NO_THREAD_SAFETY_ANALYSIS {
            shard_guard_->processing_keys.erase(processing_it_);
            processing_it_ = shard_guard_->processing_keys.end();
        }

        void EraseReplicationTask() NO_THREAD_SAFETY_ANALYSIS {
            shard_guard_->replication_tasks.erase(replication_task_it_);
            replication_task_it_ = shard_guard_->replication_tasks.end();
        }

        void Create(const UUID& client_id, uint64_t total_length,
                    std::vector<Replica> replicas, bool enable_soft_pin) {
            if (Exists()) {
                throw std::logic_error("Already exists");
            }
            const auto now = std::chrono::system_clock::now();
            auto result = shard_guard_->metadata.emplace(
                std::piecewise_construct, std::forward_as_tuple(key_),
                std::forward_as_tuple(client_id, now, total_length,
                                      std::move(replicas), enable_soft_pin));
            it_ = result.first;
        }

       private:
        MasterService* service_;
        std::string key_;
        size_t shard_idx_;
        MetadataShardAccessorRW shard_guard_;
        std::unordered_map<std::string, ObjectMetadata>::iterator it_;
        std::unordered_set<std::string>::iterator processing_it_;
        std::unordered_map<std::string, const ReplicationTask>::iterator
            replication_task_it_;
    };

    class MetadataSerializer {
       public:
        MetadataSerializer(MasterService* service) : service_(service) {}

        // Serialize metadata of all shards
        tl::expected<std::vector<uint8_t>, SerializationError> Serialize();

        tl::expected<void, SerializationError> Deserialize(
            const std::vector<uint8_t>& data);

        void Reset();

       private:
        MasterService* service_;

        // Serialize a single ObjectMetadata
        tl::expected<void, SerializationError> SerializeMetadata(
            const ObjectMetadata& metadata, MsgpackPacker& packer) const;

        // Deserialize a single ObjectMetadata
        [[nodiscard]] tl::expected<std::unique_ptr<ObjectMetadata>,
                                   SerializationError>
        DeserializeMetadata(const msgpack::object& obj) const;

        // Serialize a single MetadataShard
        tl::expected<void, SerializationError> SerializeShard(
            const MetadataShard& shard, MsgpackPacker& packer) const;

        // Deserialize a single MetadataShard
        tl::expected<void, SerializationError> DeserializeShard(
            const msgpack::object& obj, MetadataShard& shard);

        // Serialize discarded replicas
        tl::expected<void, SerializationError> SerializeDiscardedReplicas(
            MsgpackPacker& packer) const;

        // Deserialize discarded replicas
        tl::expected<void, SerializationError> DeserializeDiscardedReplicas(
            const msgpack::object& obj);
    };

    friend class MetadataAccessor;
    class MetadataAccessorRO {
       public:
        MetadataAccessorRO(const MasterService* service, const std::string& key)
            : service_(service),
              key_(key),
              shard_idx_(service_->getShardIndex(key)),
              shard_guard_(service_, shard_idx_),
              it_(shard_guard_->metadata.find(key)),
              processing_it_(shard_guard_->processing_keys.find(key)) {}

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return it_ != shard_guard_->metadata.end() && it_->second.IsValid();
        }

        bool InProcessing() const NO_THREAD_SAFETY_ANALYSIS {
            return processing_it_ != shard_guard_->processing_keys.end();
        }

        // Get metadata (only call when Exists() is true)
        const ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS {
            return it_->second;
        }

        MetadataShardAccessorRO& GetShard() NO_THREAD_SAFETY_ANALYSIS {
            return shard_guard_;
        }

       private:
        const MasterService* service_;
        const std::string key_;
        const size_t shard_idx_;
        MetadataShardAccessorRO shard_guard_;
        std::unordered_map<std::string, ObjectMetadata>::const_iterator it_;
        std::unordered_set<std::string>::const_iterator processing_it_;
    };

    friend class MetadataAccessorRW;
    friend class MetadataAccessorRO;

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
    std::shared_ptr<AllocationStrategy> allocation_strategy_;

    bool enable_snapshot_restore_ = false;

    bool enable_snapshot_ = false;
    std::string snapshot_backup_dir_ = DEFAULT_SNAPSHOT_BACKUP_DIR;
    uint64_t snapshot_interval_seconds_ = DEFAULT_SNAPSHOT_INTERVAL_SEC;
    uint64_t snapshot_child_timeout_seconds_ =
        DEFAULT_SNAPSHOT_CHILD_TIMEOUT_SEC;
    std::unique_ptr<SerializerBackend> snapshot_backend_;

    // Discarded replicas management
    const std::chrono::seconds put_start_discard_timeout_sec_;
    const std::chrono::seconds put_start_release_timeout_sec_;
    const std::string cxl_path_;
    const size_t cxl_size_;
    bool enable_cxl_;

    class DiscardedReplicas {
       public:
        DiscardedReplicas() = delete;

        DiscardedReplicas(std::vector<Replica>&& replicas,
                          std::chrono::system_clock::time_point ttl)
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

        bool isExpired(const std::chrono::system_clock::time_point& now) const {
            return ttl_ <= now;
        }

       private:
        friend class MetadataSerializer;
        std::vector<Replica> replicas_;
        std::chrono::system_clock::time_point ttl_;
        uint64_t mem_size_;
    };
    std::mutex discarded_replicas_mutex_;
    std::list<DiscardedReplicas> discarded_replicas_
        GUARDED_BY(discarded_replicas_mutex_);
    size_t offloading_queue_limit_ = 50000;

    // Task manager
    ClientTaskManager task_manager_;
};

}  // namespace mooncake
