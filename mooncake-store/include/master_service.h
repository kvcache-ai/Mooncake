#pragma once

#include <atomic>
#include <boost/functional/hash.hpp>
#include <boost/lockfree/queue.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <limits>
#include <list>
#include <memory>
#include <optional>
#include <set>
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
#include "tenant_quota.h"
#include "types.h"
#include "master_config.h"
#include "rpc_types.h"
#include "replica.h"
#include "ha/ha_types.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "task_manager.h"

namespace mooncake {
namespace ha {
class SnapshotCatalogStore;
}

class EtcdOpLogStore;

// Forward declarations
class AllocationStrategy;
class EvictionStrategy;

// Forward declarations for test classes
namespace test {
class MasterServiceSnapshotTestBase;
class SnapshotChildProcessTest;
}  // namespace test

/*
 * @brief MasterService is the main class for the master server.
 *
 * Lock order. To avoid deadlocks, locks must be acquired in this order:
 * 1. client_mutex_
 * 2. metadata_shards_[shard_idx_].mutex
 * 3. segment_mutex_
 * 4. ObjectMetadata::lock (per-object SpinLock; taken under shard.mutex
 *    only -- see ReadLeaseSnapshot and GrantLease for the only two
 *    places it is taken while another lock is held)
 * 5. tenant_quotas_.mu_  (smallest granularity, shortest critical sections;
 *    tenant_quotas_ helpers never call back into shard / client / segment
 *    / metadata locks, so it is safe to acquire while holding any of the
 *    above.)
 *
 * Frontier index lock domain: the per-tenant eviction frontier sets
 * live INSIDE TenantQuotaTable::Entry and are protected by
 * tenant_quotas_.mu_ (#5 above) -- the SAME mutex that guards quota
 * accounting. Lifecycle helpers (IndexMetadata / UnindexMetadata /
 * ReindexMetadataAfterPinChange) hold the owning shard's mutex while
 * forwarding the frontier mutation to TenantQuotaTable, which then
 * acquires its own mu_ briefly. RefillTenantFrontier is a thin
 * wrapper over TenantQuotaTable::SnapshotTopK and does NOT walk
 * shards.
 *
 * Cross-shard primitive:
 *   - BatchEvictInTenant: pulls a candidate batch via SnapshotTopK
 *     (single tenant_quotas_.mu_ acquisition), then for each
 *     candidate takes that candidate's shard WRITER lock and releases
 *     it before the next candidate. Single-shard invariant holds.
 *
 * Same-shard re-entry. std::shared_mutex is NOT recursive.
 * AllocateAndInsertMetadata holds the writer lock for the inserting
 * key's shard while invoking BatchEvictInTenant in its Reserve ->
 * evict -> Reserve loop; re-acquiring the same shard would
 * self-deadlock. Contract: any caller that already holds a shard
 * writer lock MUST pass that shard's index as `excluded_shard_index`
 * to BatchEvictInTenant (forwarded to RefillTenantFrontier). The
 * primitive then skips that shard at the pull stage. The 1/kNumShards
 * (~0.1%) loss in candidate space is absorbed by the near-LRU policy.
 */
class MasterService {
    // Test friend class for snapshot/restore testing
    friend class test::MasterServiceSnapshotTestBase;
    friend class test::SnapshotChildProcessTest;

   public:
    MasterService();
    MasterService(const MasterServiceConfig& config);
    ~MasterService();

    // Direct accessor for the per-tenant quota table. Used by the HTTP
    // admin endpoints (via WrappedMasterService) to read state and apply
    // policy CRUD, and by tests to inject policies and inspect usage.
    TenantQuotaTable& GetTenantQuotas() { return tenant_quotas_; }
    const TenantQuotaTable& GetTenantQuotas() const { return tenant_quotas_; }

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

    auto GracefulUnmountSegment(const UUID& segment_id, const UUID& client_id,
                                uint64_t grace_period_ms)
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
     * @brief Start an upsert operation. If the key does not exist, behaves
     * like PutStart. If the key exists with the same size, performs in-place
     * update (reuses existing buffers). If the key exists with a different
     * size, deletes old replicas and allocates new ones.
     * @return Replica descriptors on success, or error code on failure.
     * Possible errors: OBJECT_HAS_REPLICATION_TASK (Copy/Move/Offload in
     * progress), OBJECT_REPLICA_BUSY (replicas have non-zero refcnt).
     */
    auto UpsertStart(const UUID& client_id, const std::string& key,
                     const uint64_t slice_length, const ReplicateConfig& config)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    /**
     * @brief Complete an upsert operation. Delegates to PutEnd.
     */
    auto UpsertEnd(const UUID& client_id, const std::string& key,
                   ReplicaType replica_type) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Revoke an upsert operation. Delegates to PutRevoke.
     */
    auto UpsertRevoke(const UUID& client_id, const std::string& key,
                      ReplicaType replica_type)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Start a batch of upsert operations.
     */
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchUpsertStart(const UUID& client_id,
                     const std::vector<std::string>& keys,
                     const std::vector<uint64_t>& slice_lengths,
                     const ReplicateConfig& config);

    /**
     * @brief Complete a batch of upsert operations. Delegates to BatchPutEnd.
     */
    std::vector<tl::expected<void, ErrorCode>> BatchUpsertEnd(
        const UUID& client_id, const std::vector<std::string>& keys);

    /**
     * @brief Revoke a batch of upsert operations. Delegates to BatchPutRevoke.
     */
    std::vector<tl::expected<void, ErrorCode>> BatchUpsertRevoke(
        const UUID& client_id, const std::vector<std::string>& keys);

    /**
     * @brief Evict a disk replica for a key (triggered by client-side disk
     * eviction).
     * @param client_id The client performing the eviction
     * @param key The object key whose disk replica was evicted
     * @param replica_type DISK or LOCAL_DISK
     * @return ErrorCode::OK on success, OBJECT_NOT_FOUND if key missing
     */
    auto EvictDiskReplica(const UUID& client_id, const std::string& key,
                          ReplicaType replica_type)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Batch evict disk replicas for multiple keys.
     * @param client_id The client performing the eviction
     * @param keys The object keys whose disk replicas were evicted
     * @param replica_type DISK or LOCAL_DISK
     * @return Per-key results (OK or error code)
     */
    std::vector<tl::expected<void, ErrorCode>> BatchEvictDiskReplica(
        const UUID& client_id, const std::vector<std::string>& keys,
        ReplicaType replica_type);

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
     * @brief Batch remove objects and their replicas
     * @param keys The list of keys to remove.
     * @param force If true, skip lease and replication task checks.
     * @return Vector of expected results for each key.
     */
    auto BatchRemove(const std::vector<std::string>& keys, bool force = false)
        -> std::vector<tl::expected<void, ErrorCode>>;

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

    auto ReportSsdCapacity(const UUID& client_id,
                           int64_t ssd_total_capacity_bytes)
        -> tl::expected<void, ErrorCode>;

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
     * @brief Create a drain job to gracefully evacuate one or more segments.
     */
    tl::expected<UUID, ErrorCode> CreateDrainJob(
        const CreateDrainJobRequest& request);

    /**
     * @brief Query the status of a drain job.
     */
    tl::expected<QueryJobResponse, ErrorCode> QueryDrainJob(const UUID& job_id);

    /**
     * @brief Cancel an in-flight drain job and restore draining segments to OK.
     */
    tl::expected<void, ErrorCode> CancelDrainJob(const UUID& job_id);

    /**
     * @brief Query current segment lifecycle state by segment name.
     */
    tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatus(
        const std::string& segment_name);

    /**
     * @brief Query current segment lifecycle state by segment id.
     */
    tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatusById(
        const UUID& segment_id);

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

    // ---- Test-only observability hooks ----
    // Production observability is exposed via the Prometheus metrics on
    // /metrics; the accessors below mirror those counters as plain atomics
    // so unit tests can assert on them without scraping the metrics
    // endpoint. Production callers MUST NOT depend on these; they are not
    // part of the stable service contract.

    // Mirrors mooncake_tenant_quota_reject_total: number of PutStart
    // calls that returned NO_AVAILABLE_HANDLE because the tenant quota
    // could not accommodate the request even after scoped eviction.
    uint64_t TenantQuotaRejectTotal() const {
        return tenant_quota_reject_total_.load(std::memory_order_relaxed);
    }
    // Total invocations of RefillTenantFrontier across the service's
    // lifetime. The TenantEvictionContext cache amortises this across
    // many NextVictim() calls within a single evict cycle. Each refill
    // calls SnapshotTopK under tenant_quotas_.mu_, costing O(K log K)
    // over the tenant's frontier set. Exposed for tests.
    uint64_t RefillTenantFrontierCount() const {
        return refill_tenant_frontier_count_.load(std::memory_order_relaxed);
    }

   private:
    void SnapshotThreadFunc();

    // Persist master state
    tl::expected<void, SerializationError> PersistState(
        const std::string& snapshot_id);
    tl::expected<void, SerializationError> PersistState(
        const ha::SnapshotDescriptor& descriptor);
    tl::expected<ha::SnapshotDescriptor, SerializationError>
    BuildSnapshotDescriptor(const std::string& snapshot_id,
                            const std::string& manifest_path,
                            const std::string& object_prefix) const;
    tl::expected<ha::OpLogSequenceId, SerializationError>
    ResolveSnapshotSequenceId() const;
#ifdef STORE_USE_ETCD
    tl::expected<EtcdOpLogStore*, SerializationError>
    GetSnapshotBoundaryOpLogStore() const;
#endif

    tl::expected<void, SerializationError> UploadSnapshotPayloadFile(
        const std::vector<uint8_t>& data, const std::string& path,
        const std::string& local_filename, const std::string& snapshot_id);

    std::unique_ptr<ha::SnapshotCatalogStore> CreateSnapshotCatalogStore();
    void CleanupOldSnapshot(int keep_count, const std::string& snapshot_id);
    ha::SnapshotCatalogStore* GetSnapshotCatalogStore();

    // Restore master state
    void RestoreState();
    bool TryRestoreStateFromSnapshot(
        const ha::SnapshotDescriptor& snapshot,
        const std::chrono::system_clock::time_point& now);
    void ResetStateAfterFailedRestoreAttempt();

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

    // Helper to get a snapshot of alive clients (under client_mutex_ shared
    // lock)
    std::unordered_set<UUID, boost::hash<UUID>> getAliveClientsSnapshot() const;

    // Clear invalid handles in all shards
    void ClearInvalidHandles();
    void ClearInvalidHandles(
        const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients);

    std::string FormatTimestamp(
        const std::chrono::system_clock::time_point& tp);
    // We need to clean up finished tasks periodically to avoid memory leak
    // And also we can add some task ttl mechanism in the future
    void TaskCleanupThreadFunc();
    void JobDispatchThreadFunc();

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
            bool enable_soft_pin, bool enable_hard_pin = false,
            ObjectDataType data_type_ = ObjectDataType::UNKNOWN,
            std::string tenant_id_ = std::string("default"))
            : client_id(client_id_),
              put_start_time(put_start_time_),
              size(value_length),
              data_type(data_type_),
              tenant_id(std::move(tenant_id_)),
              lease_timeout(),
              soft_pin_timeout(std::nullopt),
              hard_pinned(enable_hard_pin),
              replicas_(std::move(reps)) {
            if (tenant_id.empty()) {
                tenant_id = "default";
            }
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

        // Updated by UpsertStart (Case B) to reflect the new writer.
        UUID client_id;
        // Updated by UpsertStart (Case B) to reset the discard timeout.
        std::chrono::system_clock::time_point put_start_time;
        const size_t size;
        const ObjectDataType data_type{ObjectDataType::UNKNOWN};
        // Tenant identity. Set at creation from ReplicateConfig.tenant_id;
        // Case B reuses the existing buffers in place (so the field is not
        // touched), and Case C destroys the old metadata and constructs a
        // new one — quota is released from this object's tenant first, the
        // new object is then charged to whatever tenant_id the caller passed.
        //
        // Note: the per-shard metadata map is keyed by the raw key string,
        // so two tenants using the same raw key still collide in the
        // underlying map. Quota accounting is correct across such a
        // collision (Case C transfers bytes between tenants), but identical
        // raw keys are not independently namespaced by tenant.
        std::string tenant_id;

        // Live-byte cost actually committed against the tenant quota when
        // this object was admitted. Captured once at PutStart time (after
        // the allocator returned and we counted memory replicas) and
        // never recomputed afterwards: erase paths often pop replicas
        // BEFORE deciding to release quota, which would zero the count if
        // we re-derived it from the (now-empty) replica list. Reading this
        // snapshot guarantees release amount == commit amount.
        uint64_t quota_committed_bytes{0};

        // ---- Per-tenant eviction frontier bookkeeping ----
        //
        // Frontier sets live inside TenantQuotaTable::Entry (one std::set
        // per (tenant, bucket)) and are protected by tenant_quotas_.mu_.
        // ObjectMetadata only remembers the snapshot triple
        // (lease_timeout + bucket) used at indexing time so
        // UnindexFrontier / DropStaleFrontier / MoveFrontier can locate
        // the std::set node in O(log N) without scanning, and so
        // evict-time consumers can detect stale entries (lease has been
        // bumped since insertion).
        //
        // Concurrency: BOTH fields are read and written ONLY while the
        // owning MetadataShard's mutex is held in write mode. The hot
        // lease-renewal path (GrantLease) intentionally leaves them
        // alone (lazy maintenance); the resulting staleness is
        // reconciled at evict time by comparing frontier_lease_snapshot
        // against the live lease_timeout. kNone is reserved for objects
        // that must NEVER appear as eviction candidates (today: only
        // hard-pinned ones).
        FrontierBucket frontier_bucket{FrontierBucket::kNone};
        std::chrono::system_clock::time_point frontier_lease_snapshot{};

        mutable SpinLock lock;
        // Default constructor, creates a time_point representing
        // the Clock's epoch (i.e., time_since_epoch() is zero).
        mutable std::chrono::system_clock::time_point lease_timeout
            GUARDED_BY(lock);  // hard lease
        mutable std::optional<std::chrono::system_clock::time_point>
            soft_pin_timeout GUARDED_BY(lock);  // optional soft pin, only
                                                // set for vip objects
        const bool hard_pinned{false};          // immutable, set at creation

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
        bool IsSoftPinned(
            const std::chrono::system_clock::time_point& now) const {
            SpinLocker locker(&lock);
            return soft_pin_timeout && now < *soft_pin_timeout;
        }

        bool IsHardPinned() const { return hard_pinned; }

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

    struct OffloadingTask {
        ReplicaID source_id;
        std::chrono::system_clock::time_point start_time;
    };

    static constexpr size_t kNumShards = 1024;  // Number of metadata shards

    // ---- Per-tenant eviction frontier index ----
    //
    // The frontier candidate sets live on TenantQuotaTable::Entry
    // (one std::set<TenantFrontierEntry> per tenant per bucket),
    // protected by tenant_quotas_.mu_. See tenant_quota.h for the
    // entry struct, ordering and lazy-maintenance contract. Lifecycle
    // helpers (IndexMetadata / UnindexMetadata) forward to the
    // TenantQuotaTable API while still holding the owning shard's
    // writer lock for the underlying ObjectMetadata mutation.

    // Sharded metadata maps and their mutexes
    struct MetadataShard {
        mutable SharedMutex mutex;
        std::unordered_map<std::string, ObjectMetadata> metadata
            GUARDED_BY(mutex);
        std::unordered_set<std::string> processing_keys GUARDED_BY(mutex);
        std::unordered_map<std::string, const ReplicationTask> replication_tasks
            GUARDED_BY(mutex);
        std::unordered_map<std::string, const OffloadingTask> offloading_tasks
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

        // Try-lock constructor: attempts to acquire the shard's writer
        // lock without blocking. Caller MUST check owns_lock() before
        // dereferencing.
        MetadataShardAccessorRW(MasterService* master_service,
                                size_t shard_index, std::try_to_lock_t)
            : shard_(master_service->metadata_shards_[shard_index]),
              lock_(&shard_.mutex, defer_lock) {
            lock_.try_lock();
        }

        bool owns_lock() const NO_THREAD_SAFETY_ANALYSIS {
            return lock_.is_locked();
        }

        MetadataShard* operator->() { return &shard_; }

        const MetadataShard* operator->() const { return &shard_; }

        MetadataShard& GetShard() { return shard_; }

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

    // ---- Per-tenant eviction context ----
    //
    // Stateful helper that drives BatchEvictInTenant across multiple
    // attempts: it caches refilled candidates so a single Reserve()
    // failure -> evict -> retry cycle does not fan out kNumShards-wide
    // shared-lock acquisitions on every iteration.
    //
    // Lifecycle:
    //   - Constructed lazily inside AllocateAndInsertMetadata when the
    //     first ReserveTenantBytes call fails; lives for the duration
    //     of the eviction attempt loop only.
    //   - NextVictim(bucket) returns the next candidate from
    //     the requested cache (no-pin or soft-pin), refilling from
    //     RefillTenantFrontier when the cache is empty.
    //   - Refill counts are tracked separately for the two passes;
    //     once a pass exhausts kMaxRefills the helper returns nullopt
    //     for that pass to bound worst-case work per allocation.
    //
    // Thread-safety: NOT thread-safe. The owning thread must serialize
    // calls; this matches how AllocateAndInsertMetadata holds the
    // shard write lock for the failing key throughout the retry loop.
    // The helper itself only takes tenant_quotas_.mu_ briefly via
    // SnapshotTopK and never any shard lock, so it is safe to call
    // while holding any shard's write lock.
    class TenantEvictionContext {
       public:
        // The two pull-stage tunables. See the consolidated tunables
        // block right after this nested class for the full rationale
        // (kBatchSize / kMaxRefills / kMaxEvictAttempts form one
        // family). They are class-static so tests can read them in
        // one place; they are compile-time constants on purpose --
        // promoting any of them to a runtime gflag must come with a
        // matching observability metric.
        //
        // kBatchSize: how many candidates SnapshotTopK returns in one
        //   refill. 64 mirrors the global BatchEvict batch size; the
        //   cost of a refill is one tenant_quotas_.mu_ acquisition +
        //   an O(K) std::set walk.
        // kMaxRefills: per-pass cap on refills. 4 sweeps * 64
        //   candidates = 256 candidates examined per pass before we
        //   give up on the tenant -- enough to absorb lazy-stale
        //   skips (lease bumped between refill and consumption).
        static constexpr size_t kBatchSize = 64;
        static constexpr size_t kMaxRefills = 4;

        TenantEvictionContext(const MasterService* master_service,
                              std::string tenant_id);

        // Returns the next candidate from the requested pass, or
        // std::nullopt if the cache is empty AND the per-pass refill
        // budget has been exhausted (or SnapshotTopK returned empty,
        // meaning the tenant has no more frontier entries of that
        // flavour anywhere in the system).
        std::optional<TenantFrontierEntry> NextVictim(FrontierBucket bucket);

        const std::string& tenant_id() const { return tenant_id_; }

       private:
        // Replenishes the requested cache from SnapshotTopK.
        // Increments the matching refill counter. Returns true if at
        // least one entry was added.
        bool Refill(FrontierBucket bucket);

        const MasterService* master_;
        std::string tenant_id_;
        std::deque<TenantFrontierEntry> cached_no_pin_;
        std::deque<TenantFrontierEntry> cached_soft_pin_;
        size_t refills_no_pin_{0};
        size_t refills_soft_pin_{0};
    };

    // ---- Eviction primitive ----
    //
    // EvictCycleContext bundles the per-cycle offload state that used
    // to live in BatchEvict's lambda captures. Promoting it to a named
    // struct lets BatchEvictInTenant reuse the exact same eviction
    // primitive without copy-pasting the lambda. Each top-level
    // eviction call (BatchEvict / BatchEvictInTenant) constructs ONE
    // EvictCycleContext on the stack; the counters scope to that call
    // only.
    struct EvictCycleContext {
        // Inputs (set by the caller, not modified by the primitive):
        bool offload_on_evict;
        bool offload_force_evict;
        long offload_cap;  // ignored when offload_on_evict is false
        std::chrono::system_clock::time_point now;

        // Counters maintained by TryEvictOrOffload across the cycle:
        long offload_queued_this_cycle{0};
        long offload_deferred_count{0};
        long offload_cap_forced_count{0};
        long offload_push_failed_forced{0};
    };

    // EvictReconcileResult captures the outcome of one
    // TryEvictAndReconcile call so the calling loop can decide how to
    // advance its iterator and how many objects it has reclaimed.
    struct EvictReconcileResult {
        // Bytes freed on the tenant's quota. Zero when the candidate
        // could not be evicted (offload queued but no extra replicas
        // erased, push-fail in soft mode, etc.).
        uint64_t freed_bytes{0};
        // True if the ObjectMetadata was erased from the shard map.
        // The caller MUST advance its iterator via the returned
        // iterator (see next_iterator) when this is true; otherwise
        // it is a normal post-increment.
        bool erased{false};
    };

    // Tries to evict (or offload-defer) one object's memory replicas
    // and reconciles all the bookkeeping the surviving / erased paths
    // need: tenant quota release, frontier unindex, and the "decrement
    // quota_committed_bytes by `freed`" guard that all three of
    // BatchEvict's passes share.
    //
    // Caller MUST hold the shard's WRITE lock (this function may erase
    // from shard->metadata and mutate frontier sets).
    //
    // The iterator passed in `it` is consumed: on return,
    // - if result.erased == true,  `it` has been invalidated; the
    //   caller should NOT use it. The function returns the iterator
    //   that "took its place" in shard->metadata (i.e. the result of
    //   shard->metadata.erase(it)). The caller assigns this to its
    //   loop iterator and must NOT post-increment.
    // - if result.erased == false, `it` remains valid and unchanged;
    //   the caller post-increments as normal.
    std::unordered_map<std::string, ObjectMetadata>::iterator
    TryEvictAndReconcile(
        EvictCycleContext& ctx, MetadataShardAccessorRW& shard,
        std::unordered_map<std::string, ObjectMetadata>::iterator it,
        EvictReconcileResult& result);

    // Lower-level primitive: actually invoke offload-on-evict logic and
    // return how many bytes were freed. Used by TryEvictAndReconcile;
    // exposed here so callers that need to handle survive/erase
    // bookkeeping in some non-standard way (currently none) can reuse
    // it. Caller must hold the shard write lock.
    uint64_t TryEvictOrOffload(EvictCycleContext& ctx, const std::string& key,
                               ObjectMetadata& metadata,
                               MetadataShardAccessorRW& shard);

    // ---- Per-tenant eviction tunables ----
    //
    // kMaxEvictAttempts: outer-loop bound for the
    //   AllocateAndInsertMetadata Reserve -> BatchEvictInTenant ->
    //   Reserve cycle. Each attempt may free a fraction of the
    //   requested target_bytes if the pulled batch is partially
    //   stale (lazy maintenance discards bumped-lease candidates).
    //   Bounding the attempts at 8 caps the worst-case allocation
    //   latency at:
    //
    //       kMaxEvictAttempts * 2_passes * kMaxRefills * kBatchSize
    //         = 8 * 2 * 4 * 64 = 4096 candidates examined
    //
    //   (each BatchEvictInTenant call creates a fresh
    //   TenantEvictionContext whose two passes have independent
    //   refill budgets: 2 * kMaxRefills * kBatchSize = 512 per call)
    //
    //   for a single failing PutStart, before we surface
    //   NO_AVAILABLE_HANDLE to the client. Above that the tenant is
    //   either fully pinned or genuinely over quota -- spinning more
    //   would only hurt latency without changing the verdict.
    //
    // All three constants are compile-time. They can be promoted to
    // gflags if operators need to tune quota-pressure behavior; the
    // matching mooncake_tenant_quota_reject_total counter and the
    // RefillTenantFrontierCount() observable already let callers
    // correlate any tuning change with observed pressure.
    static constexpr size_t kMaxEvictAttempts = 8;

    // Helper to get shard index from key
    size_t getShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % kNumShards;
    }

    // Helper to clean up stale handles pointing to unmounted segments
    // or local_disk replicas whose owner client is no longer alive.
    bool CleanupStaleHandles(
        ObjectMetadata& metadata,
        const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients);

    // Helper: allocate replicas, create ObjectMetadata, insert into shard,
    // and return descriptor list.  Shared by PutStart and UpsertStart.
    auto AllocateAndInsertMetadata(
        MetadataShardAccessorRW& shard, const UUID& client_id,
        const std::string& key, uint64_t value_length,
        const ReplicateConfig& config,
        const std::chrono::system_clock::time_point& now)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    /**
     * @brief Helper to discard expired processing keys.
     */
    void DiscardExpiredProcessingReplicas(
        MetadataShardAccessorRW& shard,
        const std::chrono::system_clock::time_point& now);

    // ---- Eviction predicates (pure helpers on ObjectMetadata) ----
    //
    // Hoisted out of BatchEvict so BatchEvictInTenant can reuse the
    // same definition of "evictable memory replica". Side-effect free
    // except EvictReplicas (which mutates), depend only on the
    // metadata's own state. Declared as private static members because
    // ObjectMetadata is a private nested type and free functions in an
    // anonymous namespace would not have access to it.

    // True when the object owns at least one MEMORY replica that is
    // completed and currently has zero outstanding refcount, i.e. one
    // we could erase right now without breaking an in-flight reader.
    static bool CanEvictReplicas(const ObjectMetadata& metadata);

    // Erase every MEMORY replica that currently satisfies the
    // CanEvictReplicas predicate. Returns the number of replicas erased.
    static size_t EvictReplicas(ObjectMetadata& metadata);

    // True when the object owns at least one local-disk replica.
    static bool HasLocalDiskReplica(const ObjectMetadata& metadata);

    // Returns the frontier bucket this object should currently live in
    // given its hard-pin / soft-pin state. kNone means "must NEVER
    // appear as eviction candidate" (today: only hard-pinned ones).
    static FrontierBucket ChooseFrontierBucket(
        const ObjectMetadata& metadata,
        const std::chrono::system_clock::time_point& now);

    // Reads lease_timeout under the metadata SpinLock so the frontier
    // entry's primary key is consistent with what GrantLease saw at
    // indexing time. Subsequent GrantLease calls bump the live
    // lease_timeout but leave the snapshot alone -- evict-time
    // consumers reconcile via metadata.frontier_lease_snapshot.
    static std::chrono::system_clock::time_point ReadLeaseSnapshot(
        const ObjectMetadata& metadata);

    // ---- Per-tenant frontier index helpers ----
    //
    // IndexMetadata / UnindexMetadata are the single entry / exit
    // points for the per-tenant frontier sets that
    // BatchEvictInTenant pops from. Every code path that inserts a
    // new ObjectMetadata into a shard MUST call IndexMetadata
    // exactly once after the insert, and every code path that
    // removes an ObjectMetadata from a shard MUST call
    // UnindexMetadata exactly once before the erase. Mismatched
    // calls leak set entries and silently break the eviction
    // algorithm.
    //
    // The frontier sets themselves live on TenantQuotaTable::Entry;
    // these helpers translate the lifecycle event into IndexFrontier /
    // UnindexFrontier calls on tenant_quotas_. They REQUIRE the
    // caller to hold the owning shard's mutex in write mode (so the
    // per-object snapshot triple they read off ObjectMetadata is
    // consistent), but the frontier mutation itself runs under
    // tenant_quotas_.mu_ -- callers MUST
    // NOT already hold tenant_quotas_.mu_.
    //
    // Lazy-maintenance contract: lease-renewal hot paths
    // (GrantLease / GetReplicaList) intentionally skip these
    // helpers. The resulting staleness in
    // frontier_lease_snapshot is reconciled at evict time
    // (BatchEvictInTenant compares the snapshot against the live
    // lease_timeout and drops stale candidates via
    // DropStaleFrontier). Soft-pin state changes DO need to update
    // the frontier (the candidate moves between buckets); that is
    // the ReindexMetadataAfterPinChange helper.
    void IndexMetadata(MetadataShard& shard, const std::string& key,
                       ObjectMetadata& metadata);
    void UnindexMetadata(MetadataShard& shard, const std::string& key,
                         ObjectMetadata& metadata);
    // Called when an object's soft-pin status flips. Migrates the
    // object's frontier entry between buckets via
    // TenantQuotaTable::MoveFrontier. No-op if the object is not
    // currently indexed (e.g. hard-pinned, or never reached
    // IndexMetadata).
    void ReindexMetadataAfterPinChange(MetadataShard& shard,
                                       const std::string& key,
                                       ObjectMetadata& metadata);

    // Sentinel for "no shard is excluded" -- used by the
    // excluded_shard_index parameter of BatchEvictInTenant.
    static constexpr size_t kInvalidShardIndex =
        std::numeric_limits<size_t>::max();

    // Try to evict at least `target_bytes` from `tenant_id`'s memory
    // footprint by walking the per-tenant frontier sets stored on
    // TenantQuotaTable::Entry.
    //
    // Two-pass strategy mirroring the global BatchEvict policy:
    //   Pass 1 -- no-pin frontier: evict objects whose lease has the
    //     earliest expiry first (snapshot ordering inside the std::set).
    //   Pass 2 -- soft-pin frontier: only entered if Pass 1 freed less
    //     than target_bytes AND allow_evict_soft_pinned_objects_ is
    //     enabled at the service level.
    //
    // Per candidate returned by TenantEvictionContext::NextVictim:
    //   1. Skip the candidate if its key hashes to excluded_shard_index.
    //   2. Acquire the owning shard's WRITE lock.
    //   3. Re-validate the candidate: key still present, not
    //      hard-pinned, lease snapshot still matches metadata
    //      (lazy-maintenance via the snapshot stored on the
    //      ObjectMetadata), has evictable memory replicas via
    //      CanEvictReplicas. On lease-bump miss the candidate is
    //      cleaned up via TenantQuotaTable::DropStaleFrontier.
    //   4. On success, call TryEvictAndReconcile to do the actual
    //      eviction + frontier unindex + quota release.
    //   5. Release the shard write lock.
    //
    // Returns the cumulative bytes freed across both passes. Caller
    // (AllocateAndInsertMetadata) then re-tries ReserveTenantBytes.
    //
    // `excluded_shard_index`: when not equal to kInvalidShardIndex,
    // candidates whose key hashes to that shard index are skipped.
    // This is the deadlock-avoidance contract for callers that
    // already hold a shard write lock (typically
    // AllocateAndInsertMetadata): std::shared_mutex is NOT recursive,
    // so re-acquiring the same shard's writer lock would
    // self-deadlock. Skipping the caller's own shard preserves
    // correctness with at most a 1/kNumShards = 0.1% reduction in
    // candidate space.
    //
    // Caller MUST NOT hold any shard mutex EXCEPT the one whose
    // index they pass as excluded_shard_index. The frontier pull
    // itself runs under tenant_quotas_.mu_, which is acquired AFTER
    // any shard mutex per the documented lock order, so the contract
    // is satisfied automatically.
    uint64_t BatchEvictInTenant(
        const std::string& tenant_id, uint64_t target_bytes,
        size_t excluded_shard_index = kInvalidShardIndex);

    // Thin wrapper over TenantQuotaTable::SnapshotTopK. Bumps
    // refill_tenant_frontier_count_ unconditionally so the perf-
    // baseline test can assert how often TenantEvictionContext
    // refills its cache.
    //
    // Caller must NOT hold tenant_quotas_.mu_; SnapshotTopK acquires
    // it internally.
    std::deque<TenantFrontierEntry> RefillTenantFrontier(
        const std::string& tenant_id, FrontierBucket bucket,
        size_t batch_size) const;

    // ---- Per-tenant quota accounting helpers. ----
    // These wrap TenantQuotaTable and respect the enable_tenant_quota_ feature
    // gate: when the gate is OFF every helper is a strict no-op so that
    // existing single-tenant deployments observe identical behavior.
    //
    // Lock order: callers usually hold a MetadataShard mutex; tenant_quotas_
    // has the smallest granularity and shortest critical sections, so it MUST
    // be acquired AFTER any shard / client / segment mutex. None of these
    // helpers call back into shard locks, satisfying that invariant.
    //
    // Returns true if the reservation succeeded (or the feature gate is off);
    // false means the tenant exceeded its quota and the caller must abort.
    bool ReserveTenantBytes(const std::string& tenant_id, uint64_t bytes);
    void CommitTenantBytes(const std::string& tenant_id, uint64_t bytes);
    void AbortTenantBytes(const std::string& tenant_id, uint64_t bytes);
    void ReleaseTenantBytes(const std::string& tenant_id, uint64_t bytes);
    // Partial release for survive-after-evict / partial-replica-clear paths
    // where the object is still alive. Decrements used_bytes only; does not
    // touch committed_count. See TenantQuotaTable::ReleasePartial.
    void ReleaseTenantBytesPartial(const std::string& tenant_id,
                                   uint64_t bytes);
    // How many bytes need to leave the tenant before it can fit
    // `incoming_bytes`. Returns 0 when the tenant is unlimited or
    // already has headroom. No-op + 0 when the feature gate is off.
    uint64_t ComputeTenantEvictTarget(const std::string& tenant_id,
                                      uint64_t incoming_bytes) const;
    // Compute the live-byte cost of a single object for quota accounting.
    // Only counts memory replicas (matches AccountLiveBytes semantics).
    uint64_t TenantBytesForMetadata(const ObjectMetadata& metadata) const;

    // Republish per-tenant quota gauges from the current TenantQuotaTable
    // state. `known_before_tenant_ids` lists tenants whose gauges existed
    // prior to the call site (typically captured before a snapshot
    // restore); any tenant in this set that no longer appears in
    // `tenant_quotas_.ListAll()` after the call has its gauges zeroed so
    // /metrics does not retain stale labels for tenants that disappeared.
    // No-op when enable_tenant_quota_ is false.
    void RefreshTenantQuotaMetrics(
        const std::unordered_set<std::string>& known_before_tenant_ids);

    /**
     * @brief Helper to release space of expired discarded replicas.
     * @return Number of released objects that have memory replicas
     */
    uint64_t ReleaseExpiredDiscardedReplicas(
        const std::chrono::system_clock::time_point& now);

    // Eviction thread function
    void EvictionThreadFunc();

    tl::expected<void, ErrorCode> PushOffloadingQueue(const std::string& key,
                                                      Replica& replica);

    // Graceful unmount scheduler
    class GracefulUnmountScheduler {
       public:
        explicit GracefulUnmountScheduler(MasterService* service);
        ~GracefulUnmountScheduler();
        void Schedule(const UUID& segment_id, const UUID& client_id,
                      std::chrono::steady_clock::time_point expire_time);
        void RemoveClientRecords(const UUID& client_id);
        void Stop();

       private:
        void TimerLoop();
        struct Record {
            UUID segment_id;
            UUID client_id;
            std::chrono::steady_clock::time_point expire_time;
            bool operator>(const Record& other) const {
                return expire_time > other.expire_time;
            }
        };
        MasterService* service_;
        std::mutex mutex_;
        std::priority_queue<Record, std::vector<Record>, std::greater<Record>>
            queue_;
        std::thread timer_thread_;
        std::atomic<bool> timer_running_{false};
        bool stopping_{false};
        std::condition_variable timer_cv_;
    } graceful_unmount_scheduler_;

    // Lease related members
    const uint64_t default_kv_lease_ttl_;     // in milliseconds
    const uint64_t default_kv_soft_pin_ttl_;  // in milliseconds
    const bool allow_evict_soft_pinned_objects_;

    // How many PutStart/UpsertStart calls have been rejected with
    // NO_AVAILABLE_HANDLE specifically because the tenant could not
    // be made to fit (Reserve failed AND BatchEvictInTenant could not
    // free enough). Kept distinct from the global allocator-OOM path
    // so operators can tell quota pressure apart from cluster-wide
    // OOM.
    mutable std::atomic<uint64_t> tenant_quota_reject_total_{0};

    // Total RefillTenantFrontier invocations -- bumped at every entry
    // into the helper (regardless of whether it returned candidates).
    // Lets the perf-baseline test assert that the EvictionContext
    // cache amortizes refills (~O(1) per evict cycle) instead of
    // degenerating into a per-victim shard scan. mutable because
    // RefillTenantFrontier is const.
    mutable std::atomic<uint64_t> refill_tenant_frontier_count_{0};

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
            // Automatically clean up invalid handles (memory replicas only).
            // Note: We only check memory replicas here to avoid lock order
            // violation (client_mutex_ must be acquired before metadata shard).
            // local_disk replicas are cleaned up by ClearInvalidHandles() in
            // ClientMonitorFunc.
            if (it_ != shard_guard_->metadata.end()) {
                // Erase invalid memory replicas (those with unmounted
                // segments). No client_mutex_ needed since we only check memory
                // replicas.
                it_->second.EraseReplicas([](const Replica& replica) {
                    return replica.has_invalid_mem_handle();
                });
                // If no valid replicas remain, delete the whole object.
                if (!it_->second.IsValid()) {
                    const std::string tid = it_->second.tenant_id;
                    const uint64_t committed =
                        it_->second.quota_committed_bytes;
                    service_->UnindexMetadata(shard_guard_.GetShard(), key_,
                                              it_->second);
                    this->Erase();
                    service_->ReleaseTenantBytes(tid, committed);
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

        // Convenience accessor returning the underlying MetadataShard by
        // reference. Avoids the verbose `*GetShard().operator->()` pattern
        // that appears at every call site needing a MetadataShard&.
        MetadataShard& GetShardRef() NO_THREAD_SAFETY_ANALYSIS {
            return shard_guard_.GetShard();
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
                    std::vector<Replica> replicas, bool enable_soft_pin,
                    bool enable_hard_pin = false,
                    ObjectDataType data_type = ObjectDataType::UNKNOWN) {
            if (Exists()) {
                throw std::logic_error("Already exists");
            }
            const auto now = std::chrono::system_clock::now();
            auto result = shard_guard_->metadata.emplace(
                std::piecewise_construct, std::forward_as_tuple(key_),
                std::forward_as_tuple(client_id, now, total_length,
                                      std::move(replicas), enable_soft_pin,
                                      enable_hard_pin, data_type));
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

        // Serialize tenant quota policies (default + explicit overrides).
        // Quota *usage* is intentionally excluded -- it is rebuilt from
        // restored object metadata via AccumulateUsage during
        // DeserializeShard.
        tl::expected<void, SerializationError> SerializeTenantQuotas(
            MsgpackPacker& packer) const;

        // Deserialize and apply tenant quota policies. Tolerant of legacy
        // snapshots that omit the field: callers detect that case (the
        // top-level "tenant_quotas" key is absent) and skip the call,
        // preserving whatever default the master was started with.
        tl::expected<void, SerializationError> DeserializeTenantQuotas(
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

    // Offload-on-evict: defer disk offload to eviction time
    // (config: offload_on_evict)
    bool offload_on_evict_{false};
    // Force-evict: allow evicting MEMORY replicas without disk offload when cap
    // exceeded (config: offload_force_evict, only effective when
    // offload_on_evict_=true)
    bool offload_force_evict_{false};

    const std::string ha_backend_type_;

    const std::string ha_backend_connstring_;

    // cluster id for persistent sub directory
    const std::string cluster_id_;
    // root filesystem directory for persistent storage
    const std::string root_fs_dir_;
    // global 3fs/nfs segment size
    int64_t global_file_segment_size_;
    // storage backend eviction configuration
    const bool enable_disk_eviction_;
    const uint64_t quota_bytes_;

    // Per-tenant quota
    const bool enable_tenant_quota_;
    // Captured at construction time so the failed-restore reset path can
    // reinstate the operator-supplied default before retrying another
    // snapshot candidate. Without this, a restore that ran
    // RestorePolicies() on a snapshot and then failed mid-flight would
    // leave the previous attempt's snapshot-default in place; if the
    // next candidate happened to be a legacy snapshot (no quota field),
    // the deserializer would skip the default-overwrite and the leftover
    // policy would silently survive.
    const TenantQuotaPolicy startup_tenant_quota_policy_;
    TenantQuotaTable tenant_quotas_;

    bool use_disk_replica_{false};

    // Segment management
    SegmentManager segment_manager_;
    BufferAllocatorType memory_allocator_type_;
    std::shared_ptr<AllocationStrategy> allocation_strategy_;

    bool enable_snapshot_restore_ = false;

    bool enable_snapshot_ = false;
    std::string snapshot_backup_dir_;
    bool use_snapshot_backup_dir_{false};
    uint64_t snapshot_interval_seconds_ = DEFAULT_SNAPSHOT_INTERVAL_SEC;
    uint64_t snapshot_child_timeout_seconds_ =
        DEFAULT_SNAPSHOT_CHILD_TIMEOUT_SEC;
    uint32_t snapshot_retention_count_ = DEFAULT_SNAPSHOT_RETENTION_COUNT;
    std::string snapshot_catalog_store_type_{};
    std::string snapshot_catalog_store_connstring_;
    std::unique_ptr<SnapshotObjectStore> snapshot_object_store_;
    std::unique_ptr<ha::SnapshotCatalogStore> snapshot_catalog_store_;
    mutable std::shared_mutex snapshot_mutex_;
#ifdef STORE_USE_ETCD
    mutable std::mutex snapshot_boundary_oplog_store_mutex_;
    mutable std::unique_ptr<EtcdOpLogStore> snapshot_boundary_oplog_store_;
#endif

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
                if (replica.is_memory_replica()) {
                    mem_size_ += replica.get_memory_buffer_size();
                }
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

    struct ActiveDrainTask {
        UUID task_id;
        std::string key;
        std::string source_segment;
        std::string target_segment;
        size_t bytes;
        std::string unit_key;
    };

    struct DrainJob {
        mutable std::mutex mutex;
        UUID id;
        JobType type{JobType::DRAIN};
        JobStatus status{JobStatus::CREATED};
        CreateDrainJobRequest request;
        std::chrono::system_clock::time_point created_at;
        std::chrono::system_clock::time_point last_updated_at;
        std::string message;
        uint64_t succeeded_units{0};
        uint64_t failed_units{0};
        uint64_t blocked_units{0};
        uint64_t migrated_bytes{0};
        std::unordered_map<UUID, ActiveDrainTask, boost::hash<UUID>>
            active_tasks;
        std::unordered_set<std::string> completed_unit_keys;
        std::unordered_map<std::string, uint32_t> retry_counts;
        std::unordered_set<std::string> terminal_failed_unit_keys;
    };

    static constexpr uint32_t kMaxDrainUnitRetries = 3;

    tl::expected<void, ErrorCode> ValidateDrainRequest(
        const CreateDrainJobRequest& request);
    tl::expected<void, ErrorCode> ValidateDrainRequestLocked(
        ScopedSegmentAccess& segment_access,
        const CreateDrainJobRequest& request);
    void ProcessDrainJobs();
    void RefreshDrainJobTasks(DrainJob& job);
    void ScheduleDrainJobTasks(DrainJob& job);
    bool MaybeCompleteDrainJob(DrainJob& job);
    std::optional<std::string> SelectDrainTargetForKey(
        const ObjectMetadata& metadata, const std::string& source_segment,
        const std::vector<std::string>& requested_targets);
    std::string MakeDrainUnitKey(const std::string& key,
                                 const std::string& source_segment) const;

    std::thread job_dispatch_thread_;
    std::atomic<bool> job_dispatch_running_{false};
    static constexpr uint64_t kJobDispatchThreadSleepMs = 500;
    std::mutex job_mutex_;
    std::unordered_map<UUID, std::shared_ptr<DrainJob>, boost::hash<UUID>>
        drain_jobs_ GUARDED_BY(job_mutex_);
};

}  // namespace mooncake
