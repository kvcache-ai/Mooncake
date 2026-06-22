#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "replica.h"
#include "rpc_types.h"
#include "segment.h"
#include "task_manager.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Abstract interface for metadata storage and retrieval.
 *
 * MetadataBackend defines the contract for storing and managing object metadata,
 * replica information, segment registration, client heartbeats, offload/promotion
 * work queues, and replication tasks. The MasterService delegates all metadata
 * operations to a concrete implementation of this interface.
 *
 * The initial concrete implementation (NativeMetadataBackend) preserves the
 * existing in-memory behavior. Future implementations (e.g., RedisMetadataBackend)
 * may persist metadata to external stores for scalability or fault tolerance.
 *
 * All methods return tl::expected<T, ErrorCode> where ErrorCode communicates
 * the reason for failure. Implementations must be safe for concurrent access
 * from multiple RPC threads.
 *
 * Implementations that hold internal locks should document their lock order
 * and ensure a consistent acquisition sequence to prevent deadlocks.
 */
class MetadataBackend {
   public:
    virtual ~MetadataBackend() = default;

    // -----------------------------------------------------------------------
    // Key existence and enumeration
    // -----------------------------------------------------------------------

    /**
     * @brief Check if an object exists.
     * @param key The user-facing object key.
     * @param tenant_id Tenant that owns the key.
     * @return true if the key exists and has at least one valid replica.
     */
    virtual auto ExistKey(const std::string& key, const std::string& tenant_id)
        -> tl::expected<bool, ErrorCode> = 0;

    /**
     * @brief Batch existence check for multiple keys.
     * @param keys Vector of user-facing object keys.
     * @param tenant_id Tenant that owns the keys.
     * @return Per-key existence results in the same order as the input.
     */
    virtual std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys,
        const std::string& tenant_id) = 0;

    /**
     * @brief Fetch all keys for a single tenant.
     * @param tenant_id Tenant whose keys should be listed.
     * @return Vector of keys belonging to the tenant.
     */
    virtual auto GetAllKeys(const std::string& tenant_id)
        -> tl::expected<std::vector<std::string>, ErrorCode> = 0;

    /**
     * @brief Get the total count of keys across all tenants.
     * @return Number of keys currently tracked.
     */
    virtual size_t GetKeyCount() const = 0;

    // -----------------------------------------------------------------------
    // Replica queries
    // -----------------------------------------------------------------------

    /**
     * @brief Get the list of replicas for an object, refreshing the lease.
     * @param key The user-facing object key.
     * @param tenant_id Tenant that owns the key.
     * @return GetReplicaListResponse containing replica descriptors and the
     *         lease TTL in milliseconds, or ErrorCode::REPLICA_IS_NOT_READY
     *         if no valid replicas are available.
     */
    virtual auto GetReplicaList(const std::string& key,
                                const std::string& tenant_id)
        -> tl::expected<GetReplicaListResponse, ErrorCode> = 0;

    /**
     * @brief Get replica lists for all keys matching a regex pattern.
     * @param regex_pattern Regular expression to match against object keys.
     * @param tenant_id Tenant that owns the keys.
     * @return Map from matching keys to their replica descriptor vectors.
     */
    virtual auto GetReplicaListByRegex(const std::string& regex_pattern,
                                       const std::string& tenant_id)
        -> tl::expected<
            std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
            ErrorCode> = 0;

    // -----------------------------------------------------------------------
    // Put (object creation) lifecycle
    // -----------------------------------------------------------------------

    /**
     * @brief Begin a put operation: allocate replica buffers and register
     *        the object as in-progress.
     * @param client_id UUID of the client performing the put.
     * @param key Object key.
     * @param tenant_id Tenant that owns the key.
     * @param slice_length Size of the object in bytes.
     * @param config Replication configuration (replica count, preferred
     *               segments, etc.).
     * @return Vector of replica descriptors for the allocated buffers.
     */
    virtual auto PutStart(const UUID& client_id, const std::string& key,
                          const std::string& tenant_id,
                          const uint64_t slice_length,
                          const ReplicateConfig& config)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> = 0;

    /**
     * @brief Complete a put operation, marking replicas as COMPLETE.
     * @param client_id UUID of the client.
     * @param key Object key.
     * @param tenant_id Tenant that owns the key.
     * @param replica_type Which replica types to finalize (MEMORY, DISK, ALL).
     */
    virtual auto PutEnd(const UUID& client_id, const std::string& key,
                        const std::string& tenant_id,
                        ReplicaType replica_type)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Add an externally created replica to an existing object.
     * @param client_id UUID of the client.
     * @param key Object key.
     * @param tenant_id Tenant that owns the key.
     * @param replica The replica to add. Non-const because the replica may be
     *                moved from when no existing LOCAL_DISK replica exists for
     *                this client; otherwise only its descriptor fields are read.
     */
    virtual auto AddReplica(const UUID& client_id, const std::string& key,
                            const std::string& tenant_id, Replica& replica)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Revoke (abort) a put operation, cleaning up allocated resources.
     * @param client_id UUID of the client.
     * @param key Object key.
     * @param tenant_id Tenant that owns the key.
     * @param replica_type Which replica types to revoke.
     */
    virtual auto PutRevoke(const UUID& client_id, const std::string& key,
                           const std::string& tenant_id,
                           ReplicaType replica_type)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Complete a batch of put operations.
     * @return Per-key results in the same order as the input.
     */
    virtual std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id,
        ReplicaType replica_type = ReplicaType::ALL) = 0;

    /**
     * @brief Revoke a batch of put operations.
     * @return Per-key results in the same order as the input.
     */
    virtual std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id,
        ReplicaType replica_type = ReplicaType::ALL) = 0;

    // -----------------------------------------------------------------------
    // Upsert lifecycle
    // -----------------------------------------------------------------------

    /**
     * @brief Begin an upsert operation. If the key does not exist, behaves
     *        like PutStart. If the key exists with the same size, performs
     *        an in-place update. If sizes differ, deletes old replicas and
     *        allocates new ones.
     * @return Replica descriptors for the allocated buffers.
     */
    virtual auto UpsertStart(const UUID& client_id, const std::string& key,
                             const std::string& tenant_id,
                             const uint64_t slice_length,
                             const ReplicateConfig& config)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> = 0;

    /**
     * @brief Complete an upsert operation. Delegates to PutEnd internally.
     */
    virtual auto UpsertEnd(const UUID& client_id, const std::string& key,
                           const std::string& tenant_id,
                           ReplicaType replica_type)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Revoke an upsert operation. Delegates to PutRevoke internally.
     */
    virtual auto UpsertRevoke(const UUID& client_id, const std::string& key,
                              const std::string& tenant_id,
                              ReplicaType replica_type)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Begin a batch of upsert operations.
     */
    virtual std::vector<
        tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchUpsertStart(const UUID& client_id,
                     const std::vector<std::string>& keys,
                     const std::string& tenant_id,
                     const std::vector<uint64_t>& slice_lengths,
                     const ReplicateConfig& config) = 0;

    /**
     * @brief Complete a batch of upsert operations.
     */
    virtual std::vector<tl::expected<void, ErrorCode>> BatchUpsertEnd(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id) = 0;

    /**
     * @brief Revoke a batch of upsert operations.
     */
    virtual std::vector<tl::expected<void, ErrorCode>> BatchUpsertRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id) = 0;

    // -----------------------------------------------------------------------
    // Disk replica eviction (client-initiated)
    // -----------------------------------------------------------------------

    /**
     * @brief Evict a disk replica for a key, triggered by client-side
     *        disk eviction.
     * @param client_id The client performing the eviction.
     * @param key Object key whose disk replica was evicted.
     * @param tenant_id Tenant that owns the key.
     * @param replica_type DISK or LOCAL_DISK.
     */
    virtual auto EvictDiskReplica(const UUID& client_id,
                                  const std::string& key,
                                  const std::string& tenant_id,
                                  ReplicaType replica_type)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Batch evict disk replicas for multiple keys.
     * @return Per-key results in the same order as the input.
     */
    virtual std::vector<tl::expected<void, ErrorCode>> BatchEvictDiskReplica(
        const UUID& client_id, const std::vector<std::string>& keys,
        const std::string& tenant_id, ReplicaType replica_type) = 0;

    // -----------------------------------------------------------------------
    // Replica clear
    // -----------------------------------------------------------------------

    /**
     * @brief Batch clear KV cache replicas for specified object keys.
     * @param object_keys Vector of object key strings to clear.
     * @param client_id UUID of the client that owns the keys.
     * @param segment_name Segment to clear from; empty means all segments.
     * @return Vector of successfully cleared keys.
     */
    virtual auto BatchReplicaClear(const std::vector<std::string>& object_keys,
                                   const UUID& client_id,
                                   const std::string& segment_name)
        -> tl::expected<std::vector<std::string>, ErrorCode> = 0;

    // -----------------------------------------------------------------------
    // Copy operations
    // -----------------------------------------------------------------------

    /**
     * @brief Start a copy: allocate target replica buffers.
     * @param client_id Client submitting the copy request.
     * @param key Object key.
     * @param tenant_id Tenant that owns the key.
     * @param src_segment Source segment name.
     * @param tgt_segments Target segment names.
     * @return CopyStartResponse with source and target descriptors.
     */
    virtual tl::expected<CopyStartResponse, ErrorCode> CopyStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, const std::string& src_segment,
        const std::vector<std::string>& tgt_segments) = 0;

    /** @brief Mark a copy operation as complete. */
    virtual tl::expected<void, ErrorCode> CopyEnd(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id) = 0;

    /** @brief Revoke (abort) a copy operation. */
    virtual tl::expected<void, ErrorCode> CopyRevoke(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id) = 0;

    // -----------------------------------------------------------------------
    // Move operations
    // -----------------------------------------------------------------------

    /**
     * @brief Start a move: allocate target replica buffer.
     * @param client_id Client submitting the move request.
     * @param key Object key.
     * @param tenant_id Tenant that owns the key.
     * @param src_segment Source segment name.
     * @param tgt_segment Target segment name.
     * @return MoveStartResponse with source and target descriptors.
     */
    virtual tl::expected<MoveStartResponse, ErrorCode> MoveStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, const std::string& src_segment,
        const std::string& tgt_segment) = 0;

    /** @brief Mark a move operation as complete. */
    virtual tl::expected<void, ErrorCode> MoveEnd(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id) = 0;

    /** @brief Revoke (abort) a move operation. */
    virtual tl::expected<void, ErrorCode> MoveRevoke(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id) = 0;

    // -----------------------------------------------------------------------
    // Object removal
    // -----------------------------------------------------------------------

    /**
     * @brief Remove an object and its replicas.
     * @param key Object key.
     * @param tenant_id Tenant that owns the key.
     * @param force If true, skip lease and replication task checks.
     */
    virtual auto Remove(const std::string& key, const std::string& tenant_id,
                        bool force = false)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Remove all objects whose keys match a regex pattern.
     * @param str Regular expression to match against object keys.
     * @param tenant_id Tenant that owns the keys.
     * @param force If true, skip lease and replication task checks.
     * @return Number of objects removed.
     */
    virtual auto RemoveByRegex(const std::string& str,
                               const std::string& tenant_id,
                               bool force = false)
        -> tl::expected<long, ErrorCode> = 0;

    /**
     * @brief Remove all objects across all tenants.
     * @param force If true, skip lease and replication task checks.
     * @return Number of objects removed, or ErrorCode on failure.
     */
    virtual auto RemoveAll(bool force = false)
        -> tl::expected<long, ErrorCode> = 0;

    /**
     * @brief Remove all objects for a single tenant.
     * @param tenant_id Tenant whose objects should be removed.
     * @param force If true, skip lease and replication task checks.
     * @return Number of objects removed, or ErrorCode on failure.
     */
    virtual auto RemoveAll(const std::string& tenant_id, bool force = false)
        -> tl::expected<long, ErrorCode> = 0;

    /**
     * @brief Batch remove objects and their replicas.
     * @param keys List of keys to remove.
     * @param tenant_id Tenant that owns the keys.
     * @param force If true, skip lease and replication task checks.
     * @return Per-key results in the same order as the input.
     */
    virtual auto BatchRemove(const std::vector<std::string>& keys,
                             const std::string& tenant_id,
                             bool force = false)
        -> std::vector<tl::expected<void, ErrorCode>> = 0;

    // -----------------------------------------------------------------------
    // Segment management
    // -----------------------------------------------------------------------

    /**
     * @brief Mount a memory segment for buffer allocation. Idempotent.
     * @param segment Segment descriptor.
     * @param client_id UUID of the client mounting the segment.
     */
    virtual auto MountSegment(const Segment& segment, const UUID& client_id)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Mount a NoF SSD segment for buffer allocation. Idempotent.
     */
    virtual auto MountNoFSegment(const NoFSegment& segment,
                                 const UUID& client_id)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Re-mount memory segments after client reconnection or Ping TTL
     *        expiry. Idempotent.
     */
    virtual auto ReMountSegment(const std::vector<Segment>& segments,
                                const UUID& client_id)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Re-mount NoF SSD segments after client reconnection or Ping TTL
     *        expiry. Idempotent.
     */
    virtual auto ReMountNoFSegment(const std::vector<NoFSegment>& segments,
                                   const UUID& client_id)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Unmount a memory segment. Idempotent.
     */
    virtual auto UnmountSegment(const UUID& segment_id,
                                const UUID& client_id)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Gracefully unmount a segment with a grace period before forced
     *        removal.
     */
    virtual auto GracefulUnmountSegment(const UUID& segment_id,
                                        const UUID& client_id,
                                        uint64_t grace_period_ms)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Unmount a NoF SSD segment. Idempotent.
     */
    virtual auto UnmountNoFSegment(const UUID& segment_id,
                                   const UUID& client_id)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Mount a file storage (local disk) segment into the master.
     * @param client_id UUID of the client.
     * @param enable_offloading If true, enables write-to-file offloading.
     */
    virtual auto MountLocalDiskSegment(const UUID& client_id,
                                       bool enable_offloading)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Fetch all registered segment names.
     */
    virtual auto GetAllSegments()
        -> tl::expected<std::vector<std::string>, ErrorCode> = 0;

    /**
     * @brief Fetch all mounted NoF segments.
     */
    virtual auto GetAllNoFSegments()
        -> tl::expected<std::vector<NoFSegment>, ErrorCode> = 0;

    /**
     * @brief Query mounted NoF segments by name and return their owner info.
     */
    virtual auto GetNoFSegmentsByName(const std::string& segment_name)
        -> tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode> = 0;

    /**
     * @brief Query a segment's capacity and used size in bytes.
     * @return Pair of (used_bytes, capacity_bytes).
     */
    virtual auto QuerySegments(const std::string& segment)
        -> tl::expected<std::pair<size_t, size_t>, ErrorCode> = 0;

    /**
     * @brief Get detailed information of all segments, including the
     *        relationships between segment_id, client_id, segment_name,
     *        status, allocator used/capacity, etc.
     * @return A vector of SegmentDetailInfo on success, error code otherwise.
     */
    virtual auto GetSegmentsDetail()
        -> tl::expected<std::vector<SegmentDetailInfo>, ErrorCode> = 0;

    /**
     * @brief Query current segment lifecycle state by segment name.
     */
    virtual tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatus(
        const std::string& segment_name) = 0;

    /**
     * @brief Query current segment lifecycle state by segment id.
     */
    virtual tl::expected<SegmentStatus, ErrorCode> QuerySegmentStatusById(
        const UUID& segment_id) = 0;

    // -----------------------------------------------------------------------
    // Client identity and IP lookup
    // -----------------------------------------------------------------------

    /**
     * @brief Query IP addresses for a given client ID.
     * @param client_id UUID of the client.
     * @return Vector of IP addresses (empty if client has no IPs).
     */
    virtual auto QueryIp(const UUID& client_id)
        -> tl::expected<std::vector<std::string>, ErrorCode> = 0;

    /**
     * @brief Batch query IP addresses for multiple client IDs.
     * @param client_ids Vector of client UUIDs.
     * @return Map from client_id to their IP address lists. Non-existent
     *         clients are omitted; clients with no IPs have empty vectors.
     */
    virtual auto BatchQueryIp(const std::vector<UUID>& client_ids)
        -> tl::expected<
            std::unordered_map<UUID, std::vector<std::string>, UUIDHash>,
            ErrorCode> = 0;

    // -----------------------------------------------------------------------
    // Client heartbeat
    // -----------------------------------------------------------------------

    /**
     * @brief Heartbeat from a client. Refreshes the client's TTL and returns
     *        the current view version and client status (OK or NEED_REMOUNT).
     */
    virtual auto Ping(const UUID& client_id)
        -> tl::expected<PingResponse, ErrorCode> = 0;

    // -----------------------------------------------------------------------
    // Configuration queries
    // -----------------------------------------------------------------------

    /**
     * @brief Get the master service cluster directory name.
     */
    virtual tl::expected<std::string, ErrorCode> GetFsdir() const = 0;

    /**
     * @brief Get storage backend configuration including eviction settings.
     */
    virtual tl::expected<GetStorageConfigResponse, ErrorCode>
    GetStorageConfig() const = 0;

    // -----------------------------------------------------------------------
    // Offload workflow (disk offload-on-evict)
    // -----------------------------------------------------------------------

    /**
     * @brief Heartbeat call to collect offload statistics and retrieve the
     *        set of objects pending offload to disk.
     * @param client_id UUID of the client.
     * @param enable_offloading Whether offloading is enabled for this segment.
     * @return Vector of offload task items for the client to execute.
     */
    virtual auto OffloadObjectHeartbeat(const UUID& client_id,
                                        bool enable_offloading)
        -> tl::expected<std::vector<OffloadTaskItem>, ErrorCode> = 0;

    /**
     * @brief Report SSD capacity from a client.
     */
    virtual auto ReportSsdCapacity(const UUID& client_id,
                                   int64_t ssd_total_capacity_bytes)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Notify the master that offloading of specified objects succeeded.
     * @param client_id UUID of the client that performed the offload.
     * @param tasks List of successfully offloaded objects.
     * @param metadatas Corresponding metadata for each offloaded object.
     */
    virtual auto NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas)
        -> tl::expected<void, ErrorCode> = 0;

    // -----------------------------------------------------------------------
    // Promotion workflow (LOCAL_DISK -> MEMORY promotion-on-hit)
    // -----------------------------------------------------------------------

    /**
     * @brief Heartbeat-driven pull of pending promotion work for a client.
     * @param client_id UUID of the client.
     * @return Vector of promotion task items for the client to execute.
     */
    virtual auto PromotionObjectHeartbeat(const UUID& client_id)
        -> tl::expected<std::vector<PromotionTaskItem>, ErrorCode> = 0;

    /**
     * @brief Allocate a PROCESSING MEMORY replica for an existing key as part
     *        of a LOCAL_DISK -> MEMORY promotion.
     * @param client_id Holder client (owner of the source LOCAL_DISK replica).
     * @param key Object key.
     * @param tenant_id Tenant that owns the key.
     * @param size Object size in bytes (must match the source replica).
     * @param preferred_segments Segments preferred for the new allocation.
     * @return PromotionAllocStartResponse with the staged replica descriptor.
     */
    virtual auto PromotionAllocStart(
        const UUID& client_id, const std::string& key,
        const std::string& tenant_id, uint64_t size,
        const std::vector<std::string>& preferred_segments)
        -> tl::expected<PromotionAllocStartResponse, ErrorCode> = 0;

    /**
     * @brief Commit a staged MEMORY replica to COMPLETE after a successful
     *        promotion transfer.
     */
    virtual auto NotifyPromotionSuccess(const UUID& client_id,
                                        const std::string& key,
                                        const std::string& tenant_id)
        -> tl::expected<void, ErrorCode> = 0;

    /**
     * @brief Notify that a promotion transfer failed, releasing the task
     *        state immediately rather than waiting for the reaper.
     */
    virtual auto NotifyPromotionFailure(const UUID& client_id,
                                        const std::string& key,
                                        const std::string& tenant_id)
        -> tl::expected<void, ErrorCode> = 0;

    // -----------------------------------------------------------------------
    // Replication tasks (async copy/move/drain)
    // -----------------------------------------------------------------------

    /**
     * @brief Create a copy task to replicate an object to target segments.
     * @return Task ID on success.
     */
    virtual tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const std::string& tenant_id,
        const std::vector<std::string>& targets) = 0;

    /**
     * @brief Create a move task to relocate an object's replica.
     * @return Task ID on success.
     */
    virtual tl::expected<UUID, ErrorCode> CreateMoveTask(
        const std::string& key, const std::string& tenant_id,
        const std::string& source, const std::string& target) = 0;

    /**
     * @brief Create a drain job to gracefully evacuate one or more segments.
     * @return Job ID on success.
     */
    virtual tl::expected<UUID, ErrorCode> CreateDrainJob(
        const CreateDrainJobRequest& request) = 0;

    /**
     * @brief Query the status of a drain job.
     */
    virtual tl::expected<QueryJobResponse, ErrorCode> QueryDrainJob(
        const UUID& job_id) = 0;

    /**
     * @brief Cancel an in-flight drain job and restore draining segments.
     */
    virtual tl::expected<void, ErrorCode> CancelDrainJob(
        const UUID& job_id) = 0;

    /**
     * @brief Query the status of a task.
     */
    virtual tl::expected<QueryTaskResponse, ErrorCode> QueryTask(
        const UUID& task_id) = 0;

    /**
     * @brief Fetch tasks assigned to a client.
     * @param client_id UUID of the client.
     * @param batch_size Maximum number of tasks to return.
     * @return Vector of task assignments.
     */
    virtual tl::expected<std::vector<TaskAssignment>, ErrorCode> FetchTasks(
        const UUID& client_id, size_t batch_size) = 0;

    /**
     * @brief Mark a task as complete.
     * @param client_id UUID of the client completing the task.
     * @param request Task completion details (task ID, status, message).
     */
    virtual tl::expected<void, ErrorCode> MarkTaskToComplete(
        const UUID& client_id, const TaskCompleteRequest& request) = 0;
};

}  // namespace mooncake
