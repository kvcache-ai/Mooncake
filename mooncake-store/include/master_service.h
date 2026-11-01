#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/functional/hash.hpp>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <list>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "allocation_strategy.h"
#include "background_worker.h"
#include "client_liveness.h"
#include "client_offboarding.h"
#include "count_min_sketch.h"
#include "deadline_scheduler.h"
#include "dynamic_replication_controller.h"
#include "lease.h"
#include "master_metric_manager.h"
#include "mutex.h"
#include "segment.h"
#include "local_ssd/manager.h"
#include "tenant/quota_policy.h"
#include "types.h"
#include "weight_store_manager.h"
#include "master_config.h"
#include "object_metadata.h"
#include "object_runtime_state.h"
#include "metadata/namespace_table.h"
#include "metadata/object_route.h"
#include "metadata/object_state.h"
#include "promotion_candidate_tracker.h"
#include "rpc_types.h"
#include "replica.h"
#include "ha/ha_types.h"
#include "ha/snapshot/batch_oplog/promotion.h"
#include "task_manager.h"
#include "kv_event/kv_event_publisher.h"
#include "ha/oplog/oplog_types.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "allocator.h"
#include "metadata_store.h"

namespace mooncake {

class ShardAllocator;
class DfsAllocatorInterface;
class ImmutableBucketAllocator;

// Forward declarations
class AllocationStrategy;
class HaKvBackend;
class HttpMetadataServer;
class OpLogBatchStorage;
class OrderedOpLogWriter;

namespace test {
class MasterServiceTestPeer;
class PrefetchTaskMasterTest;
}  // namespace test

/*
 * @brief MasterService is the main class for the master server.
 * Lock order: To avoid deadlocks, the following lock order should be followed:
 * 1. client_mutex_
 * 2. the namespace policy's admission token (NamespacePolicy::AdmitWrite;
 *    the tenant quota policy's policy lock)
 * 3. master_state_mutex_
 * 4. one key of a namespace (see metadata::Namespace)
 * 5. the tenant quota policy's recompute lock
 * 6. the quota table's shard locks or segment_mutex_
 * 7. soft_pin_deadline_index_ mutex
 *
 * Strict tenant admission and policy mutation paths that need both the
 * quota policy lock and master_state_mutex_ must acquire the policy lock
 * first, then master_state_mutex_.
 * The recompute lock serializes the capacity snapshot and the corresponding
 * quota-table update. The segment mutex is released before entering the
 * quota table, so these two locks are never nested.
 */

class MasterService {
    friend class MasterStoreBackend;
    friend class test::MasterServiceTestPeer;
    friend class test::PrefetchTaskMasterTest;
    friend class ClientOffboardingWorker;

   public:
    using NoFProbeFn =
        std::function<bool(const std::string&, uint32_t, std::string*)>;
    using DurableFinalizeCallback =
        std::function<void(const OpLogEntry& durable_entry)>;
    using BatchOpLogWriterFactory =
        std::function<std::unique_ptr<OrderedOpLogWriter>(
            OrderedOpLogWriterConfig, OrderedOpLogWriter::WriteBatchFn)>;

    MasterService();
    MasterService(const MasterServiceConfig& config);
    ~MasterService();

    [[nodiscard]] TieredStorageUsageSnapshot GetStorageUsageSnapshot() const;
    // Whether requests name their tenant, which multi-tenancy turns on.
    bool IsTenantQuotaEnabled() const {
        return tenant_quota_policy_ != nullptr;
    }
    // Multi-tenancy's control plane; null when multi-tenancy is off.
    TenantQuotaPolicy* tenant_quota_policy() const {
        return tenant_quota_policy_.get();
    }

    WeightMetadataStore::Result<WeightRevisionLease> AcquireWeightRevisionLease(
        const AcquireWeightRevisionLeaseRequest& request);
    WeightMetadataStore::Result<WeightRevisionLease> RenewWeightRevisionLease(
        const RenewWeightRevisionLeaseRequest& request);
    WeightMetadataStore::Result<void> ReleaseWeightRevisionLease(
        const ReleaseWeightRevisionLeaseRequest& request);

    WeightMetadataStore::Result<WeightResidencyOperation>
    StartWeightResidencyOperation(
        const StartWeightResidencyOperationRequest& request);
    WeightMetadataStore::Result<WeightResidencyOperation> QueryWeightOperation(
        const QueryWeightOperationRequest& request) const;
    WeightMetadataStore::Result<WeightRevisionMetadata> ReconcileWeightRevision(
        const ReconcileWeightRevisionRequest& request);
    WeightMetadataStore::Result<WeightRevisionMetadata> DeleteWeightRevision(
        const DeleteWeightRevisionRequest& request);

    WeightMetadataStore::Result<WeightRevisionMetadata> BeginWeightImport(
        const BeginWeightImportRequest& request);
    WeightMetadataStore::Result<WeightRevisionMetadata> CommitWeightImport(
        const CommitWeightImportRequest& request);
    WeightMetadataStore::Result<WeightRevisionMetadata> AbortWeightImport(
        const AbortWeightImportRequest& request);
    WeightMetadataStore::Result<WeightRevisionView> GetWeightRevision(
        const GetWeightRevisionRequest& request) const;
    WeightMetadataStore::Result<ListWeightRevisionsResponse>
    ListWeightRevisions(const ListWeightRevisionsRequest& request) const;

    void SetBatchOpLogTerminalCallback(
        OrderedOpLogWriter::TerminalCallback callback);
    void StopBatchOpLogWriter();

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
     * @brief Mount a NoF SSD segment for buffer allocation. This function is
     * idempotent.
     * @return ErrorCode::OK on success,
     *         ErrorCode::INVALID_PARAMS on invalid parameters,
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment cannot
     *         be mounted temporarily,
     *         ErrorCode::INTERNAL_ERROR on internal errors.
     */
    auto MountNoFSegment(const NoFSegment& segment, const UUID& client_id)
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
     * @brief Re-mount NoF SSD segments, invoked when the client is the first
     * time to connect to the master or the client Ping TTL is expired and need
     * to remount. This function is idempotent. Client should retry if the
     * return code is not ErrorCode::OK.
     * @return ErrorCode::OK means either all segments are remounted
     * successfully or the fail is not solvable by a new remount request.
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment cannot
     *         be mounted temporarily.
     *         ErrorCode::INTERNAL_ERROR if something temporary error happens.
     */
    auto ReMountNoFSegment(const std::vector<NoFSegment>& segments,
                           const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

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
     * @brief Unmount a NoF ssd segment. This function is idempotent.
     * @return ErrorCode::OK on success,
     *         ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS if the segment is
     *         currently unmounting.
     */
    auto UnmountNoFSegment(const UUID& segment_id, const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Check if an object exists
     * @return ErrorCode::OK if exists, otherwise return other ErrorCode
     */
    auto ExistKey(const std::string& key, const TenantId& tenant_id)
        -> tl::expected<bool, ErrorCode>;

    std::vector<tl::expected<bool, ErrorCode>> BatchExistKey(
        const std::vector<std::string>& keys, const TenantId& tenant_id);

    /**
     * @brief Point-in-time existence check that grants no read lease.
     *        A `true` result only means the object existed at the time of
     *        the call; it may be evicted before a subsequent Get.
     * @return ErrorCode::OK if exists, otherwise return other ErrorCode
     */
    auto ProbeKey(const std::string& key, const TenantId& tenant_id)
        -> tl::expected<bool, ErrorCode>;

    std::vector<tl::expected<bool, ErrorCode>> BatchProbeKey(
        const GrantLeasePolicy& policy, const std::vector<std::string>& keys,
        const TenantId& tenant_id);

    /**
     * @brief Fetch all keys for a single tenant.
     * @return ErrorCode::OK if exists
     */
    auto GetAllKeys(const TenantId& tenant_id)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Fetch all segments, each node has a unique real client with fixed
     * segment name : segment name, preferred format : {ip}:{port}, bad format :
     * localhost:{port}
     * @return ErrorCode::OK if exists
     */
    auto GetAllSegments() -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Fetch all mounted NoF segments.
     * @return std::vector<MountedNoFSegmentSnapshot> on success, error code
     * otherwise.
     */
    auto GetAllNoFSegments()
        -> tl::expected<std::vector<NoFSegment>, ErrorCode>;

    /**
     * @brief Query mounted NoF segments by segment name and return their
     * segment ids together with owner client ids.
     * @param segment_name Mounted NoF segment name.
     * @return Matching segment owner info list on success, error code
     * otherwise.
     */
    auto GetNoFSegmentsByName(const std::string& segment_name)
        -> tl::expected<std::vector<NoFSegmentOwnerInfo>, ErrorCode>;

    /**
     * @brief Detailed information about a single segment.
     * Keeps original types so callers can use values directly without
     * needing to parse strings back to uuid/address/enum.
     */
    struct SegmentDetailInfo {
        std::string segment_name;
        UUID segment_id{0, 0};
        UUID client_id{0, 0};
        uintptr_t base_address{0};
        uint64_t size_bytes{0};
        std::string te_endpoint;
        std::string protocol;
        SegmentStatus status{SegmentStatus::UNDEFINED};
        uint64_t allocator_used_bytes{0};
        uint64_t allocator_capacity_bytes{0};
    };

    /**
     * @brief Get detailed information of all segments, including the
     * relationships between segment_id, client_id, segment_name, status,
     * allocator used/capacity, etc.
     * @return A vector of SegmentDetailInfo on success, error code otherwise.
     */
    auto GetSegmentsDetail()
        -> tl::expected<std::vector<SegmentDetailInfo>, ErrorCode>;

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

    bool KvEventsEnabled() const;
    KvEventPublisher::Stats GetKvEventStats() const;

    /**
     * @brief Batch clear KV cache replicas for specified object keys.
     * @param object_keys Vector of object key strings to clear.
     * @param client_id The UUID of the client that owns the object keys.
     * @param segment_name The name of the segment (storage device) to clear
     * from. If empty, clears replicas from all segments for the given
     * client_id.
     * @param tenant_id The tenant that owns the keys.
     * @return An expected object containing a vector of successfully cleared
     * keys on success, or an ErrorCode on failure. Only successfully
     * cleared keys are included in the result.
     */
    auto BatchReplicaClear(
        const std::vector<std::string>& object_keys, const UUID& client_id,
        const std::string& segment_name,
        const std::string& tenant_id = std::string(TenantId::kDefaultValue))
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    /**
     * @brief Retrieves replica lists for object keys that match a regex
     * pattern.
     * @param str The regular expression string to match against object keys.
     * @return An expected object containing a map from object keys to their
     * replica descriptors on success, or an ErrorCode on failure.
     */
    auto GetReplicaListByRegex(const std::string& regex_pattern,
                               const TenantId& tenant_id)
        -> tl::expected<
            std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
            ErrorCode>;

    /**
     * @brief Get list of replicas for an object
     * @param[out] replica_list Vector to store replica information
     * @return ErrorCode::OK on success, ErrorCode::REPLICA_IS_NOT_READY if not
     * ready
     */
    auto GetReplicaList(const std::string& key, const TenantId& tenant_id)
        -> tl::expected<GetReplicaListResponse, ErrorCode>;

    /**
     * @brief Read-only single-key replica list query for admin use.
     * Unlike GetReplicaList, this does not grant leases, trigger
     * promotion, or update cache-hit metrics.
     */
    auto GetReplicaListForAdmin(const std::string& key,
                                const TenantId& tenant_id)
        -> tl::expected<GetReplicaListResponse, ErrorCode>;

    /**
     * @brief Get replica lists for a batch of objects.
     */
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaList(const std::vector<std::string>& keys,
                        const TenantId& tenant_id);

    /**
     * @brief Read-only batch replica list query for admin use.
     * Unlike BatchGetReplicaList, this does not grant leases, trigger
     * promotion, or update cache-hit metrics.
     */
    std::vector<tl::expected<GetReplicaListResponse, ErrorCode>>
    BatchGetReplicaListForAdmin(const std::vector<std::string>& keys,
                                const TenantId& tenant_id);

    /**
     * @brief Start a put operation for an object
     * @param[out] replica_list Vector to store replica information for the
     * slice
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if exists,
     *         ErrorCode::NO_AVAILABLE_HANDLE if allocation fails,
     *         ErrorCode::INVALID_PARAMS if slice size is invalid
     */
    auto PutStart(const UUID& client_id, const std::string& key,
                  const TenantId& tenant_id, const uint64_t slice_length,
                  const ReplicateConfig& config)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    /**
     * @brief Complete a put operation, replica_type indicates the type of
     * replica to complete (memory or disk)
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    auto PutEnd(const UUID& client_id, const ObjectMeta& object_meta,
                const TenantId& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode>;

    auto PutEnd(const UUID& client_id, const std::string& key,
                const TenantId& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Adds a replica instance associated with the given client and key.
     */
    auto AddReplica(const UUID& client_id, const std::string& key,
                    const TenantId& tenant_id, Replica& replica)
        -> tl::expected<bool, ErrorCode>;

    /**
     * @brief Revoke a put operation, replica_type indicates the type of
     * replica to revoke (memory or disk)
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    auto PutRevoke(const UUID& client_id, const std::string& key,
                   const TenantId& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Complete a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPutEnd(
        const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
        const TenantId& tenant_id, ReplicaType replica_type = ReplicaType::ALL);

    /**
     * @brief Revoke a batch of put operations
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found, ErrorCode::INVALID_WRITE if replica status is invalid
     */
    std::vector<tl::expected<void, ErrorCode>> BatchPutRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        const TenantId& tenant_id, ReplicaType replica_type = ReplicaType::ALL);

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
                     const TenantId& tenant_id, const uint64_t slice_length,
                     const ReplicateConfig& config)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    /**
     * @brief Complete an upsert operation. Delegates to PutEnd.
     */
    auto UpsertEnd(const UUID& client_id, const ObjectMeta& object_meta,
                   const TenantId& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode>;

    auto UpsertEnd(const UUID& client_id, const std::string& key,
                   const TenantId& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Revoke an upsert operation. Delegates to PutRevoke.
     */
    auto UpsertRevoke(const UUID& client_id, const std::string& key,
                      const TenantId& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Start a batch of upsert operations.
     */
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchUpsertStart(const UUID& client_id,
                     const std::vector<std::string>& keys,
                     const TenantId& tenant_id,
                     const std::vector<uint64_t>& slice_lengths,
                     const ReplicateConfig& config);

    /**
     * @brief Complete a batch of upsert operations. Delegates to BatchPutEnd.
     */
    std::vector<tl::expected<void, ErrorCode>> BatchUpsertEnd(
        const UUID& client_id, const std::vector<ObjectMeta>& object_metas,
        const TenantId& tenant_id);

    /**
     * @brief Revoke a batch of upsert operations. Delegates to BatchPutRevoke.
     */
    std::vector<tl::expected<void, ErrorCode>> BatchUpsertRevoke(
        const UUID& client_id, const std::vector<std::string>& keys,
        const TenantId& tenant_id);

    /**
     * @brief Evict a disk replica for a key (triggered by client-side disk
     * eviction).
     * @param client_id The client performing the eviction
     * @param key The object key whose disk replica was evicted
     * @param replica_type DISK or LOCAL_DISK
     * @return ErrorCode::OK on success, OBJECT_NOT_FOUND if key missing
     */
    auto EvictDiskReplica(const UUID& client_id, const std::string& key,
                          const TenantId& tenant_id, ReplicaType replica_type)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Evict a disk replica only if the current record is still the
     * replica the caller failed on.
     *
     * The precondition is checked against the replica id under the same
     * write lock on the key as the eviction, so a replica replaced between the
     * caller's query and this call is left untouched.
     * @param client_id The client performing the eviction
     * @param key The object key whose disk replica was evicted
     * @param replica_type DISK or LOCAL_DISK
     * @param expected_replica_id The replica id the caller failed on
     * @return true if the replica was evicted, false if the precondition did
     * not hold (the replica was replaced or is already gone), or an error
     */
    auto EvictDiskReplicaIfCurrent(const UUID& client_id,
                                   const std::string& key,
                                   const TenantId& tenant_id,
                                   ReplicaType replica_type,
                                   ReplicaID expected_replica_id)
        -> tl::expected<bool, ErrorCode>;

    /**
     * @brief Batch evict disk replicas for multiple keys.
     * @param client_id The client performing the eviction
     * @param keys The object keys whose disk replicas were evicted
     * @param replica_type DISK or LOCAL_DISK
     * @return Per-key results (OK or error code)
     */
    std::vector<tl::expected<void, ErrorCode>> BatchEvictDiskReplica(
        const UUID& client_id, const std::vector<std::string>& keys,
        const TenantId& tenant_id, ReplicaType replica_type);

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
        const TenantId& tenant_id, const std::string& src_segment,
        const std::vector<std::string>& tgt_segments,
        const UUID& dynamic_replication_lease_id = UUID{},
        uint64_t dynamic_replication_version_epoch = 0);

    tl::expected<void, ErrorCode> CopyEnd(
        const UUID& client_id, const std::string& key,
        const TenantId& tenant_id,
        const UUID& dynamic_replication_lease_id = UUID{},
        uint64_t dynamic_replication_version_epoch = 0);

    tl::expected<void, ErrorCode> CopyRevoke(
        const UUID& client_id, const std::string& key,
        const TenantId& tenant_id,
        const UUID& dynamic_replication_lease_id = UUID{},
        uint64_t dynamic_replication_version_epoch = 0);

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
        const TenantId& tenant_id, const std::string& src_segment,
        const std::string& tgt_segment);

    tl::expected<void, ErrorCode> MoveEnd(const UUID& client_id,
                                          const std::string& key,
                                          const TenantId& tenant_id);

    tl::expected<void, ErrorCode> MoveRevoke(const UUID& client_id,
                                             const std::string& key,
                                             const TenantId& tenant_id);

    /**
     * @brief Remove an object and its replicas
     * @param key The key to remove.
     * @param force If true, skip lease and replication task checks.
     * @return ErrorCode::OK on success, ErrorCode::OBJECT_NOT_FOUND if not
     * found
     */
    auto Remove(const std::string& key, const TenantId& tenant_id,
                bool force = false) -> tl::expected<void, ErrorCode>;

    /**
     * @brief Removes objects from the master whose keys match a regex pattern.
     * @param str The regular expression string to match against object keys.
     * @param force If true, skip lease and replication task checks.
     * @return An expected object containing the number of removed objects on
     * success, or an ErrorCode on failure.
     */
    auto RemoveByRegex(const std::string& str, const TenantId& tenant_id,
                       bool force = false) -> tl::expected<long, ErrorCode>;

    /**
     * @brief Remove all objects and their replicas across all tenants.
     * @param force If true, skip lease and replication task checks.
     * @return return the number of objects removed
     */
    long RemoveAll(bool force = false);

    /**
     * @brief Remove all objects and their replicas for a single tenant.
     * @param tenant_id The tenant whose objects should be removed.
     * @param force If true, skip lease and replication task checks.
     * @return return the number of objects removed
     */
    long RemoveAll(const TenantId& tenant_id, bool force = false);

    // Update existing objects without transferring data. Results follow input
    // order; batches are not atomic. A zero TTL clears the soft pin.
    auto BatchUpdateSoftPin(const std::vector<std::string>& keys,
                            const TenantId& tenant_id, uint64_t ttl_ms)
        -> std::vector<tl::expected<void, ErrorCode>>;

    /**
     * @brief Batch remove objects and their replicas
     * @param keys The list of keys to remove.
     * @param force If true, skip lease and replication task checks.
     * @return Vector of expected results for each key.
     */
    auto BatchRemove(const std::vector<std::string>& keys,
                     const TenantId& tenant_id, bool force = false)
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
     * @brief Deregisters a client's file storage segment from the master. This
     * function is idempotent.
     *
     * Drops the client's LOCAL_DISK registration and then its LOCAL_DISK
     * replicas -- the outcome the client-expiry branch of ClientMonitorFunc
     * reaches after one client_ttl. Exposing it as an operation lets a store
     * that is shutting down deregister while it can still serve, instead of
     * leaving the master advertising it as an owner until the TTL elapses.
     * Object metadata whose last replica was on that disk is erased, exactly
     * as on expiry; a store that comes back re-adopts its files through the
     * MountLocalDiskSegment/NotifyOffloadSuccess path, which recreates them.
     *
     * The replica sweep targets exactly this owner (see
     * ClearLocalDiskHandlesOwnedBy), and the deregistration runs under the
     * exclusive master_state_mutex_ so no registration admitted against the old
     * one can land after the sweep: NotifyOffloadSuccess checks the
     * registration and writes the replica inside one shared-lock section,
     * which therefore falls entirely before the deregistration (registered,
     * then swept) or entirely after (refused with SEGMENT_NOT_FOUND).
     */
    auto UnmountLocalDiskSegment(const UUID& client_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Heartbeat call to collect object-level statistics and retrieve the
     * set of non-offloaded objects.
     * @param enable_offloading Indicates whether offloading is enabled for this
     * segment.
     */
    auto OffloadObjectHeartbeat(const UUID& client_id, bool enable_offloading)
        -> tl::expected<std::vector<OffloadTaskItem>, ErrorCode>;

    /**
     * @brief Client polls whether master has requested a full SSD clear
     * (triggered by RemoveAll). Atomically checks and clears the flag.
     * @param client_id The client polling for the remove-all signal
     * @return true if client should clear all SSD files, false otherwise
     */
    auto PollRemoveAll(const UUID& client_id) -> tl::expected<bool, ErrorCode>;

    auto ReportSsdCapacity(const UUID& client_id,
                           int64_t ssd_total_capacity_bytes)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Notifies the master that offloading of specified objects has
     * succeeded.
     * @param tasks        A list of tenant-scoped objects that were
     * successfully offloaded.
     * @param metadatas    The corresponding metadata for each offloaded object,
     * including size, storage location, etc.
     */
    auto NotifyOffloadSuccess(
        const UUID& client_id, const std::vector<OffloadTaskItem>& tasks,
        const std::vector<StorageObjectMetadata>& metadatas)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Heartbeat-driven pull of pending promotion work for a client.
     * Returns tenant-scoped promotion tasks for the holder client and clears
     * its per-client promotion_objects queue. The object's promotion task
     * remains the source of truth until NotifyPromotionSuccess commits the new
     * MEMORY replica.
     */
    auto PromotionObjectHeartbeat(const UUID& client_id)
        -> tl::expected<std::vector<PromotionTaskItem>, ErrorCode>;

    /**
     * @brief Stage a PROCESSING MEMORY replica for an existing key. Allocates
     * DRAM via the existing AllocationStrategy, optionally biased toward the
     * caller's local memory segment via preferred_segments. The new replica is
     * invisible to readers until NotifyPromotionSuccess flips it to COMPLETE.
     *
     * Only the holder client (the one owning the source LOCAL_DISK replica)
     * is authorized to call this. Other clients receive INVALID_PARAMS.
     * `size` must match the source replica's object_size captured at task
     * admission; mismatch returns INVALID_PARAMS to avoid allocating an
     * arbitrary buffer size from a buggy or malicious caller.
     */
    auto PromotionAllocStart(const UUID& client_id, const std::string& key,
                             const TenantId& tenant_id, uint64_t size,
                             const std::vector<std::string>& preferred_segments)
        -> tl::expected<PromotionAllocStartResponse, ErrorCode>;

    /**
     * @brief Register an in-flight promotion task for SSD prefetch.
     *
     * Records a PromotionTask without going through the promotion-on-hit
     * admission gates (frequency sketch, DRAM watermark) and without pushing
     * onto the holder client's promotion heartbeat mailbox. The caller (the
     * holder of the LOCAL_DISK replica) is expected to execute the transfer
     * immediately via PromotionAllocStart + PromotionWrite +
     * NotifyPromotionSuccess (e.g. FileStorage::PrefetchKeys).
     *
     * Shares the promotion_in_flight_ / promotion_queue_limit_ cap with
     * promotion-on-hit: prefetch and on-hit promotion compete for the same
     * DRAM resource, so they draw from the same budget.
     *
     * Returns PROMOTION_ALREADY_EXISTS when a MEMORY replica or an in-flight
     * promotion task already exists for the key — a normal outcome for
     * best-effort prefetch, not an error; callers should skip silently.
     * Only the holder client may register (holder_id == client_id), others
     * receive INVALID_PARAMS.
     */
    auto RegisterPrefetchTask(const UUID& client_id, const std::string& key,
                              const TenantId& tenant_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Commit a staged MEMORY replica to COMPLETE; decrement source
     * refcnt; erase the key's and the client's task entries. Mirror of
     * NotifyOffloadSuccess.
     */
    auto NotifyPromotionSuccess(const UUID& client_id, const std::string& key,
                                const TenantId& tenant_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Holder-side failure notification: the client got past
     * PromotionAllocStart but a downstream step (local SSD read, RDMA
     * write, etc.) failed and it will not be calling
     * NotifyPromotionSuccess. Releases the master-side task state
     * immediately rather than waiting put_start_release_timeout_sec_
     * for the reaper to do it. Without this call every transient
     * client-side error (SSD throttling, RDMA flake, etc.) pins a
     * task slot and a staged DRAM buffer for the full reaper TTL,
     * which can saturate promotion_queue_limit_ on busy clusters.
     *
     * Authorization is the same as NotifyPromotionSuccess: only the
     * holder client may release a task. Effects mirror the reaper's
     * expiry path: drop source LOCAL_DISK refcnt, pop the staged
     * PROCESSING MEMORY replica if alloc_id was recorded, erase the
     * task, decrement the global in-flight counter, and clear the
     * holder's promotion_objects entry.
     */
    auto NotifyPromotionFailure(const UUID& client_id, const std::string& key,
                                const TenantId& tenant_id)
        -> tl::expected<void, ErrorCode>;

    /**
     * @brief Create a copy task to copy an object's replicas to target segments
     * @return Copy task ID on success, ErrorCode on failure
     */
    tl::expected<UUID, ErrorCode> CreateCopyTask(
        const std::string& key, const TenantId& tenant_id,
        const std::vector<std::string>& targets);

    /**
     * @brief Create a move task to move an object's replica from source segment
     * to target segment
     * @return Move task ID on success, ErrorCode on failure
     */
    tl::expected<UUID, ErrorCode> CreateMoveTask(const std::string& key,
                                                 const TenantId& tenant_id,
                                                 const std::string& source,
                                                 const std::string& target);

    // Admin-only, grow-only DFS capacity management. Existing placements remain
    // valid.
    tl::expected<int, ErrorCode> GetDfsShardCount() const;
    tl::expected<int, ErrorCode> ExpandDfsShards(int shard_count);

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
     * @brief Switch a segment between OK and DRAINING without a drain job.
     * DRAINING stops new allocations on the segment while its existing
     * replicas stay readable. Rejected while an unfinished drain job lists
     * the segment as a source.
     */
    tl::expected<void, ErrorCode> SetSegmentStatus(
        const std::string& segment_name, SegmentStatus status);

    /**
     * @brief Restore primary state from standby promotion context.
     * Called once at promotion time before serving requests.
     */
    tl::expected<void, ErrorCode> RestoreFromStandbySnapshot(
        const std::vector<StandbyObjectEntry>& objects,
        uint64_t initial_oplog_sequence_id,
        const std::vector<StandbySegmentInfo>& segments,
        const WeightMetadataSnapshot& weight_metadata = {});
    // Copies objects out of the handoff. The caller keeps ownership so a
    // failed restore or a later lease loss can still PreservePromotionBaseline.
    tl::expected<void, ErrorCode> RestoreFromBatchOpLogPromotion(
        const BatchOpLogPromotionHandoff& handoff,
        size_t chunk_object_count = kDefaultBatchOpLogPromotionChunkObjects);
    tl::expected<void, ErrorCode> RestoreFromBatchOpLogPromotion(
        const StandbyMetadataStore& metadata_store,
        DurablePrefix applied_cursor,
        const std::vector<StandbySegmentInfo>& segments,
        ReplicaID max_replica_id,
        size_t chunk_object_count = kDefaultBatchOpLogPromotionChunkObjects);

    /**
     * @brief Restore lifecycle state machine (RFC #4700, slice 1).
     *
     * The machine below is the one proposed by the RFC:
     *
     *   Constructed -> Recovering -> Ready -> Serving
     *        |              |           (non-HA: Constructed -> Ready)
     *        +----------> Failed
     *
     * The absence of a Serving -> Recovering edge is a proposed constraint,
     * not an established API guarantee. Slice 1 wires the transitions that
     * exist today and uses the state to pause business workers while a
     * promotion restore is in flight; the master_state_mutex_ stays in
     * place, so this slice changes no locking behavior. The Stopping state
     * and the RPC-level admission gate land with the follow-up slices that
     * actually remove the global lock from ordinary paths.
     *
     * Serving is a commitment about restore, not an observation about
     * reachability. It marks the point after which this instance refuses
     * restore; it does NOT mean the RPC endpoint is already accepting
     * requests. Each entry point commits at the moment its instance stops
     * needing restore, which lands on both sides of the publish boundary:
     *
     *   non-HA master    Constructed -> Ready -> Serving, then register RPC.
     *                    There is no restore path, so committing early is free.
     *   EmbeddedMaster   Constructed -> Ready -> Serving, then register RPC.
     *                    Same reasoning, aligned with the non-HA path so that
     *                    the contract does not depend on the entry point.
     *   HA supervisor    promotion restore -> Ready, publish readiness, then
     *                    Ready -> Serving. The commit is last because the term
     *                    is only committed once readiness is out; a failed
     *                    publication destroys the instance (release_service)
     *                    rather than reusing it, so the retry builds a fresh
     *                    service and no Serving -> Recovering edge is needed.
     *
     * The invariant the follow-up admission work can rely on is the same on
     * every path: once Serving is observed, restore cannot happen for this
     * instance, so an admission decision taken in that state needs no
     * restore-window reasoning. Callers that need "is it reachable yet" must
     * consult the transport, not this state machine.
     */
    enum class RestoreLifecycleState {
        kConstructed,  // components built; workers admitted (see below)
        kRecovering,   // HA promotion restore in flight; workers paused
        kReady,        // restore + durable repair done; serving not activated
        kServing,      // committed to normal operation; restore refused
        kFailed,       // restore failed; the candidate must be destroyed
    };

    /// Current lifecycle state; safe to call from any thread.
    RestoreLifecycleState GetRestoreLifecycleState() const;

    /**
     * @brief Non-HA initialization step: Constructed -> Ready.
     * The non-HA and embedded paths have no restore, so they open the
     * lifecycle directly. Refuses (logs) any other source state.
     */
    void MarkReadyForServing();

    /**
     * @brief Activate serving: Ready -> Serving.
     * The commit point: after this, restore is refused for the lifetime of
     * this instance. Whether it runs before or after the transport starts
     * accepting requests depends on the entry point -- see the contract
     * above. Refuses (logs) any other source state: serving must never be
     * activated on a service whose restore did not succeed.
     */
    void ActivateServing();

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

    /**
     * @brief Set the HttpMetadataServer pointer for cleanup on client timeout.
     * @param server Pointer to HttpMetadataServer. If nullptr, cleanup is
     * disabled.
     */
    void setHttpMetadataServer(HttpMetadataServer* server);

    /**
     * @brief Configure cleanup against a separately-deployed HTTP metadata
     * server (not co-located in the master process). The master sends HTTP
     * DELETE requests to this endpoint when a client times out. Only http://
     * and https:// connection strings are supported; other schemes (etcd /
     * redis / P2PHANDSHAKE) are ignored with a warning and leave cleanup
     * disabled.
     * @param metadata_connstring e.g. "http://host:8080/metadata".
     */
    void setHttpMetadataRemoteUrl(const std::string& metadata_connstring);

    /**
     * @brief Stop the remote HTTP metadata cleanup worker: drop queued work,
     * skip the rest of a running batch and join. Returns after at most one
     * in-flight DELETE completes or times out: the wait is the HTTP client's
     * connect and request timeouts (name resolution included), and the
     * shutdown watchdog bounds the process. Later cleanup requests are
     * ignored. Safe when the worker never started. Call from one thread;
     * sequential calls are safe.
     */
    void StopHttpMetadataCleanup();

   private:
    // Lifecycle wrapper around the restore implementation: performs the
    // {Constructed, Ready, Failed} -> Recovering -> {Ready, Failed}
    // transitions so a promotion restore has an explicit lifecycle
    // bookkeeping point (RFC #4700, slice 1). Entry from Serving is
    // refused per the RFC's proposed no-cycles constraint; the existing
    // restore retry semantics (replacement restore from Ready or Failed)
    // are unchanged and remain covered by the HA tests.
    tl::expected<void, ErrorCode> RestoreFromStandbyState(
        const std::vector<StandbyObjectEntry>* legacy_objects,
        const StandbyMetadataStore* metadata_store,
        uint64_t initial_oplog_sequence_id,
        const std::vector<StandbySegmentInfo>& segments,
        size_t chunk_object_count,
        std::optional<ReplicaID> expected_max_replica_id,
        const WeightMetadataSnapshot* legacy_weight_metadata);

    // The restore implementation itself; entered only through
    // RestoreFromStandbyState above.
    tl::expected<void, ErrorCode> RestoreFromStandbyStateCore(
        const std::vector<StandbyObjectEntry>* legacy_objects,
        const StandbyMetadataStore* metadata_store,
        uint64_t initial_oplog_sequence_id,
        const std::vector<StandbySegmentInfo>& segments,
        size_t chunk_object_count,
        std::optional<ReplicaID> expected_max_replica_id,
        const WeightMetadataSnapshot* legacy_weight_metadata);

    // Tolerant legacy snapshot restore (#3760), one phase per method so each
    // stays single-purpose.
    struct LegacyRestoreContext;
    void NoteLegacyStandbyRejection(LegacyRestoreContext& ctx,
                                    const StandbyObjectEntry& entry,
                                    const char* reason);
    tl::expected<void, ErrorCode> ValidateLegacyStandbyEntries(
        LegacyRestoreContext& ctx);
    tl::expected<void, ErrorCode> ConstructLegacyStandbyReplicas(
        LegacyRestoreContext& ctx);
    tl::expected<size_t, ErrorCode> InstallLegacyStandbyObjects(
        LegacyRestoreContext& ctx);

    // A standby object whose replicas are built and that waits to be
    // published under its own tenant and key.
    struct PreparedObject;
    // Publishes each of `objects` and records the publication in `installed`.
    // Publishes nothing if a key already holds an object when it starts.
    tl::expected<void, ErrorCode> PublishRestoredObjects(
        std::vector<PreparedObject>& objects,
        std::vector<std::pair<TenantId, route::ObjectRef>>& installed);

    // A restored local-disk replica reads only while its owner has a liveness
    // record; share the existing one or stage a fresh record for install.
    std::shared_ptr<ClientLivenessRecord> RecordRestoreKnownOwner(
        std::unordered_map<UUID, std::shared_ptr<ClientLivenessRecord>,
                           boost::hash<UUID>>& new_known_owner_records,
        const UUID& owner);

    // Shared lookup path for ExistKey/ProbeKey (and their batch variants).
    // When grant_lease is false, the check acquires no read lease and is a
    // point-in-time existence probe only.
    auto ExistKeyImpl(const std::string& key, const TenantId& tenant_id,
                      bool grant_lease) -> tl::expected<bool, ErrorCode>;

    // Shared body for EvictDiskReplica and EvictDiskReplicaIfCurrent. When
    // expected_replica_id is set, the eviction runs only if a matching
    // replica with that id is still present under the key's write lock;
    // otherwise the record is left untouched and false is returned.
    auto EvictDiskReplicaImpl(const UUID& client_id, const std::string& key,
                              const TenantId& tenant_id,
                              ReplicaType replica_type,
                              const ReplicaID* expected_replica_id)
        -> tl::expected<bool, ErrorCode>;
    std::vector<tl::expected<bool, ErrorCode>> BatchExistKeyImpl(
        const std::vector<std::string>& keys, const TenantId& tenant_id,
        bool grant_lease);

    // One pool-wide round of memory eviction (see eviction::Evictor), after
    // the housekeeping that frees expired writes and deferred releases. Each
    // namespace gives up `evict_ratio_target` of the objects its eviction
    // index filed, oldest lease first, soft-pinned objects only when
    // allow_evict_soft_pinned_objects_ is set and only up to
    // `evict_ratio_lowerbound`. It consumes the allocation-pressure signal and
    // raises it again when the round could free nothing that was there to
    // free.
    void BatchEvict(double evict_ratio_target, double evict_ratio_lowerbound);
    void NoFBatchEvict(double evict_ratio_target,
                       double evict_ratio_lowerbound);
    using NamespaceEvictionResult = StoreControl::EvictionResult;
    // Evicts memory replicas of one namespace's objects, oldest lease first,
    // until `target_bytes` are freed or nothing more qualifies (see
    // eviction::Evictor); what a policy asks for through StoreControl.
    NamespaceEvictionResult EvictNamespaceMemory(const TenantId& tenant_id,
                                                 uint64_t target_bytes);

    std::shared_ptr<ClientLivenessRecord> FindClientRecord(
        const UUID& client_id) const;
    // Caller holds the Replica owner's retaining guard.
    auto AddReplicaForRetainedClient(const UUID& client_id,
                                     const std::string& key,
                                     const TenantId& tenant_id,
                                     Replica& replica)
        -> tl::expected<bool, ErrorCode>;
    // Caller must hold client_mutex_.
    std::unordered_set<UUID, boost::hash<UUID>> GetRetainingClientIdsLocked()
        const;
    void UpdateClientHostId(const UUID& client_id, const std::string& host_id);
    std::string GetClientHostId(const UUID& client_id) const;

    void ClearInvalidHandles();
    // Caller owns master_state_mutex_ (shared) while metadata is swept.
    void ClearInvalidHandles(
        const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients);
    // Clear completed LOCAL_DISK replicas owned by exactly this client, in
    // every tenant. Owner-targeted on purpose: a liveness-complement sweep
    // classifies by absence from a point-in-time set, so an owner that
    // mounts and registers between taking that set and the sweep reaching
    // its object would be swept as stale. A predicate on the owner id cannot
    // misclassify a concurrent mount, whatever the interleaving.
    void ClearLocalDiskHandlesOwnedBy(const UUID& owner);
    // Walk shared by the two sweeps above; removes completed replicas
    // matching is_stale, erasing a key when no valid replica remains. Each
    // tenant is first read to pick the objects that match, and only those are
    // cleaned, each under its own write lock, since a mass client expiry marks
    // handles stale table-wide.
    tl::expected<void, ErrorCode> ClearStaleHandles(
        const std::function<bool(const Replica&)>& is_stale);
    bool ProcessClientOffboardingJob(ClientOffboardingJob& job);
    // We need to clean up finished tasks periodically to avoid memory leak
    // And also we can add some task ttl mechanism in the future
    void TaskCleanupThreadFunc();
    void WeightReconciliationThreadFunc();
    void JobDispatchThreadFunc();

    // Internal data structures
    struct ObjectIdentity {
        TenantId tenant_id;
        std::string user_key;
    };

    // What the namespace policy may see and ask of this service.
    class PolicyStoreAccess final : public StoreControl {
       public:
        explicit PolicyStoreAccess(MasterService& service)
            : service_(service) {}
        void VisitNamespaces(
            const std::function<void(const TenantId&, route::ObjectRoute&)>& fn)
            override;
        size_t ObjectCount(const TenantId& tenant_id) const override;
        uint64_t AllocatableMemoryBytes() const override;
        EvictionResult EvictNamespaceMemory(const TenantId& tenant_id,
                                            uint64_t target_bytes) override;

       private:
        MasterService& service_;
    };
    PolicyStoreAccess policy_store_access_{*this};
    // Multi-tenancy, when enabled; the namespace policy below points at it.
    std::unique_ptr<TenantQuotaPolicy> tenant_quota_policy_;
    // The policy every namespace is created under; null without one. Set in
    // the constructor, before any namespace is created, and declared before
    // the namespaces so that it outlives them.
    NamespacePolicy* policy_ = nullptr;

    // One namespace per tenant id written to, each owning that tenant's object
    // route; a namespace stays put once created (see NamespaceTable).
    metadata::NamespaceTable namespaces_{[this](const TenantId& tenant_id) {
        return std::make_unique<metadata::Namespace>(
            tenant_id, policy_,
            policy_ == nullptr ? nullptr
                               : policy_->OnNamespaceCreated(tenant_id));
    }};
    MasterStoreBackend weight_backend_{*this};
    tl::expected<void, ErrorCode> RemoveObject(const std::string& key,
                                               const TenantId& tenant_id,
                                               bool force,
                                               bool allow_managed_weight);
    WeightStoreManager weight_manager_{weight_backend_};

    class SoftPinDeadlineIndex {
        friend class test::MasterServiceTestPeer;

       public:
        using TimePoint = std::chrono::system_clock::time_point;

        struct Entry {
            TimePoint deadline;
            std::string scoped_key;
        };

        void Upsert(std::string scoped_key, const TimePoint& deadline);
        void Remove(const std::string& scoped_key);
        void RemoveIfMatches(const std::string& scoped_key,
                             const TimePoint& deadline);
        std::vector<Entry> PopExpired(const TimePoint& now);
        void Clear();

       private:
        struct Registration {
            TimePoint deadline;
        };

        struct EarlierDeadline {
            bool operator()(const Entry& lhs, const Entry& rhs) const {
                return lhs.deadline > rhs.deadline;
            }
        };

        static constexpr size_t kMinCompactionThreshold = 4096;
        static constexpr size_t kCompactionRatio = 2;

        void MaybeCompactLocked() REQUIRES(mutex_);

        mutable std::mutex mutex_;
        std::priority_queue<Entry, std::vector<Entry>, EarlierDeadline> heap_
            GUARDED_BY(mutex_);
        std::unordered_map<std::string, Registration> registrations_
            GUARDED_BY(mutex_);
    };

    mutable SoftPinDeadlineIndex soft_pin_deadline_index_;

    static bool HasCompletedMemoryCacheReplica(const ObjectMetadata& metadata);
    static bool HasCompletedDiskCacheReplica(const ObjectMetadata& metadata);
    static void SyncCacheTotalAccounting(ObjectMetadata& metadata);
    static void AccountCacheTotalRemoval(ObjectMetadata& metadata);
    std::vector<Replica> PopReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& pred_fn);
    std::vector<Replica> PopReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata);
    size_t EraseReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& pred_fn,
        std::vector<ReplicaID>* erased_replica_ids = nullptr);

    static ObjectIdentity MakeObjectIdentity(const std::string& user_key,
                                             TenantId tenant_id) {
        return {std::move(tenant_id), user_key};
    }
    const TenantId& ResolveRequestTenantId(const TenantId& tenant_id) const;
    ObjectIdentity MakeObjectIdentityForRequest(
        const std::string& user_key, const TenantId& tenant_id) const;
    tl::expected<TenantId, ErrorCode> ResolveTenantIdForWrite(
        const TenantId& tenant_id) const;
    bool IsTenantRegistered(const TenantId& tenant_id) const;

    // Reads the member keys registered for `group_id` in the tenant's route;
    // empty if the tenant or the group is unregistered.
    std::vector<std::string> GetGroupMemberKeys(
        const TenantId& tenant_id, const std::string& group_id) const;

    // Takes the memory replicas off the named members of a managed weight
    // revision's group that hold a readable cold replica, the way memory
    // eviction does, except that a hard pin does not keep them.
    void EvictManagedWeightMembersToCold(
        const WeightRevisionMetadata& metadata,
        const std::vector<std::string>& member_keys);

    void ReleaseLocalDiskUsage(std::span<const Replica> replicas);
    // How an erase is published to KV-event subscribers: as the object's
    // removal, or, when an upsert replaces it under the same lock, as the
    // state the replacement starts from.
    enum class EraseMode {
        kFull,
        kReplace,
    };
    // The memory every segment mounted on this service adds up to,
    // saturating at the 64-bit range; what tenant quotas are carved from.
    uint64_t AllocatableMemoryBytes();
    // Restore: lets the namespace policy rebuild what it derives from the
    // restored metadata.
    void NotifyPolicyRestored();
    // A segment came or went: lets the namespace policy re-derive what it
    // carves from the allocatable memory.
    void NotifyPolicyCapacityChanged();
    // Erases the object `hold` holds in `ns` and every record that hangs off
    // its key, under that key's lock: the tasks in flight on it, its replicas'
    // resources and its KV events. The key stays held, so the caller may
    // publish a replacement.
    void EraseMetadata(
        metadata::Namespace& ns, const route::WriteGuard& hold,
        EraseMode erase_mode,
        const std::vector<std::string>& previous_media_hint = {});
    void FinalizeRemovedReplicasAfterDurable(
        const OpLogEntry& durable_entry,
        const std::vector<ReplicaID>& replica_ids);
    // The durable finalizers of a decision taken under a key's lock that was
    // let go of before the OpLog entry became durable. Those that name the
    // object by `ObjectRef` act only on that same publication, so a same-key
    // recreate that landed meanwhile is left alone.
    void FinalizeMetadataEraseAfterDurable(route::ObjectRef object,
                                           const TenantId& tenant_id);
    void FinalizeExpiredProcessingReplicasAfterDurable(
        route::ObjectRef object, const OpLogEntry& durable_entry,
        const std::chrono::system_clock::time_point& ttl);
    void FinalizeExpiredReplicationTaskAfterDurable(
        const OpLogEntry& durable_entry, ReplicaID source_id,
        const std::vector<ReplicaID>& target_ids,
        const UUID& dynamic_replication_lease_id,
        uint64_t dynamic_replication_version_epoch,
        const std::chrono::system_clock::time_point& ttl);
    struct StaleHandleCleanupPlan {
        std::vector<ReplicaID> removed_ids;
        std::vector<Replica::Descriptor> remaining;
        bool would_invalidate{false};
    };
    StaleHandleCleanupPlan BuildStaleHandleCleanupPlan(
        const ObjectMetadata& metadata,
        const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients)
        const;
    StaleHandleCleanupPlan BuildStaleHandleCleanupPlan(
        const ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& is_stale) const;
    tl::expected<void, ErrorCode> PersistStaleHandleCleanupForHA(
        const std::string& why, const TenantId& tenant_id,
        const std::string& key, ObjectMetadata& metadata,
        const StaleHandleCleanupPlan& plan);
    // Removes the object under HA with the oplog on: marks its completed
    // replicas removed and queues a REMOVE whose durable callback drops them,
    // and the object with them.
    tl::expected<void, ErrorCode> MarkRemovedAndPersist(
        const TenantId& tenant_id, const std::string& key,
        ObjectMetadata& metadata);
    // Returns why a remove may not take the object, or nothing if it may: its
    // lease is still held (unless `force`), a replica is incomplete, or a
    // replication task reads it.
    static tl::expected<void, ErrorCode> CheckRemovable(
        const ObjectMetadata& metadata, const route::ObjectState& state,
        bool force, std::chrono::system_clock::time_point now);
    struct RemoveResult {
        long removed_count{0};
        int64_t freed_bytes{0};
        // Whether the walk reached any object, and whether it left any
        // behind (not removable, or its removal could not be queued).
        bool saw_object{false};
        bool skipped_object{false};
    };
    // Removes each of the tenant's objects whose key `matches_key` accepts
    // (every key, if unset), excluding managed weights and objects that
    // CheckRemovable refuses.
    RemoveResult RemoveTenantObjects(
        const TenantId& tenant_id, bool force,
        std::chrono::system_clock::time_point now,
        const std::function<bool(const std::string& key)>& matches_key);
    static void ApplySoftPinMetricDelta(int metric_delta);
    void ApplySoftPinEvaluation(
        const ObjectMetadata& metadata,
        const ObjectMetadata::SoftPinEvaluation& result) const;
    bool IsSoftPinActive(
        const ObjectMetadata& metadata,
        const std::chrono::system_clock::time_point& now) const;
    void CleanupExpiredSoftPins(
        const std::chrono::system_clock::time_point& now);
    auto ResolveSoftPinRequest(const ReplicateConfig& config) const
        -> tl::expected<ResolvedSoftPinRequest, ErrorCode>;

    // Helper to clean up stale handles pointing to unmounted segments
    // or local_disk replicas whose owner client no longer retains resources.
    bool CleanupStaleHandles(
        const std::string& key, const TenantId& tenant_id,
        ObjectMetadata& metadata, route::ObjectState& state,
        const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients);
    // Predicate form, so the owner-targeted LOCAL_DISK sweep can reuse the
    // accounting (promotion-task cancellation) instead of
    // duplicating it.
    bool CleanupStaleHandles(
        const std::string& key, const TenantId& tenant_id,
        ObjectMetadata& metadata, route::ObjectState& state,
        const std::function<bool(const Replica&)>& is_stale);

    // True when client_id currently has a LOCAL_DISK registration.
    // Momentarily takes the LocalSsdManager registry lock, so callers must not
    // hold it; call before taking a key's lock. Callers that need the
    // answer to stay true across a later metadata write must hold
    // master_state_mutex_ (shared) across both -- UnmountLocalDiskSegment
    // deregisters the client under the exclusive lock, so the check and the
    // write cannot straddle a deregistration.
    bool HasMountedLocalDiskSegment(const UUID& client_id);

    // Allocate the physical replicas without changing object metadata.  This
    // is used by leased upserts to prove that replacement storage is available
    // before the old metadata is removed.
    auto AllocateReplicas(const std::string& key, uint64_t value_length,
                          const ReplicateConfig& config,
                          const std::string& writer_host_id,
                          bool* dfs_allocation_failed = nullptr)
        -> tl::expected<std::vector<Replica>, ErrorCode>;

    class MetadataAccessorRW;
    // Publishes a new object under the key `accessor` holds, which must hold
    // none.
    auto InsertMetadata(MetadataAccessorRW& accessor, const UUID& client_id,
                        uint64_t value_length, const ReplicateConfig& config,
                        const std::string& group_id,
                        const std::chrono::system_clock::time_point& now,
                        const ResolvedSoftPinRequest& soft_pin_request,
                        std::vector<Replica>&& replicas,
                        std::optional<std::chrono::system_clock::time_point>
                            committed_soft_pin_timeout = std::nullopt)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    // Helper: allocate replicas, publish the object under the key `accessor`
    // holds, which must hold none, and return descriptor list.  Shared by
    // PutStart and UpsertStart.
    auto AllocateAndInsertMetadata(
        MetadataAccessorRW& accessor, const UUID& client_id,
        uint64_t value_length, const ReplicateConfig& config,
        const std::string& writer_host_id, const std::string& group_id,
        const std::chrono::system_clock::time_point& now,
        const ResolvedSoftPinRequest& soft_pin_request,
        std::optional<std::chrono::system_clock::time_point>
            committed_soft_pin_timeout = std::nullopt,
        bool* dfs_allocation_failed = nullptr)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    // What PutStart and UpsertStart settle before they take the key.
    struct WriteStartPlan {
        ResolvedSoftPinRequest soft_pin_request;
        // The writer's host, for same-node placement; empty when unused.
        std::string writer_host_id;
        std::string group_id;
    };
    // The checks and setup PutStart and UpsertStart share before they take the
    // key: validates `config` for a write of `slice_length` bytes, records the
    // client's host, resolves the soft pin, the writer's host and the group,
    // and refuses a tenant that is not registered. `action` names the caller
    // in the log.
    auto PrepareWriteStart(const UUID& client_id, const std::string& key,
                           const TenantId& tenant_id, uint64_t slice_length,
                           const ReplicateConfig& config,
                           std::string_view action)
        -> tl::expected<WriteStartPlan, ErrorCode>;
    // A write's first step under the key: drops the held object's stale
    // replica handles and, without the oplog, the object itself when none is
    // left. With the oplog the cleanup is only queued and the write is refused
    // with OBJECT_ALREADY_EXISTS.
    auto CleanupStaleBeforeWrite(
        const char* why, MetadataAccessorRW& accessor,
        const std::unordered_set<UUID, boost::hash<UUID>>& retaining_clients)
        -> tl::expected<void, ErrorCode>;
    // Runs a write's admission, which reports through its argument whether
    // DFS allocation itself ran out of capacity. Only then is the admission
    // retried: once behind any recovery already running, then once more
    // after one forced bucket eviction. Memory or NoF exhaustion is left to
    // the background eviction, so it cannot delete unrelated DFS data. Each
    // attempt takes the key afresh, since recovery evicts other keys.
    auto AdmitWithDfsRecovery(
        const ReplicateConfig& config,
        const std::function<
            tl::expected<std::vector<Replica::Descriptor>, ErrorCode>(
                bool* dfs_allocation_failed)>& admit)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;
    // The first step of an accessor that holds a key by name: drops the held
    // object's memory replicas whose segment is gone, and the object with them
    // when none is left valid.
    void CleanupInvalidMemoryReplicas(MetadataAccessorRW& accessor);

    // Discards the namespace's expired processing replicas and expired tasks.
    void DiscardExpiredProcessingReplicas(
        const metadata::Namespace& ns,
        const std::chrono::system_clock::time_point& now);
    void FreeDfsReplicas(const std::string& key,
                         std::span<const Replica> replicas);
    void RunDfsEviction();
    void RunShardDfsEviction();
    void RunBucketDfsEviction();
    bool RunBucketDfsEvictionInternal(bool force_one);
    bool TryRecoverDfsSpaceAfterAllocationFailure();
    void InitDfsAllocatorFromEnvironment(const MasterServiceConfig& config);
    /**
     * @brief Helper to release space of expired discarded replicas.
     * @return Number of released objects that have memory replicas
     */
    uint64_t ReleaseExpiredDiscardedReplicas(
        const std::chrono::system_clock::time_point& now);

    // Eviction thread function
    void EvictionThreadFunc();
    void NofHeartbeatThreadFunc();
    bool TryUnmountNoFSegmentByHeartbeat(
        const MountedNoFSegmentSnapshot& snapshot,
        const std::string& error_reason);
    bool ProbeNoFSegment(const std::string& te_endpoint,
                         std::string* error_reason);

    // Pushes an offload mirror for `replica` onto its host client's LocalSSD
    // mailbox. When `mirror_clients` is non-null, the destination client is
    // appended to it on success.
    tl::expected<void, ErrorCode> PushOffloadingQueue(
        const ObjectIdentity& object_id, Replica& replica,
        std::vector<UUID>* mirror_clients = nullptr);

    // Cancels the offload task on `object_id`, releasing the source refcnt
    // and dropping the task marker along with its mirrors. Returns false
    // without touching the task if any mirror has already been drained by a
    // store worker.
    bool CancelQueuedOffloadTask(ObjectMetadata& metadata,
                                 route::ObjectState& state,
                                 const ObjectIdentity& object_id);

    struct GracefulUnmountDeadlineRecord {
        UUID segment_id;
        UUID client_id;
    };

    DeadlineScheduler<GracefulUnmountDeadlineRecord>
        graceful_unmount_scheduler_;
    BackgroundWorker replica_cleanup_worker_;
    const bool enable_async_segment_cleanup_;

    /**
     * @brief Mirror of PushOffloadingQueue for promotion-on-hit. Inserts an
     * task into the holder client's LocalSSD mailbox.
     * Caller is responsible for refcnt-pinning the source replica and
     * recording the task on the object.
     */
    tl::expected<void, ErrorCode> PushPromotionQueue(
        const ObjectIdentity& object_id, Replica& source_replica);

    /**
     * @brief Helper invoked from GetReplicaList when an only-LOCAL_DISK key is
     * observed. Applies the gating chain (frequency / watermark / dedup /
     * cap), refcnt-pins the source LOCAL_DISK replica, records a
     * PromotionTask, and pushes onto the holder client's LocalSSD mailbox.
     * Acquires its own RW accessor; safe to call after
     * GetReplicaList's RO accessor has been released.
     */
    PromotionQueueResult TryPushPromotionQueue(const ObjectIdentity& object_id,
                                               bool record_candidate = true,
                                               bool force = false);
    void EraseCandidate(const ObjectIdentity& object_id);
    void BackoffCandidate(const ObjectIdentity& object_id,
                          PromotionQueueResult result);
    size_t RunPromotionCandidateRetry();

    // Erase the object's in-flight PromotionTask and decrement the
    // cluster-wide in-flight counter. Safe no-op if no task exists.
    void ErasePromotionTaskIfPresent(route::ObjectState& state)
        NO_THREAD_SAFETY_ANALYSIS {
        if (state.promotion_task) {
            state.promotion_task.reset();
            promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
            MasterMetricManager::instance().dec_promotion_in_flight();
            MasterMetricManager::instance().inc_promotion_cancelled();
        }
    }
    void CancelPromotionTaskForRemovedReplicas(
        ObjectMetadata& metadata, route::ObjectState& state,
        const std::vector<ReplicaID>& removed_replica_ids)
        NO_THREAD_SAFETY_ANALYSIS;

    // Lease related members
    const uint64_t default_kv_lease_ttl_;     // in milliseconds
    const uint64_t default_kv_soft_pin_ttl_;  // in milliseconds
    const uint64_t max_kv_soft_pin_ttl_;      // in milliseconds
    const bool allow_evict_soft_pinned_objects_;

    // Eviction related members
    std::atomic<bool> need_mem_eviction_{
        false};  // Set to trigger memory eviction when allocation fails
    std::atomic<bool> need_nof_eviction_{
        false};  // Set to trigger NoF eviction when allocation fails
    const double eviction_ratio_;                 // in range [0.0, 1.0]
    const double eviction_high_watermark_ratio_;  // in range [0.0, 1.0]
    const double nof_eviction_ratio_;             // in range [0.0, 1.0]
    const double nof_eviction_high_watermark_ratio_;  // in range [0.0, 1.0]

    // Memory eviction (see eviction::Evictor) and what it is built from: the
    // policy, the reclaimer the HA and offload settings call for, and the
    // observers. Built once the settings are read, before the eviction thread
    // starts.
    struct MemoryEviction;
    std::unique_ptr<MemoryEviction> memory_eviction_;

    // Eviction thread related members
    std::thread eviction_thread_;
    std::atomic<bool> eviction_running_{false};
    static constexpr uint64_t kEvictionThreadSleepMs =
        10;  // 10 ms sleep between eviction checks

    // Task cleanup thread related members
    std::thread task_cleanup_thread_;
    std::atomic<bool> task_cleanup_running_{false};
    static constexpr uint64_t kTaskCleanupThreadSleepMs =
        30000;  // 30000 ms sleep between task cleanup checks

    // Used to wake task cleanup thread immediately during shutdown.
    std::mutex task_cleanup_mutex_;
    std::condition_variable task_cleanup_cv_;

    std::thread weight_reconciliation_thread_;
    std::atomic<bool> weight_reconciliation_running_{false};
    std::mutex weight_reconciliation_mutex_;
    std::condition_variable weight_reconciliation_cv_;

    // Helper class for accessing metadata with automatic locking and cleanup
    //
    // Resolves the namespace of `object_id` and write-holds the key for the
    // accessor's scope. A thread holds one key at a time, so nothing in the
    // accessor's scope may reach another key through another accessor.
    class MetadataAccessorRW {
       public:
        struct AsIs {};
        // Holds the key only while it holds an object, and leaves that object
        // as it is, without the invalid-replica cleanup: for a pass that acts
        // on what the object's own state says, such as an expiry sweep.
        MetadataAccessorRW(MasterService* service, ObjectIdentity object_id,
                           AsIs)
            : service_(service),
              object_id_(std::move(object_id)),
              ns_(service_->namespaces_.Lookup(object_id_.tenant_id)),
              guard_(ns_ == nullptr ? std::optional<route::WriteGuard>()
                                    : ns_->objects.Write(object_id_.user_key)) {
        }

        // Holds the key only while it holds an object.
        MetadataAccessorRW(MasterService* service, ObjectIdentity object_id)
            : MetadataAccessorRW(service, std::move(object_id), AsIs{}) {
            // Invalid memory replicas go first, which may tear the object down.
            if (IsPublished()) {
                service_->CleanupInvalidMemoryReplicas(*this);
            }
        }

        struct CreateKey {
            bool cleanup_invalid = true;
        };
        // Holds the key whether or not it holds an object, creating the
        // namespace if needed, so the caller can publish one under it.
        MetadataAccessorRW(MasterService* service, ObjectIdentity object_id,
                           CreateKey options)
            : service_(service),
              object_id_(std::move(object_id)),
              ns_(&service_->namespaces_.GetOrCreate(object_id_.tenant_id)),
              guard_(ns_->objects.WriteKey(object_id_.user_key)) {
            if (options.cleanup_invalid && IsPublished()) {
                service_->CleanupInvalidMemoryReplicas(*this);
            }
        }

        // Holds the key only while `ref` is still its current publication: a
        // key republished since `ref` was taken, or emptied, is not held.
        // The held object is left as it is, without the invalid-replica
        // cleanup: the caller comes back to act on what it judged, and the
        // cleanup's KV sync would bump the tenant epoch a RemoveAll compares
        // its `cleared` against.
        MetadataAccessorRW(MasterService* service, const TenantId& tenant_id,
                           const route::ObjectRef& ref)
            : service_(service),
              object_id_{tenant_id, ref.key},
              ns_(service_->namespaces_.Lookup(tenant_id)),
              guard_(ns_ == nullptr ? std::optional<route::WriteGuard>()
                                    : ns_->objects.Write(ref)) {}

        MetadataAccessorRW(const MetadataAccessorRW&) = delete;
        MetadataAccessorRW& operator=(const MetadataAccessorRW&) = delete;

        // Check if metadata exists
        bool Exists() const {
            return IsPublished() && guard_->metadata().IsValid();
        }

        // Holds an object, whether or not a replica is still valid.
        bool IsPublished() const {
            return guard_.has_value() && guard_->has_object();
        }

        bool InProcessing() const {
            return IsPublished() && guard_->state().is_processing;
        }

        bool HasReplicationTask() const {
            return IsPublished() && guard_->state().replication_task;
        }

        const std::string& GetKey() const { return object_id_.user_key; }
        // The tenant the object was resolved under.
        const TenantId& GetTenantId() const { return object_id_.tenant_id; }
        // The held publication; like `Get()`, only while one is held.
        route::ObjectRef GetRef() const { return guard_->ref(); }

        // The namespace policy's say on the held key taking `bytes` more
        // memory, asked before it is allocated; zero bytes asks only whether
        // the namespace admits writes. Only while a key is held. A caller that
        // fails after growing needs no rollback: releasing the key settles the
        // account to what the object holds then, which gives the growth back.
        [[nodiscard]] tl::expected<void, ErrorCode> Grow(uint64_t bytes) const {
            return ns_->Grow(*guard_, bytes);
        }

        // Tells the namespace policy the held object is about to be replaced
        // under this same lock (see NamespacePolicy::OnReplace).
        void BeforeReplace() const { ns_->BeforeReplace(*guard_); }

        // Get metadata (only call when IsPublished() is true)
        ObjectMetadata& Get() { return guard_->metadata(); }

        // The tasks in flight for the object (only call when IsPublished()
        // is true)
        route::ObjectState& GetState() { return guard_->state(); }

        ReplicationTask& GetReplicationTask() {
            return *guard_->state().replication_task;
        }

        // Publishes an object under the held key, which must hold none:
        // builds its metadata in place from `metadata_args`, ObjectMetadata's
        // constructor arguments, with an empty state, and wires a grouped
        // object to its group's shared lease. Only a CreateKey accessor holds
        // a key with no object. Returns the new publication's generation.
        template <typename... Args>
        route::Generation Publish(Args&&... metadata_args) {
            assert(guard_.has_value() && !guard_->has_object());
            return guard_->Publish(std::forward<Args>(metadata_args)...);
        }

        // Erases the held object and everything keyed to it; the key stays
        // held, so the caller may publish a replacement under it.
        void Erase(const std::vector<std::string>& previous_media_hint = {}) {
            Erase(EraseMode::kFull, previous_media_hint);
        }
        void Erase(EraseMode erase_mode,
                   const std::vector<std::string>& previous_media_hint = {}) {
            service_->EraseMetadata(*ns_, *guard_, erase_mode,
                                    previous_media_hint);
        }

        void EraseFromProcessing() {
            if (IsPublished()) {
                guard_->state().is_processing = false;
            }
        }

        void EraseReplicationTask() {
            if (IsPublished()) {
                guard_->state().replication_task.reset();
            }
        }

       private:
        MasterService* service_;
        ObjectIdentity object_id_;
        // Null when the namespace does not exist; a namespace stays put once
        // created, so the pointer outlives the guard.
        metadata::Namespace* ns_;
        std::optional<route::WriteGuard> guard_;
    };

    class MetadataAccessorRO {
       public:
        // Reads the object without copying its identity, which a batch read
        // would otherwise pay for once per key.
        MetadataAccessorRO(const MasterService* service,
                           const TenantId& tenant_id,
                           const std::string& user_key)
            : guard_(Hold(service->namespaces_.Lookup(tenant_id), user_key)) {}
        MetadataAccessorRO(const MasterService* service,
                           const ObjectIdentity& object_id)
            : MetadataAccessorRO(service, object_id.tenant_id,
                                 object_id.user_key) {}

        // Check if metadata exists
        bool Exists() const {
            return IsPublished() && guard_->metadata().IsValid();
        }

        // Holds an object, whether or not a replica is still valid.
        bool IsPublished() const { return guard_.has_value(); }

        bool InProcessing() const {
            return IsPublished() && guard_->state().is_processing;
        }

        // Get metadata (only call when Exists() is true)
        const ObjectMetadata& Get() const { return guard_->metadata(); }

        // The tasks in flight for the object (only call when IsPublished()
        // is true)
        const route::ObjectState& GetState() const { return guard_->state(); }

       private:
        static std::optional<route::ReadGuard> Hold(
            const metadata::Namespace* ns, const std::string& user_key) {
            if (ns == nullptr) {
                return std::nullopt;
            }
            return ns->objects.Read(user_key);
        }

        std::optional<route::ReadGuard> guard_;
    };

    // Calls `visit` with each namespace that holds an object when the walk
    // starts, holding no lock, so `visit` may reach any key. A namespace that
    // gains its first object meanwhile may or may not be visited. Its objects
    // are walked with a cursor of its route:
    //
    //     for (auto object : ns.objects.ReadCursor()) { ... }
    //
    // which holds a stripe lock across the loop body, so the body must not
    // reach any key; one that acts afterwards keeps `ref()` and comes back
    // through MetadataAccessorRW(service, tenant_id, ref).
    void VisitNamespaces(
        const std::function<void(const TenantId&, metadata::Namespace&)>& visit)
        const;

    // The work client reads leave for when they hold no lock on any key:
    // queueing a promotion for each read served only from LOCAL_DISK, and
    // counting each object's access towards another memory replica once.
    class ReadFollowUps {
       public:
        void Promote(const TenantId& tenant_id, const std::string& key) {
            promotions_.push_back({tenant_id, key});
        }
        void RecordAccess(const TenantId& tenant_id, const std::string& key) {
            if (accessed_.insert(tenant_id.MakeScopedKey(key)).second) {
                accesses_.push_back({tenant_id, key});
            }
        }
        // The caller holds master_state_mutex_ (shared) and no key.
        void Run(MasterService& service) const;

       private:
        std::vector<ObjectIdentity> promotions_;
        std::vector<ObjectIdentity> accesses_;
        std::unordered_set<std::string> accessed_;
    };

    // What ExistKey answers for an object: whether it has a replica to read,
    // granting it a read lease if it does and `grant_lease` is set.
    bool ReadExistence(const ObjectMetadata& metadata, bool grant_lease) const;
    // A client read of an object: its readable replicas, after the read's
    // effects on it (the read lease, the DFS LRU, hit metrics). What is left
    // for when no key is held goes to `follow_ups`.
    auto ReadReplicaList(const TenantId& tenant_id, const std::string& key,
                         const ObjectMetadata& metadata,
                         ReadFollowUps& follow_ups)
        -> tl::expected<GetReplicaListResponse, ErrorCode>;
    // An admin read of an object, which leaves it as it is.
    auto ReadReplicaListForAdmin(const ObjectMetadata& metadata) const
        -> tl::expected<GetReplicaListResponse, ErrorCode>;

    ViewVersionId view_version_;

    // Client related members
    mutable std::shared_mutex client_mutex_;
    std::unordered_map<UUID, std::shared_ptr<ClientLivenessRecord>,
                       boost::hash<UUID>>
        client_liveness_records_;
    std::unordered_set<UUID, boost::hash<UUID>>
        ok_client_;  // client with ok status
    std::unordered_map<UUID, std::string, boost::hash<UUID>> client_host_id_;
    ClientOffboardingWorker client_offboarding_worker_{this};
    void ClientMonitorFunc();
    std::thread client_monitor_thread_;
    std::atomic<bool> client_monitor_running_{false};
    static constexpr uint64_t kClientMonitorSleepMs =
        1000;  // 1000 ms sleep between client monitor checks
    const int64_t client_active_ttl_sec_;
    const int64_t client_suspicion_ttl_sec_;
    const std::chrono::seconds nof_heartbeat_interval_sec_;
    const std::chrono::milliseconds nof_heartbeat_probe_timeout_ms_;
    const uint32_t nof_heartbeat_failures_threshold_;

    struct NoFHeartbeatState {
        std::chrono::steady_clock::time_point next_probe_at{};
        std::chrono::steady_clock::time_point last_success_at{};
        uint32_t consecutive_failures{0};
    };
    std::mutex nof_heartbeat_mutex_;
    std::unordered_map<UUID, NoFHeartbeatState, boost::hash<UUID>>
        nof_heartbeat_states_;
    std::thread nof_heartbeat_thread_;
    std::atomic<bool> nof_heartbeat_running_{false};
    static constexpr uint64_t kNoFHeartbeatThreadSleepMs = 100;
    mutable std::mutex nof_probe_fn_mutex_;
    NoFProbeFn nof_probe_fn_;

    // Shared NoF probe default, used by both the constructor and the
    // test-seam reset. A single function-static initiator keeps the probe
    // path aligned with the process-global NofPageRegistry and avoids
    // duplicating the lambda body in two places. Private but reachable from
    // the friend test peer, which resets nof_probe_fn_ to this default.
    static bool DefaultProbeNofSegment(const std::string& te_endpoint,
                                       uint32_t timeout_ms,
                                       std::string* error_reason);

    // if high availability features enabled
    const bool enable_ha_;

    const bool enable_offload_;

    // Offload-on-evict: defer disk offload to eviction time
    // (config: offload_on_evict)
    bool offload_on_evict_{false};

    // Promotion-on-hit: opt-in flag enabling LOCAL_DISK -> MEMORY promotion
    // when a Get observes a key with only LOCAL_DISK replicas.
    bool promotion_on_hit_{false};
    uint32_t promotion_admission_threshold_{2};
    uint32_t promotion_queue_limit_{50000};
    uint32_t promotion_max_per_heartbeat_{1};
    // Global in-flight task counter, checked against promotion_queue_limit_
    // as the gate cap. Promotion specifically targets skewed
    // access (hot keys re-accessed after eviction), so the global counter
    // is the correct primitive. Incremented in TryPushPromotionQueue after
    // successful enqueue; decremented in NotifyPromotionSuccess and in the
    // promotion task reaper after the task entry is erased. Relaxed memory
    // order is safe — the value is an advisory soft cap, not a barrier.
    std::atomic<uint64_t> promotion_in_flight_{0};
    // Promotion retry candidates: the index of due keys and the retry rules.
    PromotionCandidateTracker promotion_candidates_;
    // At most this many due candidates are tried per retry round.
    static constexpr size_t kPromotionRetryBatchSize = 128;
    // Bound on self-sustaining execution-failure cycles: a key whose
    // promotion keeps failing at execution time (AllocStart under DRAM
    // pressure, TE-write flake, SSD error) is re-recorded at most this many
    // times. Bounds a persistently-failing ("poison") key to this many
    // delivery slots (~this many heartbeat ticks, ~30s at the 10s default)
    // before it stops re-queueing itself; genuine reads can still re-admit
    // it afterwards with a fresh count.
    static constexpr uint32_t kMaxPromotionExecutionFailures = 3;

    // Master-side frequency sketch. Constructed only when promotion_on_hit_ is
    // true. CountMinSketch is mutex-protected internally so we can call into it
    // from any GetReplicaList caller without additional locking.
    std::unique_ptr<CountMinSketch> promotion_sketch_;

    // Hot-object MEMORY replication: heat admission, placement and the leases
    // a client copies inside. The pending task of a key stays on its record.
    DynamicReplicationController dynamic_replication_;

    void TrySubmitDynamicReplicaProposal(const ObjectIdentity& object_id);
    tl::expected<ReplicaActionLease, ErrorCode>
    SubmitReplicaActionProposalLocked(const ReplicaActionProposal& proposal);
    void CleanupExpiredDynamicReplicationState();
    tl::expected<UUID, ErrorCode> SubmitDynamicReplicaCopyTask(
        const ObjectIdentity& object_id,
        const DynamicReplicationController::Plan& plan, const UUID& lease_id,
        uint64_t version_epoch);
    // Runs under the key's lock CopyStart holds for the whole operation, so
    // the pending state it reads is the one that copy was admitted for.
    tl::expected<void, ErrorCode> ValidateDynamicReplicaPendingForCopyStart(
        route::ObjectState& state, const TenantId& tenant_id,
        const std::string& key, const UUID& dynamic_replication_lease_id,
        const UUID& client_id, const std::string& source_segment,
        uint64_t current_version_epoch,
        uint64_t dynamic_replication_version_epoch,
        const std::vector<std::string>& target_segments);

    const bool enable_oplog_;
    const bool weight_management_mutations_enabled_;
    const uint32_t oplog_batch_max_entries_;
    const uint64_t restore_repair_durable_timeout_ms_;

    // cluster id for persistent sub directory
    const std::string cluster_id_;
    // root filesystem directory for persistent storage
    const std::string root_fs_dir_;
    // storage backend eviction configuration
    const bool enable_disk_eviction_;
    const uint64_t quota_bytes_;
    const bool enable_multi_tenants_;

    // HTTP metadata server pointer for cleanup on client timeout
    // nullptr means cleanup is disabled
    HttpMetadataServer* http_metadata_server_{nullptr};

    // Remote HTTP metadata server URL, used when the metadata server is
    // deployed separately. Empty = no remote cleanup (co-located prefers the
    // pointer).
    std::string http_metadata_remote_url_;

    // Cached HTTP metadata key prefix (initialized once at startup)
    std::string http_metadata_prefix_;

    // Async worker for remote cleanup: segments are enqueued from the client
    // monitor thread so a slow/unreachable server never blocks heartbeats.
    std::thread http_metadata_cleanup_thread_;
    std::atomic<bool> http_metadata_cleanup_running_{false};
    std::mutex http_metadata_cleanup_mutex_;
    std::condition_variable http_metadata_cleanup_cv_;
    std::vector<std::string> http_metadata_cleanup_queue_;

    void HttpMetadataCleanupThreadFunc();
    // Sends an HTTP DELETE for one key to the remote metadata server; true on
    // success.
    bool removeRemoteHttpMetadataKey(const std::string& key) const;

    // Clean up HTTP metadata (mooncake/ram/*, mooncake/rpc_meta/*) for a
    // segment. For the co-located case this is synchronous (no network I/O);
    // for the remote case it enqueues to the async cleanup worker.
    void cleanupHttpMetadata(const std::string& segment_name);

    bool use_disk_replica_{false};
    bool enable_dfs_{false};
    std::unique_ptr<DfsAllocatorInterface> dfs_allocator_;
    ShardAllocator* shard_allocator_{nullptr};
    ImmutableBucketAllocator* bucket_allocator_{nullptr};
    // Serializes allocation-failure recovery so concurrent writers can reuse
    // capacity made available by the first recovery instead of each evicting
    // a different frozen bucket.
    std::mutex dfs_bucket_recovery_mutex_;

    // Segment management
    SegmentManager segment_manager_;
    LocalSsdManager local_ssd_manager_;
    NoFSegmentManager nof_segment_manager_;
    BufferAllocatorType memory_allocator_type_;
    const AllocationStrategyType allocation_strategy_type_;
    std::shared_ptr<AllocationStrategy> allocation_strategy_;

    mutable std::shared_mutex master_state_mutex_;

    // Restore lifecycle state (RFC #4700, slice 1). Declared next to
    // master_state_mutex_ because the follow-up slices replace that lock's
    // restore-window protection with this state machine. kConstructed keeps
    // business workers admitted so direct construction in tests keeps the
    // constructor-starts-workers behavior; tightening that to "paused until
    // activation" is part of the later slices once callers carry explicit
    // activation. The embedded master no longer relies on this: it commits
    // Constructed -> Ready -> Serving before it serves RPCs.
    std::atomic<RestoreLifecycleState> lifecycle_state_{
        RestoreLifecycleState::kConstructed};

    // True in every lifecycle state except the restore windows
    // (kRecovering / kFailed). Background workers that mutate business
    // metadata consult this at the top of each loop iteration so a
    // promotion restore never races them, independent of
    // master_state_mutex_.
    bool AdmitsBusinessWorkers() const;

    // Discarded replicas management
    const std::chrono::seconds put_start_discard_timeout_sec_;
    const std::chrono::seconds put_start_release_timeout_sec_;
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
        TenantId tenant_id;
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
    std::string MakeDrainUnitKey(const TenantId& tenant_id,
                                 const std::string& key,
                                 const std::string& source_segment) const;

    std::thread job_dispatch_thread_;
    std::atomic<bool> job_dispatch_running_{false};
    static constexpr uint64_t kJobDispatchThreadSleepMs = 500;
    std::mutex job_mutex_;
    std::unordered_map<UUID, std::shared_ptr<DrainJob>, boost::hash<UUID>>
        drain_jobs_ GUARDED_BY(job_mutex_);

    std::unique_ptr<KvEventPublisher> kv_event_publisher_;

    // RemoveAll holds no lock across its walk, so a concurrent commit can land
    // in a tenant or stripe it already passed. Publishing `cleared` from the
    // scan's own bookkeeping would then order it after that commit's `stored`
    // and tell subscribers to drop a live object. Every
    // announcement of a newly available object bumps its tenant's epoch, and a
    // clear is published only if the epoch still matches the value read before
    // the scan began. A racing commit therefore suppresses the
    // clear instead of superseding it; subscribers fall back to the per-object
    // `removed` stream, which is what they saw before `cleared` existed.
    //
    // Slots are a fixed hashed array rather than a per-tenant map so the
    // structure cannot grow with tenant churn. A hash collision makes two
    // tenants share an epoch, which can only suppress a clear that was safe to
    // send — never publish one that was not.
    static constexpr size_t kKvTenantEpochSlots = 1024;
    mutable std::mutex kv_tenant_epoch_mutex_;
    std::array<uint64_t, kKvTenantEpochSlots> kv_tenant_epochs_
        GUARDED_BY(kv_tenant_epoch_mutex_) = {};
    // Set from KvEventsEnabled() at construction. Kept separate so tests can
    // exercise the ordering rule without a live ZMQ publisher.
    bool kv_track_tenant_epochs_{false};
    std::atomic<uint64_t> kv_cleared_published_{0};
    std::atomic<uint64_t> kv_cleared_suppressed_by_epoch_{0};
    // Fires once per tenant whose objects a RemoveAll has finished removing,
    // so a test can commit into a tenant the walk already passed.
    std::function<void(const TenantId&)> kv_remove_all_tenant_hook_;

    static size_t KvTenantEpochSlot(const std::string& tenant) {
        return std::hash<std::string>{}(tenant) % kKvTenantEpochSlots;
    }
    // Called while the object's key lock is held, before the `stored` is
    // enqueued, so any clear that observes the old epoch has not yet published.
    void BumpKvTenantEpoch(const std::string& tenant) {
        if (!kv_track_tenant_epochs_) {
            return;
        }
        std::lock_guard<std::mutex> lock(kv_tenant_epoch_mutex_);
        ++kv_tenant_epochs_[KvTenantEpochSlot(tenant)];
    }
    uint64_t ReadKvTenantEpoch(const std::string& tenant) {
        if (!kv_track_tenant_epochs_) {
            return 0;
        }
        std::lock_guard<std::mutex> lock(kv_tenant_epoch_mutex_);
        return kv_tenant_epochs_[KvTenantEpochSlot(tenant)];
    }
    // A whole-array copy taken before a scan begins. The global RemoveAll does
    // not know which tenants it will meet, and reading a tenant's epoch only
    // once the scan reaches it is too late — see the call site.
    std::array<uint64_t, kKvTenantEpochSlots> SnapshotKvTenantEpochs(
        bool needed) {
        if (!needed || !kv_track_tenant_epochs_) {
            return {};
        }
        std::lock_guard<std::mutex> lock(kv_tenant_epoch_mutex_);
        return kv_tenant_epochs_;
    }
    // Re-reads the epoch and publishes under the same lock that guards the
    // bump, so a commit cannot slip between the check and the enqueue.
    void PublishKvClearedIfEpochUnchanged(const TenantId& tenant_id,
                                          uint64_t expected_epoch);

    // Gated snapshots for the call sites that capture a pre-mutation medium
    // set. They run on hot metadata paths, so skip the VisitReplicas walk
    // entirely when no publisher is listening.
    std::vector<std::string> KvMediaSnapshot(const ObjectMetadata& metadata) {
        return KvEventsEnabled() ? KvMediaForMetadata(metadata)
                                 : std::vector<std::string>{};
    }
    std::vector<std::string> KvRemovalSnapshot(const ObjectMetadata& metadata) {
        return KvEventsEnabled() ? KvMediaForRemoval(metadata)
                                 : std::vector<std::string>{};
    }

    static KvEventConfig BuildKvEventConfig(const MasterServiceConfig& config);
    static std::vector<std::string> KvMediaForMetadata(
        const ObjectMetadata& metadata);
    // Removal paths may run after replicas have transitioned to PROCESSING or
    // REMOVED. Keep their former medium visible as a conservative hint even
    // though only COMPLETE replicas count as currently available media.
    static std::vector<std::string> KvMediaForRemoval(
        const ObjectMetadata& metadata);
    // The medium is derived from the object's full replica set, not from the
    // replica type that triggered the commit, so no replica type is taken.
    void PublishKvStored(const std::string& key, const ObjectMetadata& metadata,
                         const TenantId& tenant_id);
    void SyncKvObjectState(
        const std::string& key, const ObjectMetadata& metadata,
        const TenantId& tenant_id,
        const std::vector<std::string>& previous_media_hint = {});
    void PublishKvRemoved(
        const std::string& key, const ObjectMetadata& metadata,
        const TenantId& tenant_id,
        const std::vector<std::string>& previous_media_hint = {});
    // evicted_replica_count is the number of replicas the caller actually
    // dropped. It is deliberately not a byte count: a zero-length object still
    // needs its removal announced.
    void PublishKvRemovedAfterEvict(const std::string& key,
                                    size_t evicted_replica_count,
                                    const std::string& medium,
                                    const ObjectMetadata& metadata,
                                    const TenantId& tenant_id);
    void PublishKvCleared(const TenantId& tenant_id);

    // OpLog publishing
    std::shared_ptr<HaKvBackend> batch_oplog_kv_backend_;
    std::unique_ptr<OpLogBatchStorage> batch_oplog_storage_;
    std::unique_ptr<OrderedOpLogWriter> ordered_oplog_writer_;
    BatchOpLogWriterFactory batch_oplog_writer_factory_;

    // OpLog publishing helpers
    std::string SerializeMetadataForOpLog(const ObjectMetadata& metadata) const;
    std::string SerializeMetadataForOpLogFromReplicaDescriptors(
        const ObjectMetadata& metadata,
        const std::vector<Replica::Descriptor>& replicas) const;
    ErrorCode InitializeBatchOpLogWriter(std::shared_ptr<HaKvBackend> backend,
                                         bool require_fenced_writer);
    tl::expected<uint64_t, ErrorCode> AppendOpLogVisibleBeforeDurable(
        OpType type, const std::string& tenant_id, const std::string& key,
        const std::string& payload);
    tl::expected<OpLogEntry, ErrorCode> AppendOpLogWithDurableFinalize(
        OpType type, const std::string& tenant_id, const std::string& key,
        const std::string& payload, DurableFinalizeCallback callback);
    // Commit a set of entries as one indivisible batch record, so a durable
    // prefix either covers the whole set or none of it. Returns the last
    // assigned sequence id on success.
    tl::expected<uint64_t, ErrorCode> AppendOpLogBatchWithDurableFinalize(
        std::vector<OpLogEntry> entries, DurableFinalizeCallback callback);
    tl::expected<OrderedOpLogWriter::Reservation, ErrorCode>
    ReserveBatchOpLogSlot();
    tl::expected<OpLogEntry, ErrorCode> AppendReservedOpLogWithDurableFinalize(
        OrderedOpLogWriter::Reservation&& reservation, OpType type,
        const std::string& tenant_id, const std::string& key,
        const std::string& payload, DurableFinalizeCallback callback);

    // Standby-restored memory endpoints remain unreadable until the owning
    // Client has successfully remounted them.
    std::unordered_set<std::string> invalid_replica_endpoints_;

    // Keep DummyBufferAllocator alive after standby restore.
    // Key: transport_endpoint, Value: allocator.
    std::unordered_map<std::string, std::shared_ptr<BufferAllocatorBase>>
        standby_allocator_keepalive_;
    std::vector<StandbySegmentInfo> standby_memory_segments_;
    std::unordered_map<std::string, uint64_t> standby_accounted_memory_bytes_;

    ErrorCode ValidateStandbyRemountSegment(const Segment& segment) const;

    // True while the replica sits on an endpoint in
    // invalid_replica_endpoints_. The read path asks only this, never the set.
    bool IsAwaitingRemount(const Replica& replica) const;
    bool TryGetReadableReplicaDescriptor(const Replica& replica,
                                         Replica::Descriptor& descriptor) const;
    std::vector<Replica::Descriptor> GetReadableReplicaDescriptors(
        const ObjectMetadata& metadata) const;
    bool IsReplicaReadable(const Replica& replica) const;
    bool HasReadableReplica(const ObjectMetadata& metadata) const;
    bool IsEvictableMemoryReplica(const Replica& replica) const;

    /**
     * Appends a SEGMENT_MOUNT / SEGMENT_UNMOUNT entry without waiting for it
     * to become durable. The local segment change has already happened and
     * cannot be rolled back, so a failed append is only logged; nothing
     * retries it.
     */
    void PersistSegmentOpForHA(const char* why, OpType type,
                               const std::string& key,
                               const std::string& payload);

    /**
     * Appends a REMOVE entry for the key, without waiting for it to become
     * durable.
     * @return OK on success, error on append failure (caller must skip erase)
     */
    tl::expected<void, ErrorCode> PersistRemoveForHA(const char* why,
                                                     const TenantId& tenant_id,
                                                     const std::string& key);

    /**
     * Build replica descriptors after removing replicas matching pred_fn.
     * Returns empty if no complete replicas remain.
     */
    std::vector<Replica::Descriptor> BuildRemainingReplicaDescriptors(
        const ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& should_remove) const;
};

}  // namespace mooncake
