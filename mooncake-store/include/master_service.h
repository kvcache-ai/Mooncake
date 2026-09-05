#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <boost/functional/hash.hpp>
#include <boost/lockfree/queue.hpp>
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
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>
#include <ylt/util/expected.hpp>
#include <ylt/util/tl/expected.hpp>

#include "allocation_strategy.h"
#include "background_worker.h"
#include "count_min_sketch.h"
#include "deadline_scheduler.h"
#include "lease.h"
#include "master_metric_manager.h"
#include "mutex.h"
#include "segment.h"
#include "local_ssd/manager.h"
#include "tenant_quota_ledger.h"
#include "tenant_quota_sharded.h"
#include "tenant_quota_policy_store.h"
#include "types.h"
#include "master_config.h"
#include "rpc_types.h"
#include "replica.h"
#include "ha/ha_types.h"
#include "ha/snapshot/object/snapshot_object_store.h"
#include "task_manager.h"
#include "kv_event/kv_event_publisher.h"
#include "ha/oplog/oplog_types.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "allocator.h"
#include "metadata_store.h"
#include "storage/distributed/global_allocator_interface.h"

namespace mooncake {

// Forward declaration for MasterSnapshotManager
class MasterSnapshotManager;
class MasterSnapshotRepository;

namespace ha {
class SnapshotCatalogStore;
class MasterSnapshotCodec;
struct MasterSnapshotPayloads;
class MasterSnapshotCodecTest;  // test fixture, needs private state access
}  // namespace ha

class EtcdOpLogStore;
class DfsGlobalAllocator;
class ImmutableBucketAllocator;
class GlobalAllocatorInterface;

// Forward declarations
class AllocationStrategy;
class EvictionStrategy;
class HaKvBackend;
class HttpMetadataServer;
class OpLogBatchStorage;
class OrderedOpLogWriter;
struct MetadataStoragePlugin;

// Forward declarations for test classes
namespace test {
class MasterServiceTest;
class MasterServiceSnapshotTestBase;
class SnapshotChildProcessTest;
// Friended so the promotion-on-hit tests can drive a serialize/reset/
// deserialize cycle directly via the otherwise-private
// MetadataSerializer, and inspect private clamp fields. This avoids
// standing up a full snapshot catalog + child-process harness, and
// exposing test-only accessors on MasterService itself.
class PromotionOnHitTest;
class DynamicReplicationTest;
class MasterServiceTenantQuotaTest;
class MasterScenario;
class MasterServiceHATest;
// Friended so the processing_keys double-erase reproduction test can
// invalidate a segment allocator via PrepareUnmountSegment WITHOUT the
// ClearInvalidHandles sweep that MasterService::UnmountSegment performs.
class MasterServiceProcessingKeyDoubleEraseTest;
// Friended so the LOCAL_DISK deregistration interleaving tests can run the
// two halves of UnmountLocalDiskSegment (deregistration, replica sweep)
// with a competing mount + register serialized between them, pinning the
// interleaving instead of hoping a thread scheduler produces it.
class LocalDiskUnmountInterleavingTest;
// Friended so the #2997 regression test can call the private
// PushOffloadingQueue directly with degenerate replica states that the
// public PutStart/PutEnd path never produces.
class MasterServiceSSDTest;
}  // namespace test
namespace benchmarks {
class BatchEvictBench;
}  // namespace benchmarks

// std::unordered_map/set never shrink their bucket array on erase, so a
// container that once held millions of entries keeps its high-water bucket
// memory (8 bytes per bucket) forever. ShrinkBucketsIfSparse rehashes a
// container down to roughly twice its live size once the bucket array is
// both large enough to matter and less than a quarter full. The bucket
// floor avoids rehash churn on small containers; the 2x headroom keeps a
// freshly shrunk container from growing again right away.
// Rehashing invalidates iterators: callers must hold the lock guarding the
// container and must not be iterating it.
inline constexpr size_t kShrinkMinBucketCount = 1024;

template <typename UnorderedContainer>
void ShrinkBucketsIfSparse(UnorderedContainer& container) {
    if (container.bucket_count() > kShrinkMinBucketCount &&
        container.size() < container.bucket_count() / 4) {
        container.rehash(container.size() * 2);
    }
}

/*
 * @brief MasterService is the main class for the master server.
 * Lock order: To avoid deadlocks, the following lock order should be followed:
 * 1. client_mutex_
 * 2. tenant_quota_policy_mutex_
 * 3. snapshot_mutex_
 * 4. metadata_shards_[shard_idx_].mutex
 * 5. tenant_quota_recompute_mutex_
 * 6. ShardedTenantQuotaTable internal mutex or segment_mutex_
 * 7. soft_pin_deadline_index_ mutex
 *
 * Strict tenant admission and policy mutation paths that need both
 * tenant_quota_policy_mutex_ and snapshot_mutex_ must acquire the tenant
 * policy mutex first, then snapshot_mutex_.
 * tenant_quota_recompute_mutex_ serializes the capacity snapshot and the
 * corresponding quota-table update. The segment mutex is released before
 * entering ShardedTenantQuotaTable, so these two locks are never nested.
 */

class MasterService {
    // Test friend class for snapshot/restore testing
    friend class test::MasterServiceSnapshotTestBase;
    friend class test::MasterServiceTest;
    friend class test::SnapshotChildProcessTest;
    friend class test::PromotionOnHitTest;
    friend class test::DynamicReplicationTest;
    friend class benchmarks::BatchEvictBench;
    friend class test::MasterServiceTenantQuotaTest;
    // The scenario DSL controls lease timestamps so eviction tests do not
    // depend on sleeps or the background eviction thread.
    friend class test::MasterScenario;
    // double-erase processing_keys UAF repro (2026-08-03 prod segfault)
    friend class test::MasterServiceProcessingKeyDoubleEraseTest;
    friend class test::LocalDiskUnmountInterleavingTest;
    // #2997 regression: exercises PushOffloadingQueue's no-op paths directly.
    friend class test::MasterServiceSSDTest;
    friend class MasterSnapshotManager;    // Allow access to internal state for
                                           // snapshot
    friend class ha::MasterSnapshotCodec;  // Allow codec to access private
                                           // members
    friend class ha::MasterSnapshotCodecTest;  // codec round-trip unit test
    friend class test::MasterServiceHATest;

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

    void SetNoFProbeFnForTesting(NoFProbeFn fn);
    size_t GetMountedNoFSegmentCountForTesting();
    bool IsNoFSegmentMountedForTesting(const UUID& segment_id);
    std::optional<uint32_t> GetNoFHeartbeatFailureCountForTesting(
        const UUID& segment_id);
    [[nodiscard]] TieredStorageUsageSnapshot GetStorageUsageSnapshot() const;
    bool IsTenantQuotaEnabled() const;
    std::vector<TenantQuotaSnapshot> ListTenantQuotaSnapshots() const;
    std::optional<TenantQuotaSnapshot> GetTenantQuotaSnapshot(
        const TenantId& tenant_id) const;
    tl::expected<TenantQuotaSnapshot, ErrorCode> UpsertTenantQuotaPolicy(
        const TenantId& tenant_id, uint64_t requested_quota_bytes);
    tl::expected<std::optional<TenantQuotaSnapshot>, ErrorCode>
    DeleteTenantQuotaPolicy(const TenantId& tenant_id);
    uint64_t GetTenantQuotaAllocatableCapacityBytes();
    tl::expected<int64_t, ErrorCode> SetDfsMaxBucketCount(
        int64_t new_max_bucket_count);

    ErrorCode SetBatchOpLogBackendForTesting(
        std::shared_ptr<HaKvBackend> backend);
    void SetBatchOpLogWriterFactoryForTesting(BatchOpLogWriterFactory factory);

    /**
     * @brief Test-only wrapper around BatchEvict / NoFBatchEvict so that
     *        unit tests can drive a single eviction cycle synchronously
     *        without standing up the periodic eviction thread.
     */
    void RunBatchEvictForTesting(double evict_ratio_target,
                                 double evict_ratio_lowerbound);
    void RunNoFBatchEvictForTesting(double evict_ratio_target,
                                    double evict_ratio_lowerbound);
    void RunDfsEvictionForTesting();

    /**
     * @brief Enables the tenant-epoch bookkeeping that decides whether
     *        RemoveAll may publish `cleared`. Production turns this on from the
     *        publisher config; tests need it without a live ZMQ socket.
     */
    void SetKvTenantEpochTrackingForTesting(bool enabled);
    /**
     * @brief Installs a callback invoked after each shard's lock is released
     *        during a RemoveAll scan, receiving the shard index just finished.
     *        Lets a test commit into an already-scanned shard
     *        deterministically.
     */
    void SetRemoveAllShardHookForTesting(std::function<void(size_t)> hook);
    /**
     * @brief Counts of `cleared` publications and of clears withheld because a
     *        concurrent commit advanced the tenant epoch mid-scan.
     */
    uint64_t GetKvClearedPublishedForTesting() const;
    uint64_t GetKvClearedSuppressedForTesting() const;

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
     * @return An expected object containing a vector of successfully cleared
     * keys on success, or an ErrorCode on failure. Only successfully
     * cleared keys are included in the result.
     */
    // Existing key-only overload (signature unchanged): kept for legacy
    // callers; delegates with "default".
    auto BatchReplicaClear(const std::vector<std::string>& object_keys,
                           const UUID& client_id,
                           const std::string& segment_name)
        -> tl::expected<std::vector<std::string>, ErrorCode>;

    // New: tenant-aware overload
    auto BatchReplicaClear(const std::vector<std::string>& object_keys,
                           const UUID& client_id,
                           const std::string& segment_name,
                           const std::string& tenant_id)
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

    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchPutStart(const UUID& client_id, const std::vector<std::string>& keys,
                  const TenantId& tenant_id,
                  const std::vector<uint64_t>& slice_lengths,
                  const ReplicateConfig& config);

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
     * exclusive snapshot_mutex_ so no registration admitted against the old
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
     * its per-client promotion_objects queue. The per-shard promotion_tasks
     * map remains populated as the source of truth until NotifyPromotionSuccess
     * commits the new MEMORY replica.
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
     * @brief Commit a staged MEMORY replica to COMPLETE; decrement source
     * refcnt; erase per-shard and per-client task entries. Mirror of
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
     * @brief Submit a dynamic replica action proposal after Master-side
     * hotness admission.
     */
    tl::expected<ReplicaActionLease, ErrorCode> SubmitReplicaActionProposal(
        const ReplicaActionProposal& proposal);

    /**
     * @brief Create a move task to move an object's replica from source segment
     * to target segment
     * @return Move task ID on success, ErrorCode on failure
     */
    tl::expected<UUID, ErrorCode> CreateMoveTask(const std::string& key,
                                                 const TenantId& tenant_id,
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
     * @brief Restore primary state from standby promotion context.
     * Called once at promotion time before serving requests.
     */
    tl::expected<void, ErrorCode> RestoreFromStandbySnapshot(
        const std::vector<StandbyObjectEntry>& objects,
        uint64_t initial_oplog_sequence_id,
        const std::vector<StandbySegmentInfo>& segments);

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

   private:
    std::unique_ptr<ha::SnapshotCatalogStore> CreateSnapshotCatalogStore(
        const MasterServiceConfig& config);

    // Restore master state
    void RestoreState();
    void ResetStateAfterFailedRestoreAttempt();

    /**
     * @brief Apply decoded snapshot state to running master service
     * @param payloads Decoded snapshot payloads
     * @param now Current time for cleanup logic
     * @return void on success, SerializationError on failure
     */
    tl::expected<void, SerializationError> ApplySnapshotState(
        const std::chrono::system_clock::time_point& now);

    // BatchEvict evicts objects in a near-LRU way, i.e., prioritizes to evict
    // object with smaller lease timeout. It has two passes. The first pass only
    // evicts objects without soft pin. The second pass prioritizes objects
    // without soft pin, but also allows to evict soft pinned objects if
    // allow_evict_soft_pinned_objects_ is true. The first pass tries fulfill
    // evict ratio target. If the actual evicted ratio is less than
    // evict_ratio_lowerbound, the second pass will be triggered and try to
    // fulfill evict ratio lowerbound.
    void BatchEvict(double evict_ratio_target, double evict_ratio_lowerbound);
    void NoFBatchEvict(double evict_ratio_target,
                       double evict_ratio_lowerbound);
    struct TenantQuotaEvictionResult {
        uint64_t freed_bytes{0};
        uint64_t evicted_objects{0};
    };
    TenantQuotaEvictionResult EvictTenantMemoryForQuota(
        const TenantId& tenant_id, uint64_t target_bytes);

    void UpdateClientHostId(const UUID& client_id, const std::string& host_id);
    std::string GetClientHostId(const UUID& client_id) const;

    void ClearInvalidHandles();
    // Caller owns snapshot_mutex_ (shared) while metadata is swept.
    void ClearInvalidHandles(
        const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients);
    // Clear completed LOCAL_DISK replicas owned by exactly this client, in
    // all shards. Owner-targeted on purpose: a liveness-complement sweep
    // classifies by absence from a point-in-time set, so an owner that
    // mounts and registers between taking that set and the sweep reaching
    // its shard would be swept as stale. A predicate on the owner id cannot
    // misclassify a concurrent mount, whatever the interleaving.
    void ClearLocalDiskHandlesOwnedBy(const UUID& owner);
    // Shard walk shared by the two sweeps above; removes completed replicas
    // matching is_stale, erasing a key when no valid replica remains.
    void ClearStaleHandles(const std::function<bool(const Replica&)>& is_stale);

    std::string FormatTimestamp(
        const std::chrono::system_clock::time_point& tp);
    // We need to clean up finished tasks periodically to avoid memory leak
    // And also we can add some task ttl mechanism in the future
    void TaskCleanupThreadFunc();
    void JobDispatchThreadFunc();
    void DynamicReplicationAdmissionThreadFunc();

    // Internal data structures
    struct ObjectIdentity {
        TenantId tenant_id;
        std::string user_key;
    };

    struct ResolvedSoftPinRequest {
        SoftPinAction action{SoftPinAction::PRESERVE};
        uint64_t ttl_ms{0};
    };

    struct ObjectMetadata {
        struct SoftPinEvaluation {
            bool active{false};
            int metric_delta{0};
            std::optional<std::chrono::system_clock::time_point>
                removed_deadline;
            std::optional<std::chrono::system_clock::time_point>
                deadline_to_index;
        };

        struct PendingSoftPinAction {
            SoftPinAction action{SoftPinAction::PRESERVE};
            uint64_t ttl_ms{0};
            std::vector<ReplicaID> eligible_replica_ids;
        };

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
            std::optional<std::chrono::system_clock::time_point>
                committed_soft_pin_timeout = std::nullopt,
            bool enable_hard_pin = false,
            ObjectDataType data_type_ = ObjectDataType::UNKNOWN,
            std::string group_id_ = "", TenantId tenant_id_ = TenantId(),
            std::string user_key_ = {})
            : client_id(client_id_),
              put_start_time(put_start_time_),
              size(value_length),
              data_type(data_type_),
              group_id(std::move(group_id_)),
              tenant_id(std::move(tenant_id_)),
              user_key(std::move(user_key_)),
              soft_pin_timeout(std::move(committed_soft_pin_timeout)),
              hard_pinned(enable_hard_pin),
              replicas_(std::move(reps)) {
            MasterMetricManager::instance().inc_key_count(1);
            if (soft_pin_timeout) {
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
        std::optional<uint64_t> object_checksum;
        const ObjectDataType data_type{ObjectDataType::UNKNOWN};
        const std::string group_id;
        const TenantId tenant_id;
        const std::string user_key;

        mutable SpinLock lock;
        // Authoritative lease: ungrouped objects own one; grouped objects share
        // the group's. Never null after construction.
        mutable std::shared_ptr<Lease> lease_ GUARDED_BY(lock) =
            std::make_shared<Lease>();
        mutable std::optional<std::chrono::system_clock::time_point>
            soft_pin_timeout GUARDED_BY(lock);  // committed object soft-pin
                                                // deadline
        // Replica IDs scope this action to the current write. PutEnd does not
        // carry a generation token, so a stale End from the same client cannot
        // otherwise be distinguished from the current write.
        std::optional<PendingSoftPinAction> pending_soft_pin_action;
        const bool hard_pinned{false};  // immutable, set at creation
        bool memory_cache_total_accounted{false};
        bool disk_cache_total_accounted{false};
        TenantQuotaLedger quota_ledger;

        struct DynamicReplicaRecord {
            std::chrono::system_clock::time_point created_at;
            std::string source_segment;
            std::string target_segment;
            std::string target_domain;
            bool complete{false};
        };

        std::unordered_map<ReplicaID, DynamicReplicaRecord> dynamic_replicas;
        std::chrono::steady_clock::time_point
            dynamic_replication_recreate_after{};

        void MarkDynamicReplica(ReplicaID replica_id,
                                DynamicReplicaRecord record) {
            dynamic_replicas[replica_id] = std::move(record);
        }

        void MarkDynamicReplicasComplete(
            const std::vector<ReplicaID>& replica_ids) {
            for (const auto& replica_id : replica_ids) {
                auto it = dynamic_replicas.find(replica_id);
                if (it != dynamic_replicas.end()) {
                    it->second.complete = true;
                }
            }
        }

        size_t ForgetDynamicReplicas(
            const std::vector<ReplicaID>& replica_ids) {
            size_t forgotten = 0;
            for (const auto& replica_id : replica_ids) {
                forgotten += dynamic_replicas.erase(replica_id);
            }
            return forgotten;
        }

        size_t DynamicReplicaCount() const { return dynamic_replicas.size(); }

        bool DynamicReplicationRecreateBlocked(
            std::chrono::steady_clock::time_point now) const {
            return now < dynamic_replication_recreate_after;
        }

        void SetDynamicReplicationRecreateAfter(
            std::chrono::steady_clock::time_point deadline) {
            dynamic_replication_recreate_after =
                std::max(dynamic_replication_recreate_after, deadline);
        }

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

        size_t EraseReplica(ReplicaType replica_type) {
            return EraseReplicas([replica_type](const Replica& replica) {
                if (replica_type == ReplicaType::ALL) {
                    return replica.is_memory_replica() ||
                           replica.is_nof_replica() || replica.is_dfs_replica();
                }
                return replica.type() == replica_type;
            });
        }

        bool HasMemReplica() const {
            return HasReplica(&Replica::fn_is_memory_replica);
        }

        bool HasNoFReplica() const {
            return HasReplica(&Replica::fn_is_nof_replica);
        }

        size_t GetMemReplicaCount() const {
            return CountReplicas(&Replica::fn_is_memory_replica);
        }

        size_t GetNoFReplicaCount() const {
            return CountReplicas(&Replica::fn_is_nof_replica);
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

        // Grant a read lease at now() + ttl. Grouped objects extend the shared
        // group TTL; a zero ttl is a no-op on a live lease (PutEnd cannot
        // expire a live group).
        void GrantReadLease(std::chrono::milliseconds ttl) const {
            SpinLocker locker(&lock);
            lease_->GrantReadLease(ttl);
        }

        // Whether the authoritative lease needs a refresh.
        bool NeedsReadLeaseRefresh(std::chrono::milliseconds ttl) const {
            SpinLocker locker(&lock);
            const auto now = std::chrono::system_clock::now();
            return lease_->ExpiresAt() <= now + ttl / 2;
        }

        // Whether the authoritative lease is expired.
        bool IsLeaseExpired() const {
            SpinLocker locker(&lock);
            return lease_->IsExpired(std::chrono::system_clock::now());
        }

        bool IsLeaseExpired(std::chrono::system_clock::time_point& now) const {
            SpinLocker locker(&lock);
            return lease_->IsExpired(now);
        }

        // Lease deadline for the eviction census.
        std::chrono::system_clock::time_point EvictionDeadline() const {
            return lease_->ExpiresAt();
        }

        SoftPinEvaluation EvaluateSoftPin(
            const std::chrono::system_clock::time_point& now) const {
            SpinLocker locker(&lock);
            if (soft_pin_timeout && now >= *soft_pin_timeout) {
                const auto removed_deadline = *soft_pin_timeout;
                soft_pin_timeout.reset();
                return {.active = false,
                        .metric_delta = -1,
                        .removed_deadline = removed_deadline,
                        .deadline_to_index = std::nullopt};
            }
            return {.active = soft_pin_timeout.has_value(),
                    .metric_delta = 0,
                    .removed_deadline = std::nullopt,
                    .deadline_to_index = std::nullopt};
        }

        bool ExpireSoftPinIfDeadlineMatches(
            const std::chrono::system_clock::time_point& expected_deadline,
            const std::chrono::system_clock::time_point& now) const {
            SpinLocker locker(&lock);
            if (!soft_pin_timeout || *soft_pin_timeout != expected_deadline ||
                now < expected_deadline) {
                return false;
            }
            soft_pin_timeout.reset();
            return true;
        }

        static std::chrono::system_clock::time_point ComputeSoftPinDeadline(
            const std::chrono::system_clock::time_point& now, uint64_t ttl_ms) {
            using Milliseconds = std::chrono::milliseconds;
            using MillisecondsRep = Milliseconds::rep;
            const auto max_time = std::chrono::system_clock::time_point::max();
            if (ttl_ms > static_cast<uint64_t>(
                             std::numeric_limits<MillisecondsRep>::max())) {
                return max_time;
            }
            const auto remaining_ms =
                std::chrono::duration_cast<Milliseconds>(max_time - now)
                    .count();
            if (remaining_ms < 0 ||
                ttl_ms > static_cast<uint64_t>(remaining_ms)) {
                return max_time;
            }
            const auto ttl = Milliseconds(static_cast<MillisecondsRep>(ttl_ms));
            return now + ttl;
        }

        std::optional<std::chrono::system_clock::time_point>
        GetCommittedSoftPinTimeout() const {
            SpinLocker locker(&lock);
            return soft_pin_timeout;
        }

        void BeginSoftPinAction(const ResolvedSoftPinRequest& request,
                                std::vector<ReplicaID> eligible_replica_ids) {
            pending_soft_pin_action =
                PendingSoftPinAction{request.action, request.ttl_ms,
                                     std::move(eligible_replica_ids)};
        }

        bool PendingSoftPinOwnsReplica(ReplicaID replica_id) const {
            if (!pending_soft_pin_action) {
                return false;
            }
            const auto& eligible =
                pending_soft_pin_action->eligible_replica_ids;
            return std::find(eligible.begin(), eligible.end(), replica_id) !=
                   eligible.end();
        }

        void ClearPendingSoftPinAction() { pending_soft_pin_action.reset(); }

        SoftPinEvaluation CommitPendingSoftPin(
            const std::chrono::system_clock::time_point& now) {
            if (!pending_soft_pin_action) {
                return EvaluateSoftPin(now);
            }

            const PendingSoftPinAction pending =
                std::move(*pending_soft_pin_action);
            pending_soft_pin_action.reset();

            SpinLocker locker(&lock);
            int metric_delta = 0;
            std::optional<std::chrono::system_clock::time_point>
                removed_deadline;
            std::optional<std::chrono::system_clock::time_point>
                deadline_to_index;
            if (soft_pin_timeout && now >= *soft_pin_timeout) {
                removed_deadline = *soft_pin_timeout;
                soft_pin_timeout.reset();
                --metric_delta;
            }

            switch (pending.action) {
                case SoftPinAction::PRESERVE:
                    break;
                case SoftPinAction::ENABLE:
                    if (pending.ttl_ms == 0) {
                        if (soft_pin_timeout) {
                            removed_deadline = *soft_pin_timeout;
                            soft_pin_timeout.reset();
                            --metric_delta;
                        }
                    } else {
                        if (!soft_pin_timeout) {
                            ++metric_delta;
                        }
                        soft_pin_timeout =
                            ComputeSoftPinDeadline(now, pending.ttl_ms);
                        deadline_to_index = soft_pin_timeout;
                        // Upserting the latest registration supersedes any
                        // expired or previously active deadline.
                        removed_deadline.reset();
                    }
                    break;
                case SoftPinAction::DISABLE:
                    if (soft_pin_timeout) {
                        removed_deadline = *soft_pin_timeout;
                        soft_pin_timeout.reset();
                        --metric_delta;
                    }
                    break;
            }
            return {.active = soft_pin_timeout.has_value(),
                    .metric_delta = metric_delta,
                    .removed_deadline = removed_deadline,
                    .deadline_to_index = deadline_to_index};
        }

        void ClearPendingSoftPinIfNoViableReplica() {
            if (!pending_soft_pin_action) {
                return;
            }
            const auto& eligible =
                pending_soft_pin_action->eligible_replica_ids;
            const bool has_viable_replica =
                std::any_of(replicas_.begin(), replicas_.end(),
                            [&eligible](const Replica& replica) {
                                const bool belongs_to_write =
                                    std::find(eligible.begin(), eligible.end(),
                                              replica.id()) != eligible.end();
                                const bool valid_handle =
                                    !replica.has_invalid_mem_handle() &&
                                    !replica.has_invalid_nof_handle();
                                return belongs_to_write &&
                                       replica.is_processing() && valid_handle;
                            });
            if (!has_viable_replica) {
                pending_soft_pin_action.reset();
            }
        }

        bool IsHardPinned() const { return hard_pinned; }

        bool IsGrouped() const { return !group_id.empty(); }

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

    struct DynamicReplicaPending {
        UUID proposal_id{};
        UUID lease_id{};
        std::string source_segment;
        std::string target_segment;
        std::string target_domain;
        uint64_t version_epoch{0};
        int64_t expire_at_ms_epoch{0};
        UUID task_id{};
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
        uint64_t pending_quota_charge_bytes{0};
        UUID dynamic_replication_lease_id{};
        uint64_t dynamic_replication_version_epoch{0};
        bool durable_cleanup_pending{false};
    };

    struct OffloadingTask {
        ReplicaID source_id;
        std::chrono::system_clock::time_point start_time;
        // Clients whose LocalDiskSegment::offloading_objects hold a mirror
        // for this key. One marker can cover several mirrors, since the
        // offload is pushed once per completed MEMORY replica and those
        // replicas may live on different clients.
        std::vector<UUID> mirror_clients;
    };

    // Tracks an in-flight LOCAL_DISK -> MEMORY copy. The source
    // LOCAL_DISK replica is refcnt-pinned for the duration of the task
    // so it cannot be evicted.
    //
    // alloc_id pins down which staged PROCESSING MEMORY replica
    enum class PromotionQueueResult {
        kQueued,
        kDisabled,
        kFrequencyRejected,
        kWatermarkRejected,
        kQueueCapRejected,
        kAlreadyInFlight,
        kMemoryReplicaPresent,
        kNoLocalDiskSource,
        kNotFound,
        kPushFailed,
    };

    enum class PromotionCandidateReason {
        kWatermark,
        kQueueCap,
        kPushFailed,
        kExecutionFailed,
    };

    struct PromotionCandidate {
        uint8_t sketch_score{0};
        std::chrono::steady_clock::time_point first_seen;
        std::chrono::steady_clock::time_point last_seen;
        std::chrono::steady_clock::time_point retry_after;
        PromotionCandidateReason last_reason{
            PromotionCandidateReason::kQueueCap};
        ErrorCode last_error{ErrorCode::OK};
        uint32_t retry_count{0};
        // Execution failures in this admission chain (AllocStart / TE-write /
        // SSD failures reported via NotifyPromotionFailure). Propagated into
        // PromotionTask at admission so the bound survives the candidate's
        // consumption; reset only when a genuinely new chain starts (fresh
        // insert with 0, e.g. after a give-up or a success).
        uint32_t execution_failures{0};
    };

    // NotifyPromotionSuccess should commit, so a concurrent Put on the
    // same key cannot be confused with ours. 0 until
    // PromotionAllocStart records the new replica.
    //
    // start_time is the reaper deadline anchor. Set at task admission
    // and reset at PromotionAllocStart so each phase (queue-wait and
    // active-transfer) gets its own full put_start_release_timeout_sec_
    // window. Without the reset a backlogged task could enter active
    // transfer with little TTL left, and the reaper could free the
    // staged replica via EraseReplicaByID mid-RDMA-write.
    //
    // holder_id is the client owning the source LOCAL_DISK segment and
    // the only one authorized to commit (NotifyPromotionSuccess) or
    // abort (NotifyPromotionFailure) the task. Without it, any client
    // knowing the key could flip the staged PROCESSING replica to
    // COMPLETE before the holder's RDMA write landed, exposing torn
    // data to readers.
    struct PromotionTask {
        ReplicaID source_id;    // the LOCAL_DISK replica being promoted
        ReplicaID alloc_id{0};  // the new MEMORY replica staged by AllocStart
        uint64_t object_size;
        uint64_t pending_quota_charge_bytes{0};
        std::chrono::system_clock::time_point start_time;
        UUID holder_id;  // owner of source LOCAL_DISK; only Notifier allowed
        // Execution failures so far in this admission chain. Read by
        // NotifyPromotionFailure before the task is erased and re-recorded as
        // execution_failures+1 until kMaxPromotionExecutionFailures. Note the
        // asymmetry with PromotionCandidate::execution_failures: admission
        // copies candidate -> task verbatim, failure re-record writes
        // task+1 -> candidate.
        uint32_t execution_failures{0};
    };

    static constexpr size_t kNumShards = 1024;  // Number of metadata shards

    struct TenantState {
        TenantQuotaHandle quota_account{nullptr};
        std::unordered_map<std::string, ObjectMetadata> metadata;
        std::unordered_set<std::string> processing_keys;
        std::unordered_map<std::string, ReplicationTask> replication_tasks;
        std::unordered_map<std::string, const OffloadingTask> offloading_tasks;
        std::unordered_map<std::string, PromotionTask> promotion_tasks;
        std::unordered_map<std::string, PromotionCandidate>
            promotion_candidates;

        std::unordered_map<std::string, DynamicReplicaPending>
            dynamic_replication_pending;
        std::unordered_map<UUID, ReplicaActionLease, boost::hash<UUID>>
            dynamic_replication_leases;
        std::unordered_map<std::string, std::chrono::steady_clock::time_point>
            dynamic_replication_cooldowns;

        bool Empty() const {
            return metadata.empty() && processing_keys.empty() &&
                   replication_tasks.empty() && offloading_tasks.empty() &&
                   promotion_tasks.empty() && promotion_candidates.empty() &&
                   dynamic_replication_pending.empty() &&
                   dynamic_replication_leases.empty() &&
                   dynamic_replication_cooldowns.empty();
        }
    };

    // Sharded metadata maps and their mutexes
    struct MetadataShard {
        mutable SharedMutex mutex;
        std::unordered_map<TenantId, TenantState, TenantIdHash> tenants
            GUARDED_BY(mutex);
        // Count of objects that have at least one completed LOCAL_DISK replica.
        // Used to compute eviction_base = metadata.size() - disk_object_count,
        // excluding disk-only objects from the eviction denominator.
        long disk_object_count GUARDED_BY(mutex) = 0;
    };
    std::array<MetadataShard, kNumShards> metadata_shards_;

    // Group domain: all groups in one shard. Routing stays hash(tenant, key);
    // group state is just member keys + one shared lease, consulted at eviction
    // (all-or-none) — the read path never touches this table.
    struct GroupState {
        std::unordered_set<std::string> member_keys;
        std::shared_ptr<Lease> lease;

        bool Empty() const { return member_keys.empty(); }
    };

    // Small and low-frequency (put/register + eviction), so one lock is enough.
    struct GroupDomain {
        mutable SharedMutex mutex;
        // key: tenant_id.MakeScopedKey(group_id)
        std::unordered_map<std::string, GroupState> groups GUARDED_BY(mutex);
    };
    GroupDomain group_domain_;

    class SoftPinDeadlineIndex {
       public:
        using TimePoint = std::chrono::system_clock::time_point;

        struct Entry {
            TimePoint deadline;
            size_t shard_idx;
            std::string scoped_key;
        };

        void Upsert(std::string scoped_key, size_t shard_idx,
                    const TimePoint& deadline);
        void Remove(const std::string& scoped_key);
        void RemoveIfMatches(const std::string& scoped_key, size_t shard_idx,
                             const TimePoint& deadline);
        std::vector<Entry> PopExpired(const TimePoint& now);
        void Clear();

        size_t HeapSizeForTest() const;
        size_t RegistrationCountForTest() const;

       private:
        struct Registration {
            TimePoint deadline;
            size_t shard_idx;
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
    void RebuildCacheTotalAccounting();
    static void AccountCacheTotalRemoval(ObjectMetadata& metadata);
    std::vector<Replica> PopReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& pred_fn);
    std::vector<Replica> PopReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata);
    size_t RecordDynamicReplicaRemoval(
        ObjectMetadata& metadata, const std::vector<ReplicaID>& replica_ids);
    size_t EraseReplicasWithCacheTotalAccounting(
        ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& pred_fn,
        std::vector<ReplicaID>* erased_replica_ids = nullptr);

    static constexpr size_t kObjectOperationLockStripes = 4096;

    struct ObjectOperationLock {
        std::unique_lock<std::mutex> lock;
    };

    ObjectOperationLock AcquireObjectOperationLock(const TenantId& tenant_id,
                                                   const std::string& key);

    std::array<std::mutex, kObjectOperationLockStripes> object_operation_locks_;

    // For accessing a metadata shard with read-write permission
    class MetadataShardAccessorRW {
       public:
        MetadataShardAccessorRW(MasterService* master_service,
                                size_t shard_index)
            : shard_(master_service->metadata_shards_[shard_index]),
              lock_(&shard_.mutex) {}

        MetadataShard* operator->() { return &shard_; }

        const MetadataShard* operator->() const { return &shard_; }

        MetadataShard& get() { return shard_; }

        const MetadataShard& get() const { return shard_; }

        // Called after adding a LOCAL_DISK replica. Increments
        // disk_object_count if this is the first completed LOCAL_DISK
        // replica for the object (i.e., exactly 1 completed disk replica now).
        void OnDiskReplicaAdded(const ObjectMetadata& metadata) {
            size_t disk_count = metadata.CountReplicas([](const Replica& r) {
                return r.is_local_disk_replica() && r.is_completed();
            });
            if (disk_count == 1) shard_.disk_object_count++;
        }

        // Called after removing a LOCAL_DISK replica, or when erasing an
        // object that had one. Pass had_completed_disk=true if the object
        // had at least one completed LOCAL_DISK replica before the removal.
        // When the entire object is being erased, call the one-arg overload.
        void OnDiskReplicaRemoved(bool had_completed_disk,
                                  const ObjectMetadata& metadata) {
            if (!had_completed_disk) return;
            bool still_has_disk = metadata.HasReplica([](const Replica& r) {
                return r.is_local_disk_replica() && r.is_completed();
            });
            if (!still_has_disk) shard_.disk_object_count--;
        }

        // Overload for full object erasure — no metadata needed.
        void OnDiskReplicaRemoved(bool had_completed_disk) {
            if (had_completed_disk) shard_.disk_object_count--;
        }

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

        const MetadataShard& get() const { return shard_; }

       private:
        const MetadataShard& shard_;
        SharedMutexLocker lock_;
    };

    // For accessing the group domain with read-write permission
    class GroupDomainAccessorRW {
       public:
        explicit GroupDomainAccessorRW(MasterService* master_service)
            : shard_(master_service->group_domain_), lock_(&shard_.mutex) {}

        GroupDomain* operator->() { return &shard_; }

        const GroupDomain* operator->() const { return &shard_; }

        GroupDomain& get() { return shard_; }

        const GroupDomain& get() const { return shard_; }

       private:
        GroupDomain& shard_;
        SharedMutexLocker lock_;
    };

    // For accessing the group domain with read-only permission
    class GroupDomainAccessorRO {
       public:
        explicit GroupDomainAccessorRO(const MasterService* master_service)
            : shard_(master_service->group_domain_),
              lock_(&shard_.mutex, shared_lock) {}

        const GroupDomain* operator->() const { return &shard_; }

        const GroupDomain& get() const { return shard_; }

       private:
        const GroupDomain& shard_;
        SharedMutexLocker lock_;
    };

    static ObjectIdentity MakeObjectIdentity(const std::string& user_key,
                                             TenantId tenant_id) {
        return {std::move(tenant_id), user_key};
    }
    const TenantId& ResolveRequestTenantId(const TenantId& tenant_id) const;
    ObjectIdentity MakeObjectIdentityForRequest(
        const std::string& user_key, const TenantId& tenant_id) const;
    tl::expected<TenantId, ErrorCode> ResolveTenantIdForWrite(
        const TenantId& tenant_id) const;
    tl::expected<TenantId, ErrorCode> ResolveTenantIdForWriteLocked(
        const TenantId& tenant_id) const;
    bool IsTenantRegistered(const TenantId& tenant_id) const;
    bool TenantHasObjects(const TenantId& tenant_id) const;

    // Helper to get shard index from tenant-scoped object identity.
    size_t getShardIndex(const TenantId& tenant_id,
                         const std::string& user_key) const {
        if (tenant_id.IsDefault()) {
            return std::hash<std::string>{}(user_key) % kNumShards;
        }
        size_t seed = std::hash<std::string>{}(tenant_id.value());
        boost::hash_combine(seed, user_key);
        return seed % kNumShards;
    }

    // Legacy helper routes plain keys to the default tenant.
    size_t getShardIndex(const std::string& key) const {
        return std::hash<std::string>{}(key) % kNumShards;
    }

    // Registers a member key under a group and returns the group's shared
    // Lease (creating the group/lease on first member). Callers wire the
    // returned lease into the object metadata so the read path can extend the
    // group TTL without touching this table. Returns nullptr for empty
    // group_id.
    std::shared_ptr<Lease> RegisterGroupMember(const TenantId& tenant_id,
                                               const std::string& key,
                                               const std::string& group_id);
    void UnregisterGroupMember(const TenantId& tenant_id,
                               const std::string& key,
                               const std::string& group_id);
    // Reads the member keys registered for `group_id`; empty if unregistered.
    std::vector<std::string> GetGroupMemberKeys(
        const TenantId& tenant_id, const std::string& group_id) const;

    // A single group member's eviction outcome, fed back by the
    // EvictGroupOrObject callback.
    struct EvictMemberOutcome {
        uint64_t freed_bytes{0};
        long evicted_objects{0};
        bool stop_scan{false};
        ErrorCode error{ErrorCode::OK};
    };
    // Aggregated outcome of a group eviction.
    struct GroupEvictionResult {
        uint64_t freed_bytes{0};
        long evicted_objects{0};
        bool stop_scan{false};
        ErrorCode error{ErrorCode::OK};
    };

    // Evicts every member of `group_id` across its metadata shards. MUST be
    // called WITHOUT holding any metadata shard lock: the caller releases the
    // trigger shard lock first, so a caller-held trigger lock is never held
    // while other shard locks are acquired (that ordering is the AB/BA
    // cross-shard deadlock this function exists to remove). It acquires each
    // member shard lock itself in canonical ascending shard order, so any two
    // concurrent group evictions that touch the same shards acquire them in the
    // same global order and cannot deadlock.
    //
    // Each member is re-looked-up and re-validated under its own lock (lease,
    // hard/soft pin, evictable replica — all against `now`) because state may
    // have changed since the caller's snapshot; members that no longer qualify
    // are skipped without invoking the callback. `evict_one_member` therefore
    // performs only the path-specific member eviction (oplog persist, offload,
    // quota charge, publish) and may erase members other than `key`; the
    // trigger `key` itself is left to the caller. Each call returns that
    // member's contribution. Returns the aggregated outcome.
    GroupEvictionResult EvictGroupOrObject(
        const TenantId& tenant_id, const std::string& key,
        const std::string& group_id, bool allow_soft_pinned,
        std::chrono::system_clock::time_point now,
        const std::function<EvictMemberOutcome(
            const std::string&, ObjectMetadata&, TenantState&,
            MetadataShardAccessorRW&)>& evict_one_member);

    std::unordered_map<std::string, ObjectMetadata>::iterator EraseMetadata(
        TenantState& tenant_state,
        std::unordered_map<std::string, ObjectMetadata>::iterator it,
        const TenantId& tenant_id);
    void ReleaseLocalDiskUsage(const std::vector<Replica>& replicas);
    enum class QuotaEraseMode {
        kFull,
        kPreserveOld,
        kAbortOnly,
    };
    std::unordered_map<std::string, ObjectMetadata>::iterator EraseMetadata(
        TenantState& tenant_state,
        std::unordered_map<std::string, ObjectMetadata>::iterator it,
        const TenantId& tenant_id, QuotaEraseMode quota_mode);
    tl::expected<void, ErrorCode> SettlePrimaryWriteQuotaIfReady(
        TenantState& tenant_state, ObjectMetadata& metadata);
    uint64_t CompletedMemoryQuotaCharge(const ObjectMetadata& metadata) const;
    uint64_t RequestedMemoryQuotaCharge(uint64_t value_length,
                                        const ReplicateConfig& config) const;
    TenantState& GetOrCreateTenantState(MetadataShard& shard,
                                        const TenantId& tenant_id);
    TenantQuotaHandle GetBoundTenantQuotaHandle(
        const TenantState& tenant_state) const;
    tl::expected<void, ErrorCode> ChargeTenantQuota(
        TenantQuotaHandle account, uint64_t bytes,
        uint64_t* deficit_bytes = nullptr);
    void ReleaseTenantQuota(TenantQuotaHandle account, uint64_t bytes);
    void RecomputeTenantEffectiveQuotas();
    void RebuildTenantQuotaUsageFromMetadata();
    void LoadTenantQuotaPoliciesFromStoreOrThrow();
    void ApplyTenantQuotaPolicies(const TenantQuotaPolicySnapshot& snapshot);
    TenantQuotaPolicySnapshot BuildTenantQuotaPolicySnapshot() const;
    std::unordered_map<std::string, ObjectMetadata>::iterator EraseMetadata(
        TenantState& tenant_state,
        std::unordered_map<std::string, ObjectMetadata>::iterator it,
        const TenantId& tenant_id, QuotaEraseMode quota_mode,
        MetadataShardAccessorRW* shard,
        const std::vector<std::string>& previous_media_hint = {});
    void FinalizeRemovedReplicasAfterDurable(
        const OpLogEntry& durable_entry,
        const std::vector<ReplicaID>& replica_ids, QuotaEraseMode quota_mode,
        const std::vector<std::string>& previous_media_hint = {});
    void FinalizeMetadataEraseAfterDurable(const OpLogEntry& durable_entry,
                                           QuotaEraseMode quota_mode);
    void FinalizeExpiredProcessingReplicasAfterDurable(
        const OpLogEntry& durable_entry,
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
        const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients) const;
    StaleHandleCleanupPlan BuildStaleHandleCleanupPlan(
        const ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& is_stale) const;
    tl::expected<void, ErrorCode> PersistStaleHandleCleanupForHA(
        const std::string& why, const TenantId& tenant_id,
        const std::string& key, ObjectMetadata& metadata,
        const StaleHandleCleanupPlan& plan);
    void RebuildGroupState();
    // Post-restore migration: re-route every object to its hash(tenant, key)
    // shard, fixing snapshots that placed grouped objects on hash(group_id)
    // shards. No-op for correctly-routed snapshots.
    void ReRouteRestoredObjectsByKey();
    static void ApplySoftPinMetricDelta(int metric_delta);
    size_t GetMetadataShardIndex(const ObjectMetadata& metadata) const;
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
    // or local_disk replicas whose owner client is no longer alive.
    bool CleanupStaleHandles(
        const std::string& key, const TenantId& tenant_id,
        TenantState& tenant_state, ObjectMetadata& metadata,
        const std::unordered_set<UUID, boost::hash<UUID>>& alive_clients,
        MetadataShardAccessorRW* shard = nullptr);
    // Predicate form, so the owner-targeted LOCAL_DISK sweep can reuse the
    // accounting (quota release, promotion-task cancellation, disk-replica
    // shard bookkeeping) instead of duplicating it.
    bool CleanupStaleHandles(
        const std::string& key, const TenantId& tenant_id,
        TenantState& tenant_state, ObjectMetadata& metadata,
        const std::function<bool(const Replica&)>& is_stale,
        MetadataShardAccessorRW* shard = nullptr);

    // True when client_id currently has a LOCAL_DISK registration.
    // Momentarily takes the LocalSsdManager registry lock, so callers must not
    // hold it; call before taking a metadata shard lock. Callers that need the
    // answer to stay true across a later metadata write must hold
    // snapshot_mutex_ (shared) across both -- UnmountLocalDiskSegment
    // deregisters the client under the exclusive lock, so the check and the
    // write cannot straddle a deregistration.
    bool HasMountedLocalDiskSegment(const UUID& client_id);

    // Helper: allocate replicas, create ObjectMetadata, insert into shard,
    // and return descriptor list.  Shared by PutStart and UpsertStart.
    auto AllocateAndInsertMetadata(
        MetadataShardAccessorRW& shard, const UUID& client_id,
        const std::string& key, uint64_t value_length,
        const ReplicateConfig& config, const std::string& writer_host_id,
        const std::string& group_id, const TenantId& tenant_id,
        const std::chrono::system_clock::time_point& now,
        const ResolvedSoftPinRequest& soft_pin_request,
        uint64_t& quota_deficit_bytes,
        std::optional<std::chrono::system_clock::time_point>
            committed_soft_pin_timeout = std::nullopt,
        const std::optional<DistributedFSDescriptor>& preallocated_dfs =
            std::nullopt)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    auto PutStartInternal(
        const UUID& client_id, const std::string& key,
        const TenantId& tenant_id, uint64_t slice_length,
        const ReplicateConfig& config,
        const std::optional<DistributedFSDescriptor>& preallocated_dfs)
        -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode>;

    std::vector<BatchAllocateResult> ReserveDfsSpaceForBatch(
        const std::vector<std::string>& keys,
        const std::vector<uint64_t>& value_lengths,
        const std::vector<bool>& needs_dfs);

    /**
     * @brief Helper to discard expired processing keys.
     */
    void DiscardExpiredProcessingReplicas(
        MetadataShardAccessorRW& shard,
        const std::chrono::system_clock::time_point& now);
    void FreeDfsReplicas(const std::string& key,
                         const std::vector<Replica>& replicas);
    void RunDfsEviction();
    void RunBucketDfsEviction();
    bool RunBucketDfsEvictionInternal(bool force_one);
    bool TryRecoverDfsSpaceAfterAllocationFailure();
    void RunShardDfsEviction();
    void RestoreRecoveredDfsReplicas();
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
    bool CancelQueuedOffloadTask(TenantState& tenant_state,
                                 ObjectMetadata& metadata,
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
     * recording the task in the shard's promotion_tasks map.
     */
    tl::expected<void, ErrorCode> PushPromotionQueue(
        const ObjectIdentity& object_id, Replica& source_replica);

    /**
     * @brief Helper invoked from GetReplicaList when an only-LOCAL_DISK key is
     * observed. Applies the gating chain (frequency / watermark / dedup /
     * cap), refcnt-pins the source LOCAL_DISK replica, records a
     * PromotionTask, and pushes onto the holder client's LocalSSD mailbox.
     * Acquires its own RW shard accessor; safe to call after
     * GetReplicaList's RO accessor has been released.
     */
    PromotionQueueResult TryPushPromotionQueue(const ObjectIdentity& object_id,
                                               bool record_candidate = true);
    void RecordOrUpdateCandidate(TenantState& tenant_state,
                                 const std::string& key, uint8_t sketch_score,
                                 PromotionCandidateReason reason,
                                 ErrorCode last_error,
                                 uint32_t execution_failures = 0);
    void EraseCandidate(TenantState& tenant_state, const std::string& key);
    void EraseCandidate(const ObjectIdentity& object_id);
    void DecrementCandidateCount();
    void BackoffCandidate(const ObjectIdentity& object_id,
                          PromotionQueueResult result);
    void ClearCandidatesForReload();
    std::chrono::milliseconds CandidateBackoff(uint32_t retry_count) const;
    bool IsTransientResult(PromotionQueueResult result) const;
    size_t RunPromotionCandidateRetry(size_t max_shards_to_scan);
    size_t RunPromotionCandidateRetry();
    size_t RunPromotionCandidateRetryForTesting();
    size_t CountCandidatesForTesting(const TenantId& tenant_id);
    void ResetCandidateBackoffsForTesting();

    // Erase any in-flight PromotionTask for `key`, refund its pending charge,
    // and decrement the cluster-wide in-flight counter. Safe no-op if no task
    // exists.
    void ErasePromotionTaskIfPresent(TenantState& tenant_state,
                                     const std::string& key)
        NO_THREAD_SAFETY_ANALYSIS {
        auto task_it = tenant_state.promotion_tasks.find(key);
        if (task_it != tenant_state.promotion_tasks.end()) {
            ReleaseTenantQuota(
                GetBoundTenantQuotaHandle(tenant_state),
                std::exchange(task_it->second.pending_quota_charge_bytes, 0));
            tenant_state.promotion_tasks.erase(task_it);
            promotion_in_flight_.fetch_sub(1, std::memory_order_relaxed);
            MasterMetricManager::instance().dec_promotion_in_flight();
            MasterMetricManager::instance().inc_promotion_cancelled();
        }
    }
    void CancelPromotionTaskForRemovedReplicas(
        TenantState& tenant_state, ObjectMetadata& metadata,
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
    const double eviction_ratio_;                     // in range [0.0, 1.0]
    const double eviction_high_watermark_ratio_;      // in range [0.0, 1.0]
    const double nof_eviction_ratio_;                 // in range [0.0, 1.0]
    const double nof_eviction_high_watermark_ratio_;  // in range [0.0, 1.0]

    // Eviction thread related members
    std::thread eviction_thread_;
    std::atomic<bool> eviction_running_{false};
    static constexpr uint64_t kEvictionThreadSleepMs =
        10;  // 10 ms sleep between eviction checks

    // Snapshot manager handles snapshot lifecycle orchestration
    std::unique_ptr<MasterSnapshotManager> snapshot_manager_;

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
        MetadataAccessorRW(MasterService* service, ObjectIdentity object_id)
            : service_(service),
              object_id_(std::move(object_id)),
              shard_idx_(service_->getShardIndex(object_id_.tenant_id,
                                                 object_id_.user_key)),
              shard_guard_(service_, shard_idx_),
              tenant_it_(shard_guard_->tenants.find(object_id_.tenant_id)),
              tenant_state_(tenant_it_ == shard_guard_->tenants.end()
                                ? nullptr
                                : &tenant_it_->second),
              it_(tenant_state_ == nullptr
                      ? ObjectMetadataIterator{}
                      : tenant_state_->metadata.find(object_id_.user_key)),
              processing_it_(tenant_state_ == nullptr
                                 ? ProcessingIterator{}
                                 : tenant_state_->processing_keys.find(
                                       object_id_.user_key)),
              replication_task_it_(tenant_state_ == nullptr
                                       ? ReplicationTaskIterator{}
                                       : tenant_state_->replication_tasks.find(
                                             object_id_.user_key)) {
            if (tenant_state_ != nullptr) {
                service_->GetBoundTenantQuotaHandle(*tenant_state_);
            }
            // Automatically clean up invalid handles (memory replicas only).
            // Note: We only check memory replicas here to avoid lock order
            // violation (client_mutex_ must be acquired before metadata shard).
            // local_disk replicas are cleaned up by ClearInvalidHandles() in
            // ClientMonitorFunc.
            if (!(service_->enable_ha_ && service_->enable_oplog_) &&
                tenant_state_ != nullptr &&
                it_ != tenant_state_->metadata.end()) {
                // Gate the snapshot on the publisher being live: this runs on
                // every read-write metadata access, and the VisitReplicas walk
                // plus vector allocation is pure overhead when KV events are
                // off (the default).
                const auto previous_kv_media =
                    service_->KvMediaSnapshot(it_->second);
                // Erase invalid memory replicas (those with unmounted
                // segments). No client_mutex_ needed since we only check memory
                // replicas.
                const uint64_t before_charge =
                    service_->CompletedMemoryQuotaCharge(it_->second);
                std::vector<ReplicaID> removed_replica_ids;
                service_->EraseReplicasWithCacheTotalAccounting(
                    it_->second,
                    [](const Replica& replica) {
                        return replica.has_invalid_mem_handle();
                    },
                    &removed_replica_ids);
                service_->CancelPromotionTaskForRemovedReplicas(
                    *tenant_state_, it_->second, removed_replica_ids);
                const uint64_t after_charge =
                    service_->CompletedMemoryQuotaCharge(it_->second);
                if (service_->enable_multi_tenants_ &&
                    before_charge > after_charge) {
                    auto release_result =
                        it_->second.quota_ledger.ReleaseCommitted(
                            service_->GetBoundTenantQuotaHandle(*tenant_state_),
                            before_charge - after_charge);
                    if (!release_result) {
                        LOG(ERROR)
                            << "tenant quota committed release mismatch tenant="
                            << object_id_.tenant_id.value()
                            << ", key=" << object_id_.user_key
                            << ", bytes=" << before_charge - after_charge;
                    }
                }
                service_->SyncKvObjectState(object_id_.user_key, it_->second,
                                            object_id_.tenant_id,
                                            previous_kv_media);
                // If no valid replicas remain, delete the whole object.
                if (!it_->second.IsValid()) {
                    // NOTE: Erase() -> EraseMetadata() already removes the key
                    // from processing_keys (by key), so calling
                    // EraseFromProcessing() here would re-erase the same node
                    // via the now-dangling processing_it_ iterator
                    // (use-after-free, prod segfault 2026-08-03).
                    this->Erase();
                    if (tenant_state_ != nullptr) {
                        service_->ErasePromotionTaskIfPresent(
                            *tenant_state_, object_id_.user_key);
                        MaybeEraseEmptyTenant();
                    }
                }
            }
        }

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_ != nullptr &&
                   it_ != tenant_state_->metadata.end() &&
                   it_->second.IsValid();
        }

        bool InProcessing() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_ != nullptr &&
                   processing_it_ != tenant_state_->processing_keys.end();
        }

        bool HasReplicationTask() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_ != nullptr &&
                   replication_task_it_ !=
                       tenant_state_->replication_tasks.end();
        }

        MetadataShardAccessorRW& GetShard() NO_THREAD_SAFETY_ANALYSIS {
            return shard_guard_;
        }

        TenantState& GetTenantState() NO_THREAD_SAFETY_ANALYSIS {
            EnsureTenantState();
            return *tenant_state_;
        }

        // Get metadata (only call when Exists() is true)
        ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS { return it_->second; }

        ReplicationTask& GetReplicationTask() NO_THREAD_SAFETY_ANALYSIS {
            return replication_task_it_->second;
        }

        // Delete current metadata (for PutRevoke or Remove operations)
        void Erase(const std::vector<std::string>& previous_media_hint = {})
            NO_THREAD_SAFETY_ANALYSIS {
            service_->EraseMetadata(*tenant_state_, it_, object_id_.tenant_id,
                                    QuotaEraseMode::kFull, &shard_guard_,
                                    previous_media_hint);
            it_ = tenant_state_->metadata.end();
            MaybeEraseEmptyTenant();
        }

        void EraseFromProcessing() NO_THREAD_SAFETY_ANALYSIS {
            tenant_state_->processing_keys.erase(processing_it_);
            processing_it_ = tenant_state_->processing_keys.end();
            MaybeEraseEmptyTenant();
        }

        void EraseReplicationTask() NO_THREAD_SAFETY_ANALYSIS {
            tenant_state_->replication_tasks.erase(replication_task_it_);
            replication_task_it_ = tenant_state_->replication_tasks.end();
            MaybeEraseEmptyTenant();
        }

        void Create(const UUID& client_id, uint64_t total_length,
                    std::vector<Replica> replicas, bool enable_hard_pin = false,
                    ObjectDataType data_type = ObjectDataType::UNKNOWN,
                    std::string group_id = "") {
            if (Exists()) {
                throw std::logic_error("Already exists");
            }
            const auto now = std::chrono::system_clock::now();
            EnsureTenantState();
            auto result = tenant_state_->metadata.emplace(
                std::piecewise_construct,
                std::forward_as_tuple(object_id_.user_key),
                std::forward_as_tuple(
                    client_id, now, total_length, std::move(replicas),
                    std::nullopt, enable_hard_pin, data_type, group_id,
                    object_id_.tenant_id, object_id_.user_key));
            it_ = result.first;
        }

       private:
        using ObjectMetadataIterator =
            std::unordered_map<std::string, ObjectMetadata>::iterator;
        using ProcessingIterator = std::unordered_set<std::string>::iterator;
        using ReplicationTaskIterator =
            std::unordered_map<std::string, ReplicationTask>::iterator;

        void EnsureTenantState() NO_THREAD_SAFETY_ANALYSIS {
            if (tenant_state_ != nullptr) {
                return;
            }
            tenant_state_ = &service_->GetOrCreateTenantState(
                shard_guard_.get(), object_id_.tenant_id);
            tenant_it_ = shard_guard_->tenants.find(object_id_.tenant_id);
            it_ = tenant_state_->metadata.end();
            processing_it_ = tenant_state_->processing_keys.end();
            replication_task_it_ = tenant_state_->replication_tasks.end();
        }

        void MaybeEraseEmptyTenant() NO_THREAD_SAFETY_ANALYSIS {
            if (tenant_state_ == nullptr || !tenant_state_->Empty()) {
                return;
            }
            shard_guard_->tenants.erase(tenant_it_);
            tenant_state_ = nullptr;
        }

        MasterService* service_;
        ObjectIdentity object_id_;
        size_t shard_idx_;
        MetadataShardAccessorRW shard_guard_;
        std::unordered_map<TenantId, TenantState, TenantIdHash>::iterator
            tenant_it_;
        TenantState* tenant_state_;
        ObjectMetadataIterator it_;
        ProcessingIterator processing_it_;
        ReplicationTaskIterator replication_task_it_;
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
        MetadataAccessorRO(const MasterService* service,
                           ObjectIdentity object_id)
            : service_(service),
              object_id_(std::move(object_id)),
              shard_idx_(service_->getShardIndex(object_id_.tenant_id,
                                                 object_id_.user_key)),
              shard_guard_(service_, shard_idx_),
              tenant_it_(shard_guard_->tenants.find(object_id_.tenant_id)),
              tenant_state_(tenant_it_ == shard_guard_->tenants.end()
                                ? nullptr
                                : &tenant_it_->second),
              it_(tenant_state_ == nullptr
                      ? ObjectMetadataConstIterator{}
                      : tenant_state_->metadata.find(object_id_.user_key)),
              processing_it_(tenant_state_ == nullptr
                                 ? ProcessingConstIterator{}
                                 : tenant_state_->processing_keys.find(
                                       object_id_.user_key)) {}

        // Check if metadata exists
        bool Exists() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_ != nullptr &&
                   it_ != tenant_state_->metadata.end() &&
                   it_->second.IsValid();
        }

        bool InProcessing() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_ != nullptr &&
                   processing_it_ != tenant_state_->processing_keys.end();
        }

        // Get metadata (only call when Exists() is true)
        const ObjectMetadata& Get() NO_THREAD_SAFETY_ANALYSIS {
            return it_->second;
        }

        MetadataShardAccessorRO& GetShard() NO_THREAD_SAFETY_ANALYSIS {
            return shard_guard_;
        }

        const TenantState* GetTenantState() const NO_THREAD_SAFETY_ANALYSIS {
            return tenant_state_;
        }

       private:
        using ObjectMetadataConstIterator =
            std::unordered_map<std::string, ObjectMetadata>::const_iterator;
        using ProcessingConstIterator =
            std::unordered_set<std::string>::const_iterator;

        const MasterService* service_;
        const ObjectIdentity object_id_;
        const size_t shard_idx_;
        MetadataShardAccessorRO shard_guard_;
        std::unordered_map<TenantId, TenantState, TenantIdHash>::const_iterator
            tenant_it_;
        const TenantState* tenant_state_;
        ObjectMetadataConstIterator it_;
        ProcessingConstIterator processing_it_;
    };

    friend class MetadataAccessorRW;
    friend class MetadataAccessorRO;

    ViewVersionId view_version_;

    // Client related members
    mutable std::shared_mutex client_mutex_;
    std::unordered_set<UUID, boost::hash<UUID>>
        ok_client_;  // client with ok status
    std::unordered_map<UUID, std::string, boost::hash<UUID>> client_host_id_;
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
    const std::chrono::seconds nof_heartbeat_interval_sec_;
    const std::chrono::milliseconds nof_heartbeat_probe_timeout_ms_;
    const uint32_t nof_heartbeat_failures_threshold_;

    struct NoFHeartbeatState {
        UUID owner_client_id{0, 0};
        std::string segment_name;
        std::string te_endpoint;
        std::chrono::steady_clock::time_point next_probe_at{};
        std::chrono::steady_clock::time_point last_success_at{};
        uint32_t consecutive_failures{0};
        std::string last_error_reason;
    };
    std::mutex nof_heartbeat_mutex_;
    std::unordered_map<UUID, NoFHeartbeatState, boost::hash<UUID>>
        nof_heartbeat_states_;
    std::thread nof_heartbeat_thread_;
    std::atomic<bool> nof_heartbeat_running_{false};
    static constexpr uint64_t kNoFHeartbeatThreadSleepMs = 100;
    mutable std::mutex nof_probe_fn_mutex_;
    NoFProbeFn nof_probe_fn_;

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
    // Promotion retry candidate state.
    std::atomic<uint64_t> promotion_candidate_count_{0};
    std::atomic<size_t> promotion_retry_cursor_{0};
    static constexpr size_t kPromotionCandidateLimit = 50000;
    // Retry budget is sized to the condition it waits on: the watermark /
    // queue-cap / push-failure gates clear on the client's offload heartbeat
    // (10s-scale), not in milliseconds. The old budget (8 retries ≈ 2.3s)
    // expired candidates long before their condition could clear, silently
    // killing promotions whose only trigger was a one-off read. 64 retries
    // with a 5s backoff cap spans ~5 minutes (≈ 30 heartbeat ticks); the TTL
    // bounds how long an unread key can keep a slot.
    static constexpr uint32_t kPromotionCandidateMaxRetries = 64;
    static constexpr size_t kPromotionRetryBatchSize = 128;
    static constexpr size_t kPromotionRetryShardBatch = 64;
    static constexpr std::chrono::milliseconds kPromotionCandidateTtl{300000};
    static constexpr std::chrono::milliseconds
        kPromotionCandidateInitialBackoff{10};
    static constexpr std::chrono::milliseconds kPromotionCandidateMaxBackoff{
        5000};
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

    enum class DynamicReplicationMode { kOff, kObserve, kEnforce };
    struct DynamicReplicationWindow {
        std::chrono::steady_clock::time_point window_start{};
        uint32_t hits{0};
    };
    struct DynamicReplicaPlan {
        std::string source_segment;
        std::string target_segment;
        std::string target_domain;
    };

    DynamicReplicationMode dynamic_replication_mode_{
        DynamicReplicationMode::kOff};
    uint32_t dynamic_replication_heat_window_seconds_{10};
    double dynamic_replication_admission_qps_threshold_{0.8};
    size_t dynamic_replication_max_memory_replicas_{2};
    std::mutex dynamic_replication_mutex_;
    std::unordered_map<std::string, DynamicReplicationWindow>
        dynamic_replication_windows_;
    std::deque<std::string> dynamic_replication_window_order_;
    std::chrono::steady_clock::time_point
        dynamic_replication_next_window_cleanup_{};
    std::mutex dynamic_replication_admission_mutex_;
    std::condition_variable dynamic_replication_admission_cv_;
    std::queue<ObjectIdentity> dynamic_replication_admission_queue_;
    std::unordered_set<std::string> dynamic_replication_admission_queued_;
    std::thread dynamic_replication_admission_thread_;
    std::atomic<bool> dynamic_replication_admission_running_{false};
    static constexpr std::chrono::milliseconds
        kDynamicReplicationActionCooldown{30000};
    static constexpr std::chrono::milliseconds kDynamicReplicationLeaseTtl{
        30000};
    static constexpr std::chrono::milliseconds
        kDynamicReplicationRecreateCooldown{60000};
    static constexpr std::chrono::milliseconds
        kDynamicReplicationWindowCleanupInterval{1000};
    static constexpr uint64_t kDynamicReplicationAdmissionThreadSleepMs = 100;
    static constexpr size_t kDynamicReplicationWindowEntryLimit = 50000;
    static constexpr size_t kDynamicReplicationWindowCleanupBudget = 256;
    static constexpr size_t kDynamicReplicationAdmissionQueueLimit = 50000;
    static constexpr size_t kDynamicReplicationAdmissionBatchSize = 64;
    static constexpr double kDynamicReplicationTargetHighWatermark = 0.85;

    bool DynamicReplicationEnabled() const;
    static uint64_t DynamicReplicationStableScore(const std::string& key,
                                                  const std::string& segment);
    bool DynamicReplicationEnforce() const;
    uint32_t DynamicReplicationAdmissionMinHits() const;
    void CleanupDynamicReplicationWindowsLocked(
        std::chrono::steady_clock::time_point now, std::chrono::seconds window);
    bool ObserveDynamicReplicationAccess(const ObjectIdentity& object_id);
    bool DynamicReplicationHeatAdmitted(const ObjectIdentity& object_id);
    void MaybeQueueDynamicReplicaProposal(const ObjectIdentity& object_id);
    void EnqueueDynamicReplicaProposal(const ObjectIdentity& object_id);
    void TrySubmitDynamicReplicaProposal(const ObjectIdentity& object_id);
    tl::expected<ReplicaActionLease, ErrorCode>
    SubmitReplicaActionProposalLocked(const ReplicaActionProposal& proposal);
    uint64_t DynamicReplicationVersionEpoch(
        const ObjectMetadata& metadata) const;
    void ClearDynamicReplicationStateForKey(TenantState& tenant_state,
                                            const std::string& key);
    void CleanupExpiredDynamicReplicationState();
    bool HasDynamicReplicationPending(TenantState& tenant_state,
                                      const std::string& key);
    std::optional<DynamicReplicaPlan> SelectDynamicReplicaPlan(
        const ObjectMetadata& metadata,
        const std::optional<std::string>& preferred_target_segment,
        std::string target_domain);
    tl::expected<UUID, ErrorCode> SubmitDynamicReplicaCopyTask(
        const ObjectIdentity& object_id, const DynamicReplicaPlan& plan,
        const UUID& lease_id, uint64_t version_epoch);
    tl::expected<void, ErrorCode> ValidateDynamicReplicaPendingForCopyStart(
        TenantState& tenant_state, const std::string& key,
        const UUID& dynamic_replication_lease_id, const UUID& client_id,
        const std::string& source_segment, uint64_t current_version_epoch,
        uint64_t dynamic_replication_version_epoch,
        const std::vector<std::string>& target_segments);
    void RegisterDynamicReplicaStart(
        TenantState& tenant_state, ObjectMetadata& metadata,
        const std::string& key, const std::string& source_segment,
        uint64_t version_epoch, const std::vector<std::string>& target_segments,
        const std::vector<ReplicaID>& replica_ids);
    static int64_t DynamicReplicationNowMs();

    const bool enable_oplog_;
    const uint32_t oplog_batch_max_entries_;

    // cluster id for persistent sub directory
    const std::string cluster_id_;
    // root filesystem directory for persistent storage
    const std::string root_fs_dir_;
    // storage backend eviction configuration
    const bool enable_disk_eviction_;
    const uint64_t quota_bytes_;
    const bool enable_multi_tenants_;
    std::unique_ptr<TenantQuotaPolicyStore> tenant_quota_policy_store_;
    mutable std::mutex tenant_quota_policy_mutex_;
    mutable std::mutex tenant_quota_recompute_mutex_;
    ShardedTenantQuotaTable<1024> tenant_quota_table_;

    // HTTP metadata server pointer for cleanup on client timeout
    // nullptr means cleanup is disabled
    HttpMetadataServer* http_metadata_server_{nullptr};

    // Remote HTTP metadata client, used when the metadata server is deployed
    // separately. nullptr = no remote cleanup (co-located prefers the pointer).
    std::shared_ptr<MetadataStoragePlugin> http_metadata_remote_;

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

    // Clean up HTTP metadata (mooncake/ram/*, mooncake/rpc_meta/*) for a
    // segment. For the co-located case this is synchronous (no network I/O);
    // for the remote case it enqueues to the async cleanup worker.
    void cleanupHttpMetadata(const std::string& segment_name);

    bool use_disk_replica_{false};
    bool enable_dfs_{false};
    std::unique_ptr<GlobalAllocatorInterface> dfs_allocator_;
    ImmutableBucketAllocator* bucket_allocator_{nullptr};

    // Segment management
    SegmentManager segment_manager_;
    LocalSsdManager local_ssd_manager_;
    NoFSegmentManager nof_segment_manager_;
    BufferAllocatorType memory_allocator_type_;
    const AllocationStrategyType allocation_strategy_type_;
    std::shared_ptr<AllocationStrategy> allocation_strategy_;

    std::unique_ptr<SnapshotObjectStore> snapshot_object_store_;
    std::unique_ptr<ha::SnapshotCatalogStore> snapshot_catalog_store_;
    std::unique_ptr<MasterSnapshotRepository> snapshot_repository_;
    std::unique_ptr<ha::MasterSnapshotCodec> snapshot_codec_;
    mutable std::shared_mutex snapshot_mutex_;

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
        friend class MetadataSerializer;
        std::vector<Replica> replicas_;
        std::chrono::system_clock::time_point ttl_;
        uint64_t mem_size_;
    };
    std::mutex discarded_replicas_mutex_;
    std::list<DiscardedReplicas> discarded_replicas_
        GUARDED_BY(discarded_replicas_mutex_);
    size_t offloading_queue_limit_ = 50000;
    double offload_cap_ratio_ = 0.5;

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

    // RemoveAll releases each shard lock before moving to the next one, so a
    // concurrent commit can land in an already-scanned shard. Publishing
    // `cleared` from the scan's own bookkeeping would then order it after that
    // commit's `stored` and tell subscribers to drop a live object. Every
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
    // Fires after each shard's lock is released during a RemoveAll scan, which
    // is the only point where a test can commit into an already-scanned shard.
    std::function<void(size_t)> kv_remove_all_shard_hook_;

    static size_t KvTenantEpochSlot(const std::string& tenant) {
        return std::hash<std::string>{}(tenant) % kKvTenantEpochSlots;
    }
    // Called while the object's shard lock is held, before the `stored` is
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
    std::string SerializeMetadataForOpLogWithoutMemReplicas(
        const ObjectMetadata& metadata) const;
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
    tl::expected<OrderedOpLogWriter::Reservation, ErrorCode>
    ReserveBatchOpLogSlot();
    tl::expected<OpLogEntry, ErrorCode> AppendReservedOpLogWithDurableFinalize(
        OrderedOpLogWriter::Reservation&& reservation, OpType type,
        const std::string& tenant_id, const std::string& key,
        const std::string& payload, DurableFinalizeCallback callback);

    // Invalid endpoints from standby that don't exist locally
    std::unordered_set<std::string> invalid_replica_endpoints_;

    // Keep DummyBufferAllocator alive after standby restore.
    // Key: transport_endpoint, Value: allocator.
    std::unordered_map<std::string, std::shared_ptr<BufferAllocatorBase>>
        standby_allocator_keepalive_;
    std::vector<StandbySegmentInfo> standby_memory_segments_;
    std::unordered_map<std::string, uint64_t> standby_accounted_memory_bytes_;

    ErrorCode ValidateStandbyRemountSegment(const Segment& segment) const;

    bool IsReplicaReadable(const Replica& replica) const;
    bool HasReadableReplica(const ObjectMetadata& metadata) const;
    bool IsEvictableMemoryReplica(const Replica& replica) const;

    /**
     * Segment lifecycle persist helper. Tries to durably persist the
     * SEGMENT_MOUNT / SEGMENT_UNMOUNT entry up-front; on failure enqueues
     * the same OpLogEntry (with its already-allocated sequence_id) for
     * background retry so the standby segment registry eventually
     * converges. Suitable for paths where the local segment commit has
     * already happened (UnmountSegment) and rolling back is impossible.
     */
    void PersistSegmentOpForHAOrEnqueue(const char* why, OpType type,
                                        const std::string& key,
                                        const std::string& payload);
    void PersistSegmentOpForHAOrEnqueue(const char* why, OpType type,
                                        const TenantId& tenant_id,
                                        const std::string& key,
                                        const std::string& payload);

    /**
     * Helper to persist REMOVE OpLog for a key with strong-consistency.
     * @return OK on success, error on persist failure (caller must skip erase)
     */
    tl::expected<void, ErrorCode> PersistRemoveForHA(const char* why,
                                                     const std::string& key);
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
