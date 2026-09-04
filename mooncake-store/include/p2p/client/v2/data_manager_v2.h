#pragma once

// DataManagerV2: the orchestration layer.
//
// It implements the DataManager interface by coordinating the BlockRegistry,
// the logical tilers, the lease tables, transfers and shutdown. It contains no
// allocation algorithm, no hardware selection and no eviction policy of its
// own -- those live in BlockPool, TilerManager and the policy engine.
//
// Two structural rules from the design are load-bearing here:
//
//  - A LifecycleGate::Guard travels with the TaskHandle, not with the API call
//    stack, because Put/Get do their real work inside Wait() on the caller's
//    thread (section 5.2).
//  - Movement workers belong to DataManagerV2, not to the EventCenter. They
//    call the migration and eviction engines, so if the EventCenter owned them
//    those engines would be destroyed first in member order and a destructor
//    that skipped Stop() would be a use-after-free.

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <json/value.h>
#include <ylt/coro_io/io_context_pool.hpp>
#include <ylt/util/tl/expected.hpp>

#include "p2p/client/data_manager.h"
#include "p2p/client/data_manager_test_hook.h"
#include "p2p/client/data_manager_types.h"
#include "p2p/client/v2/block_index.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/copier.h"
#include "p2p/client/v2/evict_engine.h"
#include "p2p/client/v2/event_center.h"
#include "p2p/client/v2/frequency_tracker.h"
#include "p2p/client/v2/lease_manager.h"
#include "p2p/client/v2/migration_engine.h"
#include "p2p/client/v2/local_copy_engine.h"
#include "p2p/client/v2/movement_consumers.h"
#include "p2p/client/v2/tier_graph.h"
#include "p2p/client/v2/tiler_manager.h"
#include "p2p/client/v2/transfer_coordinator.h"
#include "p2p/client/v2/v2_common.h"
#include "transfer_engine.h"
#include "types.h"

namespace mooncake {
struct TierMetric;
struct KeyRetentionMetric;
}  // namespace mooncake

namespace mooncake::v2 {

/**
 * @struct DataManagerV2Config
 */
struct DataManagerV2Config {
    BlockRegistryConfig registry;
    BlockIndexConfig block_index;
    FrequencyTrackerConfig frequency_tracker;
    EventCenterConfig events;
    MovementConsumerConfig movement;
    TierPlacementPolicyConfig tier_placement;
    AllocationFailurePolicyConfig allocation_failure;
    LocalTransferConfig local_transfer;
    CopierConfig copier;
    MovementTrackerConfig movement_tracker;
    MigrationSchedulerConfig migration;
    KeyLeaseConfig key_lease;
    std::vector<TilerConfig> tilers;

    // Bound on the "take handle -> take guard -> found retired -> retry" loop.
    // Sustained put/delete churn on one key would otherwise livelock it.
    uint32_t max_registration_retry = 8;

    // Upper bound on how long Stop() waits for in-flight guards. Exceeding it
    // is not an error: Stop switches to Cancel and keeps going, because a
    // caller may simply never call Wait() on a handle it still holds.
    std::chrono::milliseconds stop_drain_timeout{5000};

    // Shard count for the lease tables.
    size_t lease_shard_count = 64;

    // Threads consuming the movement queue. They belong to DataManagerV2, not
    // to the EventCenter: they call the migration and eviction engines, so
    // owning them here is what makes the member destruction order safe.
    size_t movement_worker_count = 2;

    // Register pool memory with the TransferEngine. See the identically named
    // field on DataManagerConfig: production leaves it true, and only a
    // local-only test with an un-initialized TransferEngine sets it false.
    bool register_tiers_with_transfer_engine = true;
};

/**
 * @brief Build a V2 configuration from the same `tiers[]` JSON that V1 uses.
 *
 * Every entry in `tiers[]` becomes one independent logical tiler with its own
 * UUID, priority and Master segment, matching V1 one-for-one. Entries are not
 * merged by type: merging would change the topology reported to Master.
 *
 * An ASCEND_NPU tier is an explicit error rather than a skipped entry. Skipping
 * would leave the operator believing data lives on the NPU while it silently
 * landed in DRAM.
 */
tl::expected<DataManagerV2Config, ErrorCode> ParseDataManagerV2Config(
    const Json::Value& tier_config, const LocalTransferConfig& local_transfer,
    const KeyLeaseConfig& key_lease);

/**
 * @brief Validate a configuration however it was built.
 *
 * Called by ParseDataManagerV2Config, and again by Init(). The second call is
 * the point: a configuration assembled in C++ rather than parsed from JSON --
 * every component test, and any embedder -- used to reach Init completely
 * unchecked, so a zero shard count or an out-of-range watermark became a
 * division by zero or a silently disabled policy at run time instead of a
 * refusal at start-up.
 */
tl::expected<void, ErrorCode> ValidateDataManagerV2Config(
    const DataManagerV2Config& config);

/**
 * @struct DataManagerV2Metrics
 * @brief The counters section 11 item 7 requires beyond the shared tier
 *        metrics. Each answers a question an operator has during a rollout.
 */
struct DataManagerV2Metrics {
    /** Rising means the event queue is saturated: the policy is still correct
     *  but is no longer producing movement commands for those events. */
    uint64_t lifecycle_event_inline_applied = 0;
    /** Non-zero means a key is being put and deleted fast enough to starve
     *  its own registration; those Puts failed with INTERNAL_ERROR. */
    uint64_t registration_retry_exhausted = 0;
    /** Non-zero means Stop() gave up waiting and cancelled: some caller was
     *  holding a TaskHandle it never waited on. */
    uint64_t stop_drain_timeout_hit = 0;
    /** Rising means RectifyReadRoute asked Master to drop replicas that were
     *  in fact live -- the accepted false-positive risk of section 7.3 turning
     *  into real damage. */
    uint64_t rectify_false_positive_suspected = 0;
    /**
     * Objects destroyed by reclamation because they existed on only one tier.
     *
     * Reclamation is tier-local by design (section 4.1): it does not demote and
     * does not wait for an offload, so an object the offload pipeline has not
     * copied down yet is lost when its tier fills. This is the number that says
     * how often that is actually happening. Non-zero means offload is not
     * keeping ahead of the write rate, or its threshold sits too close to the
     * point where reclamation starts.
     */
    uint64_t reclaim_destroyed_sole_replica = 0;
    /**
     * Movement commands the queue refused. With durability delegated to the
     * offload pipeline, a dropped offload command is a step towards the loss
     * above rather than a missed optimisation.
     */
    uint64_t movement_commands_dropped = 0;
};

/**
 * @class DataManagerV2
 */
class DataManagerV2 final : public DataManager, public DataManagerTestHook {
   public:
    DataManagerV2(
        const DataManagerV2Config& config,
        std::shared_ptr<TransferEngine> transfer_engine,
        MetadataCallbacks callbacks,
        std::shared_ptr<TierMetric> tier_metric = nullptr,
        std::shared_ptr<KeyRetentionMetric> key_retention = nullptr,
        std::shared_ptr<Clock> clock = std::make_shared<SteadyClock>());

    /** Unconditionally calls Stop(), then destroys members in reverse order. */
    ~DataManagerV2() override;

    /** Builds tilers and pools. Must succeed before any API call. */
    tl::expected<void, ErrorCode> Init();

    // --- DataManager ---
    void Stop() override;
    void Destroy() override;

    tl::expected<std::unique_ptr<TaskHandle<void>>, ErrorCode> Put(
        std::string_view key, std::vector<Slice>& slices) override;
    tl::expected<ReadTaskHandle, ErrorCode> Get(
        std::string_view key, const std::vector<Slice>& slices) override;
    tl::expected<ReadTaskHandle, ErrorCode> Get(
        std::string_view key,
        std::shared_ptr<ClientBufferAllocator> allocator) override;

    tl::expected<std::pair<UUID, uint64_t>, ErrorCode> Query(
        std::string_view key) override;
    tl::expected<size_t, ErrorCode> QueryObjectSize(
        std::string_view key) override;
    bool Exist(std::string_view key,
               std::optional<UUID> tier_id = std::nullopt) const override;
    tl::expected<void, ErrorCode> Delete(
        std::string_view key, std::optional<UUID> tier_id = std::nullopt,
        bool notify_master = true) override;
    tl::expected<long, ErrorCode> RemoveAll() override;

    std::vector<TierView> GetTierViews() const override;
    std::vector<UUID> GetReplicaTierIds(std::string_view key) const override;
    void ForEachKeyBatch(
        const std::function<bool(std::vector<ReplicaLocation>&&)>& callback)
        const override;
    AccessStats GetHotKeyStats(
        std::optional<size_t> hot_key_num = std::nullopt) const override;

    async_simple::coro::Lazy<tl::expected<void, ErrorCode>> ReadRemoteDataAsync(
        std::string_view key,
        const std::vector<RemoteBufferDesc>& dest_buffers) override;
    async_simple::coro::Lazy<tl::expected<UUID, ErrorCode>>
    WriteRemoteDataAsync(std::string_view key,
                         const std::vector<RemoteBufferDesc>& src_buffers,
                         std::optional<UUID> tier_id = std::nullopt) override;

    tl::expected<PreWriteResponse, ErrorCode> PreWrite(
        std::string_view key, size_t size_bytes,
        std::optional<UUID> tier_id = std::nullopt) override;
    tl::expected<void, ErrorCode> WriteCommit(
        std::string_view key, const UUID& write_operation_id) override;
    tl::expected<void, ErrorCode> WriteRevoke(
        std::string_view key, const UUID& write_operation_id) override;
    tl::expected<PinKeyResponse, ErrorCode> PinKey(
        std::string_view key,
        std::optional<UUID> tier_id = std::nullopt) override;
    tl::expected<void, ErrorCode> UnPinKey(
        std::string_view key, const UUID& read_operation_id) override;

    async_simple::Future<tl::expected<void, ErrorCode>> TransferDataAsync(
        void* local_transfer_base, size_t total_size,
        const std::vector<RemoteBufferDesc>& peer_buffers,
        Transport::TransferRequest::OpCode opcode) override;
    async_simple::Executor* GetCoroExecutor() const override;

    void RectifyReadRoute(std::string_view key,
                          std::optional<UUID> tier_id = std::nullopt) override;
    void SetRectifyCallback(RectifyRouteCallback fn) override;

    /** Snapshot of the V2-specific counters. */
    DataManagerV2Metrics Metrics() const;

    /** Hot-key snapshots that returned a partial answer because of the cap. */
    uint64_t HotKeyTruncationCount() const;

    // --- DataManagerTestHook ---
    void DrainForTest() override;

   private:
    struct ReplicaSite {
        TilerManager* tiler = nullptr;
        ImmutableBlock block;
    };

    /**
     * @brief Allocate on one tiler, applying that tiler's allocation-failure
     *        policy. Never falls back to another tiler: a request-path source
     *        that cannot be served from a TE-addressable tiler fails with
     *        NO_AVAILABLE_HANDLE rather than quietly landing on a slow tier.
     */
    tl::expected<MutableBlock, ErrorCode> AllocateWithPolicy(
        TilerManager& tiler, size_t size_bytes, size_t alignment,
        AllocationSource source);

    AllocationFailurePolicyConfig AllocationPolicyFor(
        const UUID& tiler_id) const;

    /** Candidate tilers for `source`, in placement order. */
    std::vector<TilerManager*> CandidateTilers(AllocationSource source) const;

    /** Exact lookup across tilers, in priority order. */
    std::optional<ReplicaSite> FindReplica(
        const BlockRegistrationHandle& registration,
        std::optional<UUID> tier_id) const;

    /**
     * @brief Size of any exact replica of `key`, without touching the access
     *        statistics.
     *
     * Get(key, allocator) needs the size before it can size its buffer, and it
     * then delegates to the slice-based Get, which records the access itself.
     * Going through the public QueryObjectSize would count that one read
     * twice.
     */
    tl::expected<size_t, ErrorCode> LookupReplicaSize(
        std::string_view key) const;

    /** Commit a completed block under a fresh canonical registration. */
    tl::expected<void, ErrorCode> CommitBlock(const std::string& key,
                                              TilerManager& tiler,
                                              CompletedBlock&& block,
                                              size_t size_bytes);

    /**
     * @brief Resolve the tiler a forward write should land on.
     *
     * An explicitly requested tiler that cannot hand out a TransferEngine
     * address is refused rather than silently redirected: the caller is about
     * to publish that address to a peer, so quietly substituting a different
     * tiler would return an address for storage the caller did not ask for.
     */
    tl::expected<TilerManager*, ErrorCode> ResolveForwardWriteTiler(
        std::optional<UUID> tier_id) const;

    /** Build the descriptor a peer will RDMA into or out of. */
    tl::expected<RemoteBufferDesc, ErrorCode> MakeRemoteBufferDesc(
        const std::optional<TransferAddress>& address) const;

    /**
     * @struct RemoteReadPlan
     * @brief Everything a reverse read needs to keep alive while the transfer
     *        runs: the lifecycle guard, the source snapshot, and -- when the
     *        source cannot be addressed directly -- the staging block its
     *        bytes were copied into.
     */
    struct RemoteReadPlan {
        LifecycleGate::Guard guard;
        ImmutableBlock source;
        MutableBlock staging;
        void* base = nullptr;
        size_t size = 0;
    };

    /** Resolve the source and, if needed, stage it into addressable memory. */
    tl::expected<RemoteReadPlan, ErrorCode> PrepareRemoteRead(
        std::string_view key);

    void NotifyAddReplica(const std::string& key, const UUID& tier_id,
                          size_t size);
    void NotifyRemoveReplica(const std::string& key, const UUID& tier_id);

    /** Consumes the movement queue until the EventCenter closes it. */
    void MovementWorkerMain();

    /** Remember that rectify just asked Master to drop `key`. */
    void NoteRectifyWitness(std::string_view key) const;
    /** Count a commit of a key rectify recently dropped as a false positive. */
    void NoteCommitForRectifyWitness(std::string_view key) const;

    DataManagerV2Config config_;
    std::shared_ptr<Clock> clock_;
    /**
     * Thread-local because reclaim recursion is a property of one call stack,
     * not of the manager: a demotion allocates on the slower tier from inside
     * an allocation, and only that thread must be stopped from reclaiming
     * again.
     */
    static thread_local bool reclaiming_;
    // Held by pointer so an outstanding Guard -- which Stop() explicitly
    // allows to survive this object -- keeps the gate alive.
    std::shared_ptr<LifecycleGate> lifecycle_ =
        std::make_shared<LifecycleGate>();
    std::atomic<uint64_t> access_tick_{0};

    BlockRegistry block_registry_;
    MultiTiler tilers_;
    std::unique_ptr<LeaseManager> leases_;
    // Shared with the placement policy so hot-key reporting has exactly one
    // source of truth.
    std::shared_ptr<FrequencyTracker> frequency_tracker_;
    std::unique_ptr<AllocationFailureMetrics> allocation_failure_metrics_;

    std::shared_ptr<TransferEngine> transfer_engine_;
    std::shared_ptr<coro_io::io_context_pool> coro_executor_pool_;
    std::shared_ptr<coro_io::io_context_pool> te_wait_pool_;
    std::unique_ptr<LocalCopyEngine> local_copy_engine_;
    std::unique_ptr<TransferCoordinator> transfer_coordinator_;

    MetadataCallbacks metadata_callbacks_;
    mutable std::mutex rectify_mu_;
    RectifyRouteCallback rectify_callback_;

    std::shared_ptr<TierMetric> tier_metric_;
    std::shared_ptr<KeyRetentionMetric> key_retention_metric_;

    std::atomic<uint64_t> registration_retry_exhausted_{0};
    std::atomic<uint64_t> stop_drain_timeout_hit_{0};
    // Mutable: the witness is updated from RectifyReadRoute, which is const on
    // neither path, and from the commit path via a const helper.
    mutable std::atomic<uint64_t> rectify_false_positive_suspected_{0};
    /**
     * Keys RectifyReadRoute recently asked Master to drop. A commit of one of
     * them means the miss was a false positive. Bounded and sampled rather
     * than exact: it must not put a lock on the commit path, and section 7.3
     * only promises the risk is observable, not measured precisely.
     */
    static constexpr size_t kRectifyWitnessSlots = 256;
    mutable std::array<std::atomic<uint64_t>, kRectifyWitnessSlots>
        rectify_witness_{};

    bool initialized_ = false;

    // Declaration order is destruction order reversed, and it is deliberate:
    // movement_workers_ is destroyed first, so the threads are joined before
    // the engines, the EventCenter and the policy they call into disappear.
    // If the EventCenter owned the workers instead, a destructor that skipped
    // Stop() would be a use-after-free.
    std::shared_ptr<const TierGraph> tier_graph_;
    std::unique_ptr<TierPlacementPolicy> tier_placement_;
    std::unique_ptr<MovementTracker> movement_tracker_;
    // Declared after everything they borrow and before the center they
    // register with, so they outlive neither.
    std::unique_ptr<EventConsumer> offload_consumer_;
    std::unique_ptr<EventConsumer> onboard_consumer_;
    std::shared_ptr<EventCenter> event_center_;
    std::unique_ptr<MigrationEngine> migration_engine_;
    std::unique_ptr<EvictEngine> evict_engine_;
    std::vector<std::thread> movement_workers_;
};

}  // namespace mooncake::v2
