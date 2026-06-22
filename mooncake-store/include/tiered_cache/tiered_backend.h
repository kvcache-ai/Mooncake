#pragma once

#include <atomic>
#include <string>
#include <string_view>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <functional>
#include <json/value.h>

#include "tiered_cache/tiers/cache_tier.h"
#include "tiered_cache/data_copier.h"
#include "tiered_cache/scheduler/stats_collector.h"
#include "rpc_types.h"
#include "utils.h"

namespace mooncake {

class TieredBackend;     // Forward declaration
class IClientScheduler;  // Forward declaration

/**
 * @struct TieredLocation
 * @brief Describes the physical location of a segment within the tiered
 * storage.
 */
struct TieredLocation {
    std::shared_ptr<CacheTier> tier;
    struct DataSource data;
};

/**
 * @struct TierView
 * @brief A snapshot of a tier's status, used for reporting topology to the
 * Master.
 */
struct TierView {
    UUID id;
    MemoryType type;
    size_t capacity;
    size_t usage;
    size_t free_space;
    int priority;
    std::vector<std::string> tags;
};

/**
 * @struct AllocationEntry
 * @brief The internal state of an allocation.
 * acts as the "Control Block" for the resource.
 * When the last shared_ptr pointing to this entry dies, the destructor
 * releases the resource through the owning tier.
 */
struct AllocationEntry {
    TieredBackend* backend;
    TieredLocation loc;

    AllocationEntry(TieredBackend* b, TieredLocation&& l)
        : backend(b), loc(std::move(l)) {}
    AllocationEntry(const AllocationEntry&) = delete;
    AllocationEntry& operator=(const AllocationEntry&) = delete;

    // Destructor: Automatically releases the resource if valid
    ~AllocationEntry();
};

/**
 * @typedef AllocationHandle
 * @brief A reference-counted handle to a storage resource.
 */
using AllocationHandle = std::shared_ptr<AllocationEntry>;

/**
 * @brief Callback for metadata synchronization when a replica is added.
 * Invoked after data copy is complete.
 * Returns true if sync succeeds, false otherwise.
 */
using AddReplicaCallback = std::function<tl::expected<void, ErrorCode>(
    std::string_view key, const UUID& tier_id, size_t size)>;

/**
 * @brief Callback for metadata synchronization when a replica is removed.
 * Returns OK on success.
 */
using RemoveReplicaCallback = std::function<tl::expected<void, ErrorCode>(
    std::string_view key, const UUID& tier_id)>;

/**
 * @brief Result of TieredBackend::conditionalExecute.
 */
template <typename T>
struct ConditionalExecuteResult {
    bool key_exists = false;
    T callback_result{};
};

template <>
struct ConditionalExecuteResult<void> {
    bool key_exists = false;
};

/**
 * @brief Callback for segment lifecycle synchronization.
 * Invoked when a tier is created (mount=true) or destroyed (mount=false).
 * The callback should register/unregister the segment with Master.
 */
using SegmentSyncCallback = std::function<tl::expected<void, ErrorCode>(
    const Segment& segment, bool mount)>;

/**
 * @class TieredBackend
 * @brief Data plane management class supporting tiered storage with RAII-based
 * resource management.
 */
class TieredBackend {
   public:
    static constexpr size_t kDefaultMetadataShardCount = 64;

    /**
     * @param metadata_shard_count Number of metadata index shards. Values <= 0
     *        use the default (64).
     */
    explicit TieredBackend(
        size_t metadata_shard_count = kDefaultMetadataShardCount);
    ~TieredBackend();

    /**
     * @brief 1. stops any backend thread;
     *        2. all public APIs will return SHUTTING_DOWN.
     */
    void Stop();

    /**
     * @brief Unmounts segments from Master and cleans up resources.
     */
    void Destroy();

    tl::expected<void, ErrorCode> Init(
        Json::Value root, TransferEngine* engine,
        AddReplicaCallback add_replica_callback,
        RemoveReplicaCallback remove_replica_callback,
        SegmentSyncCallback segment_sync_callback);

    // --- Client-Centric Operations ---
    // All the following operations are designed for Client-Centric, Client
    // should manage the resource by itself, and synchronize with Master when
    // needed.

    /**
     * @brief Allocation
     * reserves storage space. Returns a handle.
     * If the handle goes out of scope without being committed, the space is
     * auto-freed.
     * @param size: Size in bytes to allocate
     * @param preferred_tier: Preferred tier ID (optional)
     * @param strict: If true, allocation MUST succeed on preferred_tier.
     *                Will trigger sync eviction if needed, no fallback.
     *                If false (default), will fallback to other tiers.
     */
    tl::expected<AllocationHandle, ErrorCode> Allocate(
        size_t size, std::optional<UUID> preferred_tier = std::nullopt,
        bool strict = false);

    /**
     * @brief Execution (Write)
     * Writes data to the location specified by the handle.
     */
    tl::expected<void, ErrorCode> Write(const DataSource& source,
                                        AllocationHandle handle);

    /**
     * @brief Commit (Register)
     * Registers the handle in the local metadata index.
     * @param expected_version: Optimistic Concurrency Control.
     * If set, commit only if current version matches expected_version.
     * Returns CAS_FAILED if mismatch.
     */
    tl::expected<void, ErrorCode> Commit(
        std::string_view key, AllocationHandle handle,
        std::optional<uint64_t> expected_version = std::nullopt,
        bool record_access = true);

    /**
     * @brief Checks if a key exists in the backend.
     * @param key The key to check.
     * @param tier_id Optional tier ID. If specified, checks only the given
     *        tier; if nullopt, checks any tier.
     */
    bool Exist(std::string_view key,
               std::optional<UUID> tier_id = std::nullopt) const;

    /**
     * @brief Checks key existence under the metadata shard lock, then runs
     *        exactly one callback.
     * @return key_exists and the return value of the invoked callback.
     */
    template <typename R>
    ConditionalExecuteResult<R> conditionalExecute(
        std::string_view key, std::optional<UUID> tier_id,
        std::function<R()> on_exists, std::function<R()> on_not_exists) const;

    ConditionalExecuteResult<void> conditionalExecute(
        std::string_view key, std::optional<UUID> tier_id,
        std::function<void()> on_exists,
        std::function<void()> on_not_exists) const;

    /**
     * @brief Get
     * Returns a handle.
     * @param out_version: If provided, returns the current version of the
     * metadata entry.
     */
    tl::expected<AllocationHandle, ErrorCode> Get(
        std::string_view key, std::optional<UUID> tier_id = std::nullopt,
        bool record_access = true, uint64_t* out_version = nullptr);

    /**
     * @brief Delete
     * Removes the key from the metadata index.
     * @param tier_id: If specified, removes only the replica on that tier.
     * If nullopt, removes ALL replicas for this key (and the key entry itself).
     */
    tl::expected<void, ErrorCode> Delete(
        std::string_view key, std::optional<UUID> tier_id = std::nullopt,
        bool notify_master = true);

    /**
     * @brief Reverse-notification hook for bottom-up (tier-initiated) eviction.
     *
     * When a tier autonomously evicts a bucket to reclaim space, it physically
     * removes the underlying files for a batch of keys, bypassing the normal
     * top-down Delete path. This method re-synchronizes the high-level state
     * with that fact. For each evicted key whose replica lives on @p tier_id:
     *   - removes that replica from the metadata index, releasing the owning
     *     AllocationHandle OUTSIDE all locks so the tier's RAII Free path runs
     *     exactly once (the single, symmetric per-key byte decrement);
     *   - notifies the scheduler via OnDelete so LRU / fast-reclaim no longer
     *     believe a backup exists on the evicted tier;
     *   - best-effort notifies Master via the remove-replica callback.
     *
     * The physical eviction is irreversible, so this cleanup always runs to
     * completion (it is never gated on shutdown or Master reachability).
     *
     * @param keys    Keys that lived in the evicted bucket (snapshot taken
     *                before the physical eviction).
     * @param tier_id The tier that performed the eviction.
     * @return Number of replicas actually removed from the metadata index.
     */
    size_t NotifyBucketEviction(const std::vector<std::string>& keys,
                                const UUID& tier_id);

    /**
     * @brief Remove ALL keys (and all replicas) from the local tiered storage.
     * @return Number of distinct keys removed, or ErrorCode on shutdown.
     */
    tl::expected<long, ErrorCode> RemoveAll();

    // --- Composite Operations ---

    // Allocates strictly on dest_tier_id (triggering its sync eviction on a
    // full tier; never falls back to another tier), so a successful return
    // guarantees the new replica landed on dest_tier_id.
    tl::expected<void, ErrorCode> CopyData(
        std::string_view key, const DataSource& source, UUID dest_tier_id,
        std::optional<uint64_t> expected_version = std::nullopt,
        bool record_access = true);

    tl::expected<void, ErrorCode> Transfer(std::string_view key,
                                           UUID source_tier_id,
                                           UUID dest_tier_id,
                                           bool record_access = true);

    // --- Introspection & Internal ---

    std::vector<TierView> GetTierViews() const;
    std::vector<UUID> GetReplicaTierIds(std::string_view key) const;
    const CacheTier* GetTier(UUID tier_id) const;

    /**
     * @brief Id of the DRAM (fast) tier, for DRAM-only local writes.
     * @return The highest-priority MemoryType::DRAM tier (which coincides with
     *         the event-driven 'fast' role when that role is DRAM), or nullopt
     *         if the deployment has no DRAM tier — callers must then fall back
     *         to best-effort allocation.
     */
    std::optional<UUID> GetDramTierId() const;
    const DataCopier& GetDataCopier() const;

    /**
     * @brief Iterate all keys in batches.
     * Iterates per-shard to minimize lock hold time.
     * @param callback Receives each batch; return false to stop iteration.
     */
    void ForEachKeyBatch(
        const std::function<bool(std::vector<ReplicaLocation>&&)>& callback)
        const;

    /**
     * @brief Get hot key statistics from the scheduler (e.g. for HA recovery
     *        prioritization).
     * @param hot_key_num see IClientScheduler::GetHotKeyStats. Defaults to
     *        nullopt, which resolves to the startup config scheduler.hot_key_num
     *        (default 64); set scheduler.hot_key_num=0 to return all tracked
     *        keys (the historical no-arg behavior).
     */
    AccessStats GetHotKeyStats(
        std::optional<size_t> hot_key_num = std::nullopt) const;

   private:
    friend class TieredBackendTest;  // for unit tests

    tl::expected<void, ErrorCode> MountSegment(
        UUID id, size_t capacity, int priority,
        const std::vector<std::string>& tags, MemoryType memory_type);

    struct TierInfo {
        int priority;
        std::vector<std::string> tags;
    };

    /**
     * @struct MetadataEntry
     * @brief Holds all replicas for a specific key.
     * Uses a dedicated mutex to allow per-key concurrency.
     */
    struct MetadataEntry {
        mutable std::shared_mutex mutex;  // Entry-level lock
        std::vector<std::pair<UUID, AllocationHandle>>
            replicas;          // tier_id -> handle
        uint64_t version = 0;  // Monotonically increasing version
    };

    // Get list of Tier IDs sorted by priority (descending)
    std::vector<UUID> GetSortedTiers() const;

    // Low-level allocation logic
    tl::expected<void, ErrorCode> AllocateInternalRaw(
        size_t size, std::optional<UUID> preferred_tier,
        TieredLocation* out_loc);

   private:
    // Map from tier ID to the actual CacheTier instance.
    std::unordered_map<UUID, std::shared_ptr<CacheTier>> tiers_;

    // Map from tier ID to static config info
    std::unordered_map<UUID, TierInfo> tier_info_;

    // Sharded Metadata Index: Key -> Entry
    // Each shard has its own mutex for fine-grained locking.
    struct MetadataShard {
        mutable std::shared_mutex mutex;
        std::unordered_map<std::string, std::shared_ptr<MetadataEntry>,
                           StringHash, std::equal_to<>>
            index;
    };

    size_t metadata_shard_count_ = kDefaultMetadataShardCount;
    std::vector<std::unique_ptr<MetadataShard>> metadata_shards_;

    MetadataShard& GetMetadataShard(std::string_view key) {
        return *metadata_shards_[std::hash<std::string_view>{}(key) %
                                 metadata_shard_count_];
    }
    const MetadataShard& GetMetadataShard(std::string_view key) const {
        return *metadata_shards_[std::hash<std::string_view>{}(key) %
                                 metadata_shard_count_];
    }

    static bool InnerExist(const MetadataShard& shard, std::string_view key,
                           std::optional<UUID> tier_id);

    std::unique_ptr<DataCopier> data_copier_;
    // Callbacks for metadata synchronization with Master
    AddReplicaCallback add_replica_callback_;
    RemoveReplicaCallback remove_replica_callback_;
    // Callback for segment lifecycle synchronization with Master
    SegmentSyncCallback segment_sync_callback_;

    // Scheduler
    std::unique_ptr<IClientScheduler> scheduler_;

    // Shutdown flag — once set, all public APIs reject new requests.
    std::atomic<bool> is_shutting_down_{false};

    // Destroy flag
    std::atomic<bool> is_destroyed_{false};
};

template <typename R>
ConditionalExecuteResult<R> TieredBackend::conditionalExecute(
    std::string_view key, std::optional<UUID> tier_id,
    std::function<R()> on_exists, std::function<R()> on_not_exists) const {
    auto& shard = GetMetadataShard(key);
    std::shared_lock<std::shared_mutex> read_lock(shard.mutex);
    const bool exists = InnerExist(shard, key, tier_id);

    ConditionalExecuteResult<R> result;
    result.key_exists = exists;
    if (exists) {
        result.callback_result = on_exists();
    } else {
        result.callback_result = on_not_exists();
    }
    return result;
}

}  // namespace mooncake
