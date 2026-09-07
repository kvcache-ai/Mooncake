#pragma once

// TilerManager: one logical tier.
//
// A tiler is a logical layer, not a device. It knows its own UUID, its sharded
// BlockIndex, a BlockPool interface, the BlockRegistry and its own policy
// inputs. It does not know about NUMA nodes, DRAM regions, NVMe disks, files,
// file descriptors, offset allocators or TransferEngine registrations -- all of
// that is inside the injected pool.
//
// Identity is the UUID alone. MemoryType is demoted to a display field on
// TierView and a capability label, and must not be used for indexing, routing
// or branching: a deployment may legitimately configure several DRAM tiers
// (different NUMA nodes, different allocators, different priorities), each an
// independent logical layer and an independent Master segment.

#include <cstddef>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <boost/functional/hash.hpp>
#include <ylt/util/tl/expected.hpp>

#include "p2p/client/data_manager_types.h"
#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_index.h"
#include "p2p/client/v2/block_pool.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/event_center.h"
#include "p2p/client/v2/eviction_index.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @struct LogicalTilerConfig
 * @brief Everything a TilerManager itself needs. No physical fields.
 */
struct LogicalTilerConfig {
    UUID tiler_id{0, 0};
    MemoryType memory_type = MemoryType::DRAM;
    int32_t priority = 0;
    std::vector<std::string> tags;
};

/**
 * @struct TilerConfig
 * @brief Logical configuration plus the pool description. DataManagerV2 (or
 *        the factory) reads the physical half and builds the pool; only the
 *        logical half reaches TilerManager.
 */
struct TilerConfig {
    LogicalTilerConfig logical;
    BlockPoolConfig pool;
    std::optional<AllocationFailurePolicyConfig> allocation_failure_override;
    /** Which cold/hot ordering this tier's evict engine selects victims from.
     */
    EvictionIndexConfig eviction;
};

/**
 * @class TilerManager
 */
class TilerManager {
   public:
    /**
     * @param eviction_config Defaulted so a component test that does not care
     *        about victim ordering does not have to supply one. Production
     *        always passes the parsed configuration, validated at Init.
     */
    TilerManager(const LogicalTilerConfig& logical_config,
                 const BlockIndexConfig& index_config,
                 std::shared_ptr<BlockPool> block_pool,
                 BlockRegistry block_registry, EventPublisher event_publisher,
                 const EvictionIndexConfig& eviction_config = {});

    /** @param alignment 0 means the pool's minimum alignment. */
    tl::expected<MutableBlock, ErrorCode> Allocate(size_t size,
                                                   size_t alignment = 0);

    /** Register under this tiler's own view of the key's registration. */
    tl::expected<ImmutableBlock, ErrorCode> Register(std::string_view key,
                                                     CompletedBlock&& block);

    /**
     * @brief Register under a registration the caller already holds.
     *
     * Cross-tiler replicas share one registration identity, so migration must
     * use this form rather than looking the key up again.
     */
    /**
     * @param defer_notify when true the presence marker and the commit event
     *        are NOT published here, and the caller must call
     *        NotifyRegistered once it has released the registration's mutation
     *        guard. Publishing under that guard would run a queue push -- and,
     *        on a full queue, a whole inline policy update -- while the key is
     *        locked, which section 7.3 forbids.
     */
    tl::expected<ImmutableBlock, ErrorCode> RegisterWithHandle(
        CompletedBlock&& block, const BlockRegistrationHandle& registration,
        bool defer_notify = false);

    /** The deferred half of RegisterWithHandle(defer_notify = true). */
    void NotifyRegistered(const BlockRegistrationHandle& registration,
                          const BlockId& block_id, size_t size_bytes) const;

    /** Exact lookup. A registry presence hint is never a substitute. */
    tl::expected<ImmutableBlock, ErrorCode> Match(
        const BlockRegistrationHandle& registration) const;

    /**
     * @brief Publish a read of `block` as an access fact.
     *
     * Separate from Match because a lookup is not a read: Query and Exist look
     * up without consuming the data, and only an actual read should make a
     * block look warmer. Without this the placement policy sees commit order
     * and nothing else -- eviction would ignore reads entirely and onboarding
     * could never fire.
     *
     * Call it outside every index shard lock.
     */
    void NotifyAccess(const BlockRegistrationHandle& registration,
                      const ImmutableBlock& block) const;

    /**
     * @param expected_id when set, only remove a matching BlockId.
     * @return OBJECT_NOT_FOUND when this tiler holds no such entry.
     */
    /**
     * @param defer_notify see RegisterWithHandle. When true the caller must
     *        call NotifyDeleted after releasing the mutation guard.
     */
    tl::expected<void, ErrorCode> Delete(
        const BlockRegistrationHandle& registration,
        std::optional<BlockId> expected_id = std::nullopt,
        bool defer_notify = false);

    /** The deferred half of Delete(defer_notify = true). */
    void NotifyDeleted(const BlockRegistrationHandle& registration,
                       const BlockId& block_id, size_t size_bytes) const;

    /**
     * @brief This tier's cold/hot ordering.
     *
     * Non-authoritative and never a substitute for the BlockIndex: it answers
     * "what would I give up first", and every candidate it names is re-checked
     * here before anything is removed. Owned by the tiler because the two are
     * updated in the same mutation -- an ordering that can silently lose a
     * committed block leaks that block's bytes for the process lifetime, since
     * nothing else offers victims.
     */
    EvictionIndex* Eviction() const { return eviction_index_.get(); }

    /**
     * @brief A BlockIndex snapshot of one shard, as eviction-index tokens.
     *
     * Names the shard it covers and when it was taken, because Reconcile
     * treats a snapshot as proof of absence: without that scope it would drop
     * every entry outside this shard, and a block the ordering has forgotten
     * is one nothing will ever offer as a victim.
     */
    BlockIndexSnapshot SnapshotTokens(size_t shard_id) const;

    /**
     * @brief The same over every shard, still one shard lock at a time.
     *
     * The only form that can clear the eviction index's needs_reconcile flag:
     * a per-shard snapshot proves nothing about the shards it did not read.
     */
    BlockIndexSnapshot SnapshotAllTokens() const;

    /** Detach every entry. Returns the detached entries for outside release. */
    std::vector<BlockEntryPtr> DrainAll();

    /** Snapshot one index shard without locking any other. */
    std::vector<BlockEntryPtr> SnapshotShard(size_t shard_id) const;
    size_t ShardCount() const;

    TierView GetView() const;
    UUID Id() const { return tiler_id_; }
    MemoryType Medium() const { return memory_type_; }
    int32_t Priority() const { return priority_; }
    BlockPoolCapabilities Capabilities() const;
    bool IsTeAddressable() const;

    size_t Capacity() const;
    size_t Usage() const;
    size_t FreeBytes() const;
    BlockIndexStats IndexStats() const;

    /**
     * @brief Attach the event sink.
     *
     * Separate from the constructor because the sink depends on the placement
     * policy, which in turn needs the full tiler topology. Must be called
     * during initialization, before any request is served.
     */
    void SetEventPublisher(EventPublisher publisher);

    /**
     * @brief Reject new work. Allocate and Register report SHUTTING_DOWN
     *        afterwards; Match keeps serving the committed index, which stays
     *        intact until DrainAll().
     */
    void Stop();
    bool IsStopped() const;

   private:
    tl::expected<ImmutableBlock, ErrorCode> InsertRegistered(
        CompletedBlock&& block, const BlockRegistrationHandle& registration,
        bool defer_notify);

    /** Fill in what the snapshot covers, before anything is read. */
    void OpenSnapshot(BlockIndexSnapshot& snapshot) const;
    void AppendShardTokens(size_t shard_id, BlockIndexSnapshot& snapshot) const;

    UUID tiler_id_;
    MemoryType memory_type_;
    int32_t priority_ = 0;
    std::vector<std::string> tags_;
    std::shared_ptr<BlockPool> block_pool_;
    std::unique_ptr<BlockIndex> block_index_;
    std::unique_ptr<EvictionIndex> eviction_index_;
    BlockRegistry block_registry_;
    EventPublisher event_publisher_;
    std::atomic<bool> stopped_{false};
};

/**
 * @struct MultiTiler
 * @brief The set of logical tilers, ordered by priority and indexed by UUID.
 *
 * Indexed by UUID, never by MemoryType: several tilers may share a medium.
 * Both containers are built once at initialization and never mutated (V2 does
 * not support dynamic mount/unmount), so lookups need no lock.
 */
struct MultiTiler {
    // Descending priority: iteration order is placement/lookup order.
    std::vector<std::unique_ptr<TilerManager>> by_priority;
    // Non-owning index, built alongside by_priority.
    std::unordered_map<UUID, TilerManager*, boost::hash<UUID>> by_id;

    TilerManager* Find(const UUID& tiler_id) const;

    /**
     * @brief Tilers whose blocks expose a TransferEngine-usable address, in
     *        descending priority order.
     *
     * This is the request-path placement set (invariant 7.4.14) and the set a
     * forward write or pin may publish an address from. It is a medium
     * property: a slow tier exposes no address and is therefore never in it.
     * Whether the process actually registered that memory with an engine is a
     * separate deployment fact, warned about at pool init rather than used to
     * reroute writes.
     */
    std::vector<TilerManager*> TeAddressable() const;

    /** All tilers, in descending priority order. */
    std::vector<TilerManager*> All() const;

    bool Empty() const { return by_priority.empty(); }
    size_t Size() const { return by_priority.size(); }

    /** Rebuilds by_id from by_priority and sorts by descending priority. */
    void Rebuild();
};

}  // namespace mooncake::v2
