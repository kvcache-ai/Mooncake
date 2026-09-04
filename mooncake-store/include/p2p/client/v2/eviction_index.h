#pragma once

// EvictionIndex: the cold/hot ordering of one logical tiler.
//
// It is explicitly NOT authoritative. BlockIndex answers "does this block
// exist and where is it"; this answers only "which blocks would I give up
// first", and it is allowed to be approximate, sampled, lossy or stale. Every
// candidate it hands out is a name that the caller must re-check against the
// owning BlockIndex, under that index's shard lock, before removing anything.
//
// Two consequences of that split are load-bearing:
//
//  - Losing an entry here must never make a block unreclaimable. An index that
//    silently forgets a committed block would leak that block's bytes for the
//    process lifetime, because nothing else offers victims. The owner marks
//    the affected shard needs_reconcile and the evict engine falls back to a
//    BlockIndex snapshot scan.
//  - Nothing here may free memory. SelectVictims returns tokens, never
//    allocations; physical space comes back through the RAII BlockAllocation
//    when the last snapshot is dropped.
//
// Ownership: one EvictionIndex per TilerManager, alongside that tiler's
// BlockIndex. Commit, Delete and Migration update both in the same mutation;
// Access is allowed to arrive later, through the event pipeline, and may be
// sampled or coalesced on the way.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/v2/block.h"
#include "p2p/client/v2/v2_common.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @struct BlockIndexSnapshot
 * @brief The authoritative view an index reconciles against.
 *
 * Deliberately a plain vector of tokens rather than a live handle: Reconcile
 * must not hold a BlockIndex shard lock while it walks its own structures.
 *
 * A snapshot is proof of absence only for what it actually read. It therefore
 * has to say what that was -- which shard, and as of when -- or Reconcile
 * cannot tell "the authority does not have this block" from "the capture
 * never looked". Getting that wrong drops live blocks, and a block this index
 * has forgotten is one nothing will ever offer as a victim.
 */
struct BlockIndexSnapshot {
    UUID tiler_id{0, 0};
    std::vector<BlockToken> entries;
    /** True when `entries` covers the whole tiler, not just one shard. */
    bool complete = false;
    /**
     * @brief The shard `entries` covers when !complete, and the only entries
     *        Reconcile may drop.
     *
     * `shard_count` is the shard count the producer's BlockIndex is using;
     * membership is SnapshotShardOf(). 0 means the producer cannot name a
     * shard, which makes a partial snapshot add-only.
     */
    size_t shard_id = 0;
    size_t shard_count = 0;
    /**
     * @brief EvictionIndex::MutationCount() sampled *before* `entries` was
     *        read.
     *
     * Reconcile holds no BlockIndex lock, so an ordinary commit can land
     * between the capture and the pass. That block is missing from `entries`
     * through no fault of this index: it must not be dropped, and the pass
     * must not report the index whole. A snapshot that leaves this at 0 can
     * only add.
     */
    uint64_t observed_mutations = 0;
};

/**
 * @brief Which shard of `shard_count` a registration falls in; 0 when
 *        `shard_count` is 0.
 *
 * BlockIndex::ShardFor is defined in terms of this, because the eviction
 * index has no BlockIndex to ask and the two must agree exactly: a rule that
 * drifted would make a per-shard Reconcile drop entries the snapshot never
 * covered.
 */
size_t SnapshotShardOf(const RegistrationId& id, size_t shard_count);

/**
 * @struct EvictionIndexStats
 */
struct EvictionIndexStats {
    /** Blocks this index currently orders. */
    size_t tracked_blocks = 0;
    size_t tracked_bytes = 0;
    /** Candidates handed out that the caller found stale. */
    uint64_t stale_candidates = 0;
    /** Reconcile passes that ran. */
    uint64_t reconciles = 0;
    /** Updates that failed and left a shard needing reconciliation. */
    uint64_t needs_reconcile = 0;
};

/**
 * @class EvictionIndex
 * @brief Auxiliary, non-authoritative ordering. See the file comment.
 *
 * Implementations may be LRU, banded LRU (MultiLRU) or TinyLFU-assisted. They
 * differ only in the order SelectVictims produces.
 */
class EvictionIndex {
   public:
    virtual ~EvictionIndex() = default;

    /** A block became visible in this tiler. */
    virtual void OnCommit(const BlockToken& token) = 0;

    /**
     * @brief A block was read.
     *
     * May arrive out of band and be dropped under pressure; it only reorders.
     */
    virtual void OnAccess(const BlockToken& token) = 0;

    /**
     * @brief A block stopped being visible in this tiler.
     *
     * Token-scoped: if the index now holds a *different* block for the same
     * key, that newer record must survive, or the replacement would never be
     * offered as a victim again and would sit in the tier forever.
     */
    virtual void OnDelete(const BlockToken& token) = 0;

    /** A block moved between tilers; both halves are reported to both indexes.
     */
    virtual void OnMigration(const BlockToken& old_token,
                             const BlockToken& new_token) = 0;

    /**
     * @brief Coldest-first candidates totalling at least `target_bytes`.
     *
     * The result proves nothing: a candidate may already be gone. The caller
     * validates each one against the BlockIndex before acting.
     */
    virtual std::vector<BlockToken> SelectVictims(size_t target_bytes) = 0;

    /**
     * @brief Rebuild from the authoritative index.
     *
     * Adds blocks this index has lost and drops ones it invented. The recovery
     * path for a failed synchronous update, and the periodic safety net.
     *
     * Scoped to what `snapshot` actually read: a partial snapshot may only
     * drop entries from the shard it names and older than its mutation
     * sample, and only a complete, unraced snapshot whose every row could be
     * filed clears NeedsReconcile(). A repair is not an access -- a recovered
     * record enters at the cold end of its band, never ahead of a block that
     * really was touched.
     */
    virtual void Reconcile(const BlockIndexSnapshot& snapshot) = 0;

    /** Drop everything. Only safe when nothing else can be committing. */
    virtual void Clear() = 0;

    virtual EvictionIndexStats Stats() const = 0;

    /**
     * @brief True when a synchronous update failed and this index knows it is
     *        missing something.
     *
     * The evict engine reads it to decide whether a snapshot scan is needed
     * before it concludes "no candidates, nothing to reclaim".
     */
    virtual bool NeedsReconcile() const = 0;

    /**
     * @brief How many times this index has changed what it holds.
     *
     * Sampled into BlockIndexSnapshot::observed_mutations *before* the
     * authoritative entries are read, which is what lets Reconcile tell a
     * snapshot that raced a commit from one that proves this index is whole.
     * Accesses do not count: they reorder, they never add or remove a name.
     */
    virtual uint64_t MutationCount() const = 0;
};

/**
 * @struct EvictionIndexConfig
 */
struct EvictionIndexConfig {
    /** "lru" | "multi_lru" | "tinylfu_lru". */
    std::string type = "tinylfu_lru";

    /** Upper bound on the candidates one selection round inspects. */
    size_t max_victim_candidates = 256;

    /** Frequency cutoffs for the banded LRU. 0 keeps the MultiLRU defaults. */
    uint64_t band_warm_threshold = 0;
    uint64_t band_hot_threshold = 0;
    uint64_t band_veryhot_threshold = 0;

    /** 0 = derive from the tier capacity; forwarded to FrequencySketch. */
    size_t sketch_capacity = 0;
};

tl::expected<void, ErrorCode> ValidateEvictionIndexConfig(
    const EvictionIndexConfig& config);

/**
 * @param tiler_id The tiler this index orders. Every token it accepts must
 *        name that tiler; one naming another is a programming error and is
 *        rejected rather than silently mixed in.
 * @param capacity Used only to size the sketch when sketch_capacity is 0.
 */
tl::expected<std::unique_ptr<EvictionIndex>, ErrorCode> CreateEvictionIndex(
    const EvictionIndexConfig& config, const UUID& tiler_id, size_t capacity);

}  // namespace mooncake::v2
