#pragma once

// BlockIndex: the single authoritative visibility index of one logical tiler.
//
// "Unified" means one entry container and one visibility rule per tiler, not
// one global table: shards are independent, and no operation ever holds more
// than one of them. There is no second, owning eviction container anywhere --
// policy structures hold non-owning tokens and must come back here, under the
// right shard lock, to remove anything.
//
// Entries are keyed by RegistrationId rather than by key string. That is what
// makes delete-then-recreate of the same key unambiguous, and it is also why a
// key lookup has to go through the BlockRegistry first (section 9.4 tracks the
// cost of that extra hop).

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_registry.h"
#include "types.h"

namespace mooncake::v2 {

/**
 * @struct BlockIndexConfig
 */
struct BlockIndexConfig {
    size_t shard_count = 64;
    float max_load_factor = 0.875F;
};

tl::expected<void, ErrorCode> ValidateBlockIndexConfig(
    const BlockIndexConfig& config);

/**
 * @struct BlockIndexShard
 */
struct BlockIndexShard {
    mutable std::shared_mutex mu;
    std::unordered_map<RegistrationId, BlockEntryPtr, RegistrationIdHash>
        entries;
    uint64_t next_block_sequence = 1;  // under this shard's unique lock
    size_t indexed_bytes = 0;          // under this shard's mu
};

/**
 * @struct BlockIndexStats
 * @brief Aggregated by reading each shard in turn. There is deliberately no
 *        process-wide counter on the request path.
 */
struct BlockIndexStats {
    size_t entry_count = 0;
    size_t indexed_bytes = 0;
};

/**
 * @class BlockIndex
 */
class BlockIndex {
   public:
    BlockIndex(UUID tiler_id, const BlockIndexConfig& config);

    /**
     * @brief Make a completed block visible under `registration`.
     *
     * Postcondition, success or failure: `block` has been consumed. A failed
     * insert is a rollback, not a retry point -- the caller must allocate
     * again rather than reuse the block.
     *
     * @return OBJECT_ALREADY_EXISTS if this registration already has an entry
     *         in this tiler, INTERNAL_ERROR if the shard's block sequence
     *         would overflow.
     */
    tl::expected<ImmutableBlock, ErrorCode> Insert(
        CompletedBlock&& block, const BlockRegistrationHandle& registration);

    /** Copies the entry pointer under a shared lock and returns immediately. */
    std::optional<ImmutableBlock> Lookup(
        const BlockRegistrationHandle& registration) const;

    /**
     * @brief Detach the entry for `registration`.
     * @param expected_id when set, only remove if the BlockId matches; this is
     *        how a stale policy token is rejected.
     * @return the detached entry (null if there was none). The caller releases
     *         it outside the lock, which is where the physical free happens.
     */
    BlockEntryPtr Erase(const BlockRegistrationHandle& registration,
                        std::optional<BlockId> expected_id = std::nullopt);

    /** Same, addressed by identity alone (async paths hold no strong handle).
     */
    BlockEntryPtr EraseById(RegistrationId registration_id,
                            std::optional<BlockId> expected_id = std::nullopt);

    /** Snapshot of one shard; never locks more than that shard. */
    std::vector<BlockEntryPtr> SnapshotShard(size_t shard_id) const;

    /** Detach everything, shard by shard. Used by RemoveAll and Destroy. */
    std::vector<BlockEntryPtr> Drain();

    size_t ShardCount() const { return shards_.size(); }
    BlockIndexStats Stats() const;
    UUID TilerId() const { return tiler_id_; }

    size_t ShardFor(RegistrationId id) const;

   private:
    UUID tiler_id_;
    BlockIndexConfig config_;
    std::vector<std::unique_ptr<BlockIndexShard>> shards_;
};

}  // namespace mooncake::v2
