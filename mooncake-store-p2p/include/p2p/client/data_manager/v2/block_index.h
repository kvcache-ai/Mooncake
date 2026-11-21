#pragma once

// BlockIndex owns both visibility and eviction order for one logical tiler.
// A backend publishes and detaches entries together with their ordering, so
// every visible block remains available to SelectVictims. Entries are keyed by
// RegistrationId; asynchronous access and removal also validate the BlockId.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <ylt/util/tl/expected.hpp>

#include "p2p/client/data_manager/v2/block.h"
#include "p2p/client/data_manager/v2/block_registry.h"
#include "common_types.h"

namespace mooncake::v2 {

struct BlockIndexConfig {
    size_t shard_count = 64;
    float max_load_factor = 0.875F;

    /** "lru" | "multi_lru" | "tinylfu_lru". */
    std::string type = "tinylfu_lru";
    size_t max_victim_candidates = 256;

    /** 0 keeps the corresponding default (3, 8, 15). */
    uint64_t band_warm_threshold = 0;
    uint64_t band_hot_threshold = 0;
    uint64_t band_veryhot_threshold = 0;

    /** 0 derives the sketch size from the tier capacity. */
    size_t sketch_capacity = 0;
};

tl::expected<void, ErrorCode> ValidateBlockIndexConfig(
    const BlockIndexConfig& config);

/** Aggregated shard by shard; a concurrent mutation may affect the sample. */
struct BlockIndexStats {
    size_t entry_count = 0;
    size_t indexed_bytes = 0;
};

/** Replaceable backend for one tiler's entries and cold/hot ordering. */
class BlockIndex {
   public:
    virtual ~BlockIndex() = default;

    /**
     * Publish a completed block together with its eviction ordering.
     * Success and failure both consume `block`; failures reclaim its resource
     * outside index locks. Duplicate registrations return
     * OBJECT_ALREADY_EXISTS; allocation or identity exhaustion returns
     * INTERNAL_ERROR.
     */
    virtual tl::expected<ImmutableBlock, ErrorCode> Insert(
        CompletedBlock&& block,
        const BlockRegistrationHandle& registration) = 0;

    /** Copies the immutable entry pointer under its shard's shared lock. */
    virtual std::optional<ImmutableBlock> Lookup(
        const BlockRegistrationHandle& registration) const = 0;

    /**
     * Detach visibility and ordering together. A supplied BlockId must match.
     * The returned entry releases its physical resource outside index locks,
     * after its last outstanding snapshot has also been released.
     */
    virtual BlockEntryPtr Erase(
        const BlockRegistrationHandle& registration,
        std::optional<BlockId> expected_id = std::nullopt) = 0;
    virtual BlockEntryPtr EraseById(
        RegistrationId registration_id,
        std::optional<BlockId> expected_id = std::nullopt) = 0;

    /** Reorder only when both registration and block identity still match. */
    virtual void OnAccess(const BlockToken& token) = 0;

    /**
     * Coldest-first non-owning candidates, stopping at the byte target or the
     * configured candidate limit. A zero target returns no candidates. Removal
     * must revalidate each candidate's BlockId because it may become stale.
     */
    virtual std::vector<BlockToken> SelectVictims(size_t target_bytes) = 0;

    /** Snapshot or detach entries one shard at a time. */
    virtual std::vector<BlockEntryPtr> SnapshotShard(size_t shard_id) const = 0;
    virtual std::vector<BlockEntryPtr> Drain() = 0;

    virtual size_t ShardCount() const = 0;
    virtual BlockIndexStats Stats() const = 0;
    virtual UUID TilerId() const = 0;
    virtual size_t ShardFor(RegistrationId id) const = 0;

   protected:
    /**
     * Transfer CompletedBlock's private resource into an unpublished entry.
     * Backends assign local_id before publication and leave business fields
     * immutable afterward. Call before taking index locks: this may allocate.
     */
    static std::shared_ptr<BlockEntry> MakeEntry(
        CompletedBlock&& block, const BlockRegistrationHandle& registration,
        const UUID& tiler_id);
};

/** Validate configuration and construct the selected backend. */
tl::expected<std::unique_ptr<BlockIndex>, ErrorCode> CreateBlockIndex(
    const BlockIndexConfig& config, const UUID& tiler_id, size_t capacity);

}  // namespace mooncake::v2
