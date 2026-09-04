#include "p2p/client/v2/block_index.h"

#include <algorithm>
#include <limits>
#include <utility>

#include <glog/logging.h>

// For SnapshotShardOf: the sharding rule a BlockIndexSnapshot is scoped by
// lives with the snapshot, and this index is its only producer.
#include "p2p/client/v2/eviction_index.h"

namespace mooncake::v2 {

tl::expected<void, ErrorCode> ValidateBlockIndexConfig(
    const BlockIndexConfig& config) {
    if (config.shard_count == 0) {
        LOG(ERROR) << "block_index.shard_count must be greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!(config.max_load_factor > 0.0F) || config.max_load_factor > 1.0F) {
        LOG(ERROR) << "block_index.max_load_factor must be in (0, 1], got "
                   << config.max_load_factor;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

BlockIndex::BlockIndex(UUID tiler_id, const BlockIndexConfig& config)
    : tiler_id_(tiler_id), config_(config) {
    const size_t shard_count = std::max<size_t>(1, config.shard_count);
    shards_.reserve(shard_count);
    for (size_t i = 0; i < shard_count; ++i) {
        auto shard = std::make_unique<BlockIndexShard>();
        shard->entries.max_load_factor(config.max_load_factor);
        shards_.push_back(std::move(shard));
    }
}

size_t BlockIndex::ShardFor(RegistrationId id) const {
    // One definition of the rule, shared with the eviction index: a per-shard
    // snapshot of this index tells that one which entries it is allowed to
    // drop, and a rule that drifted between the two would have it drop
    // entries the snapshot never covered.
    return SnapshotShardOf(id, shards_.size());
}

tl::expected<ImmutableBlock, ErrorCode> BlockIndex::Insert(
    CompletedBlock&& block, const BlockRegistrationHandle& registration) {
    // Consume the block up front: every exit from here is a rollback, never a
    // retry point, so the caller can never accidentally reuse it.
    CompletedBlock owned = std::move(block);
    if (!owned) return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    if (!registration) return tl::make_unexpected(ErrorCode::INVALID_PARAMS);

    const RegistrationId registration_id = registration.Id();
    const size_t shard_id = ShardFor(registration_id);
    auto& shard = *shards_[shard_id];

    const size_t size_bytes = owned.Size();

    // The entry is built before the lock and, on any failure below, destroyed
    // after it: allocating the control block can throw, and destroying an
    // armed entry calls BlockPool::Free. Neither may happen under a shard lock
    // (section 7.3 constraint 5). Declaring `entry` outside the lock scope is
    // what guarantees the second half -- the lock guard is destroyed first.
    Block block_value;
    block_value.id.tiler_id = tiler_id_;
    // Copied from the physical id so a recycled slot cannot be mistaken for
    // the old block.
    block_value.id.generation = owned.PhysicalId().generation;
    block_value.size_bytes = size_bytes;
    block_value.key = owned.key_;
    block_value.allocation = std::move(owned.allocation_);
    block_value.registration = registration;
    owned.armed_ = false;
    auto entry = std::make_shared<BlockEntry>(std::move(block_value));

    {
        std::unique_lock<std::shared_mutex> lock(shard.mu);
        if (shard.entries.find(registration_id) != shard.entries.end()) {
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }

        // local_id = sequence * shard_count + shard_id keeps ids unique across
        // shards without a tiler-wide counter. The precondition is checked,
        // not assumed: wrapping would let a stale BlockId match a new block.
        const size_t shard_count = shards_.size();
        if (shard.next_block_sequence >
            (std::numeric_limits<uint64_t>::max() - shard_id) / shard_count) {
            LOG(ERROR) << "BlockIndex shard " << shard_id
                       << " exhausted its block sequence";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }

        // The sequence is only readable here, and the entry is still
        // unpublished, so assigning its id under the lock is not a mutation of
        // shared state. Everything after this point is pointer bookkeeping.
        entry->block.id.local_id =
            shard.next_block_sequence * shard_count + shard_id;
        ++shard.next_block_sequence;
        shard.entries.emplace(registration_id, entry);
        shard.indexed_bytes += size_bytes;
    }
    return ImmutableBlock(BlockEntryPtr(std::move(entry)));
}

std::optional<ImmutableBlock> BlockIndex::Lookup(
    const BlockRegistrationHandle& registration) const {
    if (!registration) return std::nullopt;
    const RegistrationId registration_id = registration.Id();
    auto& shard = *shards_[ShardFor(registration_id)];

    // Copy the entry pointer and get out: IO, events and statistics all happen
    // after this lock is released.
    BlockEntryPtr entry;
    {
        std::shared_lock<std::shared_mutex> lock(shard.mu);
        auto it = shard.entries.find(registration_id);
        if (it == shard.entries.end()) return std::nullopt;
        entry = it->second;
    }
    return ImmutableBlock(std::move(entry));
}

BlockEntryPtr BlockIndex::Erase(const BlockRegistrationHandle& registration,
                                std::optional<BlockId> expected_id) {
    if (!registration) return nullptr;
    return EraseById(registration.Id(), expected_id);
}

BlockEntryPtr BlockIndex::EraseById(RegistrationId registration_id,
                                    std::optional<BlockId> expected_id) {
    auto& shard = *shards_[ShardFor(registration_id)];

    BlockEntryPtr detached;
    {
        std::unique_lock<std::shared_mutex> lock(shard.mu);
        auto it = shard.entries.find(registration_id);
        if (it == shard.entries.end()) return nullptr;
        // A policy token may name a block that has already been replaced;
        // removing the current one on its behalf would drop live data.
        if (expected_id.has_value() &&
            !(it->second->block.id == *expected_id)) {
            return nullptr;
        }
        detached = std::move(it->second);
        shard.entries.erase(it);
        shard.indexed_bytes -=
            std::min(shard.indexed_bytes, detached->block.size_bytes);
    }
    // Returned to the caller so the physical free happens outside the lock,
    // and only once every outstanding snapshot is gone.
    return detached;
}

std::vector<BlockEntryPtr> BlockIndex::SnapshotShard(size_t shard_id) const {
    std::vector<BlockEntryPtr> out;
    if (shard_id >= shards_.size()) return out;
    auto& shard = *shards_[shard_id];

    std::shared_lock<std::shared_mutex> lock(shard.mu);
    out.reserve(shard.entries.size());
    for (const auto& [id, entry] : shard.entries) {
        out.push_back(entry);
    }
    return out;
}

std::vector<BlockEntryPtr> BlockIndex::Drain() {
    std::vector<BlockEntryPtr> out;
    // One shard at a time: no path in V2 ever holds two index shards.
    for (auto& shard : shards_) {
        std::unordered_map<RegistrationId, BlockEntryPtr, RegistrationIdHash>
            taken;
        {
            std::unique_lock<std::shared_mutex> lock(shard->mu);
            taken.swap(shard->entries);
            shard->indexed_bytes = 0;
        }
        out.reserve(out.size() + taken.size());
        for (auto& [id, entry] : taken) {
            out.push_back(std::move(entry));
        }
    }
    return out;
}

BlockIndexStats BlockIndex::Stats() const {
    BlockIndexStats stats;
    for (const auto& shard : shards_) {
        std::shared_lock<std::shared_mutex> lock(shard->mu);
        stats.entry_count += shard->entries.size();
        stats.indexed_bytes += shard->indexed_bytes;
    }
    return stats;
}

}  // namespace mooncake::v2
