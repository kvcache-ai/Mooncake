#pragma once

// LeaseEvictionIndex: a sharded time-bucket EvictionIndex that ranks keys by
// when their object's lease runs out, so eviction visits the oldest expired
// buckets first without scanning the entire keyspace.
//
// A key is filed while it holds an object that is not hard pinned and has a
// completed memory replica. Only writes move a key: a read that extends a
// lease leaves it where it was, so a key is never filed later than its lease
// runs out, only earlier, and the index does not observe reads. A cursor tells
// such a key apart by its real deadline under the key's write lock, and
// releasing that lock refiles it at its real bucket.
//
// Sharding strategy (kShards cacheline-aligned shards):
// - Standalone keys need no key-hash routing because the chosen shard is saved
//   in `SlotEvictionState::shard` for later updates and removal. Writer threads
//   stick to one shard for a batch of insertions and hop without blocking on
//   contention (`LockAnyShardForStandalone`), keeping single-threaded order
//   deterministic while eliminating cross-thread mutex contention.
// - Grouped keys must share one Group entry under a single shard lock, so they
//   route deterministically by Fibonacci-hashing their shared `Lease*` pointer
//   (`LockShardForGroup`).
// - Subsequent rank updates and removals go straight to `LockRecordedShard`.
// - A scan schedules due (rank, shard) buckets in ascending rank order, taking
//   only the oldest age band needed for small targets and widening on demand.

#include <atomic>
#include <bit>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "metadata/eviction_index.h"
#include "metadata/object_route.h"
#include "object_metadata.h"

namespace mooncake {
namespace metadata {

class LeaseEvictionIndex final : public EvictionIndex {
   public:
    static constexpr Clock::duration kBucketWidth =
        std::chrono::milliseconds(100);
    static constexpr size_t kShardBits = 9;
    static constexpr size_t kShards = size_t{1} << kShardBits;
    static_assert(std::has_single_bit(kShards));

    LeaseEvictionIndex();
    ~LeaseEvictionIndex() override;

    // Whether eviction may ever take memory from `metadata`.
    [[nodiscard]] static bool Qualifies(const ObjectMetadata& metadata);

    void OnRead(const route::ReadGuard& guard) override;
    void OnWrite(const route::WriteGuard& guard) override;

    [[nodiscard]] std::unique_ptr<Cursor> Scan(
        Clock::time_point now) const override;

    using EvictionIndex::ScanRanges;
    [[nodiscard]] std::unique_ptr<RangeCursor> ScanRanges(
        Clock::time_point now,
        const TargetForTotal& target_for_total) const override;

   private:
    struct Shard;
    struct LockedShard;
    class ShardedCursor;

    // Symmetric shard-locking entry points for the three write cases:
    // - fresh standalone key: any uncontended shard (sticky thread-local hint);
    // - fresh grouped key: fixed shard hashed from its shared `group_lease`;
    // - already-filed key: the shard previously saved in `state.shard`.
    [[nodiscard]] LockedShard LockAnyShardForStandalone() const;
    [[nodiscard]] LockedShard LockShardForGroup(const Lease* group_lease) const;
    [[nodiscard]] LockedShard LockRecordedShard(
        const route::SlotEvictionState& state) const;

    static uint16_t ShardForGroupLease(const Lease* group_lease);
    static Rank RankOf(const ObjectMetadata& metadata);
    static Rank BucketOf(Clock::time_point deadline);

    static std::atomic<uint64_t> next_index_id_;

    const uint64_t id_;
    mutable std::atomic<uint16_t> next_writer_shard_{0};
    const std::unique_ptr<Shard[]> shards_;
};

}  // namespace metadata
}  // namespace mooncake
