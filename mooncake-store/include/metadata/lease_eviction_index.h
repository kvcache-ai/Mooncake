#pragma once

// LeaseEvictionIndex: an EvictionIndex that ranks a key by when its object's
// lease runs out, so eviction visits the objects whose lease ran out first.
//
// A key is filed while it holds an object that is not hard pinned and has a
// completed memory replica. Only writes move a key: a read that extends a
// lease leaves it where it was, so a key is never filed later than its lease
// runs out, only earlier, and the index does not observe reads. A cursor tells
// such a key apart by its real deadline, and releasing its write lock files it
// there, which is the whole of the index's upkeep for reads.
//
// Keys are filed in buckets of kBucketWidth by deadline; a key's rank is its
// bucket. A walk at `now` stops after the bucket `now` falls in, which also
// holds keys whose lease has not run out yet: eviction judges each key's real
// lease anyway, and leaving the bucket out would hold back a lease that ran out
// a moment ago.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <vector>

#include "metadata/eviction_index.h"
#include "metadata/object_route.h"
#include "object_metadata.h"

namespace mooncake {
namespace metadata {

class LeaseEvictionIndex final : public EvictionIndex {
   public:
    static constexpr Clock::duration kBucketWidth =
        std::chrono::milliseconds(100);

    LeaseEvictionIndex() : EvictionIndex(/*observes_reads=*/false) {}

    // Whether eviction may ever take memory from `metadata`.
    [[nodiscard]] static bool Qualifies(const ObjectMetadata& metadata) {
        return !metadata.IsHardPinned() &&
               metadata.HasReplica([](const Replica& replica) {
                   return replica.is_memory_replica() && replica.is_completed();
               });
    }

    void OnRead(const route::ReadGuard&) override {}

    void OnWrite(const route::WriteGuard& guard) override {
        route::SlotOwnerState& owner = guard.owner_state();
        const bool file = guard.has_object() && Qualifies(guard.metadata());
        const Rank rank = file ? RankOf(guard.metadata()) : 0;
        if (file == owner.eviction_filed &&
            (!file || rank == owner.eviction_rank)) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (owner.eviction_filed) {
            Unfile(guard.slot(), owner.eviction_rank);
        }
        if (file) {
            buckets_[rank].push_back(guard.slot());
            ++size_;
        }
        owner.eviction_filed = file;
        owner.eviction_rank = rank;
    }

    [[nodiscard]] std::unique_ptr<Cursor> Scan(
        Clock::time_point now) const override {
        return std::make_unique<LeaseCursor>(*this, BucketOf(now));
    }

   private:
    using SlotList = route::KeySlot::EvictionList;

    // Reads the index one bucket at a time, so it never holds the index lock
    // for longer than one bucket takes to copy.
    class LeaseCursor final : public Cursor {
       public:
        LeaseCursor(const LeaseEvictionIndex& index, Rank last)
            : index_(index),
              last_(last),
              total_(index.size_.load(std::memory_order_relaxed)) {}

        std::optional<Entry> Next() override {
            if (pos_ == buffer_.size()) {
                buffer_.clear();
                pos_ = 0;
                next_ = index_.ReadBucket(next_, last_, buffer_);
            }
            if (pos_ == buffer_.size()) {
                return std::nullopt;
            }
            return std::move(buffer_[pos_++]);
        }

        bool StillAt(const route::WriteGuard& guard,
                     const Entry& entry) const override {
            return guard.has_object() && RankOf(guard.metadata()) <= entry.rank;
        }

        size_t Total() const override { return total_; }

       private:
        const LeaseEvictionIndex& index_;
        // The last bucket the walk reads.
        const Rank last_;
        const size_t total_;
        // The first bucket the walk has not read yet.
        Rank next_ = std::numeric_limits<Rank>::min();
        std::vector<Entry> buffer_;
        size_t pos_ = 0;
    };

    static Rank RankOf(const ObjectMetadata& metadata) {
        return BucketOf(metadata.EvictionDeadline());
    }

    static Rank BucketOf(Clock::time_point deadline) {
        const auto since_epoch = deadline.time_since_epoch();
        Rank bucket = since_epoch / kBucketWidth;
        // Round toward the earlier bucket for a deadline before the epoch.
        if (since_epoch < Clock::duration::zero() &&
            since_epoch % kBucketWidth != Clock::duration::zero()) {
            --bucket;
        }
        return bucket;
    }

    // Copies the keys of the first non-empty bucket in [from, last] into
    // `out` and returns the bucket after it; returns `last + 1` when there is
    // none.
    Rank ReadBucket(Rank from, Rank last, std::vector<Entry>& out) const {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto it = buckets_.lower_bound(from);
        if (it == buckets_.end() || it->first > last) {
            return last + 1;
        }
        for (const route::KeySlot& slot : it->second) {
            out.push_back({slot.key(), it->first});
        }
        return it->first + 1;
    }

    void Unfile(route::KeySlot& slot, Rank rank) {
        const auto it = buckets_.find(rank);
        it->second.erase(it->second.iterator_to(slot));
        if (it->second.empty()) {
            buckets_.erase(it);
        }
        --size_;
    }

    mutable std::mutex mutex_;
    // A node map, so a bucket's list stays put while others come and go.
    std::map<Rank, SlotList> buckets_;
    std::atomic<size_t> size_{0};
};

}  // namespace metadata
}  // namespace mooncake
