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
//
// The filed members of a group are filed as one entry of the index's own,
// which the group's last filed member takes with it. Members share one lease,
// which only ever grows, so the deadline the latest member write saw is the
// group's, and the group is filed at it. That lease is also how the index tells
// groups apart: a group recreated under the same id has a new one.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/intrusive/list.hpp>

#include "lease.h"
#include "metadata/eviction_index.h"
#include "metadata/object_route.h"
#include "mutex.h"
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
        const Lease* const lease =
            file ? GroupLeaseOf(guard.metadata()) : nullptr;
        // Read without the index lock: the group outlives this member, which
        // the held key lock keeps in it, and its lease never changes.
        Group* const filed_in = GroupOf(owner);
        const bool same_group =
            (filed_in != nullptr ? filed_in->lease() : nullptr) == lease;
        if (file == owner.eviction_filed &&
            (!file || (rank == owner.eviction_rank && same_group))) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (file && same_group && filed_in != nullptr) {
            filed_in->MoveTo(rank);
        } else {
            if (owner.eviction_filed) {
                Unfile(guard.slot(), owner);
            }
            if (file) {
                File(guard.slot(), guard.metadata(), lease, rank, owner);
            }
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
    class Buckets;

    // The filed members of one group, filed in `buckets` at the rank the
    // latest member write saw from construction to destruction. Members are
    // linked through the hook a key filed on its own is linked into its
    // bucket by. The group holds the lease its members share, so that lease's
    // address names it alone while it lives.
    class Group {
       public:
        Group(std::shared_ptr<const Lease> lease, uint64_t incarnation,
              Rank rank, Buckets& buckets)
            : lease_(std::move(lease)),
              incarnation_(incarnation),
              rank_(rank),
              buckets_(buckets) {
            buckets_.Add(rank_, *this);
        }
        // The members still linked are unlinked with the list.
        ~Group() { buckets_.Remove(rank_, *this); }
        Group(const Group&) = delete;
        Group& operator=(const Group&) = delete;

        [[nodiscard]] const Lease* lease() const { return lease_.get(); }
        [[nodiscard]] uint64_t incarnation() const { return incarnation_; }

        void MoveTo(Rank rank) {
            if (rank == rank_) {
                return;
            }
            buckets_.Remove(rank_, *this);
            rank_ = rank;
            buckets_.Add(rank_, *this);
        }

        void Join(route::KeySlot& slot) { members_.push_back(slot); }

        // Drops `slot`; returns whether that left the group with no member.
        [[nodiscard]] bool Leave(route::KeySlot& slot) {
            members_.erase(members_.iterator_to(slot));
            return members_.empty();
        }

        void AppendKeys(std::vector<std::string>& out) const {
            for (const route::KeySlot& slot : members_) {
                out.push_back(slot.key());
            }
        }

       private:
        using Hook = boost::intrusive::list_member_hook<
            boost::intrusive::link_mode<boost::intrusive::safe_link>>;

        const std::shared_ptr<const Lease> lease_;
        const uint64_t incarnation_;
        Rank rank_;
        Buckets& buckets_;
        SlotList members_;
        Hook hook_;

       public:
        // Named here, where the hook is accessible.
        using List = boost::intrusive::list<
            Group, boost::intrusive::member_hook<Group, Hook, &Group::hook_>>;
    };

    // The keys filed on their own and the groups, by rank. No bucket is ever
    // left empty.
    class Buckets {
       public:
        void Add(Rank rank, route::KeySlot& slot) {
            buckets_[rank].keys.push_back(slot);
        }
        void Remove(Rank rank, route::KeySlot& slot) {
            const auto it = buckets_.find(rank);
            it->second.keys.erase(it->second.keys.iterator_to(slot));
            DropIfEmpty(it);
        }
        void Add(Rank rank, Group& group) {
            buckets_[rank].groups.push_back(group);
        }
        void Remove(Rank rank, Group& group) {
            const auto it = buckets_.find(rank);
            it->second.groups.erase(it->second.groups.iterator_to(group));
            DropIfEmpty(it);
        }

        // Copies the entries of the first bucket in [from, last] into `out`
        // and returns the bucket after it; returns `last + 1` when there is
        // none.
        Rank Read(Rank from, Rank last, std::vector<Entry>& out) const {
            const auto it = buckets_.lower_bound(from);
            if (it == buckets_.end() || it->first > last) {
                return last + 1;
            }
            for (const route::KeySlot& slot : it->second.keys) {
                out.push_back(Entry{{slot.key()}, it->first, 0});
            }
            for (const Group& group : it->second.groups) {
                Entry& entry =
                    out.emplace_back(Entry{{}, it->first, group.incarnation()});
                group.AppendKeys(entry.keys);
            }
            return it->first + 1;
        }

       private:
        struct Bucket {
            SlotList keys;
            Group::List groups;
        };

        void DropIfEmpty(std::map<Rank, Bucket>::iterator it) {
            if (it->second.keys.empty() && it->second.groups.empty()) {
                buckets_.erase(it);
            }
        }

        // A node map, so a bucket's lists stay put while others come and go.
        std::map<Rank, Bucket> buckets_;
    };

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
            // A key filed in another group since, a group recreated under the
            // same id included, or into or out of one, belongs to another
            // entry, whatever its rank. The group outlives the key filed in
            // it, which the held key lock keeps there.
            const Group* group = GroupOf(guard.owner_state());
            const uint64_t incarnation =
                group != nullptr ? group->incarnation() : 0;
            return guard.has_object() && incarnation == entry.group &&
                   RankOf(guard.metadata()) <= entry.rank;
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

    // Buckets::Read under the index lock.
    Rank ReadBucket(Rank from, Rank last, std::vector<Entry>& out) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return buckets_.Read(from, last, out);
    }

    // The group `owner`'s key is filed in, if any.
    static Group* GroupOf(const route::SlotOwnerState& owner) {
        return static_cast<Group*>(owner.eviction_group);
    }

    // The lease the members of `metadata`'s group share; null for an object
    // on its own.
    static const Lease* GroupLeaseOf(const ObjectMetadata& metadata) {
        if (!metadata.IsGrouped()) {
            return nullptr;
        }
        SpinLocker locker(&metadata.lock);
        return metadata.lease_.get();
    }

    // Files `slot`, which holds `metadata` and which `owner` describes, at
    // `rank`, on its own or, with `lease`, as a member of that lease's group.
    void File(route::KeySlot& slot, const ObjectMetadata& metadata,
              const Lease* lease, Rank rank, route::SlotOwnerState& owner) {
        ++size_;
        if (lease == nullptr) {
            buckets_.Add(rank, slot);
            return;
        }
        auto it = groups_.find(lease);
        if (it == groups_.end()) {
            std::shared_ptr<const Lease> held;
            {
                SpinLocker locker(&metadata.lock);
                held = metadata.lease_;
            }
            it = groups_
                     .emplace(lease, std::make_unique<Group>(std::move(held),
                                                             ++groups_created_,
                                                             rank, buckets_))
                     .first;
        } else {
            it->second->MoveTo(rank);
        }
        it->second->Join(slot);
        owner.eviction_group = it->second.get();
    }

    // Drops `slot`, filed as `owner` says, and its group with its last member.
    void Unfile(route::KeySlot& slot, route::SlotOwnerState& owner) {
        --size_;
        Group* const group = GroupOf(owner);
        owner.eviction_group = nullptr;
        if (group == nullptr) {
            buckets_.Remove(owner.eviction_rank, slot);
        } else if (group->Leave(slot)) {
            groups_.erase(groups_.find(group->lease()));
        }
    }

    mutable std::mutex mutex_;
    Buckets buckets_;
    // Each keyed by the lease it holds. Declared after the buckets, so every
    // group leaves its bucket before the buckets go.
    std::unordered_map<const Lease*, std::unique_ptr<Group>> groups_;
    // Numbers each group's incarnation, from 1.
    uint64_t groups_created_ = 0;
    // Keys filed, on their own or in a group.
    std::atomic<size_t> size_{0};
};

}  // namespace metadata
}  // namespace mooncake
