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
// Storage is partitioned across kShards cacheline-aligned shards:
// - Standalone keys link directly through KeySlot's intrusive hook with zero
//   extra heap allocations. Writer threads batch insertions per shard and hop
//   with try_lock on contention, keeping single-threaded order deterministic
//   while eliminating cross-thread mutex contention.
// - Grouped keys hash their shared Lease pointer to one shard so all members of
//   a group share one Group entry without cross-shard locking.
// - A scan schedules due (rank, shard) buckets in ascending rank order, taking
//   only the oldest age band needed for small targets and widening on demand.

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/intrusive/list.hpp>

#include "common/shrink_buckets.h"
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
    static constexpr size_t kShards = 512;

    LeaseEvictionIndex()
        : EvictionIndex(/*observes_reads=*/false),
          id_(next_index_id_.fetch_add(1, std::memory_order_relaxed)) {}

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
        if (!guard.has_object() || !Qualifies(guard.metadata())) {
            if (owner.eviction_filed) {
                Unfile(guard.slot(), owner);
            }
            return;
        }

        const ObjectMetadata& metadata = guard.metadata();
        const Rank rank = RankOf(metadata);
        const Lease* const group_lease = GroupLeaseOf(metadata);
        const uint64_t standalone_id = guard.generation() & ~kGroupIdentityBit;

        if (IsSameFiledEntry(owner, group_lease, standalone_id)) {
            if (rank != owner.eviction_rank) {
                UpdateRankInPlace(guard.slot(), owner, rank);
            }
            return;
        }

        if (owner.eviction_filed) {
            Unfile(guard.slot(), owner);
        }

        if (group_lease == nullptr) {
            FileStandalone(guard.slot(), owner, rank, standalone_id);
        } else {
            FileInGroup(guard.slot(), owner, metadata, group_lease, rank);
        }
    }

    [[nodiscard]] std::unique_ptr<Cursor> Scan(
        Clock::time_point now) const override {
        return ScanRanges(now, /*target=*/0);
    }

    [[nodiscard]] std::unique_ptr<RangeCursor> ScanRanges(
        Clock::time_point now, size_t target) const override {
        return std::make_unique<ShardedCursor>(*this, BucketOf(now), target);
    }

   private:
    static constexpr uint16_t kShardMask = static_cast<uint16_t>(kShards - 1);
    static constexpr uint16_t kShardBatch = 512;
    static constexpr uint64_t kGroupIdentityBit = uint64_t{1} << 63;
    static constexpr size_t kBandThresholdKeys = 262'144;

    using SlotList = route::KeySlot::EvictionList;
    class Buckets;

    // One shard bucket as a cursor copied it: key bytes packed contiguously so
    // copying one bucket takes a single contiguous buffer per worker.
    class Copy {
       public:
        void Clear() {
            bytes_.clear();
            key_ends_.clear();
            keys_.clear();
            entries_.clear();
        }

        void StartEntry(Rank rank, uint64_t identity) {
            entries_.push_back(Filed{key_ends_.size(), rank, identity});
        }

        void AddKey(const std::string& key) {
            bytes_.append(key);
            key_ends_.push_back(bytes_.size());
        }

        void Seal() {
            keys_.reserve(key_ends_.size());
            size_t begin = 0;
            for (const size_t end : key_ends_) {
                keys_.emplace_back(bytes_.data() + begin, end - begin);
                begin = end;
            }
        }

        [[nodiscard]] size_t size() const { return entries_.size(); }

        [[nodiscard]] Entry operator[](size_t i) const {
            const Filed& filed = entries_[i];
            const size_t end = i + 1 < entries_.size()
                                   ? entries_[i + 1].first_key
                                   : keys_.size();
            return Entry{std::span<const std::string_view>(keys_).subspan(
                             filed.first_key, end - filed.first_key),
                         filed.rank, filed.identity};
        }

       private:
        struct Filed {
            size_t first_key;
            Rank rank;
            uint64_t identity;
        };

        std::string bytes_;
        std::vector<size_t> key_ends_;
        std::vector<std::string_view> keys_;
        std::vector<Filed> entries_;
    };

    // The filed members of one group inside its shard, linked into `buckets` at
    // the latest deadline rank seen across its members.
    class Group {
       public:
        Group(std::shared_ptr<const Lease> lease, uint64_t incarnation,
              uint64_t born_seq, Rank rank, Buckets& buckets)
            : lease_(std::move(lease)),
              incarnation_(incarnation),
              born_seq_(born_seq),
              rank_(rank),
              buckets_(buckets) {
            buckets_.Add(rank_, *this, /*refile_seq=*/0);
        }
        ~Group() { buckets_.Remove(rank_, *this); }
        Group(const Group&) = delete;
        Group& operator=(const Group&) = delete;

        [[nodiscard]] const Lease* lease() const { return lease_.get(); }
        [[nodiscard]] uint64_t incarnation() const { return incarnation_; }

        void MoveTo(Rank rank, uint64_t& shard_seq) {
            if (rank == rank_) {
                return;
            }
            const uint64_t seq = ++shard_seq;
            buckets_.Remove(rank_, *this);
            rank_ = rank;
            refile_seq_ = seq;
            buckets_.Add(rank_, *this, seq);
        }

        void Join(route::KeySlot& slot) {
            members_.push_back(slot);
            buckets_.AddGroupMember(rank_);
        }

        [[nodiscard]] bool Leave(route::KeySlot& slot) {
            members_.erase(members_.iterator_to(slot));
            buckets_.RemoveGroupMember(rank_);
            return members_.empty();
        }

       private:
        friend class Buckets;

        using Hook = boost::intrusive::list_member_hook<
            boost::intrusive::link_mode<boost::intrusive::safe_link>>;

        [[nodiscard]] uint64_t born_seq() const { return born_seq_; }
        [[nodiscard]] uint64_t refile_seq() const { return refile_seq_; }
        [[nodiscard]] size_t member_count() const { return members_.size(); }

        void AppendKeys(Copy& out) const {
            for (const route::KeySlot& slot : members_) {
                out.AddKey(slot.key());
            }
        }

        const std::shared_ptr<const Lease> lease_;
        const uint64_t incarnation_;
        const uint64_t born_seq_;
        uint64_t refile_seq_ = 0;
        Rank rank_;
        Buckets& buckets_;
        SlotList members_;
        Hook hook_;

        using List = boost::intrusive::list<
            Group, boost::intrusive::member_hook<Group, Hook, &Group::hook_>>;
    };

    // Per-shard hash table of non-empty 100ms buckets, with a single-entry hot
    // cache so consecutive writes in the same tick bypass hash lookup.
    class Buckets {
       public:
        struct Bucket {
            Bucket() = default;
            Bucket(const Bucket&) = delete;
            Bucket& operator=(const Bucket&) = delete;

            [[nodiscard]] bool Empty() const {
                return keys.empty() && groups.empty();
            }
            [[nodiscard]] size_t TotalKeys() const {
                return keys.size() + group_members;
            }

            SlotList keys;
            Group::List groups;
            size_t group_members = 0;
            uint64_t last_refile_seq = 0;
        };

        using Map = std::unordered_map<Rank, Bucket>;

        void Add(Rank rank, route::KeySlot& slot, uint64_t refile_seq) {
            Bucket& bucket = GetOrCreate(rank);
            bucket.keys.push_back(slot);
            if (refile_seq != 0) {
                bucket.last_refile_seq = refile_seq;
            }
        }

        void Remove(Rank rank, route::KeySlot& slot) {
            if (hot_bucket_ != nullptr && hot_rank_ == rank) {
                hot_bucket_->keys.erase(hot_bucket_->keys.iterator_to(slot));
                if (hot_bucket_->Empty()) {
                    hot_bucket_ = nullptr;
                    buckets_.erase(rank);
                    ShrinkBucketsIfSparse(buckets_);
                }
                return;
            }
            const auto it = buckets_.find(rank);
            it->second.keys.erase(it->second.keys.iterator_to(slot));
            DropIfEmpty(it);
        }

        void Add(Rank rank, Group& group, uint64_t refile_seq) {
            Bucket& bucket = GetOrCreate(rank);
            bucket.groups.push_back(group);
            bucket.group_members += group.member_count();
            if (refile_seq != 0) {
                bucket.last_refile_seq = refile_seq;
            }
        }

        void Remove(Rank rank, Group& group) {
            if (hot_bucket_ != nullptr && hot_rank_ == rank) {
                hot_bucket_->groups.erase(
                    hot_bucket_->groups.iterator_to(group));
                hot_bucket_->group_members -= group.member_count();
                if (hot_bucket_->Empty()) {
                    hot_bucket_ = nullptr;
                    buckets_.erase(rank);
                    ShrinkBucketsIfSparse(buckets_);
                }
                return;
            }
            const auto it = buckets_.find(rank);
            it->second.groups.erase(it->second.groups.iterator_to(group));
            it->second.group_members -= group.member_count();
            DropIfEmpty(it);
        }

        void AddGroupMember(Rank rank) { ++LookupExisting(rank).group_members; }

        void RemoveGroupMember(Rank rank) {
            --LookupExisting(rank).group_members;
        }

        void ReadMatching(Rank rank, uint64_t born_cutoff,
                          uint64_t min_refile_seq, Copy& out) const {
            const Bucket* bucket = Find(rank);
            if (bucket == nullptr) {
                return;
            }
            for (const route::KeySlot& slot : bucket->keys) {
                const route::SlotOwnerState& owner = slot.owner_state();
                if (owner.eviction_born_seq <= born_cutoff &&
                    owner.eviction_refile_seq >= min_refile_seq) {
                    out.StartEntry(rank, owner.eviction_identity);
                    out.AddKey(slot.key());
                }
            }
            for (const Group& group : bucket->groups) {
                if (group.born_seq() <= born_cutoff &&
                    group.refile_seq() >= min_refile_seq) {
                    out.StartEntry(rank,
                                   kGroupIdentityBit | group.incarnation());
                    group.AppendKeys(out);
                }
            }
        }

        [[nodiscard]] const Map& map() const { return buckets_; }

       private:
        Bucket& GetOrCreate(Rank rank) {
            if (hot_bucket_ != nullptr && hot_rank_ == rank) {
                return *hot_bucket_;
            }
            auto [it, _] = buckets_.try_emplace(rank);
            hot_rank_ = rank;
            hot_bucket_ = &it->second;
            return *hot_bucket_;
        }

        Bucket& LookupExisting(Rank rank) {
            if (hot_bucket_ != nullptr && hot_rank_ == rank) {
                return *hot_bucket_;
            }
            const auto it = buckets_.find(rank);
            hot_rank_ = rank;
            hot_bucket_ = &it->second;
            return *hot_bucket_;
        }

        [[nodiscard]] const Bucket* Find(Rank rank) const {
            if (hot_bucket_ != nullptr && hot_rank_ == rank) {
                return hot_bucket_;
            }
            const auto it = buckets_.find(rank);
            return it == buckets_.end() ? nullptr : &it->second;
        }

        void DropIfEmpty(Map::iterator it) {
            if (it->second.Empty()) {
                if (hot_bucket_ == &it->second) {
                    hot_bucket_ = nullptr;
                }
                buckets_.erase(it);
                ShrinkBucketsIfSparse(buckets_);
            }
        }

        Map buckets_;
        Rank hot_rank_ = std::numeric_limits<Rank>::min();
        Bucket* hot_bucket_ = nullptr;
    };

    struct alignas(128) Shard {
        mutable std::mutex mutex;
        Buckets buckets;
        // Declared after `buckets` so groups unlink before their buckets go.
        std::unordered_map<const Lease*, std::unique_ptr<Group>> groups;
        uint64_t seq = 0;
        uint64_t groups_created = 0;
        size_t size = 0;
    };

    struct WorkUnit {
        Rank rank = 0;
        uint64_t min_refile_seq = 0;
        mutable uint64_t observed_seq = 0;
        size_t key_count = 0;
        uint16_t shard = 0;
    };

    // Walks due (rank, shard) buckets in ascending rank order, either serially
    // via Next() or across parallel workers via VisitRange().
    class ShardedCursor final : public RangeCursor {
       public:
        ShardedCursor(const LeaseEvictionIndex& index, Rank last_rank,
                      size_t target)
            : index_(index), last_rank_(last_rank), target_(target) {
            size_t total = 0;
            for (uint16_t s = 0; s < kShards; ++s) {
                const Shard& shard = index_.shards_[s];
                std::lock_guard<std::mutex> lock(shard.mutex);
                born_cutoff_[s] = shard.seq;
                wave_seq_[s] = shard.seq;
                total += shard.size;
            }
            total_ = total;
        }

        std::optional<Entry> Next() override {
            EnsurePrepared();
            while (pos_ == copy_.size()) {
                if (unit_idx_ == schedule_.size()) {
                    if (!Widen()) {
                        return std::nullopt;
                    }
                }
                pos_ = 0;
                ReadUnit(schedule_[unit_idx_++], copy_);
                examined_.fetch_add(copy_.size(), std::memory_order_relaxed);
            }
            return copy_[pos_++];
        }

        bool StillAt(const route::WriteGuard& guard,
                     const Entry& entry) const override {
            if (!guard.has_object()) {
                return false;
            }
            const route::SlotOwnerState& owner = guard.owner_state();
            if (!owner.eviction_filed ||
                owner.eviction_identity != entry.identity) {
                return false;
            }
            if ((entry.identity & kGroupIdentityBit) == 0 &&
                (guard.generation() & ~kGroupIdentityBit) != entry.identity) {
                return false;
            }
            return RankOf(guard.metadata()) <= entry.rank;
        }

        size_t Total() const override { return total_; }

        uint64_t Limit() const override {
            EnsurePrepared();
            return schedule_.size();
        }

        uint64_t GrainSize() const override { return 1; }

        void VisitRange(uint64_t begin, uint64_t end,
                        const Visitor& visitor) const override {
            EnsurePrepared();
            Copy local;
            uint64_t inspected = 0;
            for (uint64_t i = begin; i < end; ++i) {
                ReadUnit(schedule_[i], local);
                inspected += local.size();
                for (size_t e = 0; e < local.size(); ++e) {
                    if (!visitor(local[e])) {
                        examined_.fetch_add(inspected,
                                            std::memory_order_relaxed);
                        return;
                    }
                }
            }
            examined_.fetch_add(inspected, std::memory_order_relaxed);
        }

        void SetTarget(size_t target) override {
            target_ = target;
            prepared_.store(false, std::memory_order_relaxed);
        }

        bool Widen() override {
            EnsurePrepared();

            // Record the shard sequence each visited bucket observed so only
            // entries refiled after that snapshot are revisited.
            std::array<std::unordered_map<Rank, uint64_t>, kShards> observed;
            for (const WorkUnit& unit : schedule_) {
                if (unit.observed_seq != 0) {
                    observed[unit.shard][unit.rank] = unit.observed_seq;
                }
            }

            const Rank prev_band = band_rank_;
            band_rank_ = last_rank_;
            schedule_.clear();
            unit_idx_ = 0;

            for (uint16_t s = 0; s < kShards; ++s) {
                const Shard& shard = index_.shards_[s];
                std::lock_guard<std::mutex> lock(shard.mutex);
                const uint64_t prev_wave_seq = wave_seq_[s];
                wave_seq_[s] = shard.seq;
                if (prev_band == last_rank_ && shard.seq == prev_wave_seq) {
                    continue;
                }
                for (const auto& [rank, bucket] : shard.buckets.map()) {
                    if (rank > last_rank_) {
                        continue;
                    }
                    if (rank > prev_band) {
                        schedule_.push_back(
                            WorkUnit{.rank = rank,
                                     .min_refile_seq = 0,
                                     .observed_seq = 0,
                                     .key_count = bucket.TotalKeys(),
                                     .shard = s});
                        continue;
                    }
                    uint64_t floor = prev_wave_seq;
                    const auto it = observed[s].find(rank);
                    if (it != observed[s].end()) {
                        floor = it->second;
                    }
                    if (bucket.last_refile_seq > floor) {
                        schedule_.push_back(
                            WorkUnit{.rank = rank,
                                     .min_refile_seq = floor + 1,
                                     .observed_seq = 0,
                                     .key_count = 0,
                                     .shard = s});
                    }
                }
            }

            SortSchedule();
            if (prev_band < std::numeric_limits<Rank>::max()) {
                CapScheduleToTarget(prev_band + 1);
            }
            return !schedule_.empty();
        }

        uint64_t Examined() const override {
            return examined_.load(std::memory_order_relaxed);
        }

       private:
        void EnsurePrepared() const {
            if (prepared_.load(std::memory_order_acquire)) {
                return;
            }
            std::lock_guard<std::mutex> lock(prepare_mutex_);
            if (prepared_.load(std::memory_order_relaxed)) {
                return;
            }
            BuildFirstWave();
            prepared_.store(true, std::memory_order_release);
        }

        Rank ChooseBandRank() const {
            if (target_ == 0 || total_ <= kBandThresholdKeys ||
                target_ >= total_ / 2) {
                return last_rank_;
            }
            constexpr size_t kSampleShards = 4;
            constexpr size_t kStep = kShards / kSampleShards;
            std::vector<std::pair<Rank, size_t>> sample;
            for (size_t i = 0; i < kSampleShards; ++i) {
                const Shard& shard = index_.shards_[i * kStep];
                std::lock_guard<std::mutex> lock(shard.mutex);
                for (const auto& [rank, bucket] : shard.buckets.map()) {
                    if (rank <= last_rank_) {
                        sample.emplace_back(rank, bucket.TotalKeys());
                    }
                }
            }
            if (sample.empty()) {
                return last_rank_;
            }
            std::sort(
                sample.begin(), sample.end(),
                [](const auto& a, const auto& b) { return a.first < b.first; });
            const size_t wanted = target_ * 2 + 65'536;
            size_t accumulated = 0;
            for (const auto& [rank, count] : sample) {
                accumulated += count * kStep;
                if (accumulated >= wanted) {
                    return rank;
                }
            }
            return last_rank_;
        }

        void BuildFirstWave() const {
            schedule_.clear();
            unit_idx_ = 0;
            pos_ = 0;
            copy_.Clear();

            band_rank_ = ChooseBandRank();
            size_t scheduled_keys = 0;
            for (uint16_t s = 0; s < kShards; ++s) {
                const Shard& shard = index_.shards_[s];
                std::lock_guard<std::mutex> lock(shard.mutex);
                wave_seq_[s] = shard.seq;
                for (const auto& [rank, bucket] : shard.buckets.map()) {
                    if (rank <= band_rank_) {
                        const size_t keys = bucket.TotalKeys();
                        schedule_.push_back(WorkUnit{.rank = rank,
                                                     .min_refile_seq = 0,
                                                     .observed_seq = 0,
                                                     .key_count = keys,
                                                     .shard = s});
                        scheduled_keys += keys;
                    }
                }
            }

            // If sampled shards over-estimated density, include the remaining
            // due ranks immediately rather than paying an extra wave.
            if (band_rank_ < last_rank_ && scheduled_keys < target_) {
                const Rank prev_band = band_rank_;
                band_rank_ = last_rank_;
                for (uint16_t s = 0; s < kShards; ++s) {
                    const Shard& shard = index_.shards_[s];
                    std::lock_guard<std::mutex> lock(shard.mutex);
                    wave_seq_[s] = shard.seq;
                    for (const auto& [rank, bucket] : shard.buckets.map()) {
                        if (rank > prev_band && rank <= last_rank_) {
                            schedule_.push_back(
                                WorkUnit{.rank = rank,
                                         .min_refile_seq = 0,
                                         .observed_seq = 0,
                                         .key_count = bucket.TotalKeys(),
                                         .shard = s});
                        }
                    }
                }
            }

            SortSchedule();
            CapScheduleToTarget(std::numeric_limits<Rank>::min());
        }

        // Truncates `schedule_` at the earliest complete rank (>= `min_rank`)
        // whose cumulative fresh keys reach `target_`, so parallel workers do
        // not advance into newer rank buckets while an older rank has enough
        // candidates. If skips leave the wave short, `Widen()` advances past
        // `band_rank_`.
        void CapScheduleToTarget(Rank min_rank) const {
            if (target_ == 0 || schedule_.empty()) {
                return;
            }
            size_t accumulated = 0;
            size_t i = 0;
            while (i < schedule_.size()) {
                const Rank rank = schedule_[i].rank;
                while (i < schedule_.size() && schedule_[i].rank == rank) {
                    if (schedule_[i].min_refile_seq == 0) {
                        accumulated += schedule_[i].key_count;
                    }
                    ++i;
                }
                if (rank >= min_rank && accumulated >= target_) {
                    schedule_.resize(i);
                    band_rank_ = rank;
                    return;
                }
            }
        }

        void SortSchedule() const {
            std::sort(schedule_.begin(), schedule_.end(),
                      [](const WorkUnit& a, const WorkUnit& b) {
                          return a.rank != b.rank ? a.rank < b.rank
                                                  : a.shard < b.shard;
                      });
        }

        void ReadUnit(const WorkUnit& unit, Copy& out) const {
            out.Clear();
            const Shard& shard = index_.shards_[unit.shard];
            {
                std::lock_guard<std::mutex> lock(shard.mutex);
                unit.observed_seq = shard.seq;
                shard.buckets.ReadMatching(unit.rank, born_cutoff_[unit.shard],
                                           unit.min_refile_seq, out);
            }
            out.Seal();
        }

        const LeaseEvictionIndex& index_;
        const Rank last_rank_;
        size_t target_ = 0;
        size_t total_ = 0;
        std::array<uint64_t, kShards> born_cutoff_{};
        mutable std::array<uint64_t, kShards> wave_seq_{};
        mutable std::mutex prepare_mutex_;
        mutable std::atomic<bool> prepared_{false};
        mutable Rank band_rank_ = 0;
        mutable std::vector<WorkUnit> schedule_;
        mutable size_t unit_idx_ = 0;
        mutable Copy copy_;
        mutable size_t pos_ = 0;
        mutable std::atomic<uint64_t> examined_{0};
    };

    struct ThreadShardHint {
        uint64_t index_id = 0;
        uint16_t shard = 0;
        uint16_t batch = 0;
    };

    std::unique_lock<std::mutex> LockStandaloneShard(
        uint16_t& chosen_shard) const {
        thread_local ThreadShardHint hint;
        if (hint.index_id != id_) {
            const uint16_t start =
                next_hint_shard_.fetch_add(7, std::memory_order_relaxed) &
                kShardMask;
            hint = ThreadShardHint{id_, start, 0};
        }
        if (hint.batch >= kShardBatch) {
            hint.shard = static_cast<uint16_t>((hint.shard + 1) & kShardMask);
            hint.batch = 0;
        }
        for (uint16_t probe = 0; probe < 8; ++probe) {
            const uint16_t s =
                static_cast<uint16_t>((hint.shard + probe) & kShardMask);
            std::unique_lock<std::mutex> lock(shards_[s].mutex,
                                              std::try_to_lock);
            if (lock.owns_lock()) {
                if (probe != 0) {
                    hint.shard = s;
                    hint.batch = 0;
                }
                ++hint.batch;
                chosen_shard = s;
                return lock;
            }
        }
        const uint16_t s = static_cast<uint16_t>((hint.shard + 8) & kShardMask);
        hint.shard = s;
        hint.batch = 1;
        chosen_shard = s;
        return std::unique_lock<std::mutex>(shards_[s].mutex);
    }

    static uint16_t GroupShardFor(const Lease* lease) {
        const auto addr =
            static_cast<uint64_t>(reinterpret_cast<uintptr_t>(lease));
        return static_cast<uint16_t>((addr * 11400714819323198485ULL >> 55) &
                                     kShardMask);
    }

    static Rank RankOf(const ObjectMetadata& metadata) {
        return BucketOf(metadata.EvictionDeadline());
    }

    static Rank BucketOf(Clock::time_point deadline) {
        const auto since_epoch = deadline.time_since_epoch();
        Rank bucket = since_epoch / kBucketWidth;
        if (since_epoch < Clock::duration::zero() &&
            since_epoch % kBucketWidth != Clock::duration::zero()) {
            --bucket;
        }
        return bucket;
    }

    static Group* GroupOf(const route::SlotOwnerState& owner) {
        return static_cast<Group*>(owner.eviction_group);
    }

    static const Lease* GroupLeaseOf(const ObjectMetadata& metadata) {
        if (!metadata.IsGrouped()) {
            return nullptr;
        }
        SpinLocker locker(&metadata.lock);
        return metadata.lease_.get();
    }

    static bool IsSameFiledEntry(const route::SlotOwnerState& owner,
                                 const Lease* group_lease,
                                 uint64_t standalone_id) {
        if (!owner.eviction_filed) {
            return false;
        }
        const Group* const filed_group = GroupOf(owner);
        if (group_lease == nullptr) {
            return filed_group == nullptr &&
                   owner.eviction_identity == standalone_id;
        }
        return filed_group != nullptr && filed_group->lease() == group_lease;
    }

    void Unfile(route::KeySlot& slot, route::SlotOwnerState& owner) {
        Shard& shard = shards_[owner.eviction_shard];
        std::lock_guard<std::mutex> lock(shard.mutex);
        --shard.size;
        Group* const group = GroupOf(owner);
        const Rank old_rank = owner.eviction_rank;
        owner.ClearEviction();
        if (group == nullptr) {
            shard.buckets.Remove(old_rank, slot);
        } else if (group->Leave(slot)) {
            shard.groups.erase(shard.groups.find(group->lease()));
        }
    }

    void UpdateRankInPlace(route::KeySlot& slot, route::SlotOwnerState& owner,
                           Rank rank) {
        Shard& shard = shards_[owner.eviction_shard];
        std::lock_guard<std::mutex> lock(shard.mutex);
        Group* const filed_group = GroupOf(owner);
        if (filed_group == nullptr) {
            shard.buckets.Remove(owner.eviction_rank, slot);
            const uint64_t seq = ++shard.seq;
            owner.eviction_rank = rank;
            owner.eviction_refile_seq = seq;
            shard.buckets.Add(rank, slot, seq);
        } else {
            filed_group->MoveTo(rank, shard.seq);
            owner.eviction_rank = rank;
        }
    }

    void FileStandalone(route::KeySlot& slot, route::SlotOwnerState& owner,
                        Rank rank, uint64_t standalone_id) {
        uint16_t chosen_shard = 0;
        std::unique_lock<std::mutex> lock = LockStandaloneShard(chosen_shard);
        Shard& shard = shards_[chosen_shard];
        ++shard.size;
        const uint64_t born = ++shard.seq;
        shard.buckets.Add(rank, slot, /*refile_seq=*/0);
        owner.FileStandalone(chosen_shard, rank, standalone_id, born);
    }

    void FileInGroup(route::KeySlot& slot, route::SlotOwnerState& owner,
                     const ObjectMetadata& metadata, const Lease* group_lease,
                     Rank rank) {
        const uint16_t group_shard = GroupShardFor(group_lease);
        Shard& shard = shards_[group_shard];
        std::lock_guard<std::mutex> lock(shard.mutex);
        ++shard.size;
        auto it = shard.groups.find(group_lease);
        if (it == shard.groups.end()) {
            std::shared_ptr<const Lease> held_lease;
            {
                SpinLocker locker(&metadata.lock);
                held_lease = metadata.lease_;
            }
            const uint64_t incarnation =
                (static_cast<uint64_t>(group_shard) << 48) |
                (++shard.groups_created);
            const uint64_t born = ++shard.seq;
            it = shard.groups
                     .emplace(group_lease,
                              std::make_unique<Group>(std::move(held_lease),
                                                      incarnation, born, rank,
                                                      shard.buckets))
                     .first;
        } else {
            it->second->MoveTo(rank, shard.seq);
        }
        it->second->Join(slot);
        owner.FileGroup(group_shard, rank,
                        kGroupIdentityBit | it->second->incarnation(),
                        it->second.get());
    }

    static inline std::atomic<uint64_t> next_index_id_{1};

    const uint64_t id_;
    mutable std::atomic<uint16_t> next_hint_shard_{0};
    mutable std::array<Shard, kShards> shards_;
};

}  // namespace metadata
}  // namespace mooncake
