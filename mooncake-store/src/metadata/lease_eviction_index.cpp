#include "metadata/lease_eviction_index.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <limits>
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
#include "mutex.h"

namespace mooncake {
namespace metadata {
namespace {

using Rank = EvictionIndex::Rank;
using Entry = EvictionIndex::Entry;
using SlotList = route::KeySlot::EvictionList;

constexpr uint16_t kShardMask =
    static_cast<uint16_t>(LeaseEvictionIndex::kShards - 1);
// Odd step coprime with power-of-two `kShards`, spreading new writer threads.
constexpr uint16_t kWriterSpreadStep = 7;
// Consecutive standalone insertions on one shard before rotating, keeping its
// cacheline and 1-entry `hot_bucket_` warm while preserving single-thread
// order.
constexpr uint16_t kWriterBatchSize = 512;
// Non-blocking `try_to_lock` hops before falling back to a blocking lock.
constexpr uint16_t kContentionProbes = 8;
// 64-bit Fibonacci hash multiplier (~2^64 / phi) for pointer hashing.
constexpr uint64_t kFibonacciMultiplier = 11400714819323198485ULL;

// Sampled age-band cutoff activates only above this population size.
constexpr size_t kBandThresholdKeys = 262'144;
constexpr size_t kSampleShards = 4;

// Encodes and checks `Entry::identity` values so bit layout stays in one place:
// - Bit 63 == 0: standalone publication (`KeySlot::generation` masked to 63b).
// - Bit 63 == 1: group incarnation (`shard_id` in bits 48..62 + per-shard seq).
class EntryIdentity {
   public:
    static uint64_t ForStandalone(route::Generation generation) {
        return generation & ~kGroupBit;
    }

    static uint64_t ForGroup(uint16_t shard_id, uint64_t shard_group_seq) {
        return kGroupBit | (static_cast<uint64_t>(shard_id) << 48) |
               shard_group_seq;
    }

    static bool IsGroup(uint64_t identity) {
        return (identity & kGroupBit) != 0;
    }

    static bool MatchesSlot(uint64_t entry_identity,
                            const route::WriteGuard& guard) {
        const route::SlotEvictionState& state = guard.owner_state().eviction;
        if (!state.filed || state.identity != entry_identity) {
            return false;
        }
        if (!IsGroup(entry_identity) &&
            ForStandalone(guard.generation()) != entry_identity) {
            return false;
        }
        return true;
    }

   private:
    static constexpr uint64_t kGroupBit = uint64_t{1} << 63;
};

// One shard bucket copied under the shard lock: key bytes are packed back to
// back so copying a bucket takes one contiguous string buffer per worker.
class CopiedBucket {
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
        const size_t end =
            i + 1 < entries_.size() ? entries_[i + 1].first_key : keys_.size();
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

class Buckets;

// The filed members of one group inside its shard, linked into `buckets` at
// the latest deadline rank seen across its members. `born_seq_` belongs to the
// group incarnation rather than individual members: if a new key joins an
// existing group after a scan starts, either it extends the shared group lease
// (moving the group out of due buckets, and failing StillAt/IsLeaseExpired if
// the bucket was already copied) or the shared lease remains expired and the
// whole group is still visited together without stranding the new member.
class Group {
   public:
    Group(std::shared_ptr<const Lease> lease, uint64_t identity,
          uint64_t born_seq, Rank rank, Buckets& buckets);
    ~Group();
    Group(const Group&) = delete;
    Group& operator=(const Group&) = delete;

    [[nodiscard]] const Lease* lease() const { return lease_.get(); }
    [[nodiscard]] uint64_t identity() const { return identity_; }
    [[nodiscard]] uint64_t born_seq() const { return born_seq_; }
    [[nodiscard]] uint64_t refile_seq() const { return refile_seq_; }
    [[nodiscard]] size_t member_count() const { return members_.size(); }

    void MoveTo(Rank rank, uint64_t& shard_seq);
    void Join(route::KeySlot& slot);
    [[nodiscard]] bool Leave(route::KeySlot& slot);

    void AppendKeys(CopiedBucket& out) const {
        for (const route::KeySlot& slot : members_) {
            out.AddKey(slot.key());
        }
    }

   private:
    using Hook = boost::intrusive::list_member_hook<
        boost::intrusive::link_mode<boost::intrusive::safe_link>>;

    const std::shared_ptr<const Lease> lease_;
    const uint64_t identity_;
    const uint64_t born_seq_;
    uint64_t refile_seq_ = 0;
    Rank rank_;
    Buckets& buckets_;
    SlotList members_;
    Hook hook_;

   public:
    using List = boost::intrusive::list<
        Group, boost::intrusive::member_hook<Group, Hook, &Group::hook_>>;
};

// Per-shard hash table of non-empty 100ms buckets, with a 1-entry hot cache so
// consecutive writes in the same tick bypass hash lookup.
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
            hot_bucket_->groups.erase(hot_bucket_->groups.iterator_to(group));
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

    void RemoveGroupMember(Rank rank) { --LookupExisting(rank).group_members; }

    void ReadMatching(Rank rank, uint64_t born_cutoff, uint64_t min_refile_seq,
                      CopiedBucket& out) const {
        const Bucket* bucket = Find(rank);
        if (bucket == nullptr) {
            return;
        }
        for (const route::KeySlot& slot : bucket->keys) {
            const route::SlotEvictionState& state = slot.owner_state().eviction;
            if (state.born_seq <= born_cutoff &&
                state.refile_seq >= min_refile_seq) {
                out.StartEntry(rank, state.identity);
                out.AddKey(slot.key());
            }
        }
        for (const Group& group : bucket->groups) {
            if (group.born_seq() <= born_cutoff &&
                group.refile_seq() >= min_refile_seq) {
                out.StartEntry(rank, group.identity());
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

Group::Group(std::shared_ptr<const Lease> lease, uint64_t identity,
             uint64_t born_seq, Rank rank, Buckets& buckets)
    : lease_(std::move(lease)),
      identity_(identity),
      born_seq_(born_seq),
      rank_(rank),
      buckets_(buckets) {
    buckets_.Add(rank_, *this, /*refile_seq=*/0);
}

Group::~Group() { buckets_.Remove(rank_, *this); }

void Group::MoveTo(Rank rank, uint64_t& shard_seq) {
    if (rank == rank_) {
        return;
    }
    const uint64_t refile_seq = ++shard_seq;
    buckets_.Remove(rank_, *this);
    rank_ = rank;
    refile_seq_ = refile_seq;
    buckets_.Add(rank_, *this, refile_seq);
}

void Group::Join(route::KeySlot& slot) {
    members_.push_back(slot);
    buckets_.AddGroupMember(rank_);
}

bool Group::Leave(route::KeySlot& slot) {
    members_.erase(members_.iterator_to(slot));
    buckets_.RemoveGroupMember(rank_);
    return members_.empty();
}

const Lease* GroupLeaseOf(const ObjectMetadata& metadata) {
    if (!metadata.IsGrouped()) {
        return nullptr;
    }
    SpinLocker locker(&metadata.lock);
    return metadata.lease_.get();
}

// Per-WorkUnit snapshot sequence recorded when a worker copies the bucket, used
// by a later `Widen()` wave to revisit only entries refiled after that copy.
class ObservedSeq {
   public:
    ObservedSeq() = default;
    ObservedSeq(const ObservedSeq& other) noexcept : value_(other.Load()) {}
    ObservedSeq& operator=(const ObservedSeq& other) noexcept {
        Store(other.Load());
        return *this;
    }

    [[nodiscard]] uint64_t Load() const noexcept {
        return value_.load(std::memory_order_relaxed);
    }
    void Store(uint64_t seq) const noexcept {
        value_.store(seq, std::memory_order_relaxed);
    }

   private:
    mutable std::atomic<uint64_t> value_{0};
};

struct WorkUnit {
    Rank rank = 0;
    uint64_t min_refile_seq = 0;
    ObservedSeq observed_seq{};
    size_t key_count = 0;
    uint16_t shard = 0;
};

struct ThreadShardHint {
    uint64_t index_id = 0;
    uint16_t shard = 0;
    uint16_t batch = 0;
};

}  // namespace

// --- Single-shard storage and mutations -------------------------------------

struct alignas(128) LeaseEvictionIndex::Shard {
    mutable std::mutex mutex;
    Buckets buckets;
    // Declared after `buckets` so groups unlink before their buckets go.
    std::unordered_map<const Lease*, std::unique_ptr<Group>> groups;
    uint64_t seq = 0;
    uint64_t groups_created = 0;
    size_t size = 0;

    static Group* GroupOf(const route::SlotEvictionState& state) {
        return static_cast<Group*>(state.group);
    }

    static bool IsSameFiledEntry(const route::SlotEvictionState& state,
                                 const Lease* group_lease,
                                 uint64_t standalone_id) {
        if (!state.filed) {
            return false;
        }
        const Group* const filed_group = GroupOf(state);
        if (group_lease == nullptr) {
            return filed_group == nullptr && state.identity == standalone_id;
        }
        return filed_group != nullptr && filed_group->lease() == group_lease;
    }

    void FileStandalone(uint16_t shard_id, route::KeySlot& slot,
                        route::SlotEvictionState& state, Rank rank,
                        uint64_t standalone_id) {
        ++size;
        const uint64_t born = ++seq;
        buckets.Add(rank, slot, /*refile_seq=*/0);
        state.FileStandalone(shard_id, rank, standalone_id, born);
    }

    void FileInGroup(uint16_t shard_id, route::KeySlot& slot,
                     route::SlotEvictionState& state,
                     const ObjectMetadata& metadata, const Lease* group_lease,
                     Rank rank) {
        ++size;
        auto it = groups.find(group_lease);
        if (it == groups.end()) {
            std::shared_ptr<const Lease> held_lease;
            {
                SpinLocker locker(&metadata.lock);
                held_lease = metadata.lease_;
            }
            const uint64_t identity =
                EntryIdentity::ForGroup(shard_id, ++groups_created);
            const uint64_t born = ++seq;
            it = groups
                     .emplace(group_lease, std::make_unique<Group>(
                                               std::move(held_lease), identity,
                                               born, rank, buckets))
                     .first;
        } else {
            it->second->MoveTo(rank, seq);
        }
        it->second->Join(slot);
        state.FileGroup(shard_id, rank, it->second->identity(),
                        it->second.get());
    }

    void UpdateRank(route::KeySlot& slot, route::SlotEvictionState& state,
                    Rank rank) {
        Group* const filed_group = GroupOf(state);
        if (filed_group == nullptr) {
            buckets.Remove(state.rank, slot);
            const uint64_t refile_seq = ++seq;
            state.rank = rank;
            state.refile_seq = refile_seq;
            buckets.Add(rank, slot, refile_seq);
        } else {
            filed_group->MoveTo(rank, seq);
            state.rank = rank;
        }
    }

    void Unfile(route::KeySlot& slot, route::SlotEvictionState& state) {
        --size;
        Group* const filed_group = GroupOf(state);
        const Rank old_rank = state.rank;
        state.Clear();
        if (filed_group == nullptr) {
            buckets.Remove(old_rank, slot);
        } else if (filed_group->Leave(slot)) {
            groups.erase(groups.find(filed_group->lease()));
        }
    }
};

struct LeaseEvictionIndex::LockedShard {
    uint16_t id;
    Shard& shard;
    std::unique_lock<std::mutex> lock;
};

// --- Wave-scheduled cursor --------------------------------------------------

// Walks due (rank, shard) buckets in ascending rank order, either serially via
// Next() or across parallel workers via VisitRange(). The initial wave schedule
// is built eagerly at construction; Widen() runs only after workers join.
class LeaseEvictionIndex::ShardedCursor final : public RangeCursor {
   public:
    ShardedCursor(const LeaseEvictionIndex& index, Rank last_rank,
                  const TargetForTotal& target_for_total)
        : index_(index), last_rank_(last_rank) {
        size_t total = 0;
        for (uint16_t s = 0; s < kShards; ++s) {
            const Shard& shard = index_.shards_[s];
            std::lock_guard<std::mutex> lock(shard.mutex);
            born_cutoff_[s] = shard.seq;
            wave_seq_[s] = shard.seq;
            total += shard.size;
        }
        total_ = total;
        target_ = target_for_total ? target_for_total(total_) : 0;
        BuildFirstWave();
    }

    std::optional<Entry> Next() override {
        while (pos_ == serial_copy_.size()) {
            if (unit_idx_ == schedule_.size()) {
                if (!Widen()) {
                    return std::nullopt;
                }
            }
            pos_ = 0;
            ReadUnit(schedule_[unit_idx_++], serial_copy_);
            examined_.fetch_add(serial_copy_.size(), std::memory_order_relaxed);
        }
        return serial_copy_[pos_++];
    }

    bool StillAt(const route::WriteGuard& guard,
                 const Entry& entry) const override {
        if (!guard.has_object() ||
            !EntryIdentity::MatchesSlot(entry.identity, guard)) {
            return false;
        }
        return RankOf(guard.metadata()) <= entry.rank;
    }

    size_t Total() const override { return total_; }

    uint64_t Limit() const override { return schedule_.size(); }

    uint64_t GrainSize() const override { return 1; }

    void VisitRange(uint64_t begin, uint64_t end,
                    const Visitor& visitor) const override {
        CopiedBucket local;
        uint64_t inspected = 0;
        for (uint64_t i = begin; i < end; ++i) {
            ReadUnit(schedule_[i], local);
            inspected += local.size();
            for (size_t e = 0; e < local.size(); ++e) {
                if (!visitor(local[e])) {
                    examined_.fetch_add(inspected, std::memory_order_relaxed);
                    return;
                }
            }
        }
        examined_.fetch_add(inspected, std::memory_order_relaxed);
    }

    bool Widen() override {
        if (total_ == 0) {
            return false;
        }

        // Record the shard sequence each visited bucket observed so only
        // entries refiled after that snapshot are revisited.
        std::array<std::unordered_map<Rank, uint64_t>, kShards> observed;
        for (const WorkUnit& unit : schedule_) {
            const uint64_t seq = unit.observed_seq.Load();
            if (seq != 0) {
                observed[unit.shard][unit.rank] = seq;
            }
        }

        const Rank prev_band = band_rank_;
        band_rank_ = last_rank_;
        schedule_.clear();
        unit_idx_ = 0;
        pos_ = 0;
        serial_copy_.Clear();

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
                    schedule_.push_back(WorkUnit{.rank = rank,
                                                 .min_refile_seq = floor + 1,
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
    Rank ChooseBandRank() const {
        if (target_ == 0 || total_ <= kBandThresholdKeys ||
            target_ >= total_ / 2) {
            return last_rank_;
        }
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

    void BuildFirstWave() {
        band_rank_ = last_rank_;
        if (total_ == 0) {
            return;
        }

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
                                                 .key_count = keys,
                                                 .shard = s});
                    scheduled_keys += keys;
                }
            }
        }

        // If sampled shards over-estimated density, include the remaining due
        // ranks immediately rather than paying an extra wave.
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
    // whose cumulative fresh keys reach `target_`, so parallel workers do not
    // advance into newer rank buckets while an older rank has enough
    // candidates. If skips leave the wave short, `Widen()` advances past
    // `band_rank_`.
    void CapScheduleToTarget(Rank min_rank) {
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

    void SortSchedule() {
        std::sort(schedule_.begin(), schedule_.end(),
                  [](const WorkUnit& a, const WorkUnit& b) {
                      return a.rank != b.rank ? a.rank < b.rank
                                              : a.shard < b.shard;
                  });
    }

    void ReadUnit(const WorkUnit& unit, CopiedBucket& out) const {
        out.Clear();
        const Shard& shard = index_.shards_[unit.shard];
        {
            std::lock_guard<std::mutex> lock(shard.mutex);
            unit.observed_seq.Store(shard.seq);
            shard.buckets.ReadMatching(unit.rank, born_cutoff_[unit.shard],
                                       unit.min_refile_seq, out);
        }
        out.Seal();
    }

    const LeaseEvictionIndex& index_;
    const Rank last_rank_;
    size_t total_ = 0;
    size_t target_ = 0;
    std::array<uint64_t, kShards> born_cutoff_{};
    std::array<uint64_t, kShards> wave_seq_{};
    Rank band_rank_ = 0;
    std::vector<WorkUnit> schedule_;
    size_t unit_idx_ = 0;
    CopiedBucket serial_copy_;
    size_t pos_ = 0;
    mutable std::atomic<uint64_t> examined_{0};
};

// --- LeaseEvictionIndex public contract & shard routing ---------------------

std::atomic<uint64_t> LeaseEvictionIndex::next_index_id_{1};

LeaseEvictionIndex::LeaseEvictionIndex()
    : EvictionIndex(/*observes_reads=*/false),
      id_(next_index_id_.fetch_add(1, std::memory_order_relaxed)),
      shards_(std::make_unique<Shard[]>(kShards)) {
    assert(reinterpret_cast<uintptr_t>(shards_.get()) % alignof(Shard) == 0);
}

LeaseEvictionIndex::~LeaseEvictionIndex() = default;

bool LeaseEvictionIndex::Qualifies(const ObjectMetadata& metadata) {
    return !metadata.IsHardPinned() &&
           metadata.HasReplica([](const Replica& replica) {
               return replica.is_memory_replica() && replica.is_completed();
           });
}

void LeaseEvictionIndex::OnRead(const route::ReadGuard&) {}

void LeaseEvictionIndex::OnWrite(const route::WriteGuard& guard) {
    route::SlotEvictionState& state = guard.owner_state().eviction;
    if (!guard.has_object() || !Qualifies(guard.metadata())) {
        if (state.filed) {
            LockRecordedShard(state).shard.Unfile(guard.slot(), state);
        }
        return;
    }

    const ObjectMetadata& metadata = guard.metadata();
    const Rank rank = RankOf(metadata);
    const Lease* const group_lease = GroupLeaseOf(metadata);
    const uint64_t standalone_id =
        EntryIdentity::ForStandalone(guard.generation());

    if (Shard::IsSameFiledEntry(state, group_lease, standalone_id)) {
        if (rank != state.rank) {
            LockRecordedShard(state).shard.UpdateRank(guard.slot(), state,
                                                      rank);
        }
        return;
    }

    if (state.filed) {
        LockRecordedShard(state).shard.Unfile(guard.slot(), state);
    }

    if (group_lease == nullptr) {
        LockedShard target = LockAnyShardForStandalone();
        target.shard.FileStandalone(target.id, guard.slot(), state, rank,
                                    standalone_id);
    } else {
        LockedShard target = LockShardForGroup(group_lease);
        target.shard.FileInGroup(target.id, guard.slot(), state, metadata,
                                 group_lease, rank);
    }
}

std::unique_ptr<EvictionIndex::Cursor> LeaseEvictionIndex::Scan(
    Clock::time_point now) const {
    return ScanRanges(now, /*target=*/0);
}

std::unique_ptr<EvictionIndex::RangeCursor> LeaseEvictionIndex::ScanRanges(
    Clock::time_point now, const TargetForTotal& target_for_total) const {
    return std::make_unique<ShardedCursor>(*this, BucketOf(now),
                                           target_for_total);
}

LeaseEvictionIndex::LockedShard LeaseEvictionIndex::LockAnyShardForStandalone()
    const {
    thread_local ThreadShardHint hint;
    if (hint.index_id != id_) {
        const uint16_t start =
            next_writer_shard_.fetch_add(kWriterSpreadStep,
                                         std::memory_order_relaxed) &
            kShardMask;
        hint = ThreadShardHint{id_, start, 0};
    }
    if (hint.batch >= kWriterBatchSize) {
        hint.shard = static_cast<uint16_t>((hint.shard + 1) & kShardMask);
        hint.batch = 0;
    }
    for (uint16_t probe = 0; probe < kContentionProbes; ++probe) {
        const uint16_t s =
            static_cast<uint16_t>((hint.shard + probe) & kShardMask);
        std::unique_lock<std::mutex> lock(shards_[s].mutex, std::try_to_lock);
        if (lock.owns_lock()) {
            if (probe != 0) {
                hint.shard = s;
                hint.batch = 0;
            }
            ++hint.batch;
            return LockedShard{s, shards_[s], std::move(lock)};
        }
    }
    const uint16_t s =
        static_cast<uint16_t>((hint.shard + kContentionProbes) & kShardMask);
    hint.shard = s;
    hint.batch = 1;
    return LockedShard{s, shards_[s],
                       std::unique_lock<std::mutex>(shards_[s].mutex)};
}

LeaseEvictionIndex::LockedShard LeaseEvictionIndex::LockShardForGroup(
    const Lease* group_lease) const {
    const uint16_t s = ShardForGroupLease(group_lease);
    return LockedShard{s, shards_[s],
                       std::unique_lock<std::mutex>(shards_[s].mutex)};
}

LeaseEvictionIndex::LockedShard LeaseEvictionIndex::LockRecordedShard(
    const route::SlotEvictionState& state) const {
    const uint16_t s = state.shard;
    return LockedShard{s, shards_[s],
                       std::unique_lock<std::mutex>(shards_[s].mutex)};
}

uint16_t LeaseEvictionIndex::ShardForGroupLease(const Lease* group_lease) {
    // Discard low alignment zeros by taking the top `kShardBits` of the 64-bit
    // Fibonacci hash of the shared Lease address.
    const auto addr =
        static_cast<uint64_t>(reinterpret_cast<uintptr_t>(group_lease));
    return static_cast<uint16_t>((addr * kFibonacciMultiplier) >>
                                 (64 - kShardBits));
}

// By EvictionOrder(), which differs from the deadline only for an object
// restored from a standby and not read or written since; a read or write
// only moves it later.
Rank LeaseEvictionIndex::RankOf(const ObjectMetadata& metadata) {
    return BucketOf(metadata.EvictionOrder());
}

Rank LeaseEvictionIndex::BucketOf(Clock::time_point deadline) {
    const auto since_epoch = deadline.time_since_epoch();
    Rank bucket = since_epoch / kBucketWidth;
    if (since_epoch < Clock::duration::zero() &&
        since_epoch % kBucketWidth != Clock::duration::zero()) {
        --bucket;
    }
    return bucket;
}

}  // namespace metadata
}  // namespace mooncake
