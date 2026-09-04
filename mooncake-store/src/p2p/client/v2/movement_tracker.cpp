#include "p2p/client/v2/movement_tracker.h"

#include <algorithm>
#include <utility>

#include <glog/logging.h>

#include "p2p/client/v2/block_registry.h"

namespace mooncake::v2 {

size_t MovementDedupKeyHash::operator()(
    const MovementDedupKey& key) const noexcept {
    // All four fields participate: the pair of tilers is what makes a
    // direction distinct, and the block id is what makes a proposal go stale
    // when the block underneath it is replaced.
    size_t seed = RegistrationIdHash{}(key.registration_id);
    boost::hash_combine(seed, BlockIdHash{}(key.source_block_id));
    boost::hash_combine(seed, boost::hash<UUID>{}(key.source_tiler));
    boost::hash_combine(seed, boost::hash<UUID>{}(key.destination_tiler));
    return seed;
}

const char* ToString(MovementDirection direction) {
    switch (direction) {
        case MovementDirection::kOffload:
            return "offload";
        case MovementDirection::kOnboard:
            return "onboard";
    }
    return "unknown";
}

const char* ToString(MovementRejection rejection) {
    switch (rejection) {
        case MovementRejection::kInflight:
            return "inflight";
        case MovementRejection::kCooldown:
            return "cooldown";
        case MovementRejection::kMinimumResidency:
            return "minimum_residency";
        case MovementRejection::kStopped:
            return "stopped";
        case MovementRejection::kInvalid:
            return "invalid";
    }
    return "unknown";
}

tl::expected<void, ErrorCode> ValidateMovementTrackerConfig(
    const MovementTrackerConfig& config) {
    if (config.shard_count == 0) {
        LOG(ERROR) << "movement_tracker.shard_count must be greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    // Zero is legal for both intervals and means "this gate is off", which is
    // how a benchmark asks the policy to move as soon as it decides to.
    // Negative is not: it would make every key permanently eligible while
    // still looking like a configured limit.
    if (config.cooldown < std::chrono::milliseconds::zero()) {
        LOG(ERROR) << "movement_tracker.cooldown must not be negative, got "
                   << config.cooldown.count();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.minimum_residency < std::chrono::milliseconds::zero()) {
        LOG(ERROR) << "movement_tracker.minimum_residency must not be "
                      "negative, got "
                   << config.minimum_residency.count();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.max_tracked_keys == 0) {
        LOG(ERROR) << "movement_tracker.max_tracked_keys must be greater than "
                      "zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

// ---------------------------------------------------------------------------
// MovementLease
// ---------------------------------------------------------------------------

MovementLease::MovementLease(MovementTracker* tracker, MovementDedupKey key,
                             MovementDirection direction, std::string owner_key,
                             uint64_t lease_id)
    : tracker_(tracker),
      key_(std::move(key)),
      direction_(direction),
      owner_key_(std::move(owner_key)),
      lease_id_(lease_id) {}

MovementLease::MovementLease(MovementLease&& other) noexcept
    : tracker_(other.tracker_),
      key_(other.key_),
      direction_(other.direction_),
      owner_key_(std::move(other.owner_key_)),
      lease_id_(other.lease_id_) {
    // Exactly one object owns the release, and it is now this one. Clearing
    // the source's tracker is what makes the moved-from lease inert instead
    // of a second releaser of the same dedup slot.
    other.tracker_ = nullptr;
}

MovementLease& MovementLease::operator=(MovementLease&& other) noexcept {
    if (this == &other) return *this;
    // Settling the target first is not politeness: overwriting a live lease
    // would drop its dedup slot on the floor, and nothing else in the system
    // ever frees it again.
    if (tracker_ != nullptr) {
        tracker_->Release(key_, direction_, owner_key_, lease_id_,
                          /*moved=*/false);
    }
    tracker_ = other.tracker_;
    key_ = other.key_;
    direction_ = other.direction_;
    owner_key_ = std::move(other.owner_key_);
    lease_id_ = other.lease_id_;
    other.tracker_ = nullptr;
    return *this;
}

MovementLease::~MovementLease() {
    // Reaching here unsettled means the movement never reported an outcome:
    // an early return, a dropped command, a thrown exception. moved=false is
    // the honest answer, and it deliberately starts no cooldown so the next
    // proposal for this key is free to retry immediately.
    if (tracker_ != nullptr) {
        tracker_->Release(key_, direction_, owner_key_, lease_id_,
                          /*moved=*/false);
        tracker_ = nullptr;
    }
}

void MovementLease::Settle(bool moved) {
    // Silent on a second call: a moved-from or already-settled lease is a
    // legal thing to hold, and the executor should not have to track which
    // of its exit paths already ran.
    if (tracker_ == nullptr) return;
    // Cleared before the call, not after: Release must not be reachable twice
    // even if it throws.
    MovementTracker* tracker = tracker_;
    tracker_ = nullptr;
    tracker->Release(key_, direction_, owner_key_, lease_id_, moved);
}

// ---------------------------------------------------------------------------
// MovementTracker
// ---------------------------------------------------------------------------

MovementTracker::MovementTracker(const MovementTrackerConfig& config,
                                 std::shared_ptr<Clock> clock)
    : config_(config), clock_(std::move(clock)) {
    if (clock_ == nullptr) {
        // A missing clock is a wiring bug, not a reason to crash the first
        // acquire. Everything below still reads exactly one time source.
        LOG(ERROR) << "MovementTracker built without a clock; falling back to "
                      "the steady clock";
        clock_ = std::make_shared<SteadyClock>();
    }
    const size_t shard_count = std::max<size_t>(1, config.shard_count);
    shards_.reserve(shard_count);
    for (size_t i = 0; i < shard_count; ++i) {
        shards_.push_back(std::make_unique<Shard>());
    }
}

MovementTracker::~MovementTracker() {
    // A lease holds a raw back-pointer, so an outstanding one at destruction
    // is a use-after-free waiting to happen rather than a leak we can absorb.
    // Said out loud because the owner's Stop order (section 9, invariant 11)
    // is what prevents it. The count is exact: a slot outlives a delete of
    // its key precisely so that this check cannot be fooled by one.
    size_t outstanding = 0;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard->mu);
        outstanding += shard->inflight.size();
    }
    if (outstanding != 0) {
        LOG(ERROR) << "MovementTracker destroyed with " << outstanding
                   << " outstanding lease(s); their settle will dereference a "
                      "destroyed tracker";
    }
}

MovementTracker::Shard& MovementTracker::ShardFor(std::string_view key) {
    return *shards_[StringHash{}(key) % shards_.size()];
}

const MovementTracker::Shard& MovementTracker::ShardFor(
    std::string_view key) const {
    return *shards_[StringHash{}(key) % shards_.size()];
}

uint64_t MovementTracker::NextStamp() {
    return sequence_.fetch_add(1, std::memory_order_relaxed) + 1;
}

tl::expected<MovementLease, MovementRejection> MovementTracker::TryAcquire(
    std::string_view key, const MovementDedupKey& dedup,
    MovementDirection direction) {
    // The order of the checks below is the order the reasons matter to an
    // operator reading the counters, and only the first one that applies is
    // reported: kInvalid means the caller handed over an unusable key,
    // kStopped means the tracker is closed, kInflight means commands are
    // being proposed faster than they execute, and the last two mean the
    // policy is deliberately holding this key back. Reporting a policy reason
    // for a key that is merely already moving would mix those very different
    // stories into one number.
    if (stopped_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(MovementRejection::kStopped);
    }

    // Residency is asked about the tier the block is leaving, and the dedup
    // key names that tier twice. A caller that fills the block id and forgets
    // the copy is answered from the block id rather than losing the gate in
    // silence: an all-zero lookup key matches no commit record, and "no
    // record" deliberately means "residency satisfied".
    UUID source_tiler = dedup.source_tiler;
    if (IsZeroUUID(source_tiler)) {
        source_tiler = dedup.source_block_id.tiler_id;
        if (IsZeroUUID(source_tiler)) {
            rejected_invalid_.fetch_add(1, std::memory_order_relaxed);
            // Rate limited because a miswired caller retries every policy
            // pass, and the log has to stay readable long enough to fix it.
            LOG_EVERY_N(ERROR, 100)
                << "MovementTracker rejecting a dedup key with no source "
                   "tier, key="
                << key << " direction=" << ToString(direction);
            return tl::make_unexpected(MovementRejection::kInvalid);
        }
        LOG_FIRST_N(ERROR, 1)
            << "MovementTracker got a dedup key with an unset source_tiler; "
               "falling back to source_block_id.tiler_id for the residency "
               "gate, key="
            << key;
    }

    const Clock::time_point now = clock_->Now();
    auto& shard = ShardFor(key);
    std::lock_guard<std::mutex> lock(shard.mu);

    // Read again under the lock. Stop() passes through every shard lock, so
    // this second read is what makes "no lease is minted once Stop returns"
    // true for a thread that was preempted between the first read and here.
    if (stopped_.load(std::memory_order_acquire)) {
        return tl::make_unexpected(MovementRejection::kStopped);
    }

    if (shard.inflight.find(dedup) != shard.inflight.end()) {
        rejected_inflight_.fetch_add(1, std::memory_order_relaxed);
        return tl::make_unexpected(MovementRejection::kInflight);
    }

    auto it = shard.records.find(key);
    if (it != shard.records.end()) {
        const Record& record = it->second;
        auto since = record.since.find(source_tiler);
        // No commit record for the source tier means residency is satisfied,
        // not violated. A block committed before this tracker existed -- or
        // one whose record the cap dropped -- would otherwise be immovable
        // forever, because nothing ever re-announces a commit that already
        // happened.
        if (since != record.since.end() &&
            now - since->second < config_.minimum_residency) {
            rejected_residency_.fetch_add(1, std::memory_order_relaxed);
            return tl::make_unexpected(MovementRejection::kMinimumResidency);
        }
        const Clock::time_point last = direction == MovementDirection::kOffload
                                           ? record.last_offload
                                           : record.last_onboard;
        // A default-constructed stamp means "never moved this way"; without
        // that test a fresh record would look like it moved at the clock
        // epoch and serve a cooldown it never earned.
        if (last != Clock::time_point{} && now - last < config_.cooldown) {
            rejected_cooldown_.fetch_add(1, std::memory_order_relaxed);
            return tl::make_unexpected(MovementRejection::kCooldown);
        }
    }

    Record* record = nullptr;
    if (it != shard.records.end()) {
        record = &it->second;
    } else {
        record = &shard.records.try_emplace(std::string(key)).first->second;
        // Stamped once, at birth: this is what the settle compares against
        // to know the record in front of it is the one it pinned.
        record->incarnation = NextStamp();
    }
    ++record->inflight;
    record->sequence = NextStamp();
    // The slot remembers who took it. Only that lease may give it back, so a
    // settle can never free a movement that someone else is still running.
    const uint64_t lease_id =
        next_lease_id_.fetch_add(1, std::memory_order_relaxed) + 1;
    shard.inflight.emplace(
        dedup, InflightSlot{std::string(key), lease_id, record->incarnation});
    acquired_.fetch_add(1, std::memory_order_relaxed);
    // Trimmed on the way out rather than on the way in: the record just
    // created holds a lease, so it is protected from its own trim.
    TrimLocked(shard);
    return MovementLease(this, dedup, direction, std::string(key), lease_id);
}

void MovementTracker::OnCommitted(std::string_view key, const UUID& tiler_id) {
    const Clock::time_point now = clock_->Now();
    auto& shard = ShardFor(key);
    std::lock_guard<std::mutex> lock(shard.mu);
    auto [it, inserted] = shard.records.try_emplace(std::string(key));
    Record& record = it->second;
    // A record created here is a new incarnation of the name: if a delete
    // just removed the old one, a movement of the old block may still be
    // running, and its settle must not mistake this record for its own.
    if (inserted) record.incarnation = NextStamp();
    // Overwritten, not kept: a second commit on the same tier is a different
    // block, and its residency starts now.
    record.since[tiler_id] = now;
    record.sequence = NextStamp();
    TrimLocked(shard);
}

void MovementTracker::OnDeleted(std::string_view key) {
    auto& shard = ShardFor(key);
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.records.find(key);
    if (it != shard.records.end()) shard.records.erase(it);
    // The dedup slots deliberately stay. A movement that was running an
    // instant ago is still running now, and freeing its slot here would let a
    // second command be issued against the same source block while the first
    // one is still copying it. The slots are orphaned instead: the record
    // incarnation they name no longer exists, so the settles that eventually
    // arrive record nothing, and each frees its own slot exactly once. It is
    // also why the lease keeps a copy of its key rather than a pointer into
    // the record.
}

void MovementTracker::Stop() {
    // Only the admission side closes. Outstanding leases must still settle,
    // or a shutdown racing the executor's last commands would leave dedup
    // slots wedged for whatever lifetime this process has left.
    stopped_.store(true, std::memory_order_release);
    // Then pass through every shard lock. An acquire that read the flag
    // before the store above either finishes before this loop reaches its
    // shard, or takes that shard's lock afterwards and re-reads the flag --
    // so no lease is minted once this returns. One lock at a time: the
    // tracker never holds two.
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard->mu);
    }
}

MovementTrackerStats MovementTracker::Stats() const {
    MovementTrackerStats stats;
    for (const auto& shard : shards_) {
        // One shard at a time: statistics never lock the whole tracker.
        std::lock_guard<std::mutex> lock(shard->mu);
        stats.inflight += shard->inflight.size();
        stats.tracked_keys += shard->records.size();
    }
    stats.acquired = acquired_.load(std::memory_order_relaxed);
    stats.rejected_inflight =
        rejected_inflight_.load(std::memory_order_relaxed);
    stats.rejected_cooldown =
        rejected_cooldown_.load(std::memory_order_relaxed);
    stats.rejected_residency =
        rejected_residency_.load(std::memory_order_relaxed);
    stats.rejected_invalid = rejected_invalid_.load(std::memory_order_relaxed);
    stats.settled_moved = settled_moved_.load(std::memory_order_relaxed);
    stats.settled_unmoved = settled_unmoved_.load(std::memory_order_relaxed);
    stats.evicted_records = evicted_records_.load(std::memory_order_relaxed);
    return stats;
}

void MovementTracker::Release(const MovementDedupKey& dedup,
                              MovementDirection direction, std::string_view key,
                              uint64_t lease_id, bool moved) {
    const Clock::time_point now = clock_->Now();
    auto& shard = ShardFor(key);
    std::lock_guard<std::mutex> lock(shard.mu);

    // Freeing the dedup slot is the whole contract of a settle -- but only
    // this lease's slot. Erasing by dedup key alone would free whatever
    // movement happens to hold that key now, which is how one block ends up
    // with two commands in flight.
    auto slot = shard.inflight.find(dedup);
    if (slot == shard.inflight.end() || slot->second.lease_id != lease_id) {
        // Unreachable by construction: a slot is created with its lease and
        // erased only here, by that lease. Getting here means something freed
        // a slot out of band, so it is reported rather than absorbed.
        LOG(ERROR) << "MovementTracker settle found no slot of its own, key="
                   << key << " direction=" << ToString(direction)
                   << " lease=" << lease_id;
    } else {
        const uint64_t incarnation = slot->second.record_incarnation;
        shard.inflight.erase(slot);
        auto it = shard.records.find(key);
        // Matched on the incarnation, not merely on the name. A record that
        // is missing, or one that a later Put created under the same name,
        // describes a different block: decrementing its inflight would unpin
        // a record whose own lease is still out (leaving the cap free to drop
        // it), and charging it a cooldown would hold back a block this
        // movement never touched. Nor is a missing record recreated here --
        // that would resurrect policy state for a key with no replica left.
        if (it != shard.records.end() &&
            it->second.incarnation == incarnation) {
            Record& record = it->second;
            if (record.inflight > 0) --record.inflight;
            if (moved) {
                // Only a move that actually happened starts a cooldown.
                // Charging one for a rejected, failed or cancelled attempt
                // would suppress exactly the retry meant to follow it.
                if (direction == MovementDirection::kOffload) {
                    record.last_offload = now;
                } else {
                    record.last_onboard = now;
                }
            }
            record.sequence = NextStamp();
        }
    }

    if (moved) {
        settled_moved_.fetch_add(1, std::memory_order_relaxed);
    } else {
        settled_unmoved_.fetch_add(1, std::memory_order_relaxed);
    }
    TrimLocked(shard);
}

void MovementTracker::TrimLocked(Shard& shard) {
    // The configured cap is global but enforced per shard, as an equal share.
    // A global bound would need a cross-shard lock on the acquire path to
    // police a number that is only ever approximate anyway.
    const size_t limit =
        std::max<size_t>(1, config_.max_tracked_keys / shards_.size());
    if (shard.records.size() <= limit) return;

    const size_t excess = shard.records.size() - limit;
    using RecordIt = decltype(shard.records)::iterator;
    // Only settled records are candidates. Dropping a record that has a lease
    // out would lose the cooldown that lease is about to record, and the key
    // would immediately become eligible to move back again.
    std::vector<RecordIt> victims;
    victims.reserve(shard.records.size());
    for (auto it = shard.records.begin(); it != shard.records.end(); ++it) {
        if (it->second.inflight == 0) victims.push_back(it);
    }
    if (victims.size() > excess) {
        // Least recently touched first; the exact order among the survivors
        // does not matter, so nth_element is enough.
        std::nth_element(victims.begin(), victims.begin() + excess,
                         victims.end(), [](RecordIt lhs, RecordIt rhs) {
                             return lhs->second.sequence < rhs->second.sequence;
                         });
        victims.resize(excess);
    }
    for (RecordIt victim : victims) shard.records.erase(victim);
    evicted_records_.fetch_add(victims.size(), std::memory_order_relaxed);
}

}  // namespace mooncake::v2
