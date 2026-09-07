#include "p2p/client/v2/frequency_tracker.h"

#include <algorithm>
#include <cmath>
#include <utility>

#include <glog/logging.h>

namespace mooncake::v2 {
namespace {

/** Below this a decayed score is indistinguishable from zero. */
constexpr double kNegligibleHeat = 1e-9;

}  // namespace

tl::expected<void, ErrorCode> ValidateFrequencyTrackerConfig(
    const FrequencyTrackerConfig& config) {
    if (config.shard_count == 0) {
        LOG(ERROR) << "frequency_tracker.shard_count must be greater than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.max_snapshot_keys == 0) {
        LOG(ERROR) << "frequency_tracker.max_snapshot_keys must be greater "
                      "than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.half_life.count() <= 0) {
        LOG(ERROR) << "frequency_tracker.half_life must be positive, got "
                   << config.half_life.count() << "ms";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.entry_ttl.count() <= 0) {
        LOG(ERROR) << "frequency_tracker.entry_ttl must be positive, got "
                   << config.entry_ttl.count() << "ms";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (!(config.expiry_threshold > 0.0)) {
        LOG(ERROR) << "frequency_tracker.expiry_threshold must be positive";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    if (config.max_tracked_keys == 0) {
        LOG(ERROR) << "frequency_tracker.max_tracked_keys must be greater "
                      "than zero";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    return {};
}

FrequencyTracker::FrequencyTracker(const FrequencyTrackerConfig& config,
                                   std::shared_ptr<Clock> clock)
    : config_(config), clock_(std::move(clock)) {
    if (clock_ == nullptr) clock_ = std::make_shared<SteadyClock>();
    const size_t shards = std::max<size_t>(1, config_.shard_count);
    shards_.reserve(shards);
    for (size_t i = 0; i < shards; ++i) {
        shards_.push_back(std::make_unique<Shard>());
    }
    // Precomputed so the touch path never divides. A shard that runs slightly
    // over its share is harmless; the cap exists to bound memory, not to be
    // exact.
    max_per_shard_ = std::max<size_t>(1, config_.max_tracked_keys / shards);
}

FrequencyTracker::Shard& FrequencyTracker::ShardFor(std::string_view key) {
    return *shards_[StringHash{}(key) % shards_.size()];
}

const FrequencyTracker::Shard& FrequencyTracker::ShardFor(
    std::string_view key) const {
    return *shards_[StringHash{}(key) % shards_.size()];
}

void FrequencyTracker::DecayLocked(Entry& entry, Clock::time_point now) const {
    if (now <= entry.last_touch) return;
    const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - entry.last_touch);
    if (elapsed.count() <= 0) return;
    const double half_lives = static_cast<double>(elapsed.count()) /
                              static_cast<double>(config_.half_life.count());
    // Cheaper and no less accurate than pow(0.5, n) for the range that
    // matters, and it saturates to zero rather than denormalising.
    const double factor = std::exp2(-half_lives);
    entry.heat *= factor;
    entry.read_heat *= factor;
    if (entry.heat < kNegligibleHeat) entry.heat = 0.0;
    if (entry.read_heat < kNegligibleHeat) entry.read_heat = 0.0;
    entry.last_touch = now;
}

FrequencySnapshot FrequencyTracker::Touch(const RegistrationId& registration,
                                          std::string_view key, bool is_read) {
    const Clock::time_point now = clock_->Now();
    Shard& shard = ShardFor(key);
    std::lock_guard<std::mutex> lock(shard.mu);

    auto it = shard.entries.find(key);
    if (it == shard.entries.end()) {
        TrimLocked(shard, now);
        Entry fresh;
        fresh.registration = registration;
        fresh.last_touch = now;
        it = shard.entries.emplace(std::string(key), fresh).first;
    } else if (!(it->second.registration == registration)) {
        // A different identity under the same name: the old key was deleted
        // and a new one created. Inheriting the dead key's heat would let a
        // name that used to be hot pull a brand new object up a tier.
        Entry fresh;
        fresh.registration = registration;
        fresh.last_touch = now;
        it->second = fresh;
    }

    Entry& entry = it->second;
    DecayLocked(entry, now);
    entry.heat += 1.0;
    if (is_read) entry.read_heat += 1.0;
    ++entry.raw_touches;
    entry.last_touch = now;

    FrequencySnapshot snapshot;
    snapshot.heat = entry.heat;
    snapshot.read_heat = entry.read_heat;
    snapshot.raw_touches = entry.raw_touches;
    snapshot.missing = false;
    return snapshot;
}

FrequencySnapshot FrequencyTracker::RecordAccess(
    const RegistrationId& registration, std::string_view key) {
    return Touch(registration, key, /*is_read=*/true);
}

void FrequencyTracker::OnCommit(const RegistrationId& registration,
                                std::string_view key) {
    (void)Touch(registration, key, /*is_read=*/false);
}

FrequencySnapshot FrequencyTracker::Get(const RegistrationId& registration,
                                        std::string_view key) const {
    const Clock::time_point now = clock_->Now();
    const Shard& shard = ShardFor(key);
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.entries.find(key);
    if (it == shard.entries.end()) return {};
    if (!(it->second.registration == registration)) {
        // The record belongs to a different incarnation of this name, so as
        // far as this caller is concerned there is no record.
        return {};
    }
    // Decayed for the answer without writing it back: Get is const and must
    // not be a hidden touch.
    Entry decayed = it->second;
    DecayLocked(decayed, now);
    FrequencySnapshot snapshot;
    snapshot.heat = decayed.heat;
    snapshot.read_heat = decayed.read_heat;
    snapshot.raw_touches = decayed.raw_touches;
    snapshot.missing = false;
    return snapshot;
}

void FrequencyTracker::OnDelete(const RegistrationId& registration,
                                std::string_view key) {
    Shard& shard = ShardFor(key);
    std::lock_guard<std::mutex> lock(shard.mu);
    auto it = shard.entries.find(key);
    if (it == shard.entries.end()) return;
    if (!(it->second.registration == registration)) return;
    shard.entries.erase(it);
}

void FrequencyTracker::Remove(std::string_view key) {
    Shard& shard = ShardFor(key);
    std::lock_guard<std::mutex> lock(shard.mu);
    shard.entries.erase(std::string(key));
}

void FrequencyTracker::Clear() {
    for (auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard->mu);
        shard->entries.clear();
    }
}

void FrequencyTracker::TrimLocked(Shard& shard, Clock::time_point now) {
    if (shard.entries.size() < max_per_shard_) return;

    // Expiry first: a record nobody has touched for entry_ttl and whose heat
    // has decayed to nothing costs memory and biases nothing.
    size_t removed = 0;
    for (auto it = shard.entries.begin(); it != shard.entries.end();) {
        Entry decayed = it->second;
        DecayLocked(decayed, now);
        const bool stale = now - it->second.last_touch >= config_.entry_ttl;
        if (stale && decayed.heat < config_.expiry_threshold) {
            it = shard.entries.erase(it);
            ++removed;
        } else {
            ++it;
        }
    }
    if (shard.entries.size() < max_per_shard_) {
        evicted_records_.fetch_add(removed, std::memory_order_relaxed);
        return;
    }

    // Still over: drop the coldest quarter rather than one entry, so this
    // linear pass runs once in a while instead of on every insert.
    std::vector<std::pair<double, const std::string*>> ranked;
    ranked.reserve(shard.entries.size());
    for (const auto& [key, entry] : shard.entries) {
        Entry decayed = entry;
        DecayLocked(decayed, now);
        ranked.emplace_back(decayed.heat, &key);
    }
    const size_t drop = std::max<size_t>(1, ranked.size() / 4);
    std::partial_sort(
        ranked.begin(), ranked.begin() + static_cast<long>(drop), ranked.end(),
        [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    std::vector<std::string> doomed;
    doomed.reserve(drop);
    for (size_t i = 0; i < drop; ++i) doomed.push_back(*ranked[i].second);
    for (const auto& key : doomed) shard.entries.erase(key);
    evicted_records_.fetch_add(removed + doomed.size(),
                               std::memory_order_relaxed);
}

AccessStats FrequencyTracker::Snapshot(
    std::optional<size_t> hot_key_num) const {
    const Clock::time_point now = clock_->Now();
    const size_t requested = hot_key_num.value_or(config_.default_hot_key_num);

    std::vector<AccessStatEntry> collected;
    size_t total = 0;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard->mu);
        total += shard->entries.size();
        for (const auto& [key, entry] : shard->entries) {
            Entry decayed = entry;
            DecayLocked(decayed, now);
            if (decayed.heat <= 0.0) continue;
            AccessStatEntry stat;
            stat.key = key;
            stat.recent_heat_score = decayed.heat;
            collected.push_back(std::move(stat));
        }
    }

    std::sort(collected.begin(), collected.end(),
              [](const AccessStatEntry& lhs, const AccessStatEntry& rhs) {
                  if (lhs.recent_heat_score != rhs.recent_heat_score) {
                      return lhs.recent_heat_score > rhs.recent_heat_score;
                  }
                  // Deterministic order for equal scores, so a snapshot is
                  // reproducible and the differential harness can compare two
                  // implementations without sorting again.
                  return lhs.key < rhs.key;
              });

    // 0 means "everything you track", still bounded by the cap.
    size_t limit = requested == 0 ? collected.size() : requested;
    limit = std::min(limit, config_.max_snapshot_keys);
    if (limit < collected.size()) {
        truncated_snapshots_.fetch_add(1, std::memory_order_relaxed);
        collected.resize(limit);
    }
    (void)total;

    for (size_t i = 0; i < collected.size(); ++i) {
        collected[i].recency_rank = i;
    }

    AccessStats stats;
    // Unchanged from V1's collector and from the previous V2 tracker: the
    // differential harness compares the two answers field by field.
    stats.metric = AccessStatMetric::kFrequency;
    stats.hot_keys = std::move(collected);
    return stats;
}

size_t FrequencyTracker::TrackedKeyCount() const {
    size_t total = 0;
    for (const auto& shard : shards_) {
        std::lock_guard<std::mutex> lock(shard->mu);
        total += shard->entries.size();
    }
    return total;
}

uint64_t FrequencyTracker::TruncatedSnapshotCount() const {
    return truncated_snapshots_.load(std::memory_order_relaxed);
}

uint64_t FrequencyTracker::EvictedRecordCount() const {
    return evicted_records_.load(std::memory_order_relaxed);
}

}  // namespace mooncake::v2
