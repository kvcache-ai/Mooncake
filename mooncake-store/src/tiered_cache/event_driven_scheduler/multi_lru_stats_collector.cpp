#include "tiered_cache/event_driven_scheduler/multi_lru_stats_collector.h"

#include <utility>

namespace mooncake {

MultiLRUStatsCollector::MultiLRUStatsCollector(size_t sketch_capacity,
                                               BandThresholds band_thresholds,
                                               uint32_t sample_size)
    : sketch_(sketch_capacity, sample_size), fast_lru_(band_thresholds) {}

void MultiLRUStatsCollector::SetFastTier(UUID fast_tier_id) {
    fast_tier_id_ = fast_tier_id;
    fast_tier_set_ = true;
}

void MultiLRUStatsCollector::OnAccess(std::string_view key,
                                      UUID served_tier_id) {
    // Every access bumps the global frequency memory, regardless of which tier
    // served it (separation of concerns); the fused call does the increment and
    // the post-increment estimate in one pass.
    const uint64_t freq = sketch_.IncrementAndEstimate(HashKey(key));
    // Re-classify the key's band on every fast-tier hit. No-op if the key isn't
    // a fast-tier resident.
    if (IsFastTier(served_tier_id)) {
        fast_lru_.Touch(key, freq);
    }
}

void MultiLRUStatsCollector::OnCommit(std::string_view key, UUID tier_id,
                                      size_t size_bytes) {
    if (!IsFastTier(tier_id)) {
        return;  // only fast-tier replicas enter the MultiLRU
    }
    if (size_bytes == 0) {
        // A zero-size entry contributes nothing to reclaim and cannot advance
        // the eviction target; keep it out of the MultiLRU entirely.
        return;
    }
    // A commit is NOT a frequency hit: we only read the current estimate to
    // pick the initial band, we do NOT Increment the sketch (a commit must not
    // be double-counted as an access).
    const uint64_t freq = sketch_.Estimate(HashKey(key));
    fast_lru_.Insert(key, size_bytes, freq);  // MultiLRU derives the band
}

void MultiLRUStatsCollector::OnDelete(std::string_view key,
                                      std::optional<UUID> tier_id) {
    // Tier-aware: a delete on some other tier must NOT remove the
    // key from the fast-tier MultiLRU. Only an explicit fast-tier delete, or a
    // full-key delete (nullopt), drops fast-tier residency.
    if (tier_id.has_value() && !IsFastTier(tier_id.value())) {
        return;
    }
    fast_lru_.Remove(key);
    // The sketch is intentionally left intact: it is a global frequency memory
    // and should remember a key's heat across residency churn.
}

uint64_t MultiLRUStatsCollector::GetAccessFrequency(
    std::string_view key) const {
    return sketch_.Estimate(HashKey(key));
}

AccessStats MultiLRUStatsCollector::GetHotKeyStats(size_t hot_key_num) const {
    AccessStats stats;
    stats.metric = AccessStatMetric::kFrequency;
    for (auto& entry : fast_lru_.CollectHot(hot_key_num)) {
        AccessStatEntry out;
        out.key = std::move(entry.key);
        stats.hot_keys.push_back(std::move(out));
    }
    return stats;
}

std::vector<MultiLRUEntry> MultiLRUStatsCollector::CollectEvictionCandidates(
    size_t max_n) const {
    return fast_lru_.CollectColdFirst(max_n);
}

}  // namespace mooncake
