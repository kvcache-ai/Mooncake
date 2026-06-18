#include "tiered_cache/event_driven_scheduler/multi_lru_stats_collector.h"

#include <utility>

namespace mooncake {

MultiLRUStatsCollector::MultiLRUStatsCollector(size_t sketch_capacity,
                                               uint32_t sample_size)
    : sketch_(sketch_capacity, sample_size) {}

void MultiLRUStatsCollector::SetFastTier(UUID fast_tier_id) {
    fast_tier_id_ = fast_tier_id;
    fast_tier_set_ = true;
}

void MultiLRUStatsCollector::OnAccess(std::string_view key,
                                      UUID served_tier_id) {
    const uint64_t h = HashKey(key);
    uint64_t freq = 0;
    {
        // Every access bumps the global frequency memory, regardless of which
        // tier served it (separation of concerns).
        std::lock_guard<std::mutex> lock(sketch_mutex_);
        sketch_.Increment(h);
        freq = sketch_.Estimate(h);
    }
    // Re-classify the key's band on every fast-tier hit. No-op if the
    // key isn't a fast-tier resident.
    if (IsFastTier(served_tier_id)) {
        MutexLocker lock(&lru_mutex_);
        fast_lru_.Touch(key, freq);
    }
}

void MultiLRUStatsCollector::OnCommit(std::string_view key, UUID tier_id,
                                      size_t size_bytes) {
    if (!IsFastTier(tier_id)) {
        return;  // only fast-tier replicas enter the MultiLRU
    }
    // A commit is NOT a frequency hit: we only read the current estimate to
    // pick the initial band, we do NOT Increment the sketch (a commit must not
    // be double-counted as an access).
    uint64_t freq = 0;
    {
        std::lock_guard<std::mutex> lock(sketch_mutex_);
        freq = sketch_.Estimate(HashKey(key));
    }
    MutexLocker lock(&lru_mutex_);
    fast_lru_.Insert(key, size_bytes, BandOf(freq));
}

void MultiLRUStatsCollector::OnDelete(std::string_view key,
                                      std::optional<UUID> tier_id) {
    // Tier-aware: a delete on some other tier must NOT remove the
    // key from the fast-tier MultiLRU. Only an explicit fast-tier delete, or a
    // full-key delete (nullopt), drops fast-tier residency.
    if (tier_id.has_value() && !IsFastTier(tier_id.value())) {
        return;
    }
    MutexLocker lock(&lru_mutex_);
    fast_lru_.Remove(key);
    // The sketch is intentionally left intact: it is a global frequency memory
    // and should remember a key's heat across residency churn.
}

uint64_t MultiLRUStatsCollector::GetAccessFrequency(std::string_view key) const {
    std::lock_guard<std::mutex> lock(sketch_mutex_);
    return sketch_.Estimate(HashKey(key));
}

AccessStats MultiLRUStatsCollector::GetHotKeyStats(size_t hot_key_num) const {
    // Collect band-ordered hot keys under the LRU lock, then release it before
    // touching the sketch — the two locks are never held simultaneously.
    std::vector<MultiLRUEntry> entries;
    {
        MutexLocker lock(&lru_mutex_);
        entries = fast_lru_.CollectHot(hot_key_num);
    }

    AccessStats stats;
    stats.metric = AccessStatMetric::kFrequency;
    stats.hot_keys.reserve(entries.size());
    {
        std::lock_guard<std::mutex> lock(sketch_mutex_);
        for (auto& entry : entries) {
            AccessStatEntry out;
            out.estimated_frequency = sketch_.Estimate(HashKey(entry.key));
            out.key = std::move(entry.key);
            stats.hot_keys.push_back(std::move(out));
        }
    }
    return stats;
}

std::vector<MultiLRUEntry> MultiLRUStatsCollector::CollectEvictionCandidates(
    size_t max_n) const {
    MutexLocker lock(&lru_mutex_);
    return fast_lru_.CollectColdFirst(max_n);
}

}  // namespace mooncake
