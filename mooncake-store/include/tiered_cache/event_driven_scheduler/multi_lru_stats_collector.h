#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string_view>
#include <vector>

#include "mutex.h"  // Mutex, MutexLocker, GUARDED_BY
#include "tiered_cache/event_driven_scheduler/event_driven_stats_collector.h"
#include "tiered_cache/event_driven_scheduler/frequency_sketch.h"
#include "tiered_cache/event_driven_scheduler/multi_lru.h"
#include "types.h"

namespace mooncake {

/**
 * @class MultiLRUStatsCollector
 * @brief TinyLFU frequency memory + 4-band MultiLRU over fast-tier residents.
 *
 * Concerns are separated: the FrequencySketch is a GLOBAL frequency
 * memory (never deleted on key removal), while the MultiLRU tracks ONLY keys
 * currently resident in the fast tier. Fast-tier residency is maintained purely
 * by the tier-aware OnDelete — there is no tier-agnostic
 * RemoveKey, because a fast-tier replica is often dropped while the key still
 * lives in the slow tier (so "last replica gone" would never fire).
 *
 * Locking: two independent locks, never held simultaneously (sketch_mutex_ then
 * released before lru_mutex_, and vice versa), so there is no lock-ordering
 * hazard.
 */
class MultiLRUStatsCollector : public EventDrivenStatsCollector {
   public:
    explicit MultiLRUStatsCollector(size_t sketch_capacity,
                                    uint32_t sample_size = 0);

    void SetFastTier(UUID fast_tier_id) override;
    void OnAccess(std::string_view key, UUID served_tier_id) override;
    void OnCommit(std::string_view key, UUID tier_id,
                  size_t size_bytes) override;
    void OnDelete(std::string_view key,
                  std::optional<UUID> tier_id = std::nullopt) override;
    uint64_t GetAccessFrequency(std::string_view key) const override;
    AccessStats GetHotKeyStats(size_t hot_key_num) const override;

    // Concrete-type extension used by EventDrivenClientScheduler's evict loop:
    // coldest-first fast-tier residents (cold band LRU->MRU, then warm, ...).
    std::vector<MultiLRUEntry> CollectEvictionCandidates(size_t max_n) const;

   private:
    static uint64_t HashKey(std::string_view key) {
        return std::hash<std::string_view>{}(key);
    }

    bool IsFastTier(UUID tier_id) const {
        return fast_tier_set_ && tier_id == fast_tier_id_;
    }

    mutable std::mutex sketch_mutex_;
    FrequencySketch sketch_;  // guarded by sketch_mutex_ (std::mutex)

    mutable Mutex lru_mutex_;
    MultiLRU fast_lru_ GUARDED_BY(lru_mutex_);

    UUID fast_tier_id_{};
    bool fast_tier_set_ = false;
};

}  // namespace mooncake
