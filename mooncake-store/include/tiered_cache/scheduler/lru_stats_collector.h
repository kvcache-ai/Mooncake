#pragma once

#include <list>
#include <mutex>
#include <string>
#include <unordered_map>
#include "tiered_cache/scheduler/stats_collector.h"

namespace mooncake {

/**
 * @class LRUStatsCollector
 * @brief Maintains an LRU list of accessed keys.
 * Used for LRU-based promotion and eviction policies.
 */
class LRUStatsCollector : public StatsCollector {
   public:
    void RecordAccess(const std::string& key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = key_map_.find(key);
        if (it != key_map_.end()) {
            // Move to front (MRU)
            lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        } else {
            // Add new key to front
            lru_list_.push_front(key);
            key_map_[key] = lru_list_.begin();
        }
    }

    AccessStats GetSnapshot() override {
        std::lock_guard<std::mutex> lock(mutex_);
        AccessStats stats;
        // Populate stats.hot_keys with the LRU list order.
        // Head is MRU (Hottest), Tail is LRU (Coldest).
        // Since AccessStats uses a vector of pairs, we use the position as heat
        // (conceptually). Or just simple existence.

        // MVP: Just dump equality, maybe with a fake score if needed by Policy.
        // But specialized LRUPolicy will likely just check order.
        // We'll give higher score to MRU just in case.

        double score = static_cast<double>(lru_list_.size());
        for (const auto& key : lru_list_) {
            stats.hot_keys.push_back({key, score--});
        }

        // Note: For LRU, we do NOT clear the list. We need history for eviction.
        return stats;
    }

    void RemoveKey(const std::string& key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = key_map_.find(key);
        if (it != key_map_.end()) {
            lru_list_.erase(it->second);
            key_map_.erase(it);
        }
    }

   private:
    std::mutex mutex_;
    std::list<std::string> lru_list_;
    std::unordered_map<std::string, std::list<std::string>::iterator> key_map_;
};

}  // namespace mooncake
