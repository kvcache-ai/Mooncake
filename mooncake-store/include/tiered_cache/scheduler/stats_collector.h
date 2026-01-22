#pragma once

#include <string>
#include <unordered_map>
#include <mutex>
#include <atomic>
#include "tiered_cache/scheduler/scheduler_policy.h"

namespace mooncake {

/**
 * @struct AccessStats
 * @brief Snapshot of access statistics
 */
struct AccessStats {
    std::vector<std::pair<std::string, double>> hot_keys;
};

/**
 * @class StatsCollector
 * @brief Interface for collecting runtime statistics
 */
class StatsCollector {
   public:
    virtual ~StatsCollector() = default;

    // Record an access event for a key
    virtual void RecordAccess(const std::string& key) = 0;

    // Get a snapshot of current stats for policy decision
    virtual AccessStats GetSnapshot() = 0;
};

/**
 * @class SimpleStatsCollector
 * @brief MVP implementation using thread-safe map counter
 */
class SimpleStatsCollector : public StatsCollector {
   public:
    void RecordAccess(const std::string& key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        access_counts_[key]++;
    }

    AccessStats GetSnapshot() override {
        std::lock_guard<std::mutex> lock(mutex_);
        AccessStats stats;
        // For MVP: naive copy of all keys. In production, use Top-K or Sketch.
        for (const auto& [key, count] : access_counts_) {
            stats.hot_keys.push_back({key, static_cast<double>(count)});
        }
        // Optional: clear counts after snapshot (window-based)
        access_counts_.clear();
        return stats;
    }

   private:
    std::mutex mutex_;
    std::unordered_map<std::string, uint64_t> access_counts_;
};

}  // namespace mooncake
