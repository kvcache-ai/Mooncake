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

    // Remove a key from tracking (called when key is deleted)
    virtual void RemoveKey(const std::string& key) = 0;
};

/**
 * @class SimpleStatsCollector
 * @brief MVP implementation using thread-safe map counter with decay
 */
class SimpleStatsCollector : public StatsCollector {
   public:
    // Decay factor: 0.5 means counts are halved after each snapshot
    // This preserves history while giving more weight to recent accesses
    explicit SimpleStatsCollector(double decay_factor = 0.5)
        : decay_factor_(decay_factor) {}

    void RecordAccess(const std::string& key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        access_counts_[key]++;
    }

    AccessStats GetSnapshot() override {
        std::lock_guard<std::mutex> lock(mutex_);
        AccessStats stats;
        // For MVP: naive copy of all keys. In production, use Top-K or Sketch.
        for (auto& [key, count] : access_counts_) {
            stats.hot_keys.push_back({key, static_cast<double>(count)});
            // Apply decay instead of clearing
            count = static_cast<uint64_t>(count * decay_factor_);
        }
        // Remove keys with zero count to prevent unbounded growth
        for (auto it = access_counts_.begin(); it != access_counts_.end();) {
            if (it->second == 0) {
                it = access_counts_.erase(it);
            } else {
                ++it;
            }
        }
        return stats;
    }

    void RemoveKey(const std::string& key) override {
        std::lock_guard<std::mutex> lock(mutex_);
        access_counts_.erase(key);
    }

   private:
    std::mutex mutex_;
    std::unordered_map<std::string, uint64_t> access_counts_;
    double decay_factor_;
};

}  // namespace mooncake
