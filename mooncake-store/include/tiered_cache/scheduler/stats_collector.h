#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cmath>
#include <functional>
#include <limits>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "tiered_cache/scheduler/scheduler_policy.h"

namespace mooncake {

namespace detail {

inline size_t NormalizeShardCount(size_t shard_count) {
    size_t normalized = 1;
    while (normalized < std::max<size_t>(1, shard_count)) {
        normalized <<= 1;
    }
    return normalized;
}

inline size_t DefaultStatsShardCount() {
    const auto hardware_threads =
        std::max<unsigned int>(1, std::thread::hardware_concurrency());
    return NormalizeShardCount(static_cast<size_t>(hardware_threads) * 4);
}

inline size_t NormalizeSnapshotLimit(size_t limit) {
    return limit == 0 ? std::numeric_limits<size_t>::max() : limit;
}

inline size_t DefaultSnapshotLimit() { return 4096; }

}  // namespace detail

/**
 * @enum AccessStatMetric
 * @brief Semantic meaning carried by a stats snapshot.
 */
enum class AccessStatMetric {
    kRecentHeat,
    kRecencyRank,
};

/**
 * @struct AccessStatEntry
 * @brief Per-key access metadata emitted by a stats collector.
 */
struct AccessStatEntry {
    std::string key;
    double recent_heat_score = 0.0;
    size_t recency_rank = 0;
};

/**
 * @struct AccessStats
 * @brief Snapshot of access statistics
 */
struct AccessStats {
    AccessStatMetric metric = AccessStatMetric::kRecentHeat;
    std::vector<AccessStatEntry> hot_keys;
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
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using NowFn = std::function<TimePoint()>;

    // Decay factor per second: 0.5 means scores are halved every second.
    // This preserves history while giving more weight to recent accesses.
    explicit SimpleStatsCollector(
        double decay_factor_per_second = 0.5,
        size_t shard_count = detail::DefaultStatsShardCount(),
        size_t max_snapshot_keys = detail::DefaultSnapshotLimit(),
        NowFn now_fn = []() { return Clock::now(); })
        : shards_(detail::NormalizeShardCount(shard_count)),
          shard_mask_(shards_.size() - 1),
          max_snapshot_keys_(detail::NormalizeSnapshotLimit(max_snapshot_keys)),
          decay_factor_per_second_(
              NormalizeDecayFactor(decay_factor_per_second)),
          log_decay_factor_per_second_(std::log(decay_factor_per_second_)),
          now_fn_(std::move(now_fn)) {
        last_aggregate_update_time_ = now_fn_();
    }

    void RecordAccess(const std::string& key) override {
        auto& shard = GetShard(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.pending_counts[key]++;
    }

    AccessStats GetSnapshot() override {
        const auto now = now_fn_();
        std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
        AdvanceAggregateTo(now);
        RebaseScoresIfNeeded();
        ApplyPendingChanges();
        AccessStats stats = BuildSnapshot();
        PruneColdKeys();
        return stats;
    }

    void RemoveKey(const std::string& key) override {
        auto& shard = GetShard(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.pending_counts.erase(key);
        shard.pending_deletes.push_back(key);
    }

   private:
    struct OrderedKey {
        double raw_score;
        std::string key;
    };

    struct OrderedKeyCompare {
        bool operator()(const OrderedKey& lhs, const OrderedKey& rhs) const {
            if (lhs.raw_score != rhs.raw_score) {
                return lhs.raw_score > rhs.raw_score;
            }
            return lhs.key < rhs.key;
        }
    };

    struct alignas(64) Shard {
        std::mutex mutex;
        std::unordered_map<std::string, uint64_t> pending_counts;
        std::vector<std::string> pending_deletes;
    };

    Shard& GetShard(const std::string& key) {
        const auto shard_index = std::hash<std::string>{}(key)&shard_mask_;
        return shards_[shard_index];
    }

    void ApplyPendingChanges() {
        for (auto& shard : shards_) {
            std::unordered_map<std::string, uint64_t> pending_counts;
            std::vector<std::string> pending_deletes;

            {
                std::lock_guard<std::mutex> lock(shard.mutex);
                pending_counts.swap(shard.pending_counts);
                pending_deletes.swap(shard.pending_deletes);
            }

            for (const auto& key : pending_deletes) {
                RemoveAggregateKey(key);
            }

            for (const auto& [key, count] : pending_counts) {
                AddCount(key, static_cast<double>(count));
            }
        }
    }

    void AddCount(const std::string& key, double actual_score_delta) {
        if (actual_score_delta <= 0.0) {
            return;
        }

        const double delta = actual_score_delta / global_scale_;
        auto it = raw_counts_.find(key);
        if (it != raw_counts_.end()) {
            ordered_keys_.erase(OrderedKey{it->second, key});
            it->second += delta;
            ordered_keys_.insert(OrderedKey{it->second, key});
            return;
        }

        raw_counts_[key] = delta;
        ordered_keys_.insert(OrderedKey{delta, key});
    }

    void RemoveAggregateKey(const std::string& key) {
        auto it = raw_counts_.find(key);
        if (it == raw_counts_.end()) {
            return;
        }

        ordered_keys_.erase(OrderedKey{it->second, key});
        raw_counts_.erase(it);
    }

    AccessStats BuildSnapshot() const {
        AccessStats stats;
        stats.metric = AccessStatMetric::kRecentHeat;
        stats.hot_keys.reserve(
            std::min(max_snapshot_keys_, ordered_keys_.size()));

        size_t emitted = 0;
        for (const auto& ordered_key : ordered_keys_) {
            const double score = ordered_key.raw_score * global_scale_;
            if (score < kMinTrackedScore) {
                break;
            }

            stats.hot_keys.push_back(
                AccessStatEntry{ordered_key.key, score, 0});
            emitted++;
            if (emitted >= max_snapshot_keys_) {
                break;
            }
        }
        return stats;
    }

    void AdvanceAggregateTo(TimePoint now) {
        global_scale_ =
            AdvanceScale(global_scale_, last_aggregate_update_time_, now);
    }

    double AdvanceScale(double scale, TimePoint& last_update_time,
                        TimePoint now) const {
        if (now <= last_update_time) {
            return scale;
        }

        const double elapsed_seconds =
            std::chrono::duration<double>(now - last_update_time).count();
        last_update_time = now;
        if (log_decay_factor_per_second_ == 0.0) {
            return scale;
        }

        return scale * std::exp(log_decay_factor_per_second_ * elapsed_seconds);
    }

    void RebaseScoresIfNeeded() {
        if (global_scale_ >= kMinScaleBeforeRebase) {
            return;
        }

        if (raw_counts_.empty()) {
            global_scale_ = 1.0;
            return;
        }

        std::set<OrderedKey, OrderedKeyCompare> rebased_order;
        for (auto& [key, raw_score] : raw_counts_) {
            raw_score *= global_scale_;
            rebased_order.insert(OrderedKey{raw_score, key});
        }

        ordered_keys_.swap(rebased_order);
        global_scale_ = 1.0;
    }

    void PruneColdKeys() {
        while (!ordered_keys_.empty()) {
            auto coldest_it = std::prev(ordered_keys_.end());
            if (coldest_it->raw_score * global_scale_ >= kMinTrackedScore) {
                break;
            }

            raw_counts_.erase(coldest_it->key);
            ordered_keys_.erase(coldest_it);
        }
    }

    static constexpr double kMinTrackedScore = 1.0;
    static constexpr double kMinScaleBeforeRebase = 1e-100;

    static double NormalizeDecayFactor(double decay_factor_per_second) {
        if (!std::isfinite(decay_factor_per_second) ||
            decay_factor_per_second <= 0.0) {
            return std::numeric_limits<double>::min();
        }
        return std::min(decay_factor_per_second, 1.0);
    }

    std::mutex snapshot_mutex_;
    std::vector<Shard> shards_;
    size_t shard_mask_;
    std::unordered_map<std::string, double> raw_counts_;
    std::set<OrderedKey, OrderedKeyCompare> ordered_keys_;
    size_t max_snapshot_keys_;
    TimePoint last_aggregate_update_time_{};
    double global_scale_ = 1.0;
    double decay_factor_per_second_;
    double log_decay_factor_per_second_;
    NowFn now_fn_;
};

}  // namespace mooncake
