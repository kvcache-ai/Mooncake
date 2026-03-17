#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "tiered_cache/scheduler/stats_collector.h"

namespace mooncake {

/**
 * @class LRUStatsCollector
 * @brief Maintains an LRU list of accessed keys.
 * Used for LRU-based promotion and eviction policies.
 */
class LRUStatsCollector : public StatsCollector {
   public:
    explicit LRUStatsCollector(
        size_t shard_count = detail::DefaultStatsShardCount(),
        size_t max_snapshot_keys = detail::DefaultSnapshotLimit())
        : shards_(detail::NormalizeShardCount(shard_count)),
          shard_mask_(shards_.size() - 1),
          max_snapshot_keys_(
              detail::NormalizeSnapshotLimit(max_snapshot_keys)) {}

    void RecordAccess(const std::string& key) override {
        const uint64_t sequence =
            next_sequence_.fetch_add(1, std::memory_order_relaxed);
        auto& shard = GetShard(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.pending_updates[key] = sequence;
    }

    AccessStats GetSnapshot() override {
        std::lock_guard<std::mutex> snapshot_lock(snapshot_mutex_);
        ApplyPendingChanges();
        return BuildSnapshot();
    }

    void RemoveKey(const std::string& key) override {
        auto& shard = GetShard(key);
        std::lock_guard<std::mutex> lock(shard.mutex);
        shard.pending_updates.erase(key);
        shard.pending_deletes.push_back(key);
    }

   private:
    struct OrderedKey {
        uint64_t sequence;
        std::string key;
    };

    struct OrderedKeyCompare {
        bool operator()(const OrderedKey& lhs, const OrderedKey& rhs) const {
            if (lhs.sequence != rhs.sequence) {
                return lhs.sequence > rhs.sequence;
            }
            return lhs.key < rhs.key;
        }
    };

    struct alignas(64) Shard {
        std::mutex mutex;
        std::unordered_map<std::string, uint64_t> pending_updates;
        std::vector<std::string> pending_deletes;
    };

    Shard& GetShard(const std::string& key) {
        const auto shard_index = std::hash<std::string>{}(key)&shard_mask_;
        return shards_[shard_index];
    }

    void ApplyPendingChanges() {
        for (auto& shard : shards_) {
            std::unordered_map<std::string, uint64_t> pending_updates;
            std::vector<std::string> pending_deletes;

            {
                std::lock_guard<std::mutex> lock(shard.mutex);
                pending_updates.swap(shard.pending_updates);
                pending_deletes.swap(shard.pending_deletes);
            }

            for (const auto& key : pending_deletes) {
                RemoveTrackedKey(key);
            }

            for (const auto& [key, sequence] : pending_updates) {
                auto it = latest_access_.find(key);
                if (it != latest_access_.end()) {
                    ordered_keys_.erase(OrderedKey{it->second, key});
                    it->second = sequence;
                } else {
                    latest_access_[key] = sequence;
                }
                ordered_keys_.insert(OrderedKey{sequence, key});
            }
        }
    }

    AccessStats BuildSnapshot() const {
        AccessStats stats;
        stats.metric = AccessStatMetric::kRecencyRank;
        stats.hot_keys.reserve(
            std::min(max_snapshot_keys_, ordered_keys_.size()));

        size_t recency_rank = 1;
        size_t emitted = 0;
        for (const auto& ordered_key : ordered_keys_) {
            stats.hot_keys.push_back(
                AccessStatEntry{ordered_key.key, 0.0, recency_rank++});
            emitted++;
            if (emitted >= max_snapshot_keys_) {
                break;
            }
        }
        return stats;
    }

    void RemoveTrackedKey(const std::string& key) {
        auto it = latest_access_.find(key);
        if (it == latest_access_.end()) {
            return;
        }

        ordered_keys_.erase(OrderedKey{it->second, key});
        latest_access_.erase(it);
    }

    std::atomic<uint64_t> next_sequence_{1};
    std::mutex snapshot_mutex_;
    std::vector<Shard> shards_;
    size_t shard_mask_;
    size_t max_snapshot_keys_;
    std::unordered_map<std::string, uint64_t> latest_access_;
    std::set<OrderedKey, OrderedKeyCompare> ordered_keys_;
};

}  // namespace mooncake
