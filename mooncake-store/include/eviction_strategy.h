#pragma once

#include <deque>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "types.h"

namespace mooncake {

/**
 * @brief Abstract interface for eviction strategy, responsible for choosing
 *        which kvcache object to be evicted before pool overflow.
 */
class EvictionStrategy : public std::enable_shared_from_this<EvictionStrategy> {
   public:
    virtual ~EvictionStrategy() = default;
    virtual ErrorCode AddKey(const std::string& key) = 0;
    virtual ErrorCode UpdateKey(const std::string& key) = 0;
    virtual ErrorCode RemoveKey(const std::string& key) {
        // Remove key from the list and map
        auto it = all_key_idx_map_.find(key);
        if (it != all_key_idx_map_.end()) {
            all_key_list_.erase(it->second);
            all_key_idx_map_.erase(it);
        }
        return ErrorCode::OK;
    }
    virtual std::string EvictKey(void) = 0;
    virtual size_t GetSize(void) { return all_key_list_.size(); }
    void CleanUp(void) {
        all_key_list_.clear();
        all_key_idx_map_.clear();
    }

   protected:
    std::list<std::string> all_key_list_;
    std::unordered_map<std::string, std::list<std::string>::iterator>
        all_key_idx_map_;
};

class LRUEvictionStrategy : public EvictionStrategy {
   public:
    virtual ErrorCode AddKey(const std::string& key) override {
        // Add key to the front of the list
        if (all_key_idx_map_.find(key) != all_key_idx_map_.end()) {
            all_key_list_.erase(all_key_idx_map_[key]);
            all_key_idx_map_.erase(key);
        }
        all_key_list_.push_front(key);
        all_key_idx_map_[key] = all_key_list_.begin();
        return ErrorCode::OK;
    }

    virtual ErrorCode UpdateKey(const std::string& key) override {
        // Move the key to the front of the list
        auto it = all_key_idx_map_.find(key);
        if (it != all_key_idx_map_.end()) {
            all_key_list_.erase(it->second);
            all_key_list_.push_front(key);
            all_key_idx_map_[key] = all_key_list_.begin();
        }
        return ErrorCode::OK;
    }

    virtual std::string EvictKey(void) override {
        // Evict the last key in the list
        if (all_key_list_.empty()) {
            return "";
        }
        std::string evicted_key = all_key_list_.back();
        all_key_list_.pop_back();
        all_key_idx_map_.erase(evicted_key);
        return evicted_key;
    }
};

class FIFOEvictionStrategy : public EvictionStrategy {
   public:
    virtual ErrorCode AddKey(const std::string& key) override {
        // Add key to the front of the list
        all_key_list_.push_front(key);
        return ErrorCode::OK;
    }
    virtual ErrorCode UpdateKey(const std::string& key) override {
        return ErrorCode::OK;
    }
    virtual std::string EvictKey(void) {
        if (all_key_list_.empty()) {
            return "";
        }
        std::string evicted_key = all_key_list_.back();
        all_key_list_.pop_back();
        return evicted_key;
    }
};

/**
 * @brief S3-FIFO eviction strategy (SOSP'23).
 *
 * Three-queue eviction optimized for skewed access patterns in LLM KVCache
 * workloads. Frequently accessed prefix KVCaches (system prompts, common
 * prefixes) are promoted to the main queue and protected from eviction,
 * while one-hit wonders are quickly evicted from the small queue.
 *
 * Queues:
 *   - Small (10% capacity): Entry point for new objects. Evicted on first
 *     pass unless accessed again (promoted to Main).
 *   - Main (90% capacity): Objects that were accessed at least twice.
 *     Evicted only when Small is empty.
 *   - Ghost (metadata only): Tracks recently evicted keys from Small for
 *     fast re-admission decisions.
 */
class S3FIFOEvictionStrategy : public EvictionStrategy {
   public:
    explicit S3FIFOEvictionStrategy(size_t ghost_capacity = 4096)
        : ghost_capacity_(ghost_capacity) {}

    ErrorCode AddKey(const std::string& key) override {
        // If key already tracked, treat as update
        if (freq_.count(key)) {
            return UpdateKey(key);
        }

        // Insert into small queue
        small_queue_.push_back(key);
        freq_[key] = 0;

        // Also track in base class structures for GetSize()/RemoveKey()
        all_key_list_.push_front(key);
        all_key_idx_map_[key] = all_key_list_.begin();

        return ErrorCode::OK;
    }

    ErrorCode UpdateKey(const std::string& key) override {
        auto it = freq_.find(key);
        if (it != freq_.end() && it->second < kMaxFreq) {
            it->second++;
        }
        return ErrorCode::OK;
    }

    ErrorCode RemoveKey(const std::string& key) override {
        freq_.erase(key);
        ghost_set_.erase(key);

        // Remove from small queue
        for (auto it = small_queue_.begin(); it != small_queue_.end(); ++it) {
            if (*it == key) {
                small_queue_.erase(it);
                break;
            }
        }
        // Remove from main queue
        for (auto it = main_queue_.begin(); it != main_queue_.end(); ++it) {
            if (*it == key) {
                main_queue_.erase(it);
                break;
            }
        }

        // Remove from base class tracking
        return EvictionStrategy::RemoveKey(key);
    }

    std::string EvictKey() override {
        // Try to evict from small queue first
        while (!small_queue_.empty()) {
            std::string key = small_queue_.front();
            small_queue_.pop_front();

            auto it = freq_.find(key);
            if (it == freq_.end()) {
                // Already removed externally, skip
                continue;
            }

            if (it->second > 0) {
                // Accessed at least once since insertion — promote to main
                it->second = 0;
                main_queue_.push_back(key);
                continue;
            }

            // freq == 0: one-hit wonder, evict it
            freq_.erase(it);
            addToGhost(key);
            // Remove from base class tracking
            auto map_it = all_key_idx_map_.find(key);
            if (map_it != all_key_idx_map_.end()) {
                all_key_list_.erase(map_it->second);
                all_key_idx_map_.erase(map_it);
            }
            return key;
        }

        // Small queue empty — evict from main queue (FIFO order)
        while (!main_queue_.empty()) {
            std::string key = main_queue_.front();
            main_queue_.pop_front();

            auto it = freq_.find(key);
            if (it == freq_.end()) {
                continue;
            }

            if (it->second > 0) {
                // Re-accessed in main — give another chance (reinsertion)
                it->second = 0;
                main_queue_.push_back(key);
                continue;
            }

            freq_.erase(it);
            auto map_it = all_key_idx_map_.find(key);
            if (map_it != all_key_idx_map_.end()) {
                all_key_list_.erase(map_it->second);
                all_key_idx_map_.erase(map_it);
            }
            return key;
        }

        return "";  // Nothing to evict
    }

    size_t GetSize() override { return freq_.size(); }

    /// Check if a key was recently evicted (in ghost set).
    /// Useful for admission decisions — re-inserting a ghost-hit key
    /// can skip the small queue and go directly to main.
    bool isGhostHit(const std::string& key) const {
        return ghost_set_.count(key) > 0;
    }

   private:
    static constexpr uint8_t kMaxFreq = 3;

    std::deque<std::string> small_queue_;   // FIFO, entry point
    std::deque<std::string> main_queue_;    // FIFO, promoted objects
    std::unordered_map<std::string, uint8_t> freq_;  // access frequency

    // Ghost set: metadata-only tracking of recently evicted keys
    std::deque<std::string> ghost_queue_;
    std::unordered_set<std::string> ghost_set_;
    const size_t ghost_capacity_;

    void addToGhost(const std::string& key) {
        if (ghost_set_.count(key)) return;
        ghost_queue_.push_back(key);
        ghost_set_.insert(key);
        while (ghost_queue_.size() > ghost_capacity_) {
            ghost_set_.erase(ghost_queue_.front());
            ghost_queue_.pop_front();
        }
    }
};

}  // namespace mooncake