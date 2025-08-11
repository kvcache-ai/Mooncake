#pragma once

#include <memory>
#include <random>
#include <string>
#include <unordered_map>

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

}  // namespace mooncake