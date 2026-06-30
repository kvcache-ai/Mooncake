#pragma once

#include <optional>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "replica.h"

namespace mooncake {

class DfsDescriptorCache {
   public:
    void Put(const std::string& key, const DistributedFSDescriptor& desc) {
        std::unique_lock lock(mutex_);
        cache_[key] = desc;
    }

    std::optional<DistributedFSDescriptor> Get(const std::string& key) const {
        std::shared_lock lock(mutex_);
        auto it = cache_.find(key);
        if (it == cache_.end()) return std::nullopt;
        return it->second;
    }

    void Remove(const std::string& key) {
        std::unique_lock lock(mutex_);
        cache_.erase(key);
    }

   private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, DistributedFSDescriptor> cache_;
};

}  // namespace mooncake
