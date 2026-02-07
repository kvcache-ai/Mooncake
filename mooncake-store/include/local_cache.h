#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

#include "client_buffer.hpp"
#include "replica.h"

namespace mooncake {

/**
 * @brief Thread-safe local cache for storing key-value data locally.
 *
 * Used by RealClient to avoid redundant master queries and remote
 * transfers when multiple processes on the same node request the
 * same key.
 */
class LocalCache {
   public:
    struct CacheEntry {
        std::shared_ptr<BufferHandle> buffer_handle;
        Replica::Descriptor replica_desc;
        uint64_t data_size;
    };

    /**
     * @brief Look up a key in the cache.
     * @return shared_ptr to BufferHandle if found, nullptr otherwise.
     */
    std::shared_ptr<BufferHandle> Lookup(const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            return it->second.buffer_handle;
        }
        return nullptr;
    }

    /**
     * @brief Look up a key and return the full cache entry.
     * @return pointer to CacheEntry if found, nullptr otherwise.
     *         The pointer is valid only while the caller holds no
     *         write lock (i.e., use under shared lock externally,
     *         or copy the entry).
     */
    std::optional<CacheEntry> LookupEntry(
        const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto it = cache_.find(key);
        if (it != cache_.end()) {
            return it->second;
        }
        return std::nullopt;
    }

    /**
     * @brief Insert a new cache entry. Overwrites if key exists.
     */
    void Insert(const std::string& key,
                std::shared_ptr<BufferHandle> handle,
                const Replica::Descriptor& desc, uint64_t size) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        cache_[key] = CacheEntry{std::move(handle), desc, size};
    }

    /**
     * @brief Remove a cache entry.
     * @return true if the key was found and removed.
     */
    bool Erase(const std::string& key) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return cache_.erase(key) > 0;
    }

    /**
     * @brief Remove all cache entries.
     */
    void Clear() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        cache_.clear();
    }

    /**
     * @brief Check if a key exists in the cache.
     */
    bool Contains(const std::string& key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return cache_.find(key) != cache_.end();
    }

    /**
     * @brief Return the number of cached entries.
     */
    size_t Size() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return cache_.size();
    }

   private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, CacheEntry> cache_;
};

}  // namespace mooncake
