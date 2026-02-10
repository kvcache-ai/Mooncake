#pragma once

#include <condition_variable>
#include <cstring>
#include <list>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "types.h"

namespace mooncake {

/**
 * @brief Memory block metadata for hot cache.
 */
struct HotMemBlock {
    void* addr;   // Memory address (allocated block_size_ bytes)
    size_t size;  // Actual cached data size in bytes (<= block_size_)
    bool in_use;  // Whether the block is currently in use
    std::string key_;  // Cache key bound to this block (empty if not bound)
};

/**
 * @brief Local hot cache used for hot kv cache in the distributed store.
 */
class LocalHotCache {
   public:
    /**
     * @brief Construct a LocalHotCache.
     * @param total_size_bytes Desired total local hot cache size in bytes.
     * @param block_size_bytes Block size in bytes. If 0, uses default 16MB.
     */
    LocalHotCache(size_t total_size_bytes, size_t block_size_bytes = 0);

    /**
     * @brief Destructor.
     */
    ~LocalHotCache();

    /**
     * @brief Insert or touch an entry.
     *
     * If key already exists, it only moves the node to the LRU head (KV data is
     * assumed identical, no copy). Otherwise reuses the LRU tail block, binds
     * it to the key, and moves it to the head.
     * @param key Cache key: {request key}
     * @param src Source slice to cache (size must be <= block size).
     * @return true on success, false on invalid params or no block available.
     */
    bool PutHotKey(const std::string& key, const Slice& src);

    /**
     * @brief Check if the key exists in cache.
     * @param key Cache key: {request key}
     */
    bool HasHotKey(const std::string& key) const;

    /**
     * @brief Get the underlying HotMemBlock pointer and touch LRU.
     * The block will be marked as in_use to prevent it from being reused
     * until ReleaseHotKey is called.
     * @param key : {request key}
     * @return HotMemBlock* on hit; nullptr on miss.
     */
    HotMemBlock* GetHotKey(const std::string& key);

    /**
     * @brief Release a hot key block, marking it as no longer in use.
     * This should be called after the block is no longer being read from.
     * @param key : {request key}
     */
    void ReleaseHotKey(const std::string& key);

    /**
     * @brief Touch a key if it exists in the hot cache.
     * Only touches the LRU, does not modify data or allocation.
     * @param key Cache key.
     * @return true if key exists and was touched, false otherwise.
     */
    bool TouchHotKey(const std::string& key);

    /**
     * @brief Get the number of cache blocks available.
     * @return Number of blocks in LRU queue (cache size).
     */
    size_t GetCacheSize() const;

    /**
     * @brief Get the block size used by this cache.
     * @return Block size in bytes.
     */
    size_t GetBlockSize() const { return block_size_; }

   private:
    // Touch LRU using iterator (avoids duplicate lookup)
    void touchLRU(std::unordered_map<
                  std::string, std::list<HotMemBlock*>::iterator>::iterator it);

    size_t block_size_;  // Actual block size used by this cache

    // All blocks owned by this cache (auto-cleaned on destruction)
    std::vector<std::unique_ptr<HotMemBlock>> blocks_;

    // Bulk allocated memory pointer (nullptr if bulk allocation failed)
    // Must save the original malloc pointer for correct free()
    void* bulk_memory_standard_;

    mutable std::shared_mutex lru_mutex_;
    std::list<HotMemBlock*> lru_queue_ GUARDED_BY(lru_mutex_);  // prefilled LRU
    // key -> iterator of lru_queue_
    std::unordered_map<std::string, std::list<HotMemBlock*>::iterator>
        key_to_lru_it_ GUARDED_BY(lru_mutex_);
};

/**
 * @brief Task for async hot cache put operation.
 */
struct HotCachePutTask {
    std::string key;
    std::vector<uint8_t> data;  // Deep copy of slice data
    size_t size;
    std::shared_ptr<LocalHotCache> hot_cache;

    // Default constructor for empty task
    HotCachePutTask() : size(0), hot_cache(nullptr) {}

    HotCachePutTask(const std::string& k, const Slice& slice,
                    std::shared_ptr<LocalHotCache> cache)
        : key(k), size(slice.size), hot_cache(std::move(cache)) {
        data.resize(size);
        std::memcpy(data.data(), slice.ptr, size);
    }
};

/**
 * @brief Handler for asynchronously executing PutHotKey operations.
 */
class LocalHotCacheHandler {
   public:
    /**
     * @brief Construct a LocalHotCacheHandler.
     * @param hot_cache Pointer to LocalHotCache instance (can be null if cache
     * disabled).
     * @param num_worker_threads Number of worker threads for async processing
     * (default: 2).
     */
    LocalHotCacheHandler(std::shared_ptr<LocalHotCache> hot_cache,
                         size_t num_worker_threads = 2,
                         size_t max_queue_capacity = 1024);

    ~LocalHotCacheHandler();

    // Non-copyable, non-movable
    LocalHotCacheHandler(const LocalHotCacheHandler&) = delete;
    LocalHotCacheHandler& operator=(const LocalHotCacheHandler&) = delete;
    LocalHotCacheHandler(LocalHotCacheHandler&&) = delete;
    LocalHotCacheHandler& operator=(LocalHotCacheHandler&&) = delete;

    /**
     * @brief Submit an async task to put a slice into the hot cache.
     *
     * The slice data will be deep copied, so the original slice data can be
     * safely freed after this function returns.
     * @param key Cache key: {object key}
     * @param slice Source slice to cache.
     * @return true if task was successfully submitted, false otherwise (e.g.,
     * hot_cache_ is null or handler is shutdown).
     */
    bool SubmitPutTask(const std::string& key, const Slice& slice);

   private:
    void workerThread();

    std::shared_ptr<LocalHotCache> hot_cache_;
    std::vector<std::thread> workers_;
    std::queue<HotCachePutTask> task_queue_;
    size_t max_queue_capacity_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    bool shutdown_;
};

}  // namespace mooncake
