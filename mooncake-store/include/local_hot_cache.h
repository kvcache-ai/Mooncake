#pragma once

#include <atomic>
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
    void* addr;    // Memory address
    size_t size;   // Block size in bytes
    bool in_use;   // Whether the block is currently in use
};

/**
 * @brief Local hot cache used for hot kv cache in the distributed store.
 */
class LocalHotCache {
public:
    /**
     * @brief Construct a LocalHotCache.
     * @param total_size_bytes Desired total local hot cache size in bytes.
     *        The standard block size is 16MB.
     */
    LocalHotCache(size_t total_size_bytes);

    /**
     * @brief Destructor.
     */
    ~LocalHotCache();

    /**
     * @brief Insert or touch an entry.
     *
     * If key already exists, it only moves the node to the LRU head (KV data is
     * assumed identical, no copy). Otherwise reuses the LRU tail block, binds it
     * to the key, and moves it to the head.
     * @param key Cache key: {request key}_{slice index}
     * @param src Source slice to cache (size must be <= standard block size).
     * @return true on success, false on invalid params or no block available.
     */
    bool PutHotSlice(const std::string& key, const Slice& src);

    /**
     * @brief Check if the key exists in cache.
     * @param key Cache key: {request key}_{slice index}
     */
    bool HasHotSlice(const std::string& key) const;

    /**
     * @brief Get the underlying HotMemBlock pointer and touch LRU.
     * @param key : {request key}_{slice index}
     * @return HotMemBlock* on hit; nullptr on miss.
     */
    HotMemBlock* GetHotSlice(const std::string& key);

    /**
     * @brief Get the number of cache blocks available.
     * @return Number of standard blocks in LRU queue (cache size).
     */
    size_t GetCacheSize() const;

private:
    void touchLRU(const std::string& key);

    // All blocks owned by this cache (auto-cleaned on destruction)
    std::vector<std::unique_ptr<HotMemBlock>> blocks_;

    // Bulk allocated memory pointer (nullptr if bulk allocation failed)
    // Must save the original malloc pointer for correct free()
    void* bulk_memory_standard_;

    mutable std::shared_mutex lru_mutex_;
    std::list<HotMemBlock*> lru_queue_; // prefilled LRU; front is MRU, back is LRU
    // key -> iterator of lru_queue_
    std::unordered_map<std::string, std::list<HotMemBlock*>::iterator> key_to_lru_it_;
    // block -> key, used to remove old mapping when reusing LRU tail block
    std::unordered_map<HotMemBlock*, std::string> block_to_key_map_;
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
 * @brief Handler for asynchronously executing PutHotSlice operations.
 */
class LocalHotCacheHandler {
public:
    /**
     * @brief Construct a LocalHotCacheHandler.
     * @param hot_cache Pointer to LocalHotCache instance (can be null if cache disabled).
     * @param num_worker_threads Number of worker threads for async processing (default: 2).
     */
    LocalHotCacheHandler(std::shared_ptr<LocalHotCache> hot_cache,
                         size_t num_worker_threads = 2);

    ~LocalHotCacheHandler();

    // Non-copyable, non-movable
    LocalHotCacheHandler(const LocalHotCacheHandler&) = delete;
    LocalHotCacheHandler& operator=(const LocalHotCacheHandler&) = delete;
    LocalHotCacheHandler(LocalHotCacheHandler&&) = delete;
    LocalHotCacheHandler& operator=(LocalHotCacheHandler&&) = delete;

    /**
     * @brief Submit an async task to put a slice into the hot cache.
     * 
     * The slice data will be deep copied, so the original slice data can be safely
     * freed after this function returns.
     * @param key Cache key (composite key: {object key}_{slice index}).
     * @param slice Source slice to cache.
     * @return true if task was successfully submitted, false otherwise (e.g., hot_cache_ is null or handler is shutdown).
     */
    bool SubmitPutTask(const std::string& key, const Slice& slice);

private:
    void workerThread();

    std::shared_ptr<LocalHotCache> hot_cache_;
    std::vector<std::thread> workers_;
    std::queue<HotCachePutTask> task_queue_;
    std::mutex queue_mutex_;
    std::condition_variable queue_cv_;
    std::atomic<bool> shutdown_;
};

} // namespace mooncake
