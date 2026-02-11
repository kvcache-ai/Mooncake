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

#include "mutex.h"
#include "shm_helper.h"
#include "types.h"

namespace mooncake {

/**
 * @brief Memory block metadata for hot cache.
 */
struct HotMemBlock {
    void* addr;
    size_t size;
    std::atomic<int> ref_count;
    std::string key_;
    HotMemBlock() : addr(nullptr), size(0), ref_count(0) {}
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
     * @param use_shm If true, allocate via memfd for cross-process sharing.
     */
    LocalHotCache(size_t total_size_bytes, size_t block_size_bytes = 0,
                  bool use_shm = false);

    /**
     * @brief Destructor.
     */
    ~LocalHotCache();

    /**
     * @brief Insert a populated block into the cache.
     * Takes ownership of the block and inserts it into the LRU.
     * The block must have been obtained from GetFreeBlock() and have key_ set.
     * If the key already exists (race condition) or is empty, the block is
     * cleared and returned to the pool as a free block.
     * @param block The block containing the data and key.
     * @return true if inserted successfully, false if race condition or error.
     */
    bool PutHotKey(HotMemBlock* block);

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
     * @brief Get a free block for writing.
     * Detaches a block from the LRU tail (evicting if necessary) and returns
     * it. The returned block is owned by the caller and must be returned via
     * PutHotKey.
     * @return Pointer to a HotMemBlock, or nullptr if no block is available.
     */
    HotMemBlock* GetFreeBlock();

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

    /**
     * @brief Get the shm segment backing this cache.
     * Only valid when constructed with use_shm=true.
     * @return shared_ptr to the ShmSegment, or nullptr if shm is disabled.
     */
    std::shared_ptr<ShmHelper::ShmSegment> GetShmSegment() const {
        return shm_segment_;
    }

    /**
     * @brief Compute offset of a block address relative to the bulk base.
     * Used by dummy clients to translate to their own mmap'd address.
     * @return offset in bytes, or SIZE_MAX if addr is not in the bulk region.
     */
    size_t GetBlockOffset(const void* addr) const;

   private:
    // Touch LRU using iterator (avoids duplicate lookup)
    void touchLRU(std::unordered_map<
                  std::string, std::list<HotMemBlock*>::iterator>::iterator it);

    size_t block_size_;  // Actual block size used by this cache

    // All blocks owned by this cache (auto-cleaned on destruction)
    std::vector<std::unique_ptr<HotMemBlock>> blocks_;

    // Bulk allocated memory pointer (nullptr if allocation failed)
    void* bulk_memory_standard_;
    size_t bulk_memory_size_ = 0;

    // Shared memory segment (non-null only when use_shm=true)
    std::shared_ptr<ShmHelper::ShmSegment> shm_segment_;
    bool use_shm_ = false;

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
    HotMemBlock* block;  // Pointer to the allocated block
    size_t size;
    std::shared_ptr<LocalHotCache> hot_cache;

    // Default constructor for empty task
    HotCachePutTask() : block(nullptr), size(0), hot_cache(nullptr) {}

    HotCachePutTask(const std::string& k, const Slice& slice, HotMemBlock* blk,
                    std::shared_ptr<LocalHotCache> cache)
        : key(k), block(blk), size(slice.size), hot_cache(std::move(cache)) {
        // No data copy here; memcpy is done by the caller into block->addr
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
