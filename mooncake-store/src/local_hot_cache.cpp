#include "local_hot_cache.h"

#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <shared_mutex>
#include <cstdint>
#include <glog/logging.h>

namespace mooncake {
namespace {
constexpr size_t DEFAULT_BLOCK_SIZE = 16 * 1024 * 1024;  // 16MB default block
}

LocalHotCache::LocalHotCache(size_t total_size_bytes, size_t block_size_bytes)
    : block_size_((block_size_bytes > 0) ? block_size_bytes
                                         : DEFAULT_BLOCK_SIZE),
      bulk_memory_standard_(nullptr) {
    // calculate the block number
    size_t block_num = 0;
    if (total_size_bytes > 0) {
        block_num = total_size_bytes / block_size_;
    }

    blocks_.reserve(block_num);

    // Try to allocate all blocks in one bulk allocation first
    size_t total_size = block_num * block_size_;
    if (block_num > 0 && total_size > 0) {
        bulk_memory_standard_ = std::malloc(total_size);
        if (bulk_memory_standard_) {
            // Bulk allocation succeeded: split into individual blocks
            char* base_ptr = static_cast<char*>(bulk_memory_standard_);
            for (size_t i = 0; i < block_num; ++i) {
                auto block = std::make_unique<HotMemBlock>();
                block->addr = base_ptr + i * block_size_;
                block->size = block_size_;
                block->ref_count = 0;
                block->key_.clear();  // Initialize key as empty
                lru_queue_.push_back(block.get());
                blocks_.emplace_back(std::move(block));
            }
        } else {
            // Bulk allocation failed: fall back to individual allocations
            for (size_t i = 0; i < block_num; ++i) {
                void* ptr = std::malloc(block_size_);
                if (ptr) {
                    auto block = std::make_unique<HotMemBlock>();
                    block->addr = ptr;
                    block->size = block_size_;
                    block->ref_count = 0;
                    block->key_.clear();  // Initialize key as empty
                    lru_queue_.push_back(block.get());
                    blocks_.emplace_back(std::move(block));
                }
            }
        }
    }
}

LocalHotCache::~LocalHotCache() {
    // Free bulk allocated memory if it exists
    if (bulk_memory_standard_) {
        std::free(bulk_memory_standard_);
    } else {
        // Free individually allocated blocks
        for (auto& block : blocks_) {
            if (block && block->addr) {
                std::free(block->addr);
            }
        }
    }
}

bool LocalHotCache::PutHotKey(HotMemBlock* block) {
    std::unique_lock<std::shared_mutex> lk(lru_mutex_);

    if (!block) return false;

    // Handle return-to-lru tail case (empty key or cancelled task)
    if (block->key_.empty()) {
        block->ref_count = 0;
        lru_queue_.push_back(block);  // Add to tail as free block
        return false;
    }

    const std::string& key = block->key_;

    // Race condition check: did someone else insert this key while we were
    // copying
    if (key_to_lru_it_.find(key) != key_to_lru_it_.end()) {
        // Lost race -> Return to lru tail as free block
        block->key_.clear();
        block->ref_count = 0;
        lru_queue_.push_back(block);
        return false;
    }

    // Publish the new mapping
    block->ref_count = 0;
    lru_queue_.push_front(block);
    key_to_lru_it_[key] = lru_queue_.begin();
    return true;
}

bool LocalHotCache::HasHotKey(const std::string& key) const {
    std::shared_lock<std::shared_mutex> lk(lru_mutex_);
    return key_to_lru_it_.find(key) != key_to_lru_it_.end();
}

HotMemBlock* LocalHotCache::GetHotKey(const std::string& key) {
    std::unique_lock<std::shared_mutex> lk(lru_mutex_);
    auto it = key_to_lru_it_.find(key);
    if (it == key_to_lru_it_.end()) {
        return nullptr;
    }
    HotMemBlock* blk = *(it->second);
    if (!blk) {
        LOG(ERROR) << "Invalid block for key: " << key;
        return nullptr;
    }

    // Mark block as in use to prevent it from being reused during memcpy
    blk->ref_count++;

    // update lru queue
    touchLRU(it);

    return blk;
}

void LocalHotCache::ReleaseHotKey(const std::string& key) {
    std::unique_lock<std::shared_mutex> lk(lru_mutex_);
    auto it = key_to_lru_it_.find(key);
    if (it == key_to_lru_it_.end()) {
        return;
    }
    HotMemBlock* block = *(it->second);
    if (block) {
        block->ref_count--;
    }
}

bool LocalHotCache::TouchHotKey(const std::string& key) {
    std::unique_lock<std::shared_mutex> lk(lru_mutex_);
    auto it = key_to_lru_it_.find(key);
    if (it == key_to_lru_it_.end()) {
        return false;
    }
    touchLRU(it);
    return true;
}

HotMemBlock* LocalHotCache::GetFreeBlock() {
    std::unique_lock<std::shared_mutex> lk(lru_mutex_);

    if (lru_queue_.empty()) {
        return nullptr;
    }

    // Find the first block from the tail that is not in use
    auto victim_it = lru_queue_.end();
    for (auto it = lru_queue_.rbegin(); it != lru_queue_.rend(); ++it) {
        if ((*it)->ref_count == 0) {
            // Convert reverse iterator to forward iterator
            victim_it = std::next(it).base();
            break;
        }
    }

    // If all blocks are in use, cannot reuse any block
    if (victim_it == lru_queue_.end()) {
        return nullptr;
    }

    HotMemBlock* victim = *victim_it;

    // Remove from LRU list completely (detach)
    lru_queue_.erase(victim_it);

    // If victim was bound to an old key, remove the old mapping
    if (!victim->key_.empty()) {
        auto it_map = key_to_lru_it_.find(victim->key_);
        if (it_map != key_to_lru_it_.end()) {
            key_to_lru_it_.erase(it_map);
        }
        victim->key_.clear();
    }

    // Now this block is exclusively owned by the caller.
    // It is detached from the cache structure.
    victim->ref_count = 0;

    return victim;
}

void LocalHotCache::touchLRU(
    std::unordered_map<std::string, std::list<HotMemBlock*>::iterator>::iterator
        it) {
    HotMemBlock* blk = *(it->second);
    lru_queue_.erase(it->second);
    lru_queue_.push_front(blk);
    it->second = lru_queue_.begin();
}

size_t LocalHotCache::GetCacheSize() const {
    std::shared_lock<std::shared_mutex> lk(lru_mutex_);
    return lru_queue_.size();
}

constexpr size_t kDefaultHotCacheWorkers = 2;

LocalHotCacheHandler::LocalHotCacheHandler(
    std::shared_ptr<LocalHotCache> hot_cache, size_t num_worker_threads,
    size_t max_queue_capacity)
    : hot_cache_(hot_cache),
      max_queue_capacity_(max_queue_capacity),
      shutdown_(false) {
    size_t workers =
        (num_worker_threads > 0) ? num_worker_threads : kDefaultHotCacheWorkers;

    // Start worker threads
    workers_.reserve(workers);
    for (size_t i = 0; i < workers; ++i) {
        workers_.emplace_back(&LocalHotCacheHandler::workerThread, this);
    }
}

LocalHotCacheHandler::~LocalHotCacheHandler() {
    // Signal shutdown
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        shutdown_ = true;
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

bool LocalHotCacheHandler::SubmitPutTask(const std::string& key,
                                         const Slice& slice) {
    if (!hot_cache_) {
        return false;
    }

    if (slice.ptr == nullptr || slice.size == 0) {
        return false;
    }

    // Optimization: if key exists, just touch LRU to avoid data copy
    if (hot_cache_->TouchHotKey(key)) {
        return true;
    }

    // Check queue capacity before copy
    if (max_queue_capacity_ > 0) {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (task_queue_.size() >= max_queue_capacity_) {
            LOG_EVERY_N(WARNING, 100)
                << "Hot cache task queue full (" << task_queue_.size()
                << "), dropping key: " << key;
            return false;
        }
    }

    // Try to get a free block (may evict from LRU tail)
    HotMemBlock* block = hot_cache_->GetFreeBlock();
    if (!block) {
        LOG(ERROR) << "Hot cache is fully in-use, fail to get a free block: "
                   << key;
        return false;
    }

    // Check size compatibility
    if (slice.size > block->size) {
        // Slice too big for block, return block to pool
        block->key_.clear();
        hot_cache_->PutHotKey(block);
        return false;
    }

    // Copy data directly into the block (No Lock held here!)
    // This is the only copy operation: Source -> Block
    std::memcpy(block->addr, slice.ptr, slice.size);
    block->size = slice.size;
    block->key_ = key;  // Set key for insertion

    HotCachePutTask task(key, slice, block, hot_cache_);

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_) {
            // Must return block to avoid leak
            hot_cache_->PutHotKey(block);
            LOG(WARNING)
                << "Attempting to submit task to shutdown LocalHotCacheHandler";
            return false;
        }
        task_queue_.push(std::move(task));
    }
    queue_cv_.notify_one();
    return true;
}

void LocalHotCacheHandler::workerThread() {
    VLOG(2) << "LocalHotCacheHandler worker thread started";

    while (true) {
        HotCachePutTask task;

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            while (!shutdown_ && task_queue_.empty()) {
                queue_cv_.wait(lock);
            }

            if (shutdown_ && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.hot_cache && task.block) {
            try {
                // Insert the pre-filled block into LRU
                if (task.hot_cache->PutHotKey(task.block)) {
                    VLOG(2) << "Put task completed: " << task.key;
                } else {
                    VLOG(2) << "Put task skipped: " << task.key;
                }
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async hot cache put for key "
                           << task.key << ": " << e.what();
                // Ensure block is returned to pool on exception
                // Clear key to force return-to-pool behavior
                task.block->key_.clear();
                task.hot_cache->PutHotKey(task.block);
            }
        }
    }

    VLOG(2) << "LocalHotCacheHandler worker thread exiting";
}
}  // namespace mooncake