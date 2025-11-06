#include "local_hot_cache.h"

#include <algorithm>
#include <cstring>
#include <cstdlib>
#include <shared_mutex>
#include <cstdint>
#include <glog/logging.h>

namespace mooncake {
namespace {
constexpr size_t STANDARD_BLOCK_SIZE = 16 * 1024 * 1024; // 16MB standard block
constexpr size_t SMALL_BLOCK_SIZE = 4 * 1024 * 1024;    // 4MB small block
}

LocalHotCache::LocalHotCache(size_t total_size_bytes, double ratio)
    : bulk_memory_standard_(nullptr), bulk_memory_small_(nullptr) {
    if (ratio < 0.0 || ratio > 1.0) {
        LOG(WARNING) << "invalid HOT_CACHE_BLOCK_RATIO='" << ratio << "', using default 1.0";
        ratio = 1.0;
    }

    // calculate the block number
    size_t max_standard_blocks = 0;
    size_t standard_block_num = 0;
    if (total_size_bytes > 0) {
        max_standard_blocks = total_size_bytes / STANDARD_BLOCK_SIZE;
        standard_block_num = static_cast<size_t>(static_cast<double>(max_standard_blocks) * ratio);
    }

    // compute small blocks from remaining bytes (reserved for future use)
    size_t used_standard_bytes = standard_block_num * STANDARD_BLOCK_SIZE;
    size_t remaining_bytes = total_size_bytes - used_standard_bytes;
    size_t small_block_num = remaining_bytes / SMALL_BLOCK_SIZE;

    blocks_.reserve(standard_block_num + small_block_num);

    // Try to allocate all standard blocks in one bulk allocation first
    size_t total_standard_size = standard_block_num * STANDARD_BLOCK_SIZE;
    if (standard_block_num > 0 && total_standard_size > 0) {
        bulk_memory_standard_ = std::malloc(total_standard_size);
        if (bulk_memory_standard_) {
            // Bulk allocation succeeded: split into individual blocks
            char* base_ptr = static_cast<char*>(bulk_memory_standard_);
            for (size_t i = 0; i < standard_block_num; ++i) {
                auto block = std::make_unique<HotMemBlock>();
                block->addr = base_ptr + i * STANDARD_BLOCK_SIZE;
                block->size = STANDARD_BLOCK_SIZE;
                block->in_use = false;
                lru_queue_.push_back(block.get());
                blocks_.emplace_back(std::move(block));
            }
        } else {
            // Bulk allocation failed: fall back to individual allocations
            for (size_t i = 0; i < standard_block_num; ++i) {
                void* ptr = std::malloc(STANDARD_BLOCK_SIZE);
                if (ptr) {
                    auto block = std::make_unique<HotMemBlock>();
                    block->addr = ptr;
                    block->size = STANDARD_BLOCK_SIZE;
                    block->in_use = false;
                    lru_queue_.push_back(block.get());
                    blocks_.emplace_back(std::move(block));
                }
            }
        }
    }

    // Try to allocate all small blocks in one bulk allocation first
    size_t total_small_size = small_block_num * SMALL_BLOCK_SIZE;
    if (small_block_num > 0 && total_small_size > 0) {
        bulk_memory_small_ = std::malloc(total_small_size);
        if (bulk_memory_small_) {
            // Bulk allocation succeeded: split into individual blocks
            char* base_ptr = static_cast<char*>(bulk_memory_small_);
            for (size_t i = 0; i < small_block_num; ++i) {
                auto block = std::make_unique<HotMemBlock>();
                block->addr = base_ptr + i * SMALL_BLOCK_SIZE;
                block->size = SMALL_BLOCK_SIZE;
                block->in_use = false;
                blocks_.emplace_back(std::move(block));
            }
        } else {
            // Bulk allocation failed: fall back to individual allocations
            for (size_t i = 0; i < small_block_num; ++i) {
                void* ptr = std::malloc(SMALL_BLOCK_SIZE);
                if (ptr) {
                    auto block = std::make_unique<HotMemBlock>();
                    block->addr = ptr;
                    block->size = SMALL_BLOCK_SIZE;
                    block->in_use = false;
                    blocks_.emplace_back(std::move(block));
                }
            }
        }
    }
}

LocalHotCache::~LocalHotCache() {
    // Save bulk allocation status before freeing (needed for later decision)
    bool had_bulk_standard = (bulk_memory_standard_ != nullptr);
    bool had_bulk_small = (bulk_memory_small_ != nullptr);

    // Free bulk allocated memory first if it exists
    if (bulk_memory_standard_) {
        std::free(bulk_memory_standard_);
        bulk_memory_standard_ = nullptr;
    }
    if (bulk_memory_small_) {
        std::free(bulk_memory_small_);
        bulk_memory_small_ = nullptr;
    }

    // Free individually allocated blocks (only if bulk allocation failed)
    if (!had_bulk_standard && !had_bulk_small) {
        // Both bulk allocations failed, so all blocks were individually allocated
        for (auto& block : blocks_) {
            if (block && block->addr) {
                std::free(block->addr);
            }
        }
    } else if (!had_bulk_standard) {
        // Standard blocks were individually allocated
        for (auto& block : blocks_) {
            if (block && block->addr && block->size == STANDARD_BLOCK_SIZE) {
                std::free(block->addr);
            }
        }
    } else if (!had_bulk_small) {
        // Small blocks were individually allocated
        for (auto& block : blocks_) {
            if (block && block->addr && block->size == SMALL_BLOCK_SIZE) {
                std::free(block->addr);
            }
        }
    }
}

bool LocalHotCache::PutHotSlice(const std::string& key, const Slice& src) {
    if (src.ptr == nullptr || src.size == 0) {
        return false;
    }

    // only support <= 16MB standard block since slice size is 16MB
    if (src.size > STANDARD_BLOCK_SIZE) {
        return false; 
    }

    std::unique_lock<std::shared_mutex> lk(lru_mutex_);

    // if key already exists, only touch LRU
    auto it_exist = key_to_lru_it_.find(key);
    if (it_exist != key_to_lru_it_.end()) {
        touchLRU(key);
        return true;
    }

    // use LRU tail block as victim for reuse
    if (lru_queue_.empty()) return false;
    HotMemBlock* victim = lru_queue_.back();
    lru_queue_.pop_back();
    lru_queue_.push_front(victim);
    auto it_front = lru_queue_.begin();

    // if victim is bound to old key, remove old mapping
    auto old = block_to_key_map_.find(victim);
    if (old != block_to_key_map_.end()) {
        auto it_map = key_to_lru_it_.find(old->second);
        if (it_map != key_to_lru_it_.end()) {
            key_to_lru_it_.erase(it_map);
        }
        block_to_key_map_.erase(old);
    }

    // copy data from src slice to victim block
    std::memcpy(victim->addr, src.ptr, src.size);
    victim->in_use = true;
    victim->size = src.size;

    // create new mapping
    key_to_lru_it_[key] = it_front;
    block_to_key_map_[victim] = key;

    return true;
}

bool LocalHotCache::HasHotSlice(const std::string& key) const {
    std::shared_lock<std::shared_mutex> lk(lru_mutex_);
    return key_to_lru_it_.find(key) != key_to_lru_it_.end();
}

HotMemBlock* LocalHotCache::GetHotSlice(const std::string& key) {
    std::unique_lock<std::shared_mutex> lk(lru_mutex_);
    auto it = key_to_lru_it_.find(key);
    if (it == key_to_lru_it_.end()) {
        return nullptr;
    }
    HotMemBlock* blk = *(it->second);
    if (!blk) {
        return nullptr;
    }

    // update lru queue
    lru_queue_.erase(it->second);
    lru_queue_.push_front(blk);
    it->second = lru_queue_.begin();

    return blk;
}

void LocalHotCache::touchLRU(const std::string& key) {
    auto it = key_to_lru_it_.find(key);
    if (it == key_to_lru_it_.end()) return;
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
    std::shared_ptr<LocalHotCache> hot_cache,
    size_t num_worker_threads)
    : hot_cache_(hot_cache),
      shutdown_(false) {
    size_t workers = (num_worker_threads > 0) ? num_worker_threads : kDefaultHotCacheWorkers;
    
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
        shutdown_.store(true);
    }
    queue_cv_.notify_all();

    // Wait for all workers to finish
    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

void LocalHotCacheHandler::SubmitPutTask(const std::string& key, const Slice& slice) {
    if (!hot_cache_) {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        if (shutdown_.load()) {
            LOG(WARNING) << "Attempting to submit task to shutdown LocalHotCacheHandler";
            return;
        }
        task_queue_.emplace(key, slice, hot_cache_);
    }
    queue_cv_.notify_one();
}

void LocalHotCacheHandler::workerThread() {
    VLOG(2) << "LocalHotCacheHandler worker thread started";

    while (true) {
        HotCachePutTask task;

        // Wait for task or shutdown signal
        {
            std::unique_lock<std::mutex> lock(queue_mutex_);
            queue_cv_.wait(lock, [this] {
                return shutdown_.load() || !task_queue_.empty();
            });

            if (shutdown_.load() && task_queue_.empty()) {
                break;
            }

            if (!task_queue_.empty()) {
                task = std::move(task_queue_.front());
                task_queue_.pop();
            }
        }

        // Execute the task if we have one
        if (task.hot_cache && !task.key.empty()) {
            try {
                Slice slice;
                slice.ptr = task.data.data();
                slice.size = task.size;
                task.hot_cache->PutHotSlice(task.key, slice);
                VLOG(2) << "Hot cache put task completed for key: " << task.key;
            } catch (const std::exception& e) {
                LOG(ERROR) << "Exception during async hot cache put for key " << task.key
                           << ": " << e.what();
            }
        }
    }

    VLOG(2) << "LocalHotCacheHandler worker thread exiting";
}

} // namespace mooncake
