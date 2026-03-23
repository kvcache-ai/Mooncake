#include <glog/logging.h>

#include <chrono>
#include <cstring>

#include "tiered_cache/tiers/storage_tier.h"
#include "tiered_cache/copier_registry.h"
#include "utils.h"

namespace mooncake {

StorageTier::StorageTier(UUID tier_id, const std::vector<std::string>& tags,
                         size_t capacity)
    : tier_id_(tier_id), tags_(tags), capacity_(capacity) {}

void StorageTier::FreeStagingMemory(char* ptr) {
    if (ptr) {
        free_memory("", ptr);
    }
}

StorageTier::~StorageTier() {
    // Stop flush thread
    stop_flush_thread_.store(true);
    flush_trigger_cv_.notify_all();
    if (flush_thread_.joinable()) {
        flush_thread_.join();
    }

    // Discard any pending data rather than blocking on I/O.
    // As a cache, we can tolerate data loss.
    {
        std::unique_lock<std::mutex> lock(batch_mutex_);
        if (!pending_batch_.empty()) {
            LOG(INFO) << "StorageTier dtor: Discarding "
                      << pending_batch_.size() << " pending items.";
            pending_batch_.clear();
            pending_batch_size_.store(0);
        }
    }

    if (staging_allocator_) {
        size_t allocated_size = staging_allocator_->size();
        if (allocated_size > 0) {
            LOG(WARNING) << "StorageTier " << tier_id_
                         << " is being destroyed with " << allocated_size
                         << " bytes still pinned in the staging pool. Waiting "
                            "for buffers to be released...";

            constexpr int kCheckIntervalMs = 100;
            int log_counter = 0;
            while (allocated_size > 0) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(kCheckIntervalMs));
                allocated_size = staging_allocator_->size();
                if (++log_counter == 10 && allocated_size > 0) {
                    LOG(INFO) << "StorageTier " << tier_id_ << " waiting for "
                              << allocated_size
                              << " bytes to be released from staging pool...";
                    log_counter = 0;
                }
            }
        }
    }

    staging_allocator_.reset();
    staging_memory_.reset();
}

tl::expected<void, ErrorCode> StorageTier::Init(TieredBackend* backend,
                                                TransferEngine* engine) {
    static_cast<void>(engine);
    backend_ = backend;
    try {
        auto config = FileStorageConfig::FromEnvironment();
        if (!config.Validate()) {
            LOG(ERROR) << "Invalid FileStorageConfig for StorageTier";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
        if (config.local_buffer_size <= 0) {
            LOG(ERROR) << "StorageTier local buffer size must be positive, got "
                       << config.local_buffer_size;
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        constexpr size_t kStagingAlignment = 64;
        staging_memory_.reset(
            static_cast<char*>(allocate_buffer_allocator_memory(
                static_cast<size_t>(config.local_buffer_size), "",
                kStagingAlignment)));
        if (!staging_memory_) {
            LOG(ERROR) << "Failed to preallocate storage staging pool, size="
                       << config.local_buffer_size;
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
        staging_buffer_capacity_ =
            static_cast<size_t>(config.local_buffer_size);
        std::string segment_name = "storage_tier_staging_" +
                                   std::to_string(tier_id_.first) + "-" +
                                   std::to_string(tier_id_.second);
        staging_allocator_ = std::make_shared<OffsetBufferAllocator>(
            segment_name, reinterpret_cast<uintptr_t>(staging_memory_.get()),
            staging_buffer_capacity_, segment_name, tier_id_);

        auto backend_res = CreateStorageBackend(config);
        if (!backend_res) {
            LOG(ERROR) << "Failed to create underlying storage backend: "
                       << backend_res.error();
            return tl::make_unexpected(backend_res.error());
        }
        storage_backend_ = backend_res.value();

        auto init_res = storage_backend_->Init();
        if (!init_res) {
            LOG(ERROR) << "Failed to init storage backend: "
                       << init_res.error();
            return init_res;
        }

        // Start background flush thread
        flush_thread_ = std::thread(&StorageTier::FlushWorker, this);

    } catch (const std::exception& e) {
        LOG(ERROR) << "Exception during StorageTier init: " << e.what();
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    LOG(INFO) << "StorageTier initialized successfully. staging_pool_size="
              << staging_buffer_capacity_;
    return {};
}

tl::expected<void, ErrorCode> StorageTier::Allocate(size_t size,
                                                    DataSource& data) {
    // Check capacity before allocation (pending + persisted)
    size_t current_usage = GetUsage();
    if (capacity_ > 0 && current_usage + size > capacity_) {
        size_t needed = (current_usage + size) - capacity_;
        VLOG(1) << "StorageTier capacity exceeded: usage=" << current_usage
                << ", requested=" << size << ", capacity=" << capacity_
                << ", need to free=" << needed;

        // Try to evict buckets to free up enough space
        auto evict_res = TriggerBucketEviction(needed);
        if (evict_res.has_value()) {
            size_t freed = evict_res.value();
            // Recheck capacity after eviction
            current_usage = GetUsage();
            if (current_usage + size <= capacity_) {
                LOG(INFO) << "Successfully freed " << freed
                          << " bytes via bucket eviction, new usage="
                          << current_usage;
            } else {
                LOG(WARNING) << "Freed " << freed
                             << " bytes but still insufficient space (usage="
                             << current_usage << ", requested=" << size
                             << ", capacity=" << capacity_ << ")";
                return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
            }
        } else {
            LOG(WARNING) << "Failed to evict buckets: " << evict_res.error();
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }
    }

    if (!staging_allocator_) {
        LOG(ERROR) << "StorageTier staging allocator is not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    if (size > staging_buffer_capacity_) {
        LOG(WARNING) << "Requested staging buffer exceeds configured local "
                        "buffer pool: requested="
                     << size << ", pool_capacity=" << staging_buffer_capacity_;
        return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
    }

    auto staging_handle = staging_allocator_->allocate(size);
    if (!staging_handle) {
        LOG(WARNING) << "Failed to allocate " << size
                     << " bytes from storage staging pool. largest_free_region="
                     << staging_allocator_->getLargestFreeRegion()
                     << ", pool_capacity=" << staging_buffer_capacity_
                     << ", pool_usage=" << staging_allocator_->size();
        return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
    }

    data.buffer = std::make_unique<StorageBuffer>(std::move(staging_handle),
                                                  storage_backend_.get());
    data.type = MemoryType::NVME;

    return {};
}

tl::expected<void, ErrorCode> StorageTier::Free(DataSource data) {
    if (!data.buffer) return {};

    auto* staging = static_cast<StorageBuffer*>(data.buffer.get());
    if (!staging) return {};

    size_t size = staging->size();
    std::string key = staging->GetKey();

    {
        std::unique_lock<std::mutex> lock(batch_mutex_);

        // Wait for any in-progress flush referencing this buffer.
        // State changes happen under batch_mutex_, so no TOCTOU race.
        flush_cv_.wait(lock, [staging] { return !staging->IsFlushing(); });

        if (!key.empty()) {
            auto it = pending_batch_.find(key);
            if (it != pending_batch_.end() && it->second == staging) {
                pending_batch_.erase(it);
                pending_batch_size_.fetch_sub(size);
                VLOG(1) << "Removed key " << key
                        << " from pending batch (Freed explicitly)";
                return {};
            }
        }
    }

    // Not in pending batch — update persisted accounting if needed
    if (staging->IsPersisted()) {
        persisted_live_data_bytes_.fetch_sub(size, std::memory_order_acq_rel);
        VLOG(1) << "Freed persisted live data for key " << key
                << ", size=" << size;

        // Mark key as deleted for fragmentation tracking
        if (storage_backend_) {
            auto mark_res = storage_backend_->MarkKeyDeleted(key);
            if (!mark_res) {
                LOG(WARNING) << "Failed to mark key " << key
                             << " as deleted: " << mark_res.error();
            }
        }
    }

    return {};
}

tl::expected<void, ErrorCode> StorageTier::Commit(const std::string& key,
                                                  const DataSource& data) {
    auto* staging = static_cast<StorageBuffer*>(data.buffer.get());
    if (!staging) {
        LOG(ERROR) << "Invalid buffer type for StorageTier commit";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    {
        std::unique_lock<std::mutex> lock(batch_mutex_);

        // Add to pending batch
        staging->SetKey(key);
        pending_batch_[key] = staging;
        pending_batch_size_.fetch_add(staging->size());

        // Check thresholds for async flush trigger
        const size_t batch_size = pending_batch_.size();
        if (batch_size >= batch_count_threshold_ ||
            pending_batch_size_.load() >= batch_size_threshold_) {
            flush_requested_.store(true);
            flush_trigger_cv_.notify_one();
        }
    }

    return {};
}

tl::expected<void, ErrorCode> StorageTier::Flush() { return FlushInternal(); }

void StorageTier::FlushWorker() {
    while (!stop_flush_thread_.load()) {
        std::unique_lock<std::mutex> lock(flush_trigger_mutex_);
        flush_trigger_cv_.wait(lock, [this] {
            return flush_requested_.load() || stop_flush_thread_.load();
        });

        if (stop_flush_thread_.load()) break;

        flush_requested_.store(false);
        lock.unlock();

        // Execute flush
        auto result = FlushInternal();
        if (!result.has_value()) {
            LOG(ERROR) << "Background flush failed: " << result.error();
        }
    }
}

tl::expected<void, ErrorCode> StorageTier::FlushInternal() {
    std::unordered_map<std::string, StorageBuffer*> snapshot;
    std::unordered_map<std::string, std::vector<Slice>> batch_to_write;
    size_t batch_total_size = 0;

    // 1. Snapshot under lock: mark flushing, swap out pending batch
    {
        std::unique_lock<std::mutex> lock(batch_mutex_);
        if (pending_batch_.empty()) return {};

        snapshot.swap(pending_batch_);
        pending_batch_size_.store(0);

        batch_to_write.reserve(snapshot.size());
        for (const auto& kv : snapshot) {
            kv.second->SetFlushing(true);
            batch_to_write[kv.first] = {kv.second->ToSlice()};
            batch_total_size += kv.second->size();
        }
    }

    // 2. IO without lock
    auto res = storage_backend_->BatchOffload(batch_to_write, nullptr);

    // 3. Finalize under lock: update state and notify waiters
    {
        std::unique_lock<std::mutex> lock(batch_mutex_);

        if (!res) {
            LOG(ERROR) << "Flush failed: " << res.error();
            // Restore pending entries on failure
            for (auto& kv : snapshot) {
                kv.second->SetFlushing(false);
                pending_batch_[kv.first] = kv.second;
                pending_batch_size_.fetch_add(kv.second->size());
            }
            flush_cv_.notify_all();
            return tl::make_unexpected(res.error());
        }

        for (auto& kv : snapshot) {
            kv.second->Persist();  // Releases staging memory
            kv.second->SetFlushing(false);
        }
        persisted_live_data_bytes_.fetch_add(batch_total_size);
    }
    flush_cv_.notify_all();

    return {};
}

size_t StorageTier::GetCapacity() const {
    // Use configured capacity if set, otherwise use config default
    if (capacity_ > 0) {
        return capacity_;
    }
    // Fallback to storage backend config
    if (storage_backend_) {
        return storage_backend_->file_storage_config_.total_size_limit;
    }
    return 1024ULL * 1024 * 1024 * 1024;  // 1TB fallback
}

size_t StorageTier::GetUsage() const {
    // Total usage = live bytes already persisted in the backend + all bytes
    // currently reserved from the staging pool, including uncommitted
    // allocations.
    const size_t staging_usage =
        staging_allocator_ ? staging_allocator_->size() : 0;
    return persisted_live_data_bytes_.load() + staging_usage;
}

tl::expected<size_t, ErrorCode> StorageTier::TriggerBucketEviction(
    size_t target_free_size) {
    if (!storage_backend_) {
        LOG(ERROR) << "Storage backend not initialized";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Try to cast to BucketStorageBackend
    auto* bucket_backend =
        dynamic_cast<BucketStorageBackend*>(storage_backend_.get());
    if (!bucket_backend) {
        LOG(WARNING) << "Storage backend is not BucketStorageBackend, "
                        "bucket eviction not supported";
        return tl::make_unexpected(ErrorCode::NOT_IMPLEMENTED);
    }

    size_t total_freed = 0;
    constexpr int MAX_EVICTION_ATTEMPTS = 10;  // Prevent infinite loop
    int attempts = 0;

    // If target_free_size is 0, evict just one bucket
    if (target_free_size == 0) {
        auto select_res = bucket_backend->SelectBucketForEviction();
        if (!select_res) {
            LOG(ERROR) << "Failed to select bucket for eviction: "
                       << select_res.error();
            return tl::make_unexpected(select_res.error());
        }

        int64_t bucket_id = select_res.value();
        auto evict_res = bucket_backend->EvictBucket(bucket_id);
        if (!evict_res) {
            LOG(ERROR) << "Failed to evict bucket " << bucket_id << ": "
                       << evict_res.error();
            return tl::make_unexpected(evict_res.error());
        }

        total_freed = evict_res.value();

        // Update live persisted bytes to reflect cache-visible space freed by
        // eviction.
        persisted_live_data_bytes_.fetch_sub(total_freed,
                                             std::memory_order_acq_rel);

        LOG(INFO) << "Evicted 1 bucket, freed " << total_freed << " bytes";
        return total_freed;
    }

    // Loop until we free enough space or run out of buckets
    while (total_freed < target_free_size && attempts < MAX_EVICTION_ATTEMPTS) {
        auto select_res = bucket_backend->SelectBucketForEviction();
        if (!select_res) {
            // No more buckets to evict
            if (total_freed > 0) {
                LOG(WARNING)
                    << "Freed " << total_freed << " bytes but target was "
                    << target_free_size << " bytes (no more buckets)";
                return total_freed;
            }
            LOG(ERROR) << "Failed to select bucket for eviction: "
                       << select_res.error();
            return tl::make_unexpected(select_res.error());
        }

        int64_t bucket_id = select_res.value();
        auto evict_res = bucket_backend->EvictBucket(bucket_id);
        if (!evict_res) {
            LOG(ERROR) << "Failed to evict bucket " << bucket_id << ": "
                       << evict_res.error();
            // Continue trying other buckets
            attempts++;
            continue;
        }

        size_t freed = evict_res.value();
        total_freed += freed;
        attempts++;

        // Update live persisted bytes to reflect cache-visible space freed by
        // eviction.
        persisted_live_data_bytes_.fetch_sub(freed, std::memory_order_acq_rel);

        LOG(INFO) << "Evicted bucket " << bucket_id << ", freed " << freed
                  << " bytes (total: " << total_freed << "/" << target_free_size
                  << ")";
    }

    if (total_freed >= target_free_size) {
        LOG(INFO) << "Successfully freed " << total_freed
                  << " bytes (target: " << target_free_size << ") in "
                  << attempts << " evictions";
    } else {
        LOG(WARNING) << "Only freed " << total_freed << " bytes out of target "
                     << target_free_size << " bytes after " << attempts
                     << " attempts";
    }

    return total_freed;
}

// Static registration of copy functions for NVME tier (Staging Buffer)
static CopierRegistrar storage_tier_copier_registrar(
    MemoryType::NVME,
    // To DRAM (NVME -> DRAM)
    [](const DataSource& src,
       const DataSource& dst) -> tl::expected<void, ErrorCode> {
        if (!src.buffer || !dst.buffer)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        if (dst.buffer->size() < src.buffer->size())
            return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);

        StorageBuffer* storage_buf =
            static_cast<StorageBuffer*>(src.buffer.get());
        if (storage_buf) {
            return storage_buf->ReadTo(
                reinterpret_cast<void*>(dst.buffer->data()),
                dst.buffer->size());
        }

        // Fallback or error (should be StorageBuffer)
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    },
    // From DRAM (DRAM -> NVME)
    [](const DataSource& src,
       const DataSource& dst) -> tl::expected<void, ErrorCode> {
        if (!src.buffer || !dst.buffer)
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        if (dst.buffer->size() < src.buffer->size())
            return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);

        // Snapshot destination pointer once to avoid TOCTOU with Persist()
        uint64_t dst_addr = dst.buffer->data();
        if (dst_addr == 0) {
            return tl::make_unexpected(
                ErrorCode::INVALID_PARAMS);  // OnDisk, cannot write
        }
        memcpy(reinterpret_cast<void*>(dst_addr),
               reinterpret_cast<const void*>(src.buffer->data()),
               src.buffer->size());
        return {};
    });

}  // namespace mooncake
