#pragma once

#include <memory>
#include <map>
#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <cstring>
#include <atomic>
#include <thread>
#include <condition_variable>

#include "tiered_cache/tiers/cache_tier.h"
#include "storage_backend.h"

namespace mooncake {

/**
 * @class StorageBuffer
 * @brief A unified buffer for storage tier that handles both staging (DRAM)
 *        and persisted (Disk) states.
 */
class StorageBuffer : public BufferBase {
   public:
    explicit StorageBuffer(std::unique_ptr<AllocatedBuffer> staging_buffer,
                           StorageBackendInterface* backend)
        : staging_buffer_(std::move(staging_buffer)),
          backend_(backend),
          size_(staging_buffer_ ? staging_buffer_->size() : 0) {}

    uint64_t data() const override {
        // Return valid pointer only while data is still in the staging pool.
        std::lock_guard<std::mutex> lock(data_mutex_);
        if (!is_on_disk_.load(std::memory_order_acquire) && staging_buffer_) {
            return reinterpret_cast<uint64_t>(staging_buffer_->data());
        }
        return 0;
    }

    std::size_t size() const override { return size_; }

    // Helper to get raw pointer (only valid in staging mode).
    char* data_ptr() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        return is_on_disk_.load(std::memory_order_acquire) || !staging_buffer_
                   ? nullptr
                   : static_cast<char*>(staging_buffer_->data());
    }

    const char* data_ptr() const {
        std::lock_guard<std::mutex> lock(data_mutex_);
        return is_on_disk_.load(std::memory_order_acquire) || !staging_buffer_
                   ? nullptr
                   : static_cast<const char*>(staging_buffer_->data());
    }

    Slice ToSlice() const {
        std::lock_guard<std::mutex> lock(data_mutex_);
        if (is_on_disk_.load(std::memory_order_acquire) || !staging_buffer_) {
            return Slice{nullptr, size_};
        }
        return Slice{static_cast<char*>(staging_buffer_->data()), size_};
    }

    void SetKey(const std::string& key) { key_ = key; }
    const std::string& GetKey() const { return key_; }

    // Transition from staging pool -> on disk.
    void Persist() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        if (is_on_disk_.load(std::memory_order_acquire)) return;
        if (staging_buffer_) {
            size_ = staging_buffer_->size();
            staging_buffer_.reset();
        }
        is_on_disk_.store(true, std::memory_order_release);
    }

    bool IsPersisted() const {
        return is_on_disk_.load(std::memory_order_acquire);
    }

    void SetFlushing(bool flushing) {
        is_flushing_.store(flushing, std::memory_order_release);
    }

    bool IsFlushing() const {
        return is_flushing_.load(std::memory_order_acquire);
    }

    // Read data to destination buffer (handles staging vs disk transparently).
    tl::expected<void, ErrorCode> ReadTo(void* dst, size_t length) {
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            if (!is_on_disk_.load(std::memory_order_acquire)) {
                if (!staging_buffer_) {
                    return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
                }
                if (length > size_) {
                    return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
                }
                std::memcpy(dst, staging_buffer_->data(), length);
                return {};
            }
        }

        // On disk: load from backend without holding the staging lock.
        if (!backend_ || key_.empty()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        std::unordered_map<std::string, Slice> batch;
        batch[key_] = Slice{static_cast<char*>(dst), length};
        return backend_->BatchLoad(batch);
    }

   private:
    mutable std::mutex data_mutex_;
    std::unique_ptr<AllocatedBuffer> staging_buffer_;
    StorageBackendInterface* backend_ = nullptr;
    std::string key_;
    std::atomic<bool> is_on_disk_{false};
    std::atomic<bool> is_flushing_{false};
    size_t size_ = 0;
};

/**
 * @class StorageTier
 * @brief Storage tier implementation for SSD/NVMe storage.
 */
class StorageTier : public CacheTier {
   public:
    StorageTier(UUID tier_id, const std::vector<std::string>& tags,
                size_t capacity = 0);

    ~StorageTier() override;

    tl::expected<void, ErrorCode> Init(TieredBackend* backend,
                                       TransferEngine* engine) override;

    tl::expected<void, ErrorCode> Allocate(size_t size,
                                           DataSource& data) override;

    tl::expected<void, ErrorCode> Free(DataSource data) override;

    tl::expected<void, ErrorCode> Commit(const std::string& key,
                                         const DataSource& data) override;

    tl::expected<void, ErrorCode> Flush() override;

    // --- Accessors ---
    UUID GetTierId() const override { return tier_id_; }
    size_t GetCapacity() const override;
    size_t GetUsage() const override;
    MemoryType GetMemoryType() const override { return MemoryType::NVME; }
    const std::vector<std::string>& GetTags() const override { return tags_; }

    /**
     * @brief Trigger bucket eviction to free up space.
     * @param target_free_size Target amount of space to free (in bytes).
     *                         If 0, evicts one bucket.
     * @return tl::expected<size_t, ErrorCode>
     * - On success: total amount of space freed (in bytes)
     * - On failure: error code
     */
    tl::expected<size_t, ErrorCode> TriggerBucketEviction(
        size_t target_free_size = 0);

   private:
    static void FreeStagingMemory(char* ptr);

    // Internal flush logic that triggers BatchOffload
    tl::expected<void, ErrorCode> FlushInternal();

    // Background flush thread worker
    void FlushWorker();

    UUID tier_id_;
    std::vector<std::string> tags_;

    std::shared_ptr<StorageBackendInterface> storage_backend_;

    // Pending Write Buffer for aggregation
    std::mutex batch_mutex_;
    std::condition_variable flush_cv_;  // Signaled when flush completes
    std::unordered_map<std::string, StorageBuffer*> pending_batch_;
    std::atomic<size_t> pending_batch_size_{0};

    // Async flush thread
    std::thread flush_thread_;
    std::atomic<bool> stop_flush_thread_{false};
    std::condition_variable flush_trigger_cv_;
    std::mutex flush_trigger_mutex_;
    std::atomic<bool> flush_requested_{false};

    // Configurable thresholds
    size_t batch_size_threshold_ = 64 * 1024 * 1024;  // 64MB
    size_t batch_count_threshold_ = 1000;

    // Preallocated local staging pool for SSD writes.
    std::shared_ptr<BufferAllocatorBase> staging_allocator_;
    std::unique_ptr<char, void (*)(char*)> staging_memory_{
        nullptr, &StorageTier::FreeStagingMemory};
    size_t staging_buffer_capacity_ = 0;

    // Live persisted value bytes. This excludes backend metadata and any
    // bucket files already unlinked from the cache namespace but still pending
    // async physical deletion.
    size_t capacity_ = 0;
    std::atomic<size_t> persisted_live_data_bytes_{0};
};

}  // namespace mooncake
