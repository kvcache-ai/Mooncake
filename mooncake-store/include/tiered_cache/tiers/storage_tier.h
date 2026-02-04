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
    explicit StorageBuffer(size_t size, StorageBackendInterface* backend)
        : data_(size), backend_(backend), size_(size) {}

    // For when we already have the data populated
    explicit StorageBuffer(std::vector<char> data,
                           StorageBackendInterface* backend)
        : data_(std::move(data)), backend_(backend), size_(data_.size()) {}

    uint64_t data() const override {
        // Return valid pointer only if in staging (DRAM) mode
        std::lock_guard<std::mutex> lock(data_mutex_);
        if (!is_on_disk_.load(std::memory_order_acquire)) {
            return reinterpret_cast<uint64_t>(data_.data());
        }
        return 0;  // Invalid for on-disk
    }

    std::size_t size() const override { return size_; }

    // Helper to get raw pointer (only valid in Staging mode)
    char* data_ptr() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        return is_on_disk_.load(std::memory_order_acquire) ? nullptr : data_.data();
    }
    const char* data_ptr() const {
        std::lock_guard<std::mutex> lock(data_mutex_);
        return is_on_disk_.load(std::memory_order_acquire) ? nullptr : data_.data();
    }

    Slice ToSlice() const {
        std::lock_guard<std::mutex> lock(data_mutex_);
        if (is_on_disk_.load(std::memory_order_acquire)) return Slice{nullptr, size_};
        return Slice{const_cast<char*>(data_.data()), data_.size()};
    }

    void SetKey(const std::string& key) { key_ = key; }
    const std::string& GetKey() const { return key_; }

    // Transition from Staging -> OnDisk
    void Persist() {
        std::lock_guard<std::mutex> lock(data_mutex_);
        if (is_on_disk_.load(std::memory_order_acquire)) return;
        size_ = data_.size();  // Ensure size is preserved
        is_on_disk_.store(true, std::memory_order_release);
        data_ = std::vector<char>();  // Clear after setting flag
    }

    // Check if data has been persisted to disk
    bool IsPersisted() const { return is_on_disk_.load(std::memory_order_acquire); }

    // Flushing state management (for thread safety during FlushInternal)
    void SetFlushing(bool flushing) {
        is_flushing_.store(flushing, std::memory_order_release);
    }
    bool IsFlushing() const {
        return is_flushing_.load(std::memory_order_acquire);
    }
    void WaitForFlushComplete() const {
        // Spin-wait for flush to complete (should be brief)
        while (is_flushing_.load(std::memory_order_acquire)) {
            std::this_thread::yield();
        }
    }

    // Read data to destination buffer (handles Staging vs Disk transparently)
    tl::expected<void, ErrorCode> ReadTo(void* dst, size_t length) {
        {
            std::lock_guard<std::mutex> lock(data_mutex_);
            if (!is_on_disk_.load(std::memory_order_acquire)) {
                if (length > data_.size())
                    return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
                std::memcpy(dst, data_.data(), length);
                return {};
            }
        }

        // On Disk: Load from backend (no lock needed)
        if (!backend_ || key_.empty()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        std::unordered_map<std::string, Slice> batch;
        batch[key_] = Slice{static_cast<char*>(dst), length};
        return backend_->BatchLoad(batch);
    }

   private:
    mutable std::mutex data_mutex_;  // Protects data_ access
    std::vector<char> data_;
    StorageBackendInterface* backend_ = nullptr;
    std::string key_;
    std::atomic<bool> is_on_disk_{false};
    std::atomic<bool> is_flushing_{false};  // True while being flushed
    size_t size_ = 0;  // Store size because clearing data_ loses it
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

   private:
    // Internal flush logic that triggers BatchOffload
    tl::expected<void, ErrorCode> FlushInternal();

    UUID tier_id_;
    std::vector<std::string> tags_;

    std::shared_ptr<StorageBackendInterface> storage_backend_;

    // Pending Write Buffer for aggregation
    std::mutex batch_mutex_;
    std::unordered_map<std::string, StorageBuffer*> pending_batch_;
    std::atomic<size_t> pending_batch_size_{0};

    // Configurable thresholds
    size_t batch_size_threshold_ = 64 * 1024 * 1024;  // 64MB
    size_t batch_count_threshold_ = 1000;

    // Capacity tracking
    size_t capacity_ = 0;
    std::atomic<size_t> persisted_size_{0};
};

}  // namespace mooncake
