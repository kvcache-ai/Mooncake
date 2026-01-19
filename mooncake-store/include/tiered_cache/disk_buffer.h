#pragma once

#include <vector>
#include <memory>
#include <cstring>
#include <string>
#include "tiered_cache/cache_tier.h"
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
        if (!is_on_disk_) {
            return reinterpret_cast<uint64_t>(data_.data());
        }
        return 0;  // Invalid for on-disk
    }

    std::size_t size() const override { return size_; }

    // Helper to get raw pointer (only valid in Staging mode)
    char* data_ptr() { return is_on_disk_ ? nullptr : data_.data(); }
    const char* data_ptr() const {
        return is_on_disk_ ? nullptr : data_.data();
    }

    Slice ToSlice() const {
        if (is_on_disk_) return Slice{nullptr, size_};
        return Slice{const_cast<char*>(data_.data()), data_.size()};
    }

    void SetKey(const std::string& key) { key_ = key; }

    // Transition from Staging -> OnDisk
    void Persist() {
        if (is_on_disk_) return;
        size_ = data_.size();  // Ensure size is preserved
        // data_.clear(); // Free DRAM
        // std::vector<char>().swap(data_); // Force deallocation
        // TODO: Actually free memory. For now, keep it simple.
        data_ = std::vector<char>();
        is_on_disk_ = true;
    }

    // Read data to destination buffer (handles Staging vs Disk transparently)
    tl::expected<void, ErrorCode> ReadTo(void* dst, size_t length) {
        if (!is_on_disk_) {
            if (length > data_.size())
                return tl::make_unexpected(ErrorCode::BUFFER_OVERFLOW);
            std::memcpy(dst, data_.data(), length);
            return {};
        }

        // On Disk: Load from backend
        if (!backend_ || key_.empty()) {
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }

        std::unordered_map<std::string, Slice> batch;
        batch[key_] = Slice{static_cast<char*>(dst), length};
        return backend_->BatchLoad(batch);
    }

   private:
    std::vector<char> data_;
    StorageBackendInterface* backend_ = nullptr;
    std::string key_;
    bool is_on_disk_ = false;
    size_t size_ = 0;  // Store size because clearing data_ loses it
};

}  // namespace mooncake
