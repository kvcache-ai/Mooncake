#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "tiered_cache/tiers/cache_tier.h"

namespace mooncake {

/**
 * @class AscendBuffer
 * @brief Ascend NPU memory buffer wrapper inheriting from BufferBase.
 *
 * Implements RAII pattern for automatic device memory management.
 * When the buffer goes out of scope, device memory is automatically freed.
 */
class AscendBuffer : public BufferBase {
   public:
    /**
     * @brief Constructs an AscendBuffer taking ownership of device memory.
     */
    explicit AscendBuffer(void* device_ptr, size_t size);

    /**
     * @brief Destructor that automatically releases device memory.
     */
    ~AscendBuffer() override;

    // Disable copy operations
    AscendBuffer(const AscendBuffer&) = delete;
    AscendBuffer& operator=(const AscendBuffer&) = delete;

    // Enable move operations
    AscendBuffer(AscendBuffer&& other) noexcept;
    AscendBuffer& operator=(AscendBuffer&& other) noexcept;

    /**
     * @brief Returns the device pointer address as uint64_t.
     *
     * Returns the raw device memory pointer.
     */
    uint64_t data() const override;

    /**
     * @brief Returns the size of the allocated memory.
     */
    std::size_t size() const override;

    /**
     * @brief Gets the actual device pointer.
     * @return Device pointer, or nullptr if buffer is invalid.
     */
    void* GetDevicePtr() const;

   private:
    void* device_ptr_{nullptr};
    size_t size_{0};

    // Internal function to release device memory
    void ReleaseMemory();
};

/**
 * @class AscendCacheTier
 * @brief Ascend NPU cache tier implementation for the new CacheTier interface.
 *
 * Provides device memory allocation and management for Huawei Ascend NPU
 * devices. Supports the Allocate/Free resource management pattern with RAII
 * semantics.
 */
class AscendCacheTier : public CacheTier {
   public:
    /**
     * @brief Constructs an AscendCacheTier.
     * @param tier_id Unique identifier for this tier (UUID).
     * @param capacity Total capacity in bytes.
     * @param tags Optional tags for tier identification.
     * @param device_id Ascend device ID (default: 0).
     */
    AscendCacheTier(UUID tier_id, size_t capacity,
                    const std::vector<std::string>& tags, int device_id = 0);

    ~AscendCacheTier() override;

    // Disable copy
    AscendCacheTier(const AscendCacheTier&) = delete;
    AscendCacheTier& operator=(const AscendCacheTier&) = delete;

    // CacheTier interface implementation
    tl::expected<void, ErrorCode> Init(TieredBackend* backend,
                                       TransferEngine* engine) override;

    tl::expected<void, ErrorCode> Allocate(size_t size,
                                           DataSource& data) override;

    tl::expected<void, ErrorCode> Free(DataSource data) override;

    // Accessors
    UUID GetTierId() const override { return tier_id_; }
    size_t GetCapacity() const override { return capacity_; }
    size_t GetUsage() const override;
    MemoryType GetMemoryType() const override { return MemoryType::ASCEND_NPU; }
    const std::vector<std::string>& GetTags() const override { return tags_; }

    /**
     * @brief Gets the device ID for this cache tier.
     */
    int GetDeviceId() const { return device_id_; }

   private:
    UUID tier_id_;
    size_t capacity_;
    std::vector<std::string> tags_;
    int device_id_;

    // Memory usage tracking (atomic for thread safety)
    std::atomic<size_t> current_usage_{0};

    // Initialization state
    bool is_initialized_{false};
    mutable std::mutex init_mutex_;

    // Internal device memory allocation
    void* AllocateDeviceMemory(size_t size);

    // Check if sufficient space is available
    bool HasSpace(size_t size) const;
};

}  // namespace mooncake
