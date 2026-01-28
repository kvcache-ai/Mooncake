#pragma once

#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <ylt/util/tl/expected.hpp>

#include "allocator.h"
#include "transfer_engine.h"
#include "types.h"

namespace mooncake {

class TieredBackend;

/**
 * @enum MemoryType
 * @brief Defines the physical storage medium type for a cache tier.
 */
enum class MemoryType { DRAM, NVME, UNKNOWN };

static inline std::string MemoryTypeToString(MemoryType type) {
    switch (type) {
        case MemoryType::DRAM:
            return "DRAM";
        case MemoryType::NVME:
            return "NVME";
        default:
            return "UNKNOWN";
    }
}

/**
 * @class BufferBase
 * @brief Base class for different types of memory buffers
 */
class BufferBase {
   public:
    virtual ~BufferBase() = default;
    virtual uint64_t data() const = 0;
    virtual std::size_t size() const = 0;
};

/**
 * @class DRAMBuffer
 * @brief Wrapper for DRAM AllocatedBuffer
 */
class DRAMBuffer : public BufferBase {
   public:
    explicit DRAMBuffer(std::unique_ptr<AllocatedBuffer> buffer)
        : dram_buffer_(std::move(buffer)) {}

    uint64_t data() const override {
        return dram_buffer_ ? reinterpret_cast<uint64_t>(dram_buffer_->data())
                            : 0;
    }

    std::size_t size() const override {
        return dram_buffer_ ? dram_buffer_->size() : 0;
    }

   private:
    std::unique_ptr<AllocatedBuffer> dram_buffer_;
};

/**
 * @class TempDRAMBuffer
 * @brief Wrapper for temporary DRAM buffers with RAII memory management
 */
class TempDRAMBuffer : public BufferBase {
   public:
    // Constructor that takes ownership of the buffer
    explicit TempDRAMBuffer(std::unique_ptr<char[]> buffer, size_t size)
        : buffer_(std::move(buffer)), size_(size) {}

    uint64_t data() const override {
        return reinterpret_cast<uint64_t>(buffer_.get());
    }
    std::size_t size() const override { return size_; }

   private:
    std::unique_ptr<char[]>
        buffer_;  // Owns the memory, auto-releases on destruction
    size_t size_;
};

/**
 * @class RefBuffer
 * @brief Helper class to wrap a raw pointer as BufferBase without taking
 * ownership This is used for DataCopier operations where the source memory is
 * owned elsewhere
 */
class RefBuffer : public BufferBase {
   public:
    explicit RefBuffer(void* ptr, size_t size) : ptr_(ptr), size_(size) {}

    uint64_t data() const override { return reinterpret_cast<uint64_t>(ptr_); }

    std::size_t size() const override { return size_; }

   private:
    void* ptr_;
    size_t size_;
};

/**
 * @struct DataSource
 * @brief Describes a source of data for copy/write operations.
 */
struct DataSource {
    std::unique_ptr<BufferBase> buffer;
    MemoryType type;  // Source memory type
};

/**
 * @class CacheTier
 * @brief Abstract base class for a single tier (e.g., DRAM, SSD).
 * * Update: Supports decoupled Allocation/Write/Bind operations to allow
 * flexible placement strategies (Client-centric vs Master-centric).
 */
class CacheTier {
   public:
    virtual ~CacheTier() = default;

    /**
     * @brief Initializes the cache tier.
     * @return tl::expected<void, ErrorCode> indicating success or error code.
     */
    virtual tl::expected<void, ErrorCode> Init(TieredBackend* backend,
                                               TransferEngine* engine) = 0;

    /**
     * @brief Reserve Space (Allocation)
     * Finds free space of `size` bytes. Does NOT copy data.
     * @param size Bytes to allocate.
     * @param data DataSource struct to fill with allocation info.
     * @return tl::expected<void, ErrorCode> indicating success or error code.
     */
    virtual tl::expected<void, ErrorCode> Allocate(size_t size,
                                                   DataSource& data) = 0;

    /**
     * @brief Free Space (Rollback/Cleanup)
     * Releases space at offset. Used when writes fail or explicitly freeing
     * anonymous blocks.
     * @return tl::expected<void, ErrorCode> indicating success or error code.
     */
    virtual tl::expected<void, ErrorCode> Free(DataSource data) = 0;

    // --- Accessors & Metadata ---
    virtual UUID GetTierId() const = 0;
    virtual size_t GetCapacity() const = 0;
    virtual size_t GetUsage() const = 0;
    virtual MemoryType GetMemoryType() const = 0;
    virtual const std::vector<std::string>& GetTags() const = 0;

   protected:
    // A pointer to the parent backend, allowing tiers to access shared services
    // like the DataCopier.
    TieredBackend* backend_ = nullptr;
};

}  // namespace mooncake