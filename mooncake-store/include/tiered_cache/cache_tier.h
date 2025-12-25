#pragma once

#include <string>
#include <vector>
#include <memory>
#include <optional>

#include "transfer_engine.h"

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
 * @struct DataSource
 * @brief Describes a source of data for copy/write operations.
 */
struct DataSource {
    uint64_t ptr;     // Pointer to data (if in memory) / file descriptor
    uint64_t offset;  // Offset within the source (for files/SSDs)
    size_t size;      // Size in bytes
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
     */
    virtual bool Init(TieredBackend* backend, TransferEngine* engine) = 0;

    /**
     * @brief Reserve Space (Allocation)
     * Finds free space of `size` bytes. Does NOT copy data.
     * * @param size Bytes to allocate.
     * @param data DataSource struct to fill with allocation info.
     * @return true if allocation succeeds.
     */
    virtual bool Allocate(size_t size, DataSource& data) = 0;

    /**
     * @brief Free Space (Rollback/Cleanup)
     * Releases space at offset. Used when writes fail or explicitly freeing
     * anonymous blocks.
     */
    virtual bool Free(DataSource data) = 0;

    // --- Accessors & Metadata ---
    virtual uint64_t GetTierId() const = 0;
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
