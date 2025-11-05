#pragma once

#include <string>
#include <vector>
#include <memory>
#include "transfer_engine.h"

namespace mooncake {
struct DataSource;
enum class MemoryType;
}  // namespace mooncake

namespace mooncake {

class TieredBackend;

/**
 * @enum MemoryType
 * @brief Defines the physical storage medium type for a cache tier.
 */
enum class MemoryType { DRAM, UNKNOWN };

static inline std::string MemoryTypeToString(MemoryType type) {
    switch (type) {
        case MemoryType::DRAM:
            return "DRAM";
        default:
            return "UNKNOWN";
    }
}

/**
 * @struct DataSource
 * @brief Describes a source of data for a copy operation.
 *
 * This struct is used as a generic descriptor for a block of memory, allowing
 * data to be described abstractly regardless of its physical location.
 */
struct DataSource {
    const void*
        ptr;  // Pointer to the data. Its interpretation depends on the `type`.
    size_t size;      // Size of the data in bytes.
    MemoryType type;  // The memory type where the data resides.
};

/**
 * @class CacheTier
 * @brief Abstract base class for a single tier in the tiered cache system.
 *
 * This class defines the common interface that all storage media (DRAM, VRAM,
 * SSD, etc.) must implement. The interface is designed to be simple and focuses
 * on the essential operations of a storage layer, leaving complex eviction and
 * promotion logic to the TieredBackend and CacheScheduler.
 */
class CacheTier {
   public:
    virtual ~CacheTier() = default;

    /**
     * @brief Initializes the cache tier.
     * @param backend A pointer to the parent TieredBackend for coordination.
     * @param te A pointer to the active TransferEngine, for tiers that need it.
     * @return True on success, false otherwise.
     */
    virtual bool Init(TieredBackend* backend, TransferEngine* engine) = 0;

    /**
     * @brief Retrieves a pointer to the data for a given key.
     * @param key The key to look up.
     * @param data [out] A reference to a void pointer that will be set to the
     * data's location.
     * @param size [out] A reference that will be set to the data's size.
     * @return True if the key is found, false otherwise.
     */
    virtual bool Get(const std::string& key, void*& data, size_t& size) = 0;

    /**
     * @brief Puts data into the tier from a generic data source.
     * This is the sole method for writing data. The implementation must always
     * allocate its own memory and copy the data from the source, using the
     * backend's DataCopier.
     * @param key The key for the data.
     * @param source The descriptor for the source data (pointer, size, type).
     * @return True on success, false otherwise.
     */
    virtual bool Put(const std::string& key, const DataSource& source) = 0;

    /**
     * @brief Deletes a key and its associated data from the tier.
     * @param key The key to delete.
     * @return True if the key was found and deleted, false otherwise.
     */
    virtual bool Delete(const std::string& key) = 0;

    /**
     * @brief Checks if the tier contains a given key.
     * @param key The key to check.
     * @return True if the key exists in this tier, false otherwise.
     */
    virtual bool Contains(const std::string& key) const = 0;

    /**
     * @brief Returns a DataSource descriptor for a key's data within this tier.
     * This is used to describe the data as a source for a subsequent copy
     * operation when it needs to be moved to another tier.
     * @param key The key to describe.
     * @return A DataSource object. If the key is not found, the `ptr` member
     * will be null.
     */
    virtual DataSource AsDataSource(const std::string& key) = 0;

    // --- Accessors for tier properties ---

    virtual uint64_t GetTierId() const = 0;
    virtual size_t GetCapacity() const = 0;
    virtual size_t GetUsage() const = 0;
    virtual const std::vector<std::string>& GetTags() const = 0;
    virtual MemoryType GetMemoryType() const = 0;

   protected:
    // A pointer to the parent backend, allowing tiers to access shared services
    // like the DataCopier.
    TieredBackend* backend_ = nullptr;
};

}  // namespace mooncake