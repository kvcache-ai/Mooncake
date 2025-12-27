#pragma once

#include <cstddef>
#include <string>

namespace mooncake {

/**
 * @brief Abstract interface for metadata storage on Standby
 *
 * This interface provides basic operations for storing and managing object metadata.
 * In a full implementation, this would mirror MasterService's metadata_shards_ structure.
 */
class MetadataStore {
   public:
    virtual ~MetadataStore() = default;

    /**
     * @brief Put or update metadata for a key
     * @param key Object key
     * @param payload Optional payload data (JSON serialized metadata)
     * @return true on success, false on failure
     */
    virtual bool Put(const std::string& key, const std::string& payload = std::string()) = 0;

    /**
     * @brief Remove metadata for a key
     * @param key Object key
     * @return true if key was found and removed, false otherwise
     */
    virtual bool Remove(const std::string& key) = 0;

    /**
     * @brief Check if a key exists
     * @param key Object key
     * @return true if key exists, false otherwise
     */
    virtual bool Exists(const std::string& key) const = 0;

    /**
     * @brief Get the count of keys in the store
     * @return Number of keys
     */
    virtual size_t GetKeyCount() const = 0;
};

}  // namespace mooncake

