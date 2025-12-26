#pragma once

#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <functional>
#include <json/value.h>

#include "tiered_cache/cache_tier.h"
#include "tiered_cache/data_copier.h"

namespace mooncake {

class TieredBackend;  // Forward declaration

/**
 * @struct TieredLocation
 * @brief Describes the physical location of a segment within the tiered
 * storage.
 */
struct TieredLocation {
    uint64_t tier_id;
    struct DataSource data;
};

/**
 * @struct TierView
 * @brief A snapshot of a tier's status, used for reporting topology to the
 * Master.
 */
struct TierView {
    uint64_t id;
    MemoryType type;
    size_t capacity;
    size_t usage;
    size_t free_space;
    int priority;
    std::vector<std::string> tags;
};

/**
 * @struct AllocationEntry
 * @brief The internal state of an allocation.
 * acts as the "Control Block" for the resource.
 * When the last shared_ptr pointing to this entry dies, the destructor
 * triggers the physical release of the resource via the Backend.
 */
struct AllocationEntry {
    TieredBackend* backend;
    TieredLocation loc;

    AllocationEntry(TieredBackend* b, TieredLocation l) : backend(b), loc(l) {}
    AllocationEntry(const AllocationEntry&) = delete;
    AllocationEntry& operator=(const AllocationEntry&) = delete;

    // Destructor: Automatically releases the resource if valid
    ~AllocationEntry();
};

/**
 * @typedef AllocationHandle
 * @brief A reference-counted handle to a storage resource.
 */
using AllocationHandle = std::shared_ptr<AllocationEntry>;

/**
 * @brief Callback for metadata synchronization.
 * Invoked after data copy is complete.
 * Returns true if sync succeeds, false otherwise.
 */
using MetadataSyncCallback =
    std::function<bool(const std::string& key, const TieredLocation& new_loc)>;

/**
 * @class TieredBackend
 * @brief Data plane management class supporting tiered storage with RAII-based
 * resource management.
 */
class TieredBackend {
   public:
    TieredBackend();
    ~TieredBackend() = default;

    bool Init(Json::Value root, TransferEngine* engine);

    // --- Client-Centric Operations ---
    // All the following operations are designed for Client-Centric, Client
    // should manage the resource by itself, and synchronize with Master when
    // needed.

    /**
     * @brief Allocation
     * reserves storage space. Returns a handle.
     * If the handle goes out of scope without being committed, the space is
     * auto-freed.
     */
    AllocationHandle Allocate(
        size_t size, std::optional<uint64_t> preferred_tier = std::nullopt);

    /**
     * @brief Execution (Write)
     * Writes data to the location specified by the handle.
     */
    bool Write(const DataSource& source, AllocationHandle handle);

    /**
     * @brief Commit (Register)
     * Registers the handle in the local metadata index.
     * Supports multi-tier: The key (SegmentID) can exist in multiple tiers
     * simultaneously. If a replica already exists on the same tier, it is
     * replaced.
     */
    bool Commit(const std::string& key, AllocationHandle handle);

    /**
     * @brief Get
     * Returns a handle.
     * @param tier_id: If specified, returns the handle on that specific tier.
     * If nullopt, returns the handle from the highest priority tier available.
     */
    AllocationHandle Get(const std::string& key,
                         std::optional<uint64_t> tier_id = std::nullopt);

    /**
     * @brief Delete
     * Removes the key from the metadata index.
     * @param tier_id: If specified, removes only the replica on that tier.
     * If nullopt, removes ALL replicas for this key (and the key entry itself).
     */
    bool Delete(const std::string& key,
                std::optional<uint64_t> tier_id = std::nullopt);

    // --- Composite Operations ---

    /**
     * @brief Data Migration / Replication
     * Creates a copy of data on dest_tier_id.
     * Note: This adds a new replica. It does NOT automatically delete the
     * source replica.
     */
    bool CopyData(const std::string& key, const DataSource& source,
                  uint64_t dest_tier_id, MetadataSyncCallback sync_cb);

    // --- Introspection & Internal ---

    std::vector<TierView> GetTierViews() const;
    const CacheTier* GetTier(uint64_t tier_id) const;
    const DataCopier& GetDataCopier() const;

    // Internal API called by AllocationEntry destructor
    void FreeInternal(const TieredLocation& loc);

   private:
    struct TierInfo {
        int priority;
        std::vector<std::string> tags;
    };

    /**
     * @struct MetadataEntry
     * @brief Holds all replicas for a specific key.
     * Uses a dedicated mutex to allow per-key concurrency.
     */
    struct MetadataEntry {
        mutable std::shared_mutex mutex;  // Entry-level lock
        std::vector<std::pair<uint64_t, AllocationHandle>>
            replicas;  // tier_id -> handle
    };

    // Get list of Tier IDs sorted by priority (descending)
    std::vector<uint64_t> GetSortedTiers() const;

    // Low-level allocation logic
    bool AllocateInternalRaw(size_t size,
                             std::optional<uint64_t> preferred_tier,
                             TieredLocation* out_loc);

    // Map from tier ID to the actual CacheTier instance.
    std::unordered_map<uint64_t, std::unique_ptr<CacheTier>> tiers_;

    // Map from tier ID to static config info
    std::unordered_map<uint64_t, TierInfo> tier_info_;

    // Global Metadata Index: Key -> Entry
    // map_mutex_ only protects the structure of this map (insertions/deletions
    // of keys). Accessing existing keys is highly concurrent.
    std::unordered_map<std::string, std::shared_ptr<MetadataEntry>>
        metadata_index_;
    mutable std::shared_mutex map_mutex_;

    std::unique_ptr<DataCopier> data_copier_;
};

}  // namespace mooncake