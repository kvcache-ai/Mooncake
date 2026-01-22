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
#include "rpc_types.h"

namespace mooncake {

class TieredBackend;    // Forward declaration
class ClientScheduler;  // Forward declaration

/**
 * @struct TieredLocation
 * @brief Describes the physical location of a segment within the tiered
 * storage.
 */
struct TieredLocation {
    CacheTier* tier;
    struct DataSource data;
};

/**
 * @struct TierView
 * @brief A snapshot of a tier's status, used for reporting topology to the
 * Master.
 */
struct TierView {
    UUID id;
    MemoryType type;
    size_t capacity;
    size_t usage;
    size_t free_space;
    int priority;
    std::vector<std::string> tags;
};

/**
 * @enum CALLBACK_TYPE
 * @brief The type of metadata synchronization callback.
 */
enum CALLBACK_TYPE { COMMIT = 0, DELETE = 1, DELETE_ALL = 2 };

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

    AllocationEntry(TieredBackend* b, TieredLocation&& l)
        : backend(b), loc(std::move(l)) {}
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
using MetadataSyncCallback = std::function<tl::expected<void, ErrorCode>(
    const std::string& key, const UUID& tier_id, enum CALLBACK_TYPE type)>;

/**
 * @class TieredBackend
 * @brief Data plane management class supporting tiered storage with RAII-based
 * resource management.
 */
class TieredBackend {
   public:
    TieredBackend();
    ~TieredBackend();

    tl::expected<void, ErrorCode> Init(Json::Value root, TransferEngine* engine,
                                       MetadataSyncCallback sync_callback);

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
    tl::expected<AllocationHandle, ErrorCode> Allocate(
        size_t size, std::optional<UUID> preferred_tier = std::nullopt);

    /**
     * @brief Execution (Write)
     * Writes data to the location specified by the handle.
     */
    tl::expected<void, ErrorCode> Write(const DataSource& source,
                                        AllocationHandle handle);

    /**
     * @brief Commit (Register)
     * Registers the handle in the local metadata index.
     * Supports multi-tier: The key (SegmentID) can exist in multiple tiers
     * simultaneously. If a replica already exists on the same tier, it is
     * replaced.
     */
    tl::expected<void, ErrorCode> Commit(const std::string& key,
                                         AllocationHandle handle);

    /**
     * @brief Get
     * Returns a handle.
     * @param tier_id: If specified, returns the handle on that specific tier.
     * @param record_access: If true, notifies the scheduler (OnAccess).
     */
    tl::expected<AllocationHandle, ErrorCode> Get(
        const std::string& key, std::optional<UUID> tier_id = std::nullopt,
        bool record_access = true);

    /**
     * @brief Delete
     * Removes the key from the metadata index.
     * @param tier_id: If specified, removes only the replica on that tier.
     * If nullopt, removes ALL replicas for this key (and the key entry itself).
     */
    tl::expected<void, ErrorCode> Delete(
        const std::string& key, std::optional<UUID> tier_id = std::nullopt);

    // --- Composite Operations ---

    /**
     * @brief Data Migration / Replication
     * Creates a copy of data on dest_tier_id.
     * Note: This adds a new replica. It does NOT automatically delete the
     * source replica.
     */
    tl::expected<void, ErrorCode> CopyData(const std::string& key,
                                           const DataSource& source,
                                           UUID dest_tier_id);

    // Moves/Copies data from source tier to dest tier
    tl::expected<void, ErrorCode> Transfer(const std::string& key,
                                           UUID source_tier_id,
                                           UUID dest_tier_id);

    // --- Introspection & Internal ---

    std::vector<TierView> GetTierViews() const;
    std::vector<UUID> GetReplicaTierIds(const std::string& key) const;
    const CacheTier* GetTier(UUID tier_id) const;
    const DataCopier& GetDataCopier() const;

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
        std::vector<std::pair<UUID, AllocationHandle>>
            replicas;  // tier_id -> handle
    };

    // Get list of Tier IDs sorted by priority (descending)
    std::vector<UUID> GetSortedTiers() const;

    // Low-level allocation logic
    bool AllocateInternalRaw(size_t size, std::optional<UUID> preferred_tier,
                             TieredLocation* out_loc);

    // Map from tier ID to the actual CacheTier instance.
    std::unordered_map<UUID, std::unique_ptr<CacheTier>> tiers_;

    // Map from tier ID to static config info
    std::unordered_map<UUID, TierInfo> tier_info_;

    // Global Metadata Index: Key -> Entry
    // map_mutex_ only protects the structure of this map (insertions/deletions
    // of keys). Accessing existing keys is highly concurrent.
    std::unordered_map<std::string, std::shared_ptr<MetadataEntry>>
        metadata_index_;
    mutable std::shared_mutex map_mutex_;

    std::unique_ptr<DataCopier> data_copier_;
    // Callback for metadata synchronization with Master
    MetadataSyncCallback metadata_sync_callback_;

    // Scheduler
    std::unique_ptr<ClientScheduler> scheduler_;
};

}  // namespace mooncake