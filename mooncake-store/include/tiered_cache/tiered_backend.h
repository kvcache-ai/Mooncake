#pragma once

#include "tiered_cache/cache_tier.h"
#include "tiered_cache/data_copier.h"
#include <string>
#include <vector>
#include <memory>
#include <unordered_map>
#include <shared_mutex>
#include <optional>
#include <json/value.h>

namespace mooncake {

/**
 * @struct TierView
 * @brief A snapshot of a CacheTier's status for the upper layer (e.g., Worker).
 */
struct TierView {
    uint64_t id;
    MemoryType type;
    size_t capacity;
    size_t usage;
    int priority;
    std::vector<std::string> tags;
};

/**
 * @class TieredBackend
 * @brief A pure data plane for the tiered caching system.
 */
class TieredBackend {
   public:
    TieredBackend();
    ~TieredBackend() = default;

    bool Init(Json::Value root, TransferEngine* engine);
    bool Get(const std::string& key, void*& data, size_t& size);
    bool Put(const std::string& key, uint64_t target_tier_id,
             const DataSource& source);
    bool Delete(const std::string& key);
    bool MoveData(const std::string& key, uint64_t src_tier_id,
                  uint64_t dest_tier_id);

    std::optional<uint64_t> FindKey(const std::string& key) const;
    std::vector<TierView> GetTierViews() const;
    const CacheTier* GetTier(uint64_t tier_id) const;
    const DataCopier& GetDataCopier() const;

   private:
    /**
     * @struct TierInfo
     * @brief Internal struct to hold static configuration for each tier.
     */
    struct TierInfo {
        int priority;
        std::vector<std::string> tags;
    };

    bool DeleteFromTier(const std::string& key, uint64_t tier_id);

    // Map from tier ID to the actual CacheTier instance.
    std::unordered_map<uint64_t, std::unique_ptr<CacheTier>> tiers_;

    // Map from tier ID to its static configuration info.
    std::unordered_map<uint64_t, TierInfo> tier_info_;

    // A fast lookup map from a key to the ID of the tier that holds it.
    std::unordered_map<std::string, uint64_t> key_to_tier_map_;
    mutable std::shared_mutex map_mutex_;  // Protects key_to_tier_map_

    std::unique_ptr<DataCopier> data_copier_;
};

}  // namespace mooncake