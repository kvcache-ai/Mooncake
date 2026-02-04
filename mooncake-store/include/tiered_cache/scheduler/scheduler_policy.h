#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <optional>
#include "types.h"

namespace mooncake {

/**
 * @struct TierStats
 * @brief Runtime statistics for a specific storage tier
 */
struct TierStats {
    size_t total_capacity_bytes = 0;
    size_t used_capacity_bytes = 0;
};

/**
 * @struct KeyContext
 * @brief Context for a key, including its heat and current location
 */
struct KeyContext {
    std::string key;
    double heat_score;
    std::vector<UUID> current_locations;  // Which tiers currently hold this key
    size_t size_bytes = 0;  // Size of the key's data in bytes
};

/**
 * @struct SchedAction
 * @brief A single scheduling action decision
 */
struct SchedAction {
    enum class Type {
        MIGRATE,  // Move data from Source to Target (Promote/Offload)
        EVICT,    // Delete data from Source
    };

    Type type;
    std::string key;

    // For MIGRATE: Source and Target must be specified
    // For EVICT: Only Source is required
    std::optional<UUID> source_tier_id;
    std::optional<UUID> target_tier_id;
};

/**
 * @class SchedulerPolicy
 * @brief Abstract interface for scheduling algorithms
 */
class SchedulerPolicy {
   public:
    virtual ~SchedulerPolicy() = default;

    /**
     * @brief Core decision function
     * @param tier_stats Current status of all managed tiers
     * @param active_keys List of active/hot keys with their context
     * @return List of recommended actions
     */
    virtual std::vector<SchedAction> Decide(
        const std::unordered_map<UUID, TierStats>& tier_stats,
        const std::vector<KeyContext>& active_keys) = 0;
};

}  // namespace mooncake
