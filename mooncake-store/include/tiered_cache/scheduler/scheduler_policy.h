#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <optional>

#include <ylt/util/tl/expected.hpp>

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
 * @brief Context for a key, including access metadata and current location.
 */
struct KeyContext {
    std::string key;
    double recent_heat_score = 0.0;
    size_t recency_rank = 0;
    std::vector<UUID> current_locations;  // Which tiers currently hold this key
    size_t size_bytes = 0;                // Size of the key's data in bytes
};

/**
 * @struct SchedAction
 * @brief A single scheduling action decision
 */
struct SchedAction {
    enum class Type {
        REPLICATE,  // Copy data from Source to Target and keep the source copy
        MIGRATE,    // Move data from Source to Target (Promote/Offload)
        EVICT,      // Delete data from Source
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
     * @return List of recommended actions or an error when the policy cannot
     *         produce a valid plan
     */
    virtual tl::expected<std::vector<SchedAction>, ErrorCode> Decide(
        const std::unordered_map<UUID, TierStats>& tier_stats,
        const std::vector<KeyContext>& active_keys) = 0;

    /**
     * @brief Set the fast tier ID for the policy.
     * @param id The UUID of the fast tier (e.g., DRAM).
     */
    virtual void SetFastTier(UUID id) {}
};

}  // namespace mooncake
