#pragma once

#include <variant>
#include <vector>

#include "types.h"

namespace mooncake {

// =====================================================================
// Heartbeat Task Types
// =====================================================================

/**
 * @brief Types of tasks that can be carried in a heartbeat request.
 * Only lightweight info-sync tasks; heavy operations like RegisterClient
 * should use specifical RPC.
 */
enum class HeartbeatTaskType {
    SYNC_SEGMENT_META,  // Sync segment usage metadata (for P2P structure)
};

// =====================================================================
// Heartbeat Task Params
// =====================================================================

/**
 * @brief Usage info for a single tier (e.g. segment).
 */
struct TierUsageInfo {
    UUID segment_id;
    size_t usage = 0;
};
YLT_REFL(TierUsageInfo, segment_id, usage);

/**
 * @brief Param for SYNC_SEGMENT_META task.
 */
struct SyncSegmentMetaParam {
    std::vector<TierUsageInfo> tier_usages;
};
YLT_REFL(SyncSegmentMetaParam, tier_usages);

// =====================================================================
// HeartbeatTask
// =====================================================================

/**
 * @brief A single task carried in a heartbeat request.
 */
struct HeartbeatTask {
    using ParamVariant = std::variant<SyncSegmentMetaParam>;

    HeartbeatTask() = default;

    HeartbeatTask(HeartbeatTaskType type, ParamVariant param)
        : type_(type), param_(std::move(param)) {}

    HeartbeatTaskType type_;
    ParamVariant param_;
};
YLT_REFL(HeartbeatTask, type_, param_);

struct HeartbeatTaskResult {
    HeartbeatTaskType type;
    ErrorCode error = ErrorCode::OK;
};
YLT_REFL(HeartbeatTaskResult, type, error);

}  // namespace mooncake
