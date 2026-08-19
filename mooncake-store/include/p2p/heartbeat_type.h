#pragma once

#include <cstdint>
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
    SYNC_SEGMENT_META,   // Sync segment usage metadata (for P2P structure)
    SYNC_CLIENT_METRIC,  // Sync cumulative client data-plane metrics
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
// Client Metric Snapshot (Heartbeat SYNC_CLIENT_METRIC Task)
// =====================================================================

struct DataMetricSnapshot {
    int64_t get_requests = 0;
    int64_t get_hits = 0;
    int64_t get_misses = 0;
    int64_t get_failures = 0;
    int64_t get_bytes = 0;
    int64_t put_requests = 0;
    int64_t put_failures = 0;
    int64_t put_bytes = 0;
};
YLT_REFL(DataMetricSnapshot, get_requests, get_hits, get_misses, get_failures,
         get_bytes, put_requests, put_failures, put_bytes);

struct RemoteDataMetricSnapshot {
    DataMetricSnapshot data;
    int64_t read_retries = 0;
    int64_t write_retries = 0;
};
YLT_REFL(RemoteDataMetricSnapshot, data, read_retries, write_retries);

// Metric snapshot carried by the SYNC_CLIENT_METRIC heartbeat task.
// Granularity:
// - total_request: request (batch) granularity; one BatchPut/BatchGet counts
//   as ONE request, all keys in the batch share its latency sample.
// - local_request / remote_request: per-operation granularity;
//   with multi-replica failure retries, the op counts may EXCEED total_request.
struct ClientMetricSnapshot {
    DataMetricSnapshot total_request;
    DataMetricSnapshot local_request;
    RemoteDataMetricSnapshot remote_request;
};
YLT_REFL(ClientMetricSnapshot, total_request, local_request, remote_request);

/**
 * @brief Param for SYNC_CLIENT_METRIC task.
 */
struct SyncClientMetricParam {
    ClientMetricSnapshot snapshot;
};
YLT_REFL(SyncClientMetricParam, snapshot);

// =====================================================================
// HeartbeatTask
// =====================================================================

/**
 * @brief A single task carried in a heartbeat request.
 */
struct HeartbeatTask {
    using ParamVariant =
        std::variant<SyncSegmentMetaParam, SyncClientMetricParam>;

    HeartbeatTask() = default;

    HeartbeatTask(HeartbeatTaskType type, ParamVariant param)
        : type_(type), param_(std::move(param)) {}

    HeartbeatTaskType type_;
    ParamVariant param_;
};
YLT_REFL(HeartbeatTask, type_, param_);

/**
 * @brief Detailed result for SYNC_SEGMENT_META task.
 */
struct SyncSegmentMetaResult {
    struct SubResult {
        UUID segment_id;
        ErrorCode error = ErrorCode::OK;
    };
    std::vector<SubResult> sub_results;
};
YLT_REFL(SyncSegmentMetaResult::SubResult, segment_id, error);
YLT_REFL(SyncSegmentMetaResult, sub_results);

struct HeartbeatTaskResult {
    using DetailVariant = std::variant<std::monostate, SyncSegmentMetaResult>;

    HeartbeatTaskType type;
    ErrorCode error = ErrorCode::OK;
    DetailVariant detail;
};
YLT_REFL(HeartbeatTaskResult, type, error, detail);

}  // namespace mooncake
