#pragma once

#include "types.h"
#include "replica.h"
#include "task_manager.h"

namespace mooncake {

/**
 * @brief Response structure for Ping operation
 */
struct PingResponse {
    ViewVersionId view_version_id;
    ClientStatus client_status;

    PingResponse() = default;
    PingResponse(ViewVersionId view_version, ClientStatus status)
        : view_version_id(view_version), client_status(status) {}

    friend std::ostream& operator<<(std::ostream& os,
                                    const PingResponse& response) noexcept {
        return os << "PingResponse: { view_version_id: "
                  << response.view_version_id
                  << ", client_status: " << response.client_status << " }";
    }
};
YLT_REFL(PingResponse, view_version_id, client_status);

/**
 * @brief Response structure for GetReplicaList operation
 */
struct GetReplicaListResponse {
    std::vector<Replica::Descriptor> replicas;
    uint64_t lease_ttl_ms;

    GetReplicaListResponse() : lease_ttl_ms(0) {}
    GetReplicaListResponse(std::vector<Replica::Descriptor>&& replicas_param,
                           uint64_t lease_ttl_ms_param)
        : replicas(std::move(replicas_param)),
          lease_ttl_ms(lease_ttl_ms_param) {}
};
YLT_REFL(GetReplicaListResponse, replicas, lease_ttl_ms);

/**
 * @brief Response structure for GetStorageConfig operation
 */
struct GetStorageConfigResponse {
    std::string fsdir;
    bool enable_disk_eviction;
    uint64_t quota_bytes;

    GetStorageConfigResponse() : enable_disk_eviction(true), quota_bytes(0) {}
    GetStorageConfigResponse(const std::string& fsdir_param,
                             bool enable_eviction, uint64_t quota)
        : fsdir(fsdir_param),
          enable_disk_eviction(enable_eviction),
          quota_bytes(quota) {}
};
YLT_REFL(GetStorageConfigResponse, fsdir, enable_disk_eviction, quota_bytes);

/**
 * @brief Response structure for CopyStart operation
 */
struct CopyStartResponse {
    Replica::Descriptor source;
    std::vector<Replica::Descriptor> targets;
};
YLT_REFL(CopyStartResponse, source, targets);

/**
 * @brief Response structure for MoveStart operation
 */
struct MoveStartResponse {
    Replica::Descriptor source;
    std::optional<Replica::Descriptor> target;
};
YLT_REFL(MoveStartResponse, source, target);

enum class JobType {
    DRAIN = 0,
};

inline std::ostream& operator<<(std::ostream& os, const JobType& type) {
    switch (type) {
        case JobType::DRAIN:
            os << "DRAIN";
            break;
        default:
            os << "UNKNOWN_JOB_TYPE";
            break;
    }
    return os;
}

enum class JobStatus {
    CREATED = 0,
    PLANNING,
    RUNNING,
    SUCCEEDED,
    FAILED,
    CANCELED,
};

inline std::ostream& operator<<(std::ostream& os, const JobStatus& status) {
    switch (status) {
        case JobStatus::CREATED:
            os << "CREATED";
            break;
        case JobStatus::PLANNING:
            os << "PLANNING";
            break;
        case JobStatus::RUNNING:
            os << "RUNNING";
            break;
        case JobStatus::SUCCEEDED:
            os << "SUCCEEDED";
            break;
        case JobStatus::FAILED:
            os << "FAILED";
            break;
        case JobStatus::CANCELED:
            os << "CANCELED";
            break;
        default:
            os << "UNKNOWN_JOB_STATUS";
            break;
    }
    return os;
}

struct CreateDrainJobRequest {
    std::vector<std::string> segments;
    std::vector<std::string> target_segments;
    uint32_t max_concurrency{4};
};
YLT_REFL(CreateDrainJobRequest, segments, target_segments, max_concurrency);

struct QueryJobResponse {
    UUID id;
    JobType type;
    JobStatus status;
    int64_t created_at_ms_epoch;
    int64_t last_updated_at_ms_epoch;
    std::vector<std::string> segments;
    uint64_t succeeded_units;
    uint64_t failed_units;
    uint64_t blocked_units;
    uint64_t active_units;
    uint64_t migrated_bytes;
    std::string message;
};
YLT_REFL(QueryJobResponse, id, type, status, created_at_ms_epoch,
         last_updated_at_ms_epoch, segments, succeeded_units, failed_units,
         blocked_units, active_units, migrated_bytes, message);

/**
 * @brief Response structure for QueryTask operation
 */
struct QueryTaskResponse {
    UUID id;
    TaskType type;
    TaskStatus status;
    int64_t created_at_ms_epoch;
    int64_t last_updated_at_ms_epoch;
    UUID assigned_client;
    std::string message;

    QueryTaskResponse() = default;
    QueryTaskResponse(const Task& task)
        : id(task.id),
          type(task.type),
          status(task.status),
          created_at_ms_epoch(static_cast<int64_t>(
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  task.created_at.time_since_epoch())
                  .count())),
          last_updated_at_ms_epoch(static_cast<int64_t>(
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  task.last_updated_at.time_since_epoch())
                  .count())),
          assigned_client(task.assigned_client),
          message(task.message) {}
};
YLT_REFL(QueryTaskResponse, id, type, status, created_at_ms_epoch,
         last_updated_at_ms_epoch, assigned_client, message);

/**
 * @brief Task execution structure
 */
struct TaskAssignment {
    UUID id;
    TaskType type;
    std::string payload;
    int64_t created_at_ms_epoch;
    uint32_t max_retry_attempts;

    TaskAssignment() = default;
    TaskAssignment(const Task& task)
        : id(task.id),
          type(task.type),
          payload(task.payload),
          created_at_ms_epoch(static_cast<int64_t>(
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  task.created_at.time_since_epoch())
                  .count())),
          max_retry_attempts(task.max_retry_attempts) {}
};
YLT_REFL(TaskAssignment, id, type, payload, created_at_ms_epoch,
         max_retry_attempts);

/**
 * @brief Task update structure
 */
struct TaskCompleteRequest {
    UUID id;
    TaskStatus status;
    std::string message;

    TaskCompleteRequest() = default;
};
YLT_REFL(TaskCompleteRequest, id, status, message);

struct BatchGetOffloadObjectResponse {
    uint64_t batch_id;
    std::vector<uint64_t> pointers;
    std::string transfer_engine_addr;
    uint64_t gc_ttl_ms;

    BatchGetOffloadObjectResponse() : batch_id(0), gc_ttl_ms(0) {}
    BatchGetOffloadObjectResponse(uint64_t batch_id_param,
                                  std::vector<uint64_t>&& pointers_param,
                                  std::string transfer_engine_addr_param,
                                  uint64_t gc_ttl_ms_param)
        : batch_id(batch_id_param),
          pointers(std::move(pointers_param)),
          transfer_engine_addr(std::move(transfer_engine_addr_param)),
          gc_ttl_ms(gc_ttl_ms_param) {}
};
YLT_REFL(BatchGetOffloadObjectResponse, batch_id, pointers,
         transfer_engine_addr, gc_ttl_ms);

// ---------------------------------------------------------------------------
// Cost-aware routing (Forge RL design 02)
//
// kMaxCostCandidates: hard cap on a single QueryCost request to bound
// per-RPC CPU / lock work. The router is expected to chunk requests larger
// than this client-side.
//
// QueryCost takes a candidate set of segment_names supplied by the router
// (the router is the only thing that knows which segments hold its prefix
// blocks; that decoupling keeps cost-aware orthogonal to LPM) and returns
// the same set sorted by ascending cost score, with the breakdown of every
// term that fed each score so the router can log / debug routing decisions.
//
// InflightBegin / InflightEnd are tiny telemetry RPCs the router calls when
// it dispatches / finishes a fetch; they keep the master's per-segment
// in-flight counter in sync so subsequent QueryCost calls can shed load.
// ---------------------------------------------------------------------------

inline constexpr size_t kMaxCostCandidates = 1024;

struct QueryCostRequest {
    std::vector<std::string> candidate_segment_names;
    std::string client_host;         // optional; empty == unknown host
    std::string client_zone;         // optional; empty == unknown zone
    uint64_t request_size_bytes{0};  // 0 disables the size term
    bool include_unmounted{false};   // true => keep entries with found=false
    QueryCostRequest() = default;
};
YLT_REFL(QueryCostRequest, candidate_segment_names, client_host, client_zone,
         request_size_bytes, include_unmounted);

struct CostCandidate {
    std::string segment_name;
    double cost_score{0.0};
    int32_t link_class{3};    // see LinkClass enum (3 == UNKNOWN)
    int32_t storage_tier{0};  // see StorageTier enum
    uint32_t inflight{0};
    bool found{false};  // false => segment not currently mounted
    CostCandidate() = default;
};
YLT_REFL(CostCandidate, segment_name, cost_score, link_class, storage_tier,
         inflight, found);

struct QueryCostResponse {
    std::vector<CostCandidate> candidates;  // sorted by cost_score ascending
    uint32_t topology_zone_count{0};
    uint64_t total_inflight{0};
    QueryCostResponse() = default;
};
YLT_REFL(QueryCostResponse, candidates, topology_zone_count, total_inflight);

struct InflightUpdateRequest {
    std::string segment_name;
    InflightUpdateRequest() = default;
    explicit InflightUpdateRequest(std::string n)
        : segment_name(std::move(n)) {}
};
YLT_REFL(InflightUpdateRequest, segment_name);

struct InflightUpdateResponse {
    uint32_t new_value{0};
    InflightUpdateResponse() = default;
    explicit InflightUpdateResponse(uint32_t v) : new_value(v) {}
};
YLT_REFL(InflightUpdateResponse, new_value);

}  // namespace mooncake
