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

/**
 * @brief Forge RL Design 01 — chained-prefix LPM lookup request.
 *
 * The router computes a per-block rolling hash chain (key_i = hash(key_{i-1},
 * block_i)) and sends it to the master.  An empty chain is rejected with
 * PREFIX_CHAIN_EMPTY; chains longer than kMaxPrefixChainLength are rejected
 * with PREFIX_CHAIN_TOO_LONG.
 */
struct QueryPrefixMatchRequest {
    std::vector<uint64_t> chain{};

    QueryPrefixMatchRequest() = default;
};
YLT_REFL(QueryPrefixMatchRequest, chain);

/**
 * @brief A single segment that owns a replica of the deepest matched prefix
 * key.  segment_id is left as {0,0} when only the segment_name is needed by
 * the routing layer; this avoids holding segment_mutex_ during the read path.
 */
struct PrefixMatchCandidate {
    UUID segment_id{0, 0};
    std::string segment_name{};
    int32_t replica_type{0};

    PrefixMatchCandidate() = default;
};
YLT_REFL(PrefixMatchCandidate, segment_id, segment_name, replica_type);

/**
 * @brief Forge RL Design 01 — chained-prefix LPM lookup response.
 *
 * matched_blocks is the longest prefix length (in blocks) that was found in
 * the master.  candidates is unranked; routing-aware ordering (by link
 * class, inflight, etc.) is left to the caller.  query_lease_ms is an
 * advisory TTL the caller may use to debounce repeated lookups.
 */
struct QueryPrefixMatchResponse {
    uint32_t matched_blocks{0};
    std::vector<PrefixMatchCandidate> candidates{};
    uint64_t query_lease_ms{0};

    QueryPrefixMatchResponse() = default;
};
YLT_REFL(QueryPrefixMatchResponse, matched_blocks, candidates, query_lease_ms);

}  // namespace mooncake
