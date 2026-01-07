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

    TaskAssignment() = default;
    TaskAssignment(const Task& task)
        : id(task.id),
          type(task.type),
          payload(task.payload),
          created_at_ms_epoch(static_cast<int64_t>(
              std::chrono::duration_cast<std::chrono::milliseconds>(
                  task.created_at.time_since_epoch())
                  .count())) {}
};
YLT_REFL(TaskAssignment, id, type, payload, created_at_ms_epoch);

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

}  // namespace mooncake
