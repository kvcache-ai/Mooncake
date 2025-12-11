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
 * @brief Response structure for QueryTask operation
 */
struct QueryTaskResponse {
    UUID id;
    TaskType type;
    TaskStatus status;
    int64_t created_at;
    int64_t last_updated_at;
    UUID assigned_client;
    std::string error_message;

    QueryTaskResponse() = default;
    QueryTaskResponse(const UUID id, const TaskType type, const TaskStatus status,
                      const int64_t created_at, const int64_t last_updated_at,
                      const UUID assigned_client, const std::string error_message)
        : id(id),
          type(type),
          status(status),
          created_at(created_at),
          last_updated_at(last_updated_at),
          assigned_client(assigned_client),
          error_message(error_message) {}
};
YLT_REFL(QueryTaskResponse, id, type, status, created_at, last_updated_at, assigned_client, error_message);

}  // namespace mooncake