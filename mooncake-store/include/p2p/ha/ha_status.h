#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include <ylt/reflection/user_reflect_macro.hpp>

#include "p2p/common/p2p_types.h"

namespace mooncake {

struct HAStatusSnapshot {
    bool ha_enabled{false};
    std::string cluster_id;
    std::string local_endpoint;
    std::string role{"unknown"};
    std::string state{"unknown"};
    std::string instance_id;
    std::string leader_endpoint;
    uint64_t epoch{0};
    std::string master_view_address;
    uint64_t master_view_epoch{0};
    int64_t master_view_timestamp_ms{0};
    int64_t master_view_ttl_sec{0};
    std::string master_view_candidate_id;
    uint64_t applied_sequence_id{0};
    uint64_t latest_sequence_id{0};
    uint64_t trimmed_sequence_id{0};
    uint64_t gc_cursor_sequence_id{0};
    uint64_t oplog_lag{0};
    bool snapshot_ready{false};
    bool registry_heartbeat_ok{false};
    int64_t registry_heartbeat_last_success_ms{0};
    int64_t registry_heartbeat_last_failure_ms{0};
    std::string registry_heartbeat_last_error;
    std::string last_error;
    int64_t updated_at_ms{0};

    YLT_REFL(HAStatusSnapshot, ha_enabled, cluster_id, local_endpoint, role,
             state, instance_id, leader_endpoint, epoch, master_view_address,
             master_view_epoch, master_view_timestamp_ms, master_view_ttl_sec,
             master_view_candidate_id, applied_sequence_id, latest_sequence_id,
             trimmed_sequence_id, gc_cursor_sequence_id, oplog_lag,
             snapshot_ready, registry_heartbeat_ok,
             registry_heartbeat_last_success_ms,
             registry_heartbeat_last_failure_ms, registry_heartbeat_last_error,
             last_error, updated_at_ms);
};

class HAStatusManager {
   public:
    static HAStatusManager& instance();

    HAStatusManager(const HAStatusManager&) = delete;
    HAStatusManager& operator=(const HAStatusManager&) = delete;

    void Reset();
    void Configure(bool ha_enabled, std::string cluster_id,
                   std::string local_endpoint);
    void SetRoleState(std::string role, std::string state);
    void SetInstanceId(std::string instance_id);
    void SetLeader(std::string leader_endpoint, uint64_t epoch);
    void SetMasterView(std::string address, uint64_t epoch,
                       int64_t timestamp_ms = 0, int64_t ttl_sec = 0,
                       std::string candidate_id = "");
    void SetSnapshotReady(bool snapshot_ready);
    void SetOpLogProgress(uint64_t applied_sequence_id,
                          uint64_t latest_sequence_id,
                          uint64_t trimmed_sequence_id = 0,
                          uint64_t gc_cursor_sequence_id = 0);
    void SetLastError(std::string error);
    void ClearLastError();
    void ClearLastErrorIfPrefix(std::string prefix);
    void RecordRegistryHeartbeat(ErrorCode result, std::string error);

    HAStatusSnapshot Snapshot() const;
    std::string SerializeJson() const;

   private:
    HAStatusManager();

    static int64_t NowMs();
    void TouchLocked();

    mutable std::mutex mutex_;
    HAStatusSnapshot status_;
};

}  // namespace mooncake
