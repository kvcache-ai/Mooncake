#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include <ylt/reflection/user_reflect_macro.hpp>

#include "p2p/common/p2p_types.h"

namespace mooncake {

struct HADoctorIssue {
    std::string code;
    std::string message;
    std::string category;
    std::string component;
    std::string severity;
    YLT_REFL(HADoctorIssue, code, message, category, component, severity);
};

struct HADoctorStatus {
    std::string role{"unknown"};
    std::string state{"unknown"};
    std::string local_endpoint;
    std::string leader_endpoint;
    uint64_t epoch{0};
    std::string instance_id;
    uint64_t applied_sequence_id{0};
    uint64_t latest_sequence_id{0};
    uint64_t trimmed_sequence_id{0};
    uint64_t gc_cursor_sequence_id{0};
    uint64_t oplog_lag{0};
    bool snapshot_ready{false};
    int64_t heartbeat_age_ms{0};
    YLT_REFL(HADoctorStatus, role, state, local_endpoint, leader_endpoint,
             epoch, instance_id, applied_sequence_id, latest_sequence_id,
             trimmed_sequence_id, gc_cursor_sequence_id, oplog_lag,
             snapshot_ready, heartbeat_age_ms);
};

struct HADoctorReport {
    std::string health{"unknown"};
    HADoctorStatus status;
    std::vector<HADoctorIssue> issues;
    YLT_REFL(HADoctorReport, health, status, issues);
};

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
    std::string master_view_endpoint;
    uint64_t master_view_epoch{0};
    int64_t master_view_timestamp_ms{0};
    int64_t master_view_updated_at_ms{0};
    int64_t master_view_ttl_sec{0};
    int64_t master_view_age_ms{0};
    int64_t master_view_expires_in_ms{0};
    std::string master_view_instance_id;
    uint64_t applied_sequence_id{0};
    uint64_t oplog_applied_sequence_id{0};
    uint64_t latest_sequence_id{0};
    uint64_t oplog_latest_sequence_id{0};
    bool oplog_latest_read_ok{false};
    uint64_t trimmed_sequence_id{0};
    uint64_t oplog_trimmed_sequence_id{0};
    uint64_t gc_cursor_sequence_id{0};
    uint64_t oplog_gc_cursor_sequence_id{0};
    uint64_t oplog_lag{0};
    uint64_t oplog_apply_lag{0};
    uint64_t oplog_pending_writes{0};
    uint64_t oplog_writer_pending_writes{0};
    uint64_t oplog_writer_queue_capacity{0};
    bool oplog_writer_running{false};
    uint64_t oplog_writer_last_committed_sequence_id{0};
    int64_t oplog_last_successful_poll_timestamp_ms{0};
    int64_t oplog_poll_success_age_ms{0};
    bool snapshot_ready{false};
    bool registry_heartbeat_ok{false};
    int64_t registry_heartbeat_last_success_ms{0};
    int64_t registry_heartbeat_last_success_timestamp_ms{0};
    int64_t registry_heartbeat_last_failure_ms{0};
    int64_t registry_heartbeat_last_failure_timestamp_ms{0};
    std::string registry_heartbeat_last_error;
    std::string last_error;
    int64_t updated_at_ms{0};
    int64_t status_updated_at_ms{0};

    YLT_REFL(HAStatusSnapshot, ha_enabled, cluster_id, local_endpoint, role,
             state, instance_id, leader_endpoint, epoch, master_view_address,
             master_view_endpoint, master_view_epoch, master_view_timestamp_ms,
             master_view_updated_at_ms, master_view_ttl_sec, master_view_age_ms,
             master_view_expires_in_ms, master_view_instance_id,
             applied_sequence_id, oplog_applied_sequence_id, latest_sequence_id,
             oplog_latest_sequence_id, oplog_latest_read_ok,
             trimmed_sequence_id, oplog_trimmed_sequence_id,
             gc_cursor_sequence_id, oplog_gc_cursor_sequence_id, oplog_lag,
             oplog_apply_lag, oplog_pending_writes, oplog_writer_pending_writes,
             oplog_writer_queue_capacity, oplog_writer_running,
             oplog_writer_last_committed_sequence_id,
             oplog_last_successful_poll_timestamp_ms, oplog_poll_success_age_ms,
             snapshot_ready, registry_heartbeat_ok,
             registry_heartbeat_last_success_ms,
             registry_heartbeat_last_success_timestamp_ms,
             registry_heartbeat_last_failure_ms,
             registry_heartbeat_last_failure_timestamp_ms,
             registry_heartbeat_last_error, last_error, updated_at_ms,
             status_updated_at_ms);
};

class HAStatusManager {
   public:
    static HAStatusManager& instance();

    HAStatusManager(const HAStatusManager&) = delete;
    HAStatusManager& operator=(const HAStatusManager&) = delete;

    void Reset();
    void Configure(bool ha_enabled, std::string cluster_id,
                   std::string local_endpoint,
                   uint32_t oplog_latest_stall_timeout_sec = 10,
                   uint32_t oplog_catchup_stall_timeout_sec = 10);
    void SetRoleState(std::string role, std::string state);
    void SetInstanceId(std::string instance_id);
    void SetLeader(std::string leader_endpoint, uint64_t epoch);
    void SetMasterView(std::string address, uint64_t epoch,
                       int64_t timestamp_ms = 0, int64_t ttl_sec = 0,
                       std::string instance_id = "");
    // Marks the master_view lease as freshly renewed/re-observed without
    // changing the stored view. Called by the leader renewal loop so the
    // reported remaining lease reflects the most recent TTL refresh rather
    // than the original election timestamp.
    void MarkMasterViewRefreshed();
    void SetSnapshotReady(bool snapshot_ready);
    void SetStandbySnapshotAvailability(uint32_t available, uint32_t required);
    void SetOpLogProgress(uint64_t applied_sequence_id,
                          std::optional<uint64_t> latest_sequence_id,
                          uint64_t trimmed_sequence_id = 0,
                          uint64_t gc_cursor_sequence_id = 0);
    void SetLastError(std::string error);
    void ClearLastError();
    void ClearLastErrorIfPrefix(std::string prefix);
    void RecordRegistryHeartbeat(ErrorCode result, std::string error);
    // Event-driven flag set from the P2P election readiness wait loop and
    // Redis election callback when latest is unknown or lag exceeds the gate.
    // Capture the same sample used for the decision, independently of status
    // updates from background replication. Clear when ready or election ends.
    void SetElectionReadinessBlocked(
        bool blocked, uint64_t applied_sequence_id = 0,
        std::optional<uint64_t> latest_sequence_id = std::nullopt,
        uint64_t max_lag_entries = 0);

    HAStatusSnapshot Snapshot() const;
    std::string SerializeJson() const;
    // An empty message clears the current issue. Callers own recovery and
    // lifecycle cleanup; existing failure-path logs remain the diagnostics.
    void SetDoctorIssue(std::string code, std::string message = {});
    // A new supervisor round may preserve the last failed stepdown drain until
    // a successful drain or explicit reset acknowledges it.
    void ClearDoctorIssues(bool preserve_stepdown_drain = false);
    HADoctorReport GetDoctorReport() const;
    std::string SerializeDoctorJson() const;
    // Writer events publish under its queue lock; diagnostics never take it.
    // Stopping disables late progress callbacks until the next writer starts.
    void SetOpLogWriterRunning(bool running);
    void SetOpLogWriteProgress(uint64_t committed_sequence_id, size_t pending,
                               size_t capacity = 0);
    void SetOpLogPollSuccess();
    // A rejected standby entry may have been passed by the scan cursor.
    // Keep the first rejection visible until the standby is reinitialized.
    void RecordOpLogPendingRejection(uint64_t sequence_id,
                                     uint64_t expected_sequence_id,
                                     size_t pending, size_t capacity);
    // A trusted snapshot starts a new standby lag observation window.
    void ResetOpLogLagGrowth();

   private:
    friend struct HAStatusTestPeer;

    HAStatusManager();

    static int64_t NowMs();
    HAStatusSnapshot BuildSnapshotLocked(int64_t now_ms) const;
    void TouchLocked();
    void ObserveLeaderLocked(const std::string& address, uint64_t epoch);
    std::string ElectionUnstableMessageLocked() const;
    std::string MasterViewMismatchMessageLocked() const;
    std::string MasterViewLeaseExpiredMessageLocked() const;
    std::string StandbySnapshotInsufficientMessageLocked() const;
    std::string StandbyBehindTrimMessageLocked() const;
    std::string OpLogTrimRiskMessageLocked() const;
    std::string OpLogLatestStallMessageLocked() const;
    std::string OpLogCatchUpStallMessageLocked() const;
    std::string OpLogLagGrowingMessageLocked() const;
    std::string OpLogQueuePressureMessageLocked() const;
    std::string OpLogPollStaleMessageLocked() const;

    mutable std::mutex mutex_;
    HAStatusSnapshot status_;
    // When the master_view was last refreshed locally: set on every
    // SetMasterView call (election/observation) and on lease renewal. Basis
    // for master_view_expires_in_ms instead of the fixed election timestamp.
    int64_t master_view_refreshed_at_ms_{0};
    std::map<std::string, std::string> doctor_issues_;
    // Survives supervisor rounds; the first observation only establishes a
    // baseline.
    struct ElectionHistory {
        uint64_t epoch{0};
        std::string leader;
        int64_t last_change_ms{0};
        std::deque<std::chrono::steady_clock::time_point> changes;
    } election_;
    struct OpLogWriterProgress {
        bool running{false};
        uint64_t committed_sequence_id{0};
        size_t pending{0};
        size_t capacity{0};
        std::optional<std::chrono::steady_clock::time_point> stalled_since;
    } oplog_writer_;
    struct StandbySnapshotAvailability {
        uint32_t available{0};
        uint32_t required{0};
    } standby_snapshot_;
    uint32_t oplog_latest_stall_timeout_sec_{10};
    struct OpLogCatchUpProgress {
        bool latest_known{false};
        std::optional<std::chrono::steady_clock::time_point> stalled_since;
        std::chrono::steady_clock::time_point last_observed_at;
    } oplog_catchup_;
    struct OpLogLagGrowth {
        std::optional<std::chrono::steady_clock::time_point> since;
        std::chrono::steady_clock::time_point last_observed_at;
        uint64_t baseline_lag{0};
    } oplog_lag_growth_;
    uint32_t oplog_catchup_stall_timeout_sec_{10};
    struct OpLogPollProgress {
        std::optional<std::chrono::steady_clock::time_point> last_success_at;
        int64_t last_success_ms{0};
    } oplog_poll_;
    struct ElectionReadiness {
        bool blocked{false};
        uint64_t applied_sequence_id{0};
        std::optional<uint64_t> latest_sequence_id;
        uint64_t max_lag_entries{0};
    } election_readiness_;
};

}  // namespace mooncake
