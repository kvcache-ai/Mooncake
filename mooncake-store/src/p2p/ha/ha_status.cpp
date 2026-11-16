#include "p2p/ha/ha_status.h"

#include <algorithm>
#include <chrono>
#include <utility>

#include <glog/logging.h>
#include <ylt/struct_json/json_writer.h>

#include "p2p/ha/ha_metric_manager.h"

namespace mooncake {

namespace {

constexpr auto kElectionWindow = std::chrono::seconds(300);
constexpr int kElectionChangeThreshold = 3;
constexpr size_t kOpLogQueuePressurePercent = 80;
constexpr uint64_t kOpLogLagGrowthMinEntries = 1000;
constexpr auto kOpLogLagGrowthDuration = std::chrono::seconds(30);
constexpr uint64_t kOpLogTrimRiskThresholdPercent = 10;

uint64_t CeilPercent(uint64_t value, uint64_t percent) {
    return value / 100 * percent + ((value % 100) * percent + 99) / 100;
}

int64_t RoleMetricValue(const HAStatusSnapshot& status) {
    if (status.role == "failed" || status.state == "FAILED") {
        return 6;
    }
    if (!status.ha_enabled || status.role == "disabled") {
        return 1;
    }
    if (status.role == "primary") {
        return 5;
    }
    if (status.role == "promoting") {
        return 4;
    }
    if (status.role == "standby") {
        return 3;
    }
    if (status.role == "starting") {
        return 2;
    }
    return 0;
}

void UpdateMetrics(const HAStatusSnapshot& status) {
    auto& metrics = HAMetricManager::instance();
    metrics.set_master_role(RoleMetricValue(status));
    metrics.set_snapshot_ready(status.snapshot_ready);
    metrics.set_registry_heartbeat_ok(status.registry_heartbeat_ok);
    metrics.set_status_updated_timestamp_ms(status.updated_at_ms);
}

bool StartsWith(const std::string& value, const std::string& prefix) {
    return value.rfind(prefix, 0) == 0;
}

std::string IssueCategory(const std::string& code) {
    if (StartsWith(code, "OPLOG_") || StartsWith(code, "PROMOTION_") ||
        code == "STANDBY_BEHIND_TRIM") {
        return "oplog";
    }
    if (StartsWith(code, "ELECTION_") || code == "LEADERSHIP_LOST" ||
        code == "MASTER_VIEW_MISMATCH") {
        return "election";
    }
    if (StartsWith(code, "SNAPSHOT_") ||
        StartsWith(code, "STANDBY_SNAPSHOT_")) {
        return "snapshot";
    }
    if (StartsWith(code, "REGISTRY_")) {
        return "registry";
    }
    return "ha";
}

std::string IssueComponent(const std::string& code) {
    if (StartsWith(code, "OPLOG_WRITE_") || StartsWith(code, "OPLOG_COMMIT_") ||
        StartsWith(code, "OPLOG_LATEST_") || code == "OPLOG_QUEUE_PRESSURE" ||
        code == "OPLOG_DRAIN_INCOMPLETE") {
        return "oplog_writer";
    }
    if (StartsWith(code, "OPLOG_READ_") || code == "OPLOG_POLL_STALE" ||
        StartsWith(code, "OPLOG_RECONNECT_")) {
        return "oplog_reader";
    }
    if (StartsWith(code, "OPLOG_APPLY_") || code == "OPLOG_CATCHUP_STALLED" ||
        code == "OPLOG_PENDING_REJECTED" || code == "OPLOG_LAG_GROWING" ||
        code == "STANDBY_BEHIND_TRIM" || code == "OPLOG_TRIM_RISK" ||
        StartsWith(code, "PROMOTION_")) {
        return "oplog_applier";
    }
    if (StartsWith(code, "ELECTION_WATCH_")) {
        return "leader_watch";
    }
    if (StartsWith(code, "ELECTION_") || code == "LEADERSHIP_LOST" ||
        code == "MASTER_VIEW_MISMATCH") {
        return "leader_election";
    }
    if (StartsWith(code, "SNAPSHOT_") ||
        StartsWith(code, "STANDBY_SNAPSHOT_")) {
        return "snapshot";
    }
    if (StartsWith(code, "REGISTRY_")) {
        return "registry_heartbeat";
    }
    return "ha";
}

std::string IssueSeverity(const std::string& code) {
    if (code == "ELECTION_UNSTABLE" || code == "MASTER_VIEW_MISMATCH" ||
        code == "STANDBY_SNAPSHOT_INSUFFICIENT" || code == "OPLOG_TRIM_RISK" ||
        code == "OPLOG_QUEUE_PRESSURE" || code == "OPLOG_LATEST_STALLED" ||
        code == "OPLOG_CATCHUP_STALLED" || code == "OPLOG_POLL_STALE" ||
        code == "OPLOG_LAG_GROWING" || code == "PROMOTION_CATCHUP_INCOMPLETE" ||
        code == "OPLOG_DRAIN_INCOMPLETE" ||
        code == "ELECTION_READINESS_BLOCKED" || code == "ELECTION_NOT_READY") {
        return "warning";
    }
    return "error";
}

HADoctorIssue MakeDoctorIssue(std::string code, std::string message) {
    HADoctorIssue issue;
    issue.code = std::move(code);
    issue.message = std::move(message);
    issue.category = IssueCategory(issue.code);
    issue.component = IssueComponent(issue.code);
    issue.severity = IssueSeverity(issue.code);
    return issue;
}

bool IsDoctorIssueScopedToRole(const std::string& code,
                               const std::string& role) {
    if (role == "primary") {
        return StartsWith(code, "OPLOG_WRITE_") ||
               StartsWith(code, "OPLOG_COMMIT_") ||
               code == "PROMOTION_CATCHUP_INCOMPLETE";
    }
    if (role == "standby") {
        return StartsWith(code, "OPLOG_READ_") ||
               StartsWith(code, "OPLOG_RECONNECT_") ||
               StartsWith(code, "OPLOG_APPLY_") ||
               code == "OPLOG_PENDING_REJECTED" ||
               code == "SNAPSHOT_RECOVERY_FAILED";
    }
    if (role == "promoting") {
        return StartsWith(code, "PROMOTION_");
    }
    return false;
}

int64_t HeartbeatAgeMs(const HAStatusSnapshot& status, int64_t now_ms) {
    const int64_t last_heartbeat_ms =
        std::max(status.registry_heartbeat_last_success_ms,
                 status.registry_heartbeat_last_failure_ms);
    if (last_heartbeat_ms <= 0) {
        return 0;
    }
    return std::max<int64_t>(0, now_ms - last_heartbeat_ms);
}

HADoctorStatus MakeDoctorStatus(const HAStatusSnapshot& status,
                                int64_t now_ms) {
    HADoctorStatus doctor_status;
    doctor_status.role = status.role;
    doctor_status.state = status.state;
    doctor_status.local_endpoint = status.local_endpoint;
    doctor_status.leader_endpoint = status.leader_endpoint;
    doctor_status.epoch = status.epoch;
    doctor_status.instance_id = status.instance_id;
    doctor_status.applied_sequence_id = status.applied_sequence_id;
    doctor_status.latest_sequence_id = status.latest_sequence_id;
    doctor_status.trimmed_sequence_id = status.trimmed_sequence_id;
    doctor_status.gc_cursor_sequence_id = status.gc_cursor_sequence_id;
    doctor_status.oplog_lag = status.oplog_lag;
    doctor_status.snapshot_ready = status.snapshot_ready;
    doctor_status.heartbeat_age_ms = HeartbeatAgeMs(status, now_ms);
    return doctor_status;
}

}  // namespace

HAStatusManager& HAStatusManager::instance() {
    static HAStatusManager manager;
    return manager;
}

HAStatusManager::HAStatusManager() { TouchLocked(); }

void HAStatusManager::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    status_ = HAStatusSnapshot{};
    master_view_refreshed_at_ms_ = 0;
    election_ = {};
    doctor_issues_.clear();
    oplog_writer_ = {};
    oplog_catchup_ = {};
    oplog_lag_growth_ = {};
    standby_snapshot_ = {};
    oplog_poll_ = {};
    election_readiness_ = {};
    status_.oplog_pending_writes = 0;
    oplog_latest_stall_timeout_sec_ = 10;
    oplog_catchup_stall_timeout_sec_ = 10;
    TouchLocked();
}

void HAStatusManager::SetElectionReadinessBlocked(
    bool blocked, uint64_t applied_sequence_id,
    std::optional<uint64_t> latest_sequence_id, uint64_t max_lag_entries) {
    std::lock_guard<std::mutex> lock(mutex_);
    election_readiness_ =
        blocked ? ElectionReadiness{true, applied_sequence_id,
                                    latest_sequence_id, max_lag_entries}
                : ElectionReadiness{};
}

void HAStatusManager::Configure(bool ha_enabled, std::string cluster_id,
                                std::string local_endpoint,
                                uint32_t oplog_latest_stall_timeout_sec,
                                uint32_t oplog_catchup_stall_timeout_sec) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!ha_enabled ||
        (!cluster_id.empty() && cluster_id != status_.cluster_id)) {
        election_ = {};
    }
    status_.ha_enabled = ha_enabled;
    oplog_latest_stall_timeout_sec_ = oplog_latest_stall_timeout_sec;
    oplog_catchup_stall_timeout_sec_ = oplog_catchup_stall_timeout_sec;
    if (!cluster_id.empty()) {
        status_.cluster_id = std::move(cluster_id);
    }
    if (!local_endpoint.empty()) {
        status_.local_endpoint = std::move(local_endpoint);
    }
    if (!ha_enabled) {
        oplog_writer_ = {};
        oplog_catchup_ = {};
        oplog_lag_growth_ = {};
        oplog_poll_ = {};
        standby_snapshot_ = {};
        status_.oplog_latest_read_ok = false;
        status_.oplog_pending_writes = 0;
        status_.role = "disabled";
        status_.state = "disabled";
    }
    TouchLocked();
}

void HAStatusManager::SetRoleState(std::string role, std::string state) {
    bool changed = false;
    std::string old_role;
    std::string old_state;
    std::string new_role;
    std::string new_state;
    std::lock_guard<std::mutex> lock(mutex_);
    changed = status_.role != role || status_.state != state;
    if (role != "standby" || state != "WATCHING") {
        oplog_catchup_ = {};
        oplog_lag_growth_ = {};
        oplog_poll_ = {};
    }
    if (changed) {
        old_role = status_.role;
        old_state = status_.state;
        new_role = role;
        new_state = state;
    }
    if (status_.role == "primary" && role != "primary") {
        doctor_issues_.erase("PROMOTION_CATCHUP_INCOMPLETE");
    }
    if (changed && status_.role != role) {
        for (auto it = doctor_issues_.begin(); it != doctor_issues_.end();) {
            if (IsDoctorIssueScopedToRole(it->first, status_.role)) {
                it = doctor_issues_.erase(it);
            } else {
                ++it;
            }
        }
    }
    status_.role = std::move(role);
    status_.state = std::move(state);
    TouchLocked();
    if (changed && status_.ha_enabled) {
        LOG(INFO) << "HA status transition: role=" << old_role
                  << ", state=" << old_state << " -> role=" << new_role
                  << ", state=" << new_state;
    }
}

void HAStatusManager::SetInstanceId(std::string instance_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    status_.instance_id = std::move(instance_id);
    TouchLocked();
}

void HAStatusManager::SetLeader(std::string leader_endpoint, uint64_t epoch) {
    std::lock_guard<std::mutex> lock(mutex_);
    ObserveLeaderLocked(leader_endpoint, epoch);
    status_.leader_endpoint = leader_endpoint;
    status_.epoch = epoch;
    status_.master_view_address = std::move(leader_endpoint);
    status_.master_view_epoch = epoch;
    TouchLocked();
}

void HAStatusManager::SetMasterView(std::string address, uint64_t epoch,
                                    int64_t timestamp_ms, int64_t ttl_sec,
                                    std::string instance_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    ObserveLeaderLocked(address, epoch);
    status_.leader_endpoint = address;
    status_.epoch = epoch;
    status_.master_view_address = std::move(address);
    status_.master_view_epoch = epoch;
    status_.master_view_timestamp_ms = timestamp_ms;
    status_.master_view_ttl_sec = ttl_sec;
    status_.master_view_instance_id = std::move(instance_id);
    master_view_refreshed_at_ms_ = NowMs();
    TouchLocked();
}

void HAStatusManager::MarkMasterViewRefreshed() {
    std::lock_guard<std::mutex> lock(mutex_);
    // Only refresh an active view; avoids carrying a stale refresh base for a
    // non-existent leader.
    if (status_.master_view_ttl_sec > 0) {
        master_view_refreshed_at_ms_ = NowMs();
        TouchLocked();
    }
}

void HAStatusManager::ObserveLeaderLocked(const std::string& address,
                                          uint64_t epoch) {
    if (!status_.ha_enabled || address.empty() || epoch <= election_.epoch) {
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    while (!election_.changes.empty() &&
           now - election_.changes.front() >= kElectionWindow) {
        election_.changes.pop_front();
    }
    if (election_.epoch != 0) {
        election_.changes.push_back(now);
        election_.last_change_ms = NowMs();
    }
    election_.epoch = epoch;
    election_.leader = address;
    if (auto issue = ElectionUnstableMessageLocked(); !issue.empty()) {
        // Election paths already log causes; only add a sampled aggregate.
        LOG_EVERY_N(WARNING, 30) << "HA election unstable: " << issue;
    }
}

std::string HAStatusManager::ElectionUnstableMessageLocked() const {
    const auto cutoff = std::chrono::steady_clock::now() - kElectionWindow;
    const auto changes = std::count_if(
        election_.changes.begin(), election_.changes.end(),
        [cutoff](const auto& changed_at) { return changed_at > cutoff; });
    if (changes < kElectionChangeThreshold) return {};
    return "window_sec=" + std::to_string(kElectionWindow.count()) +
           ", changes=" + std::to_string(changes) +
           ", last_change_ms=" + std::to_string(election_.last_change_ms) +
           ", leader=" + election_.leader +
           ", epoch=" + std::to_string(election_.epoch);
}

std::string HAStatusManager::MasterViewMismatchMessageLocked() const {
    if (!status_.ha_enabled || status_.role != "primary" ||
        status_.state != "RUNNING" || status_.local_endpoint.empty()) {
        return {};
    }

    const bool missing_view = status_.master_view_address.empty();
    const bool address_mismatch =
        !missing_view && status_.master_view_address != status_.local_endpoint;
    const bool instance_mismatch =
        !status_.instance_id.empty() &&
        !status_.master_view_instance_id.empty() &&
        status_.instance_id != status_.master_view_instance_id;
    const bool epoch_mismatch = status_.epoch != 0 &&
                                status_.master_view_epoch != 0 &&
                                status_.epoch != status_.master_view_epoch;
    if (!missing_view && !address_mismatch && !instance_mismatch &&
        !epoch_mismatch) {
        return {};
    }

    return "local=" + status_.local_endpoint + ", local_instance_id=" +
           (status_.instance_id.empty() ? "<empty>" : status_.instance_id) +
           ", local_epoch=" + std::to_string(status_.epoch) + ", view=" +
           (status_.master_view_address.empty() ? "<empty>"
                                                : status_.master_view_address) +
           ", view_instance_id=" +
           (status_.master_view_instance_id.empty()
                ? "<empty>"
                : status_.master_view_instance_id) +
           ", view_epoch=" + std::to_string(status_.master_view_epoch);
}

std::string HAStatusManager::MasterViewLeaseExpiredMessageLocked() const {
    if (!status_.ha_enabled || status_.role != "primary" ||
        status_.state != "RUNNING") {
        return {};
    }
    // The renew-failure path already reports LEADERSHIP_LOST; suppress here so
    // this issue only surfaces the sticky-loop blind spot where renewal never
    // reached the failure branch.
    if (doctor_issues_.find("LEADERSHIP_LOST") != doctor_issues_.end()) {
        return {};
    }
    if (status_.master_view_ttl_sec <= 0 || master_view_refreshed_at_ms_ <= 0) {
        return {};
    }
    const auto now_ms = NowMs();
    const int64_t expires_in_ms = master_view_refreshed_at_ms_ +
                                  status_.master_view_ttl_sec * 1000 - now_ms;
    // Grace below zero absorbs short clock jitter; normal renewals keep the
    // value comfortably positive because heartbeat < ttl.
    constexpr int64_t kGraceMs = 1000;
    if (expires_in_ms > -kGraceMs) {
        return {};
    }
    return "primary master_view lease expired: expires_in_ms=" +
           std::to_string(expires_in_ms) + ", last_refresh_age_ms=" +
           std::to_string(now_ms - master_view_refreshed_at_ms_) +
           ", ttl_sec=" + std::to_string(status_.master_view_ttl_sec) +
           ", view=" +
           (status_.master_view_address.empty() ? "<empty>"
                                                : status_.master_view_address) +
           ", view_epoch=" + std::to_string(status_.master_view_epoch);
}

std::string HAStatusManager::StandbyBehindTrimMessageLocked() const {
    if (!status_.ha_enabled || status_.role != "standby" ||
        status_.state != "WATCHING" || status_.trimmed_sequence_id == 0 ||
        status_.applied_sequence_id >= status_.trimmed_sequence_id) {
        return {};
    }
    return "oplog_applied_sequence_id=" +
           std::to_string(status_.applied_sequence_id) +
           ", oplog_trimmed_sequence_id=" +
           std::to_string(status_.trimmed_sequence_id) +
           ", oplog_latest_sequence_id=" +
           std::to_string(status_.latest_sequence_id) +
           ", oplog_apply_lag=" + std::to_string(status_.oplog_lag) +
           ", action=snapshot_resync_required";
}

std::string HAStatusManager::OpLogTrimRiskMessageLocked() const {
    if (!status_.ha_enabled || status_.role != "standby" ||
        status_.state != "WATCHING" || status_.trimmed_sequence_id == 0 ||
        status_.applied_sequence_id < status_.trimmed_sequence_id ||
        status_.latest_sequence_id <= status_.trimmed_sequence_id) {
        return {};
    }
    const uint64_t trim_headroom =
        status_.applied_sequence_id - status_.trimmed_sequence_id;
    const uint64_t retained_window =
        status_.latest_sequence_id - status_.trimmed_sequence_id;
    const uint64_t threshold =
        CeilPercent(retained_window, kOpLogTrimRiskThresholdPercent);
    if (trim_headroom > threshold) {
        return {};
    }
    return "oplog_applied_sequence_id=" +
           std::to_string(status_.applied_sequence_id) +
           ", oplog_trimmed_sequence_id=" +
           std::to_string(status_.trimmed_sequence_id) +
           ", oplog_latest_sequence_id=" +
           std::to_string(status_.latest_sequence_id) +
           ", oplog_apply_lag=" + std::to_string(status_.oplog_lag) +
           ", oplog_trim_headroom=" + std::to_string(trim_headroom) +
           ", oplog_retained_window=" + std::to_string(retained_window) +
           ", threshold_percent=" +
           std::to_string(kOpLogTrimRiskThresholdPercent);
}

std::string HAStatusManager::StandbySnapshotInsufficientMessageLocked() const {
    if (!status_.ha_enabled || status_.role != "primary" ||
        status_.state != "RUNNING" || standby_snapshot_.required == 0 ||
        standby_snapshot_.available >= standby_snapshot_.required) {
        return {};
    }
    return "standby_snapshot_available=" +
           std::to_string(standby_snapshot_.available) +
           ", standby_snapshot_required=" +
           std::to_string(standby_snapshot_.required) +
           ", primary_snapshot_fallback_available=" +
           std::string(status_.snapshot_ready ? "true" : "false") +
           ", action=ensure_standby_snapshot_source";
}

void HAStatusManager::SetSnapshotReady(bool snapshot_ready) {
    std::lock_guard<std::mutex> lock(mutex_);
    status_.snapshot_ready = snapshot_ready;
    TouchLocked();
}

void HAStatusManager::SetStandbySnapshotAvailability(uint32_t available,
                                                     uint32_t required) {
    std::lock_guard<std::mutex> lock(mutex_);
    standby_snapshot_.available = available;
    standby_snapshot_.required = required;
    TouchLocked();
}

void HAStatusManager::SetOpLogProgress(
    uint64_t applied_sequence_id, std::optional<uint64_t> latest_sequence_id,
    uint64_t trimmed_sequence_id, uint64_t gc_cursor_sequence_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    status_.oplog_latest_read_ok = latest_sequence_id.has_value();
    if (latest_sequence_id) {
        status_.latest_sequence_id = *latest_sequence_id;
    }
    if (status_.ha_enabled && status_.role == "standby" &&
        status_.state == "WATCHING") {
        oplog_catchup_.latest_known |= latest_sequence_id.has_value();
        oplog_catchup_.last_observed_at = std::chrono::steady_clock::now();
        if (!oplog_catchup_.latest_known ||
            applied_sequence_id >= status_.latest_sequence_id) {
            oplog_catchup_.stalled_since.reset();
        } else if (!oplog_catchup_.stalled_since ||
                   applied_sequence_id > status_.applied_sequence_id) {
            oplog_catchup_.stalled_since = oplog_catchup_.last_observed_at;
        }
        if (latest_sequence_id) {
            const uint64_t lag = *latest_sequence_id > applied_sequence_id
                                     ? *latest_sequence_id - applied_sequence_id
                                     : 0;
            if (!oplog_lag_growth_.since ||
                lag <= oplog_lag_growth_.baseline_lag) {
                oplog_lag_growth_.since = oplog_catchup_.last_observed_at;
                oplog_lag_growth_.baseline_lag = lag;
            }
            oplog_lag_growth_.last_observed_at =
                oplog_catchup_.last_observed_at;
        } else {
            oplog_lag_growth_ = {};
        }
    } else {
        oplog_catchup_ = {};
        oplog_lag_growth_ = {};
    }
    status_.applied_sequence_id = applied_sequence_id;
    status_.trimmed_sequence_id = trimmed_sequence_id;
    status_.gc_cursor_sequence_id = gc_cursor_sequence_id;
    status_.oplog_lag = status_.latest_sequence_id > applied_sequence_id
                            ? status_.latest_sequence_id - applied_sequence_id
                            : 0;
    // Use the same local watermark as /ha/status. On a primary this is the
    // latest assigned sequence, which can differ from Redis's persisted latest.
    HAMetricManager::instance().set_oplog_applied_sequence_id(
        static_cast<int64_t>(applied_sequence_id));
    TouchLocked();
}

void HAStatusManager::SetLastError(std::string error) {
    std::lock_guard<std::mutex> lock(mutex_);
    status_.last_error = std::move(error);
    TouchLocked();
}

void HAStatusManager::ClearLastError() {
    std::lock_guard<std::mutex> lock(mutex_);
    status_.last_error.clear();
    TouchLocked();
}

void HAStatusManager::ClearLastErrorIfPrefix(std::string prefix) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (status_.last_error.rfind(prefix, 0) != 0) {
        return;
    }
    status_.last_error.clear();
    TouchLocked();
}

void HAStatusManager::RecordRegistryHeartbeat(ErrorCode result,
                                              std::string error) {
    bool recovered = false;
    std::lock_guard<std::mutex> lock(mutex_);
    if (result == ErrorCode::OK) {
        doctor_issues_.erase("REGISTRY_HEARTBEAT_FAILED");
        recovered = !status_.registry_heartbeat_ok &&
                    status_.registry_heartbeat_last_failure_ms > 0;
        status_.registry_heartbeat_ok = true;
        status_.registry_heartbeat_last_success_ms = NowMs();
        if (status_.last_error == status_.registry_heartbeat_last_error) {
            status_.last_error.clear();
        }
        status_.registry_heartbeat_last_error.clear();
    } else {
        doctor_issues_["REGISTRY_HEARTBEAT_FAILED"] = error;
        status_.registry_heartbeat_ok = false;
        status_.registry_heartbeat_last_failure_ms = NowMs();
        status_.registry_heartbeat_last_error = std::move(error);
        status_.last_error = status_.registry_heartbeat_last_error;
    }
    TouchLocked();
    // The heartbeat drives aggregate stall logging; HTTP diagnosis evaluates
    // the cached timer independently and does not need this background loop.
    if (auto stall = OpLogLatestStallMessageLocked(); !stall.empty()) {
        LOG_EVERY_N(WARNING, 30)
            << "OpLog latest stalled: " << stall << ", sample_every_n=30";
    }
    if (auto stall = OpLogCatchUpStallMessageLocked(); !stall.empty()) {
        LOG_EVERY_N(WARNING, 30)
            << "OpLog catch-up stalled: " << stall << ", sample_every_n=30";
    }
    if (auto issue = OpLogLagGrowingMessageLocked(); !issue.empty()) {
        LOG_EVERY_N(WARNING, 30)
            << "OpLog lag growing: " << issue << ", sample_every_n=30";
    }
    if (recovered) {
        LOG(INFO) << "Redis Master registry heartbeat recovered"
                  << ", instance_id=" << status_.instance_id
                  << ", role=" << status_.role;
    }
}

HAStatusSnapshot HAStatusManager::Snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return BuildSnapshotLocked(NowMs());
}

std::string HAStatusManager::SerializeJson() const {
    std::string json;
    struct_json::to_json(Snapshot(), json);
    return json;
}

void HAStatusManager::SetDoctorIssue(std::string code, std::string message) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (message.empty()) {
        doctor_issues_.erase(code);
    } else {
        doctor_issues_[std::move(code)] = std::move(message);
    }
}

void HAStatusManager::ClearDoctorIssues(bool preserve_stepdown_drain) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto stepdown_drain = doctor_issues_.extract("OPLOG_DRAIN_INCOMPLETE");
    doctor_issues_.clear();
    if (preserve_stepdown_drain && !stepdown_drain.empty()) {
        doctor_issues_.insert(std::move(stepdown_drain));
    }
    oplog_writer_ = {};
    oplog_catchup_ = {};
    oplog_lag_growth_ = {};
    oplog_poll_ = {};
    election_readiness_ = {};
    status_.oplog_pending_writes = 0;
}

void HAStatusManager::RecordOpLogPendingRejection(uint64_t sequence_id,
                                                  uint64_t expected_sequence_id,
                                                  size_t pending,
                                                  size_t capacity) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!status_.ha_enabled) {
        return;
    }
    doctor_issues_.try_emplace(
        "OPLOG_PENDING_REJECTED",
        "first_rejected_sequence_id=" + std::to_string(sequence_id) +
            ", expected_sequence_id=" + std::to_string(expected_sequence_id) +
            ", pending=" + std::to_string(pending) + ", capacity=" +
            std::to_string(capacity) + ", values=snapshot_at_first_rejection" +
            ", action=verify_replay_before_promotion");
}

void HAStatusManager::ResetOpLogLagGrowth() {
    std::lock_guard<std::mutex> lock(mutex_);
    oplog_lag_growth_ = {};
}

void HAStatusManager::SetOpLogWriterRunning(bool running) {
    std::lock_guard<std::mutex> lock(mutex_);
    oplog_writer_ = {};
    oplog_writer_.running = running;
    status_.oplog_pending_writes = 0;
}

void HAStatusManager::SetOpLogWriteProgress(uint64_t committed_sequence_id,
                                            size_t pending, size_t capacity) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!oplog_writer_.running) return;
    status_.oplog_pending_writes = static_cast<uint64_t>(pending);
    if (pending == 0) {
        oplog_writer_.stalled_since.reset();
    } else if (!oplog_writer_.stalled_since ||
               committed_sequence_id != oplog_writer_.committed_sequence_id) {
        oplog_writer_.stalled_since = std::chrono::steady_clock::now();
    }
    oplog_writer_.committed_sequence_id = committed_sequence_id;
    oplog_writer_.pending = pending;
    oplog_writer_.capacity = capacity;
}

void HAStatusManager::SetOpLogPollSuccess() {
    std::lock_guard<std::mutex> lock(mutex_);
    oplog_poll_.last_success_at = std::chrono::steady_clock::now();
    oplog_poll_.last_success_ms = NowMs();
    status_.oplog_last_successful_poll_timestamp_ms =
        oplog_poll_.last_success_ms;
    TouchLocked();
}

std::string HAStatusManager::OpLogLatestStallMessageLocked() const {
    if (!status_.ha_enabled || !oplog_writer_.stalled_since ||
        oplog_latest_stall_timeout_sec_ == 0) {
        return {};
    }
    const auto elapsed =
        std::chrono::steady_clock::now() - *oplog_writer_.stalled_since;
    if (elapsed < std::chrono::seconds(oplog_latest_stall_timeout_sec_)) {
        return {};
    }
    return "latest=" + std::to_string(oplog_writer_.committed_sequence_id) +
           ", pending=" + std::to_string(oplog_writer_.pending) +
           ", stalled_ms=" +
           std::to_string(
               std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
                   .count());
}

std::string HAStatusManager::OpLogCatchUpStallMessageLocked() const {
    if (!status_.ha_enabled || !oplog_catchup_.stalled_since ||
        oplog_catchup_stall_timeout_sec_ == 0) {
        return {};
    }
    // Require a later progress sample to confirm the stall. An old sample
    // alone cannot distinguish stalled replay from delayed status sampling.
    const auto elapsed =
        oplog_catchup_.last_observed_at - *oplog_catchup_.stalled_since;
    if (elapsed < std::chrono::seconds(oplog_catchup_stall_timeout_sec_)) {
        return {};
    }
    return "oplog_applied_sequence_id=" +
           std::to_string(status_.applied_sequence_id) +
           ", oplog_latest_sequence_id=" +
           std::to_string(status_.latest_sequence_id) + ", stalled_ms=" +
           std::to_string(
               std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
                   .count());
}

std::string HAStatusManager::OpLogLagGrowingMessageLocked() const {
    if (!status_.ha_enabled || status_.role != "standby" ||
        status_.state != "WATCHING" || !status_.oplog_latest_read_ok ||
        !oplog_lag_growth_.since ||
        status_.oplog_lag < oplog_lag_growth_.baseline_lag ||
        status_.oplog_lag - oplog_lag_growth_.baseline_lag <
            kOpLogLagGrowthMinEntries) {
        return {};
    }
    const auto elapsed =
        oplog_lag_growth_.last_observed_at - *oplog_lag_growth_.since;
    if (elapsed < kOpLogLagGrowthDuration) {
        return {};
    }
    return "baseline_lag=" + std::to_string(oplog_lag_growth_.baseline_lag) +
           ", current_lag=" + std::to_string(status_.oplog_lag) + ", growth=" +
           std::to_string(status_.oplog_lag - oplog_lag_growth_.baseline_lag) +
           ", duration_ms=" +
           std::to_string(
               std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
                   .count());
}

std::string HAStatusManager::OpLogQueuePressureMessageLocked() const {
    if (!status_.ha_enabled || status_.role != "primary" ||
        status_.state != "RUNNING" || !oplog_writer_.running ||
        oplog_writer_.capacity == 0 || oplog_writer_.pending == 0) {
        return {};
    }
    const size_t usage_percent =
        oplog_writer_.pending * 100 / oplog_writer_.capacity;
    if (usage_percent < kOpLogQueuePressurePercent) {
        return {};
    }
    return "pending=" + std::to_string(oplog_writer_.pending) +
           ", capacity=" + std::to_string(oplog_writer_.capacity) +
           ", usage_percent=" + std::to_string(usage_percent) +
           ", threshold_percent=" + std::to_string(kOpLogQueuePressurePercent);
}

std::string HAStatusManager::OpLogPollStaleMessageLocked() const {
    if (!status_.ha_enabled || status_.role != "standby" ||
        status_.state != "WATCHING" || oplog_catchup_stall_timeout_sec_ == 0 ||
        !oplog_poll_.last_success_at ||
        doctor_issues_.find("OPLOG_READ_FAILED") != doctor_issues_.end()) {
        return {};
    }
    const auto elapsed =
        std::chrono::steady_clock::now() - *oplog_poll_.last_success_at;
    if (elapsed < std::chrono::seconds(oplog_catchup_stall_timeout_sec_)) {
        return {};
    }
    return "last_successful_poll_ms=" +
           std::to_string(oplog_poll_.last_success_ms) + ", stale_ms=" +
           std::to_string(
               std::chrono::duration_cast<std::chrono::milliseconds>(elapsed)
                   .count()) +
           ", timeout_sec=" + std::to_string(oplog_catchup_stall_timeout_sec_);
}

HADoctorReport HAStatusManager::GetDoctorReport() const {
    HADoctorReport report;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto now_ms = NowMs();
        report.status = MakeDoctorStatus(BuildSnapshotLocked(now_ms), now_ms);
        if (status_.ha_enabled) {
            if (auto issue = ElectionUnstableMessageLocked(); !issue.empty()) {
                report.issues.push_back(
                    MakeDoctorIssue("ELECTION_UNSTABLE", std::move(issue)));
            }
            if (election_readiness_.blocked) {
                const auto& readiness = election_readiness_;
                std::string message;
                if (!readiness.latest_sequence_id.has_value()) {
                    message =
                        "standby latest oplog sequence is unknown; "
                        "waiting before election";
                } else {
                    const auto latest = *readiness.latest_sequence_id;
                    const auto lag =
                        latest > readiness.applied_sequence_id
                            ? latest - readiness.applied_sequence_id
                            : 0;
                    message =
                        "standby oplog lag exceeds election readiness lag "
                        "gate: "
                        "lag=" +
                        std::to_string(lag) +
                        ", max=" + std::to_string(readiness.max_lag_entries);
                }
                message += " (applied_sequence_id=" +
                           std::to_string(readiness.applied_sequence_id) +
                           ", latest_sequence_id=" +
                           (readiness.latest_sequence_id.has_value()
                                ? std::to_string(*readiness.latest_sequence_id)
                                : "unknown") +
                           ")";
                report.issues.push_back(MakeDoctorIssue(
                    "ELECTION_READINESS_BLOCKED", std::move(message)));
            }
            if (auto issue = MasterViewMismatchMessageLocked();
                !issue.empty()) {
                report.issues.push_back(
                    MakeDoctorIssue("MASTER_VIEW_MISMATCH", std::move(issue)));
            }
            if (auto issue = MasterViewLeaseExpiredMessageLocked();
                !issue.empty()) {
                report.issues.push_back(MakeDoctorIssue(
                    "MASTER_VIEW_LEASE_EXPIRED", std::move(issue)));
            }
            if (auto issue = StandbySnapshotInsufficientMessageLocked();
                !issue.empty()) {
                report.issues.push_back(MakeDoctorIssue(
                    "STANDBY_SNAPSHOT_INSUFFICIENT", std::move(issue)));
            }
            if (auto issue = StandbyBehindTrimMessageLocked(); !issue.empty()) {
                report.issues.push_back(
                    MakeDoctorIssue("STANDBY_BEHIND_TRIM", std::move(issue)));
            } else if (auto issue = OpLogTrimRiskMessageLocked();
                       !issue.empty()) {
                report.issues.push_back(
                    MakeDoctorIssue("OPLOG_TRIM_RISK", std::move(issue)));
            }
            for (const auto& [code, message] : doctor_issues_) {
                // The P2P callback supplies the specific reason; Redis only
                // knows that ShouldElect rejected the same attempt.
                if (code == "ELECTION_NOT_READY" &&
                    election_readiness_.blocked) {
                    continue;
                }
                report.issues.push_back(MakeDoctorIssue(code, message));
            }
            if (auto issue = OpLogQueuePressureMessageLocked();
                !issue.empty()) {
                report.issues.push_back(
                    MakeDoctorIssue("OPLOG_QUEUE_PRESSURE", std::move(issue)));
            }
            if (auto stall = OpLogLatestStallMessageLocked(); !stall.empty()) {
                report.issues.push_back(
                    MakeDoctorIssue("OPLOG_LATEST_STALLED", std::move(stall)));
            }
            if (auto stall = OpLogCatchUpStallMessageLocked(); !stall.empty()) {
                report.issues.push_back(
                    MakeDoctorIssue("OPLOG_CATCHUP_STALLED", std::move(stall)));
            }
            if (auto issue = OpLogLagGrowingMessageLocked(); !issue.empty()) {
                report.issues.push_back(
                    MakeDoctorIssue("OPLOG_LAG_GROWING", std::move(issue)));
            }
            if (auto issue = OpLogPollStaleMessageLocked(); !issue.empty()) {
                report.issues.push_back(
                    MakeDoctorIssue("OPLOG_POLL_STALE", std::move(issue)));
            }
            if (status_.state == "FAILED") {
                report.issues.push_back(MakeDoctorIssue(
                    "HA_FAILED", status_.last_error.empty()
                                     ? "HA failed, role=" + status_.role
                                     : status_.last_error));
            }
            if (!report.issues.empty()) {
                report.health = "degraded";
            } else if ((status_.role == "primary" &&
                        status_.state == "RUNNING") ||
                       (status_.role == "standby" &&
                        status_.state == "WATCHING")) {
                // Sequence gaps, lag, and historical last_error are not
                // evidence of a current failure. Transitions stay unknown.
                report.health = "ok";
            }
        }
    }
    return report;
}

std::string HAStatusManager::SerializeDoctorJson() const {
    std::string json;
    struct_json::to_json(GetDoctorReport(), json);
    return json;
}

int64_t HAStatusManager::NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

HAStatusSnapshot HAStatusManager::BuildSnapshotLocked(int64_t now_ms) const {
    auto snapshot = status_;
    snapshot.master_view_endpoint = snapshot.master_view_address;
    snapshot.master_view_updated_at_ms = snapshot.master_view_timestamp_ms;
    if (snapshot.master_view_timestamp_ms > 0) {
        snapshot.master_view_age_ms =
            std::max<int64_t>(0, now_ms - snapshot.master_view_timestamp_ms);
        // The leader renews the master_view key TTL without rewriting the
        // stored election timestamp, so using that timestamp for the
        // remaining lease would drift negative under normal renewals. Use
        // the most recent local refresh (election, observation, or lease
        // renewal) instead: it advances while the leader is healthy and
        // freezes once observation stops, so a negative value signals a
        // stale/expired view rather than normal operation.
        if (snapshot.master_view_ttl_sec > 0 &&
            master_view_refreshed_at_ms_ > 0) {
            const int64_t remaining_ms = master_view_refreshed_at_ms_ +
                                         snapshot.master_view_ttl_sec * 1000 -
                                         now_ms;
            // Expose a lease remaining time, not a signed age. Standbys can
            // retain a stale local view because only the leader observes TTL
            // renewals, so use master_view_age_ms and doctor diagnostics for
            // staleness instead of reporting a negative value.
            snapshot.master_view_expires_in_ms =
                std::max<int64_t>(0, remaining_ms);
        }
    }

    snapshot.oplog_applied_sequence_id = snapshot.applied_sequence_id;
    snapshot.oplog_latest_sequence_id = snapshot.latest_sequence_id;
    snapshot.oplog_trimmed_sequence_id = snapshot.trimmed_sequence_id;
    snapshot.oplog_gc_cursor_sequence_id = snapshot.gc_cursor_sequence_id;
    snapshot.oplog_apply_lag = snapshot.oplog_lag;
    snapshot.oplog_writer_pending_writes = snapshot.oplog_pending_writes;
    snapshot.oplog_writer_queue_capacity =
        static_cast<uint64_t>(oplog_writer_.capacity);
    snapshot.oplog_writer_running = oplog_writer_.running;
    snapshot.oplog_writer_last_committed_sequence_id =
        oplog_writer_.committed_sequence_id;
    snapshot.oplog_last_successful_poll_timestamp_ms =
        oplog_poll_.last_success_ms;
    snapshot.registry_heartbeat_last_success_timestamp_ms =
        snapshot.registry_heartbeat_last_success_ms;
    snapshot.registry_heartbeat_last_failure_timestamp_ms =
        snapshot.registry_heartbeat_last_failure_ms;
    snapshot.status_updated_at_ms = snapshot.updated_at_ms;
    if (oplog_poll_.last_success_ms > 0) {
        snapshot.oplog_poll_success_age_ms =
            std::max<int64_t>(0, now_ms - oplog_poll_.last_success_ms);
    }
    return snapshot;
}

void HAStatusManager::TouchLocked() {
    status_.updated_at_ms = NowMs();
    UpdateMetrics(status_);
}

}  // namespace mooncake
