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

}  // namespace

HAStatusManager& HAStatusManager::instance() {
    static HAStatusManager manager;
    return manager;
}

HAStatusManager::HAStatusManager() { TouchLocked(); }

void HAStatusManager::Reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    status_ = HAStatusSnapshot{};
    election_ = {};
    doctor_issues_.clear();
    oplog_writer_ = {};
    oplog_catchup_ = {};
    oplog_poll_ = {};
    status_.oplog_pending_writes = 0;
    oplog_latest_stall_timeout_sec_ = 10;
    oplog_catchup_stall_timeout_sec_ = 10;
    TouchLocked();
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
        oplog_poll_ = {};
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
        oplog_poll_ = {};
    }
    if (changed) {
        old_role = status_.role;
        old_state = status_.state;
        new_role = role;
        new_state = state;
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
    TouchLocked();
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

void HAStatusManager::SetSnapshotReady(bool snapshot_ready) {
    std::lock_guard<std::mutex> lock(mutex_);
    status_.snapshot_ready = snapshot_ready;
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
    } else {
        oplog_catchup_ = {};
    }
    status_.applied_sequence_id = applied_sequence_id;
    status_.trimmed_sequence_id = trimmed_sequence_id;
    status_.gc_cursor_sequence_id = gc_cursor_sequence_id;
    status_.oplog_lag = status_.latest_sequence_id > applied_sequence_id
                            ? status_.latest_sequence_id - applied_sequence_id
                            : 0;
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

void HAStatusManager::ClearDoctorIssues() {
    std::lock_guard<std::mutex> lock(mutex_);
    doctor_issues_.clear();
    oplog_writer_ = {};
    oplog_catchup_ = {};
    oplog_poll_ = {};
    status_.oplog_pending_writes = 0;
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
    return "applied=" + std::to_string(status_.applied_sequence_id) +
           ", latest=" + std::to_string(status_.latest_sequence_id) +
           ", stalled_ms=" +
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

std::string HAStatusManager::SerializeDoctorJson() const {
    HADoctorReport report;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (status_.ha_enabled) {
            if (auto issue = ElectionUnstableMessageLocked(); !issue.empty()) {
                report.issues.push_back(
                    {"ELECTION_UNSTABLE", std::move(issue)});
            }
            if (auto issue = MasterViewMismatchMessageLocked();
                !issue.empty()) {
                report.issues.push_back(
                    {"MASTER_VIEW_MISMATCH", std::move(issue)});
            }
            for (const auto& [code, message] : doctor_issues_) {
                report.issues.push_back({code, message});
            }
            if (auto issue = OpLogQueuePressureMessageLocked();
                !issue.empty()) {
                report.issues.push_back(
                    {"OPLOG_QUEUE_PRESSURE", std::move(issue)});
            }
            if (auto stall = OpLogLatestStallMessageLocked(); !stall.empty()) {
                report.issues.push_back(
                    {"OPLOG_LATEST_STALLED", std::move(stall)});
            }
            if (auto stall = OpLogCatchUpStallMessageLocked(); !stall.empty()) {
                report.issues.push_back(
                    {"OPLOG_CATCHUP_STALLED", std::move(stall)});
            }
            if (auto issue = OpLogPollStaleMessageLocked(); !issue.empty()) {
                report.issues.push_back({"OPLOG_POLL_STALE", std::move(issue)});
            }
            if (status_.state == "FAILED") {
                report.issues.push_back(
                    {"HA_FAILED", status_.last_error.empty()
                                      ? "HA failed, role=" + status_.role
                                      : status_.last_error});
            }
            if (!report.issues.empty()) {
                report.status = "degraded";
            } else if ((status_.role == "primary" &&
                        status_.state == "RUNNING") ||
                       (status_.role == "standby" &&
                        status_.state == "WATCHING")) {
                // Sequence gaps, lag, and historical last_error are not
                // evidence of a current failure. Transitions stay unknown.
                report.status = "ok";
            }
        }
    }
    std::string json;
    struct_json::to_json(report, json);
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
        if (snapshot.master_view_ttl_sec > 0) {
            snapshot.master_view_expires_in_ms =
                snapshot.master_view_timestamp_ms +
                snapshot.master_view_ttl_sec * 1000 - now_ms;
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
