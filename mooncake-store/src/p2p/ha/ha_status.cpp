#include "p2p/ha/ha_status.h"

#include <chrono>
#include <utility>

#include <glog/logging.h>
#include <ylt/struct_json/json_writer.h>

#include "p2p/ha/ha_metric_manager.h"

namespace mooncake {

namespace {

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
    doctor_issues_.clear();
    oplog_writer_ = {};
    oplog_catchup_ = {};
    oplog_latest_stall_timeout_sec_ = 10;
    oplog_catchup_stall_timeout_sec_ = 10;
    TouchLocked();
}

void HAStatusManager::Configure(bool ha_enabled, std::string cluster_id,
                                std::string local_endpoint,
                                uint32_t oplog_latest_stall_timeout_sec,
                                uint32_t oplog_catchup_stall_timeout_sec) {
    std::lock_guard<std::mutex> lock(mutex_);
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
    status_.leader_endpoint = address;
    status_.epoch = epoch;
    status_.master_view_address = std::move(address);
    status_.master_view_epoch = epoch;
    status_.master_view_timestamp_ms = timestamp_ms;
    status_.master_view_ttl_sec = ttl_sec;
    status_.master_view_instance_id = std::move(instance_id);
    TouchLocked();
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
    return status_;
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
}

void HAStatusManager::SetOpLogWriterRunning(bool running) {
    std::lock_guard<std::mutex> lock(mutex_);
    oplog_writer_ = {};
    oplog_writer_.running = running;
}

void HAStatusManager::SetOpLogWriteProgress(uint64_t committed_sequence_id,
                                            size_t pending) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!oplog_writer_.running) return;
    if (pending == 0) {
        oplog_writer_.stalled_since.reset();
    } else if (!oplog_writer_.stalled_since ||
               committed_sequence_id != oplog_writer_.committed_sequence_id) {
        oplog_writer_.stalled_since = std::chrono::steady_clock::now();
    }
    oplog_writer_.committed_sequence_id = committed_sequence_id;
    oplog_writer_.pending = pending;
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

std::string HAStatusManager::SerializeDoctorJson() const {
    HADoctorReport report;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (status_.ha_enabled) {
            for (const auto& [code, message] : doctor_issues_) {
                report.issues.push_back({code, message});
            }
            if (auto stall = OpLogLatestStallMessageLocked(); !stall.empty()) {
                report.issues.push_back(
                    {"OPLOG_LATEST_STALLED", std::move(stall)});
            }
            if (auto stall = OpLogCatchUpStallMessageLocked(); !stall.empty()) {
                report.issues.push_back(
                    {"OPLOG_CATCHUP_STALLED", std::move(stall)});
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

void HAStatusManager::TouchLocked() {
    status_.updated_at_ms = NowMs();
    UpdateMetrics(status_);
}

}  // namespace mooncake
