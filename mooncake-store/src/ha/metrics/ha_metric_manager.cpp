#include "ha/metrics/ha_metric_manager.h"

#include <glog/logging.h>

#include <array>
#include <iomanip>
#include <sstream>

namespace mooncake {

namespace {

std::array<std::string, 2> MakeRuntimePhaseLabels(HARuntimeMode mode,
                                                  HARuntimePhase phase) {
    return {HARuntimeModeToString(mode), HARuntimePhaseToString(phase)};
}

std::array<std::string, 3> MakeRuntimePhaseResultLabels(
    HARuntimeMode mode, HARuntimePhase phase, HARuntimePhaseResult result) {
    return {HARuntimeModeToString(mode), HARuntimePhaseToString(phase),
            HARuntimePhaseResultToString(result)};
}

int64_t ComputeRatePerSecond(int64_t amount, std::chrono::microseconds dur) {
    if (amount <= 0 || dur.count() <= 0) {
        return 0;
    }
    return static_cast<int64_t>(
        (static_cast<long double>(amount) * 1000000.0L) / dur.count());
}

}  // namespace

const char* HARuntimeModeToString(HARuntimeMode mode) {
    switch (mode) {
        case HARuntimeMode::kSnapshotOnly:
            return "snapshot_only";
        case HARuntimeMode::kSnapshotWithOplog:
            return "snapshot_with_oplog";
    }
    return "unknown";
}

const char* HARuntimePhaseToString(HARuntimePhase phase) {
    switch (phase) {
        case HARuntimePhase::kStandbyStart:
            return "standby_start";
        case HARuntimePhase::kSnapshotBootstrap:
            return "snapshot_bootstrap";
        case HARuntimePhase::kSnapshotRefresh:
            return "snapshot_refresh";
        case HARuntimePhase::kOplogFollowingStart:
            return "oplog_following_start";
        case HARuntimePhase::kFinalCatchup:
            return "final_catchup";
        case HARuntimePhase::kStandbyPromote:
            return "standby_promote";
        case HARuntimePhase::kLeaderWarmup:
            return "leader_warmup";
        case HARuntimePhase::kMasterStart:
            return "master_start";
    }
    return "unknown";
}

const char* HARuntimePhaseResultToString(HARuntimePhaseResult result) {
    switch (result) {
        case HARuntimePhaseResult::kSuccess:
            return "success";
        case HARuntimePhaseResult::kFailure:
            return "failure";
        case HARuntimePhaseResult::kEmpty:
            return "empty";
    }
    return "unknown";
}

// --- Singleton Instance ---
HAMetricManager& HAMetricManager::instance() {
    static HAMetricManager static_instance;
    return static_instance;
}

// --- Constructor ---
HAMetricManager::HAMetricManager()
    // OpLog Sequence Gauges
    : oplog_last_sequence_id_("ha_oplog_last_sequence_id",
                              "Latest OpLog sequence ID written by Primary"),
      oplog_applied_sequence_id_("ha_oplog_applied_sequence_id",
                                 "Latest OpLog sequence ID applied by Standby"),
      oplog_standby_lag_("ha_oplog_standby_lag",
                         "Number of OpLog entries Standby is behind Primary"),
      oplog_pending_entries_(
          "ha_oplog_pending_entries",
          "Number of out-of-order entries waiting in OpLogApplier"),
      pending_mutation_queue_size_(
          "ha_pending_mutation_queue_size",
          "Number of mutations pending etcd write retry"),

      // Error Counters
      oplog_skipped_entries_total_(
          "ha_oplog_skipped_entries_total",
          "Total number of OpLog entries skipped due to timeout"),
      oplog_checksum_failures_total_(
          "ha_oplog_checksum_failures_total",
          "Total number of OpLog entries with checksum verification failures"),
      oplog_gap_resolve_attempts_total_(
          "ha_oplog_gap_resolve_attempts_total",
          "Total number of attempts to resolve missing OpLog entries"),
      oplog_gap_resolve_success_total_(
          "ha_oplog_gap_resolve_success_total",
          "Total number of successfully resolved missing OpLog entries"),
      oplog_etcd_write_failures_total_(
          "ha_oplog_etcd_write_failures_total",
          "Total number of failed etcd write operations"),
      oplog_etcd_write_retries_total_(
          "ha_oplog_etcd_write_retries_total",
          "Total number of etcd write retry attempts"),
      oplog_watch_disconnections_total_(
          "ha_oplog_watch_disconnections_total",
          "Total number of OpLog watch disconnections"),
      oplog_applied_entries_total_(
          "ha_oplog_applied_entries_total",
          "Total number of OpLog entries successfully applied"),
      oplog_dropped_put_end_total_("ha_oplog_dropped_put_end_total",
                                   "Total number of dropped PUT_END operations "
                                   "due to late arrival after "
                                   "skip"),
      oplog_batch_commits_total_(
          "ha_oplog_batch_commits_total",
          "Total number of Group Commit batches flushed to etcd"),
      oplog_sync_batch_commits_total_(
          "ha_oplog_sync_batch_commits_total",
          "Total number of sync batches (triggered by DELETE/Sync ops)"),

      // Latency Histograms (buckets in microseconds)
      // 100us, 500us, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s
      oplog_etcd_write_latency_us_(
          "ha_oplog_etcd_write_latency_us",
          "Latency of etcd write operations in microseconds",
          {100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000,
           5000000}),
      oplog_apply_latency_us_(
          "ha_oplog_apply_latency_us",
          "Latency of OpLog entry application in microseconds",
          {10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000}),

      // State Machine
      standby_state_(
          "ha_standby_state",
          "Current state of the Standby service (0=STOPPED, 1=CONNECTING, "
          "2=SYNCING, 3=WATCHING, 4=RECOVERING, 5=RECONNECTING, "
          "6=PROMOTING, 7=PROMOTED, 8=FAILED)"),
      state_transitions_total_(
          "ha_state_transitions_total",
          "Total number of Standby state machine transitions"),

      runtime_phase_runs_total_("ha_runtime_phase_runs_total",
                                "Total number of HA runtime phase executions",
                                {}, {"mode", "phase", "result"}),
      runtime_phase_duration_us_(
          "ha_runtime_phase_duration_us",
          "Duration of HA runtime phases in microseconds",
          {100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000, 5000000,
           30000000, 60000000},
          {}, {"mode", "phase", "result"}),
      runtime_phase_last_duration_us_(
          "ha_runtime_phase_last_duration_us",
          "Last observed HA runtime phase duration in microseconds",
          {"mode", "phase"}),
      runtime_phase_last_key_count_(
          "ha_runtime_phase_last_key_count",
          "Last observed logical key count processed by an HA runtime phase",
          {"mode", "phase"}),
      runtime_phase_last_logical_bytes_(
          "ha_runtime_phase_last_logical_bytes",
          "Last observed logical bytes processed by an HA runtime phase",
          {"mode", "phase"}),
      runtime_phase_last_oplog_entries_(
          "ha_runtime_phase_last_oplog_entries",
          "Last observed OpLog entry count processed by an HA runtime phase",
          {"mode", "phase"}),
      runtime_phase_last_applied_seq_id_(
          "ha_runtime_phase_last_applied_seq_id",
          "Last observed applied sequence watermark for an HA runtime phase",
          {"mode", "phase"}),
      runtime_phase_last_key_rate_per_sec_(
          "ha_runtime_phase_last_key_rate_per_sec",
          "Last observed HA runtime phase logical key throughput per second",
          {"mode", "phase"}),
      runtime_phase_last_byte_rate_per_sec_(
          "ha_runtime_phase_last_byte_rate_per_sec",
          "Last observed HA runtime phase logical byte throughput per second",
          {"mode", "phase"}),
      runtime_phase_last_oplog_entry_rate_per_sec_(
          "ha_runtime_phase_last_oplog_entry_rate_per_sec",
          "Last observed HA runtime phase OpLog entry throughput per second",
          {"mode", "phase"}) {
    // Initialize gauges to 0 for proper Prometheus output
    oplog_last_sequence_id_.update(0);
    oplog_applied_sequence_id_.update(0);
    oplog_standby_lag_.update(0);
    oplog_pending_entries_.update(0);
    pending_mutation_queue_size_.update(0);
    standby_state_.update(0);
}

// ========== OpLog Sequence Metrics (Gauge) ==========

void HAMetricManager::set_oplog_last_sequence_id(int64_t seq_id) {
    oplog_last_sequence_id_.update(seq_id);
}

int64_t HAMetricManager::get_oplog_last_sequence_id() {
    return static_cast<int64_t>(oplog_last_sequence_id_.value());
}

void HAMetricManager::set_oplog_applied_sequence_id(int64_t seq_id) {
    oplog_applied_sequence_id_.update(seq_id);
}

int64_t HAMetricManager::get_oplog_applied_sequence_id() {
    return static_cast<int64_t>(oplog_applied_sequence_id_.value());
}

void HAMetricManager::set_oplog_standby_lag(int64_t lag) {
    oplog_standby_lag_.update(lag);
}

int64_t HAMetricManager::get_oplog_standby_lag() {
    return static_cast<int64_t>(oplog_standby_lag_.value());
}

void HAMetricManager::set_oplog_pending_entries(int64_t count) {
    oplog_pending_entries_.update(count);
}

int64_t HAMetricManager::get_oplog_pending_entries() {
    return static_cast<int64_t>(oplog_pending_entries_.value());
}

void HAMetricManager::set_pending_mutation_queue_size(int64_t size) {
    pending_mutation_queue_size_.update(size);
}

int64_t HAMetricManager::get_pending_mutation_queue_size() {
    return static_cast<int64_t>(pending_mutation_queue_size_.value());
}

// ========== Error Counters ==========

void HAMetricManager::inc_oplog_skipped_entries(int64_t val) {
    oplog_skipped_entries_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_skipped_entries_total() {
    return static_cast<int64_t>(oplog_skipped_entries_total_.value());
}

void HAMetricManager::inc_oplog_checksum_failures(int64_t val) {
    oplog_checksum_failures_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_checksum_failures_total() {
    return static_cast<int64_t>(oplog_checksum_failures_total_.value());
}

void HAMetricManager::inc_oplog_gap_resolve_attempts(int64_t val) {
    oplog_gap_resolve_attempts_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_gap_resolve_attempts_total() {
    return static_cast<int64_t>(oplog_gap_resolve_attempts_total_.value());
}

void HAMetricManager::inc_oplog_gap_resolve_success(int64_t val) {
    oplog_gap_resolve_success_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_gap_resolve_success_total() {
    return static_cast<int64_t>(oplog_gap_resolve_success_total_.value());
}

void HAMetricManager::inc_oplog_etcd_write_failures(int64_t val) {
    oplog_etcd_write_failures_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_etcd_write_failures_total() {
    return static_cast<int64_t>(oplog_etcd_write_failures_total_.value());
}

void HAMetricManager::inc_oplog_etcd_write_retries(int64_t val) {
    oplog_etcd_write_retries_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_etcd_write_retries_total() {
    return static_cast<int64_t>(oplog_etcd_write_retries_total_.value());
}

void HAMetricManager::inc_oplog_watch_disconnections(int64_t val) {
    oplog_watch_disconnections_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_watch_disconnections_total() {
    return static_cast<int64_t>(oplog_watch_disconnections_total_.value());
}

void HAMetricManager::inc_oplog_applied_entries(int64_t val) {
    oplog_applied_entries_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_applied_entries_total() {
    return static_cast<int64_t>(oplog_applied_entries_total_.value());
}

void HAMetricManager::inc_oplog_dropped_put_end(int64_t val) {
    oplog_dropped_put_end_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_dropped_put_end_total() {
    return static_cast<int64_t>(oplog_dropped_put_end_total_.value());
}

void HAMetricManager::inc_oplog_batch_commits(int64_t val) {
    oplog_batch_commits_total_.inc(val);
}

void HAMetricManager::inc_oplog_sync_batch_commits(int64_t val) {
    oplog_sync_batch_commits_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_batch_commits_total() {
    return static_cast<int64_t>(oplog_batch_commits_total_.value());
}

int64_t HAMetricManager::get_oplog_sync_batch_commits_total() {
    return static_cast<int64_t>(oplog_sync_batch_commits_total_.value());
}

// ========== Latency Histograms ==========

void HAMetricManager::observe_oplog_etcd_write_latency_us(int64_t latency_us) {
    oplog_etcd_write_latency_us_.observe(latency_us);
}

void HAMetricManager::observe_oplog_apply_latency_us(int64_t latency_us) {
    oplog_apply_latency_us_.observe(latency_us);
}

// ========== State Machine Metrics ==========

void HAMetricManager::set_standby_state(int64_t state_value) {
    standby_state_.update(state_value);
}

int64_t HAMetricManager::get_standby_state() {
    return static_cast<int64_t>(standby_state_.value());
}

void HAMetricManager::inc_state_transitions(int64_t val) {
    state_transitions_total_.inc(val);
}

int64_t HAMetricManager::get_state_transitions_total() {
    return static_cast<int64_t>(state_transitions_total_.value());
}

void HAMetricManager::ObserveRuntimePhase(HARuntimeMode mode,
                                          HARuntimePhase phase,
                                          HARuntimePhaseResult result,
                                          std::chrono::microseconds duration,
                                          const HARuntimePhaseStats& stats) {
    const auto phase_labels = MakeRuntimePhaseLabels(mode, phase);
    const auto phase_result_labels =
        MakeRuntimePhaseResultLabels(mode, phase, result);
    const auto duration_us = static_cast<int64_t>(duration.count());

    runtime_phase_runs_total_.inc(phase_result_labels);
    runtime_phase_duration_us_.observe(phase_result_labels, duration_us);
    runtime_phase_last_duration_us_.update(phase_labels, duration_us);
    runtime_phase_last_key_count_.update(phase_labels, stats.key_count);
    runtime_phase_last_logical_bytes_.update(phase_labels, stats.logical_bytes);
    runtime_phase_last_oplog_entries_.update(phase_labels, stats.oplog_entries);
    runtime_phase_last_applied_seq_id_.update(phase_labels,
                                              stats.applied_seq_id);
    runtime_phase_last_key_rate_per_sec_.update(
        phase_labels, ComputeRatePerSecond(stats.key_count, duration));
    runtime_phase_last_byte_rate_per_sec_.update(
        phase_labels, ComputeRatePerSecond(stats.logical_bytes, duration));
    runtime_phase_last_oplog_entry_rate_per_sec_.update(
        phase_labels, ComputeRatePerSecond(stats.oplog_entries, duration));

    std::ostringstream oss;
    oss << "mode=" << HARuntimeModeToString(mode)
        << ", phase=" << HARuntimePhaseToString(phase)
        << ", result=" << HARuntimePhaseResultToString(result)
        << ", duration_us=" << duration_us << ", keys=" << stats.key_count
        << ", logical_bytes=" << stats.logical_bytes
        << ", oplog_entries=" << stats.oplog_entries
        << ", applied_seq_id=" << stats.applied_seq_id;
    {
        std::lock_guard<std::mutex> lock(runtime_phase_summary_mutex_);
        last_runtime_phase_summary_ = oss.str();
    }
}

int64_t HAMetricManager::get_runtime_phase_runs_total(
    HARuntimeMode mode, HARuntimePhase phase, HARuntimePhaseResult result) {
    return runtime_phase_runs_total_.value(
        MakeRuntimePhaseResultLabels(mode, phase, result));
}

int64_t HAMetricManager::get_runtime_phase_last_duration_us(
    HARuntimeMode mode, HARuntimePhase phase) {
    return runtime_phase_last_duration_us_.value(
        MakeRuntimePhaseLabels(mode, phase));
}

int64_t HAMetricManager::get_runtime_phase_last_key_count(
    HARuntimeMode mode, HARuntimePhase phase) {
    return runtime_phase_last_key_count_.value(
        MakeRuntimePhaseLabels(mode, phase));
}

int64_t HAMetricManager::get_runtime_phase_last_logical_bytes(
    HARuntimeMode mode, HARuntimePhase phase) {
    return runtime_phase_last_logical_bytes_.value(
        MakeRuntimePhaseLabels(mode, phase));
}

int64_t HAMetricManager::get_runtime_phase_last_oplog_entries(
    HARuntimeMode mode, HARuntimePhase phase) {
    return runtime_phase_last_oplog_entries_.value(
        MakeRuntimePhaseLabels(mode, phase));
}

int64_t HAMetricManager::get_runtime_phase_last_applied_seq_id(
    HARuntimeMode mode, HARuntimePhase phase) {
    return runtime_phase_last_applied_seq_id_.value(
        MakeRuntimePhaseLabels(mode, phase));
}

int64_t HAMetricManager::get_runtime_phase_last_key_rate_per_sec(
    HARuntimeMode mode, HARuntimePhase phase) {
    return runtime_phase_last_key_rate_per_sec_.value(
        MakeRuntimePhaseLabels(mode, phase));
}

int64_t HAMetricManager::get_runtime_phase_last_byte_rate_per_sec(
    HARuntimeMode mode, HARuntimePhase phase) {
    return runtime_phase_last_byte_rate_per_sec_.value(
        MakeRuntimePhaseLabels(mode, phase));
}

int64_t HAMetricManager::get_runtime_phase_last_oplog_entry_rate_per_sec(
    HARuntimeMode mode, HARuntimePhase phase) {
    return runtime_phase_last_oplog_entry_rate_per_sec_.value(
        MakeRuntimePhaseLabels(mode, phase));
}

// ========== Serialization ==========

std::string HAMetricManager::serialize_metrics() {
    std::stringstream ss;

    // Helper lambda to serialize a metric
    auto serialize_metric = [&ss](auto& metric) {
        std::string metric_str;
        metric.serialize(metric_str);
        ss << metric_str;
    };

    // Gauges
    serialize_metric(oplog_last_sequence_id_);
    serialize_metric(oplog_applied_sequence_id_);
    serialize_metric(oplog_standby_lag_);
    serialize_metric(oplog_pending_entries_);
    serialize_metric(pending_mutation_queue_size_);
    serialize_metric(standby_state_);

    // Counters
    serialize_metric(oplog_skipped_entries_total_);
    serialize_metric(oplog_checksum_failures_total_);
    serialize_metric(oplog_gap_resolve_attempts_total_);
    serialize_metric(oplog_gap_resolve_success_total_);
    serialize_metric(oplog_etcd_write_failures_total_);
    serialize_metric(oplog_etcd_write_retries_total_);
    serialize_metric(oplog_watch_disconnections_total_);
    serialize_metric(oplog_applied_entries_total_);
    serialize_metric(state_transitions_total_);

    // Histograms
    serialize_metric(oplog_etcd_write_latency_us_);
    serialize_metric(oplog_apply_latency_us_);
    serialize_metric(runtime_phase_runs_total_);
    serialize_metric(runtime_phase_duration_us_);
    serialize_metric(runtime_phase_last_duration_us_);
    serialize_metric(runtime_phase_last_key_count_);
    serialize_metric(runtime_phase_last_logical_bytes_);
    serialize_metric(runtime_phase_last_oplog_entries_);
    serialize_metric(runtime_phase_last_applied_seq_id_);
    serialize_metric(runtime_phase_last_key_rate_per_sec_);
    serialize_metric(runtime_phase_last_byte_rate_per_sec_);
    serialize_metric(runtime_phase_last_oplog_entry_rate_per_sec_);

    return ss.str();
}

std::string HAMetricManager::get_summary_string() {
    std::stringstream ss;
    ss << "HA Metrics Summary: ";
    ss << "last_seq=" << get_oplog_last_sequence_id();
    ss << ", applied_seq=" << get_oplog_applied_sequence_id();
    ss << ", lag=" << get_oplog_standby_lag();
    ss << ", pending=" << get_oplog_pending_entries();
    ss << ", mutation_queue=" << get_pending_mutation_queue_size();
    ss << ", batch_commits=" << get_oplog_batch_commits_total();
    ss << ", sync_commits=" << get_oplog_sync_batch_commits_total();
    ss << ", skipped=" << get_oplog_skipped_entries_total();
    ss << ", checksum_fail=" << get_oplog_checksum_failures_total();
    ss << ", etcd_fail=" << get_oplog_etcd_write_failures_total();
    ss << ", watch_disconn=" << get_oplog_watch_disconnections_total();
    ss << ", state=" << get_standby_state();
    {
        std::lock_guard<std::mutex> lock(runtime_phase_summary_mutex_);
        ss << ", last_runtime_phase={" << last_runtime_phase_summary_ << "}";
    }
    return ss.str();
}

}  // namespace mooncake
