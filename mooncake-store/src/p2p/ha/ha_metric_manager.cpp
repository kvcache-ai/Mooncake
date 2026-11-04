#include "p2p/ha/ha_metric_manager.h"

#include "p2p/ha/ha_status.h"

#include <glog/logging.h>

#include <initializer_list>
#include <iomanip>
#include <sstream>

namespace mooncake {

namespace {

std::string EscapeLabelValue(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (char c : value) {
        if (c == '\\' || c == '"') {
            escaped += '\\';
            escaped += c;
        } else if (c == '\n') {
            escaped += "\\n";
        } else {
            escaped += c;
        }
    }
    return escaped;
}

}  // namespace

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
      oplog_applied_sequence_id_(
          "ha_oplog_applied_sequence_id",
          "Local OpLog metadata apply watermark (same as /ha/status)"),
      oplog_standby_lag_("ha_oplog_standby_lag",
                         "Number of OpLog entries Standby is behind Primary"),
      oplog_pending_entries_(
          "ha_oplog_pending_entries",
          "Number of out-of-order entries waiting in OpLogApplier"),
      oplog_async_queue_size_(
          "ha_oplog_async_queue_size",
          "Number of OpLog writes pending asynchronous persistence"),
      election_is_leader_("ha_election_is_leader",
                          "Whether this node currently owns leadership"),
      election_last_successful_renew_timestamp_ms_(
          "ha_election_last_successful_renew_timestamp_ms",
          "Unix timestamp of the latest successful Redis leadership renewal"),
      oplog_async_workers_running_(
          "ha_oplog_async_workers_running",
          "Whether Primary OpLog asynchronous workers are running"),
      standby_degraded_("ha_standby_degraded",
                        "Whether Standby has a critical apply failure"),
      primary_degraded_("ha_primary_degraded",
                        "Whether promoted Primary restored partial metadata"),
      oplog_last_successful_poll_timestamp_ms_(
          "ha_oplog_last_successful_poll_timestamp_ms",
          "Unix timestamp of the latest successful Standby OpLog poll"),
      oplog_last_successful_write_timestamp_ms_(
          "ha_oplog_last_successful_write_timestamp_ms",
          "Unix timestamp of the latest successful Primary OpLog write"),
      p2p_snapshot_bootstrap_baseline_sequence_id_(
          "ha_p2p_snapshot_bootstrap_baseline_sequence_id",
          "Latest baseline sequence ID restored by P2P snapshot bootstrap"),
      p2p_bootstrap_catchup_target_sequence_id_(
          "ha_p2p_bootstrap_catchup_target_sequence_id",
          "Latest OpLog target sequence ID used after P2P snapshot bootstrap"),
      master_role_("ha_master_role",
                   "Current HA master role (0=unknown, 1=disabled, 2=starting, "
                   "3=standby, 4=promoting, 5=primary, 6=failed)"),
      snapshot_ready_("ha_snapshot_ready",
                      "Whether this master can serve HA snapshot requests"),
      registry_heartbeat_ok_(
          "ha_registry_heartbeat_ok",
          "Whether the latest Redis Master registry heartbeat succeeded"),
      status_updated_timestamp_ms_(
          "ha_status_updated_timestamp_ms",
          "Unix timestamp of the latest HA status update"),
      oplog_consecutive_write_failures_(
          "ha_oplog_consecutive_write_failures",
          "Current number of consecutive Primary OpLog write failures"),
      oplog_snapshot_ready_standby_count_(
          "ha_oplog_snapshot_ready_standby_count",
          "Number of alive standby instances eligible for snapshot trim"),

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
      oplog_write_failures_total_(
          "ha_oplog_write_failures_total",
          "Total number of failed OpLog persistence operations"),
      oplog_write_retries_total_("ha_oplog_write_retries_total",
                                 "Total number of OpLog write retry attempts"),
      oplog_redis_pipeline_batch_failures_total_(
          "ha_oplog_redis_pipeline_batch_failures_total",
          "Redis OpLog SET pipeline attempts with at least one failed entry"),
      oplog_redis_pipeline_batch_retries_total_(
          "ha_oplog_redis_pipeline_batch_retries_total",
          "Redis OpLog SET pipeline attempts with entries requeued for retry"),
      election_attempts_total_("ha_election_attempts_total",
                               "Total number of election attempts"),
      election_failures_total_("ha_election_failures_total",
                               "Total number of election operation failures"),
      election_leadership_lost_total_(
          "ha_election_leadership_lost_total",
          "Total number of times this node lost leadership"),
      election_renew_failures_total_(
          "ha_election_renew_failures_total",
          "Total number of failed Redis leadership renewal attempts"),
      election_reconnects_total_("ha_election_reconnects_total",
                                 "Total successful election Redis reconnects"),
      election_watch_failures_total_("ha_election_watch_failures_total",
                                     "Total number of election watch failures"),
      election_polling_fallbacks_total_(
          "ha_election_polling_fallbacks_total",
          "Total number of election watch polling fallbacks"),
      oplog_best_effort_dropped_total_(
          "ha_oplog_best_effort_dropped_total",
          "Total number of best-effort OpLog writes dropped"),
      oplog_queue_rejected_total_(
          "ha_oplog_queue_rejected_total",
          "Total number of OpLog writes rejected on queue overflow"),
      oplog_queue_bypassed_total_(
          "ha_oplog_queue_bypassed_total",
          "Total number of OpLog writes bypassed on queue overflow"),
      oplog_sync_wait_timeouts_total_(
          "ha_oplog_sync_wait_timeouts_total",
          "Total number of synchronous OpLog wait timeouts"),
      oplog_read_failures_total_("ha_oplog_read_failures_total",
                                 "Total number of Standby OpLog read failures"),
      oplog_reader_reconnect_attempts_total_(
          "ha_oplog_reader_reconnect_attempts_total",
          "Total Standby OpLog reader reconnect attempts"),
      oplog_reader_reconnect_failures_total_(
          "ha_oplog_reader_reconnect_failures_total",
          "Total failed Standby OpLog reader reconnect attempts"),
      oplog_reader_reconnects_total_(
          "ha_oplog_reader_reconnects_total",
          "Total successful Standby OpLog reader reconnects"),
      oplog_apply_failures_total_(
          "ha_oplog_apply_failures_total",
          "Total number of critical Standby OpLog apply failures"),
      oplog_best_effort_apply_skipped_total_(
          "ha_oplog_best_effort_apply_skipped_total",
          "Total number of failed best-effort entries skipped by Standby"),
      oplog_confirmed_holes_total_(
          "ha_oplog_confirmed_holes_total",
          "Total missing OpLog ID observations in scanned ranges; rereads "
          "may count the same ID again"),
      force_promotions_total_("ha_force_promotions_total",
                              "Total number of forced P2P promotions"),
      promotion_catchup_incomplete_total_(
          "ha_promotion_catchup_incomplete_total",
          "Total promotions whose final OpLog catch-up was incomplete"),
      promotion_restore_failures_total_(
          "ha_promotion_restore_failures_total",
          "Total number of P2P promotion restore failures"),
      promotion_skipped_replicas_total_(
          "ha_promotion_skipped_replicas_total",
          "Total replicas skipped during P2P promotion restore"),
      promotion_skipped_objects_total_(
          "ha_promotion_skipped_objects_total",
          "Total objects left without replicas during promotion restore"),
      p2p_snapshot_bootstrap_success_total_(
          "ha_p2p_snapshot_bootstrap_success_total",
          "Total successful P2P snapshot bootstrap attempts"),
      p2p_snapshot_bootstrap_failures_total_(
          "ha_p2p_snapshot_bootstrap_failures_total",
          "Total failed P2P snapshot bootstrap attempts"),
      p2p_snapshot_resync_success_total_(
          "ha_p2p_snapshot_resync_success_total",
          "Total successful P2P snapshot resync attempts"),
      p2p_snapshot_resync_failures_total_(
          "ha_p2p_snapshot_resync_failures_total",
          "Total failed P2P snapshot resync attempts"),
      oplog_watch_disconnections_total_(
          "ha_oplog_watch_disconnections_total",
          "Total number of OpLog watch disconnections"),
      oplog_applied_entries_total_(
          "ha_oplog_applied_entries_total",
          "Total number of OpLog entries successfully applied"),
      oplog_applied_noop_entries_total_(
          "ha_oplog_applied_noop_entries_total",
          "Total number of NOOP OpLog entries processed by the applier"),
      oplog_dropped_put_end_total_("ha_oplog_dropped_put_end_total",
                                   "Total number of dropped PUT_END operations "
                                   "due to late arrival after "
                                   "skip"),
      oplog_batch_commits_total_(
          "ha_oplog_batch_commits_total",
          "Total number of Group Commit batches persisted"),
      oplog_sync_batch_commits_total_(
          "ha_oplog_sync_batch_commits_total",
          "Total number of sync batches (triggered by DELETE/Sync ops)"),

      // Latency Histograms (buckets in microseconds)
      // 100us, 500us, 1ms, 5ms, 10ms, 50ms, 100ms, 500ms, 1s, 5s
      oplog_write_latency_us_(
          "ha_oplog_write_latency_us",
          "Latency of OpLog persistence operations in microseconds",
          {100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000,
           5000000}),
      oplog_redis_pipeline_batch_size_(
          "ha_oplog_redis_pipeline_batch_size",
          "Number of entries selected for each Redis OpLog SET pipeline "
          "attempt",
          {1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024}),
      election_duration_ms_(
          "ha_election_duration_ms",
          "Time spent waiting to acquire leadership in milliseconds",
          {10, 100, 500, 1000, 5000, 10000, 30000, 60000}),
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
          "Total number of Standby state machine transitions") {
    // Initialize gauges to 0 for proper Prometheus output
    oplog_last_sequence_id_.update(0);
    oplog_applied_sequence_id_.update(0);
    oplog_standby_lag_.update(0);
    oplog_pending_entries_.update(0);
    oplog_async_queue_size_.update(0);
    election_is_leader_.update(0);
    election_last_successful_renew_timestamp_ms_.update(0);
    oplog_async_workers_running_.update(0);
    standby_degraded_.update(0);
    primary_degraded_.update(0);
    oplog_last_successful_poll_timestamp_ms_.update(0);
    oplog_last_successful_write_timestamp_ms_.update(0);
    p2p_snapshot_bootstrap_baseline_sequence_id_.update(0);
    p2p_bootstrap_catchup_target_sequence_id_.update(0);
    master_role_.update(0);
    snapshot_ready_.update(0);
    registry_heartbeat_ok_.update(0);
    status_updated_timestamp_ms_.update(0);
    oplog_consecutive_write_failures_.update(0);
    oplog_snapshot_ready_standby_count_.update(0);
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

void HAMetricManager::set_oplog_async_queue_size(int64_t size) {
    oplog_async_queue_size_.update(size);
}

int64_t HAMetricManager::get_oplog_async_queue_size() {
    return static_cast<int64_t>(oplog_async_queue_size_.value());
}

void HAMetricManager::set_election_is_leader(bool value) {
    election_is_leader_.update(value ? 1 : 0);
}

void HAMetricManager::set_election_last_successful_renew_timestamp_ms(
    int64_t timestamp_ms) {
    election_last_successful_renew_timestamp_ms_.update(timestamp_ms);
}

int64_t HAMetricManager::get_election_last_successful_renew_timestamp_ms() {
    return static_cast<int64_t>(
        election_last_successful_renew_timestamp_ms_.value());
}

void HAMetricManager::set_oplog_async_workers_running(bool value) {
    oplog_async_workers_running_.update(value ? 1 : 0);
}

void HAMetricManager::set_standby_degraded(bool value) {
    standby_degraded_.update(value ? 1 : 0);
}

void HAMetricManager::set_primary_degraded(bool value) {
    primary_degraded_.update(value ? 1 : 0);
}

void HAMetricManager::set_oplog_last_successful_poll_timestamp_ms(
    int64_t timestamp_ms) {
    oplog_last_successful_poll_timestamp_ms_.update(timestamp_ms);
}

void HAMetricManager::set_oplog_last_successful_write_timestamp_ms(
    int64_t timestamp_ms) {
    oplog_last_successful_write_timestamp_ms_.update(timestamp_ms);
}

int64_t HAMetricManager::get_oplog_last_successful_write_timestamp_ms() {
    return static_cast<int64_t>(
        oplog_last_successful_write_timestamp_ms_.value());
}

void HAMetricManager::set_p2p_snapshot_bootstrap_baseline_sequence_id(
    int64_t seq_id) {
    p2p_snapshot_bootstrap_baseline_sequence_id_.update(seq_id);
}

void HAMetricManager::set_p2p_bootstrap_catchup_target_sequence_id(
    int64_t seq_id) {
    p2p_bootstrap_catchup_target_sequence_id_.update(seq_id);
}

void HAMetricManager::set_master_role(int64_t role) {
    master_role_.update(role);
}

int64_t HAMetricManager::get_master_role() {
    return static_cast<int64_t>(master_role_.value());
}

void HAMetricManager::set_snapshot_ready(bool value) {
    snapshot_ready_.update(value ? 1 : 0);
}

int64_t HAMetricManager::get_snapshot_ready() {
    return static_cast<int64_t>(snapshot_ready_.value());
}

void HAMetricManager::set_registry_heartbeat_ok(bool value) {
    registry_heartbeat_ok_.update(value ? 1 : 0);
}

int64_t HAMetricManager::get_registry_heartbeat_ok() {
    return static_cast<int64_t>(registry_heartbeat_ok_.value());
}

void HAMetricManager::set_status_updated_timestamp_ms(int64_t timestamp_ms) {
    status_updated_timestamp_ms_.update(timestamp_ms);
}

int64_t HAMetricManager::get_status_updated_timestamp_ms() {
    return static_cast<int64_t>(status_updated_timestamp_ms_.value());
}

void HAMetricManager::set_oplog_consecutive_write_failures(int64_t count) {
    oplog_consecutive_write_failures_.update(count);
}

int64_t HAMetricManager::get_oplog_consecutive_write_failures() {
    return static_cast<int64_t>(oplog_consecutive_write_failures_.value());
}

void HAMetricManager::set_oplog_snapshot_ready_standby_count(int64_t count) {
    oplog_snapshot_ready_standby_count_.update(count);
}

int64_t HAMetricManager::get_oplog_snapshot_ready_standby_count() {
    return static_cast<int64_t>(oplog_snapshot_ready_standby_count_.value());
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

void HAMetricManager::inc_oplog_write_failures(int64_t val) {
    oplog_write_failures_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_write_failures_total() {
    return static_cast<int64_t>(oplog_write_failures_total_.value());
}

void HAMetricManager::inc_oplog_write_retries(int64_t val) {
    oplog_write_retries_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_write_retries_total() {
    return static_cast<int64_t>(oplog_write_retries_total_.value());
}

void HAMetricManager::inc_oplog_redis_pipeline_batch_failures(int64_t val) {
    oplog_redis_pipeline_batch_failures_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_redis_pipeline_batch_failures_total() {
    return static_cast<int64_t>(
        oplog_redis_pipeline_batch_failures_total_.value());
}

void HAMetricManager::inc_oplog_redis_pipeline_batch_retries(int64_t val) {
    oplog_redis_pipeline_batch_retries_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_redis_pipeline_batch_retries_total() {
    return static_cast<int64_t>(
        oplog_redis_pipeline_batch_retries_total_.value());
}

void HAMetricManager::inc_election_attempts(int64_t val) {
    election_attempts_total_.inc(val);
}
void HAMetricManager::inc_election_failures(int64_t val) {
    election_failures_total_.inc(val);
}
void HAMetricManager::inc_election_leadership_lost(int64_t val) {
    election_leadership_lost_total_.inc(val);
}
void HAMetricManager::inc_election_renew_failures(int64_t val) {
    election_renew_failures_total_.inc(val);
}
int64_t HAMetricManager::get_election_renew_failures_total() {
    return static_cast<int64_t>(election_renew_failures_total_.value());
}
void HAMetricManager::inc_election_reconnects(int64_t val) {
    election_reconnects_total_.inc(val);
}
void HAMetricManager::inc_election_watch_failures(int64_t val) {
    election_watch_failures_total_.inc(val);
}
void HAMetricManager::inc_election_polling_fallbacks(int64_t val) {
    election_polling_fallbacks_total_.inc(val);
}
void HAMetricManager::inc_oplog_best_effort_dropped(int64_t val) {
    oplog_best_effort_dropped_total_.inc(val);
}
void HAMetricManager::inc_oplog_queue_rejected(int64_t val) {
    oplog_queue_rejected_total_.inc(val);
}
void HAMetricManager::inc_oplog_queue_bypassed(int64_t val) {
    oplog_queue_bypassed_total_.inc(val);
}
void HAMetricManager::inc_oplog_sync_wait_timeouts(int64_t val) {
    oplog_sync_wait_timeouts_total_.inc(val);
}
void HAMetricManager::inc_oplog_read_failures(int64_t val) {
    oplog_read_failures_total_.inc(val);
}
void HAMetricManager::inc_oplog_reader_reconnect_attempts(int64_t val) {
    oplog_reader_reconnect_attempts_total_.inc(val);
}
void HAMetricManager::inc_oplog_reader_reconnect_failures(int64_t val) {
    oplog_reader_reconnect_failures_total_.inc(val);
}
void HAMetricManager::inc_oplog_reader_reconnects(int64_t val) {
    oplog_reader_reconnects_total_.inc(val);
}
void HAMetricManager::inc_oplog_apply_failures(int64_t val) {
    oplog_apply_failures_total_.inc(val);
}
void HAMetricManager::inc_oplog_best_effort_apply_skipped(int64_t val) {
    oplog_best_effort_apply_skipped_total_.inc(val);
}
void HAMetricManager::inc_oplog_confirmed_holes(int64_t val) {
    oplog_confirmed_holes_total_.inc(val);
}
void HAMetricManager::inc_force_promotions(int64_t val) {
    force_promotions_total_.inc(val);
}
void HAMetricManager::inc_promotion_catchup_incomplete(int64_t val) {
    promotion_catchup_incomplete_total_.inc(val);
}
void HAMetricManager::inc_promotion_restore_failures(int64_t val) {
    promotion_restore_failures_total_.inc(val);
}
void HAMetricManager::inc_promotion_skipped_replicas(int64_t val) {
    promotion_skipped_replicas_total_.inc(val);
}
void HAMetricManager::inc_promotion_skipped_objects(int64_t val) {
    promotion_skipped_objects_total_.inc(val);
}

void HAMetricManager::inc_p2p_snapshot_bootstrap_success(int64_t val) {
    p2p_snapshot_bootstrap_success_total_.inc(val);
}

void HAMetricManager::inc_p2p_snapshot_bootstrap_failures(int64_t val) {
    p2p_snapshot_bootstrap_failures_total_.inc(val);
}

void HAMetricManager::inc_p2p_snapshot_resync_success(int64_t val) {
    p2p_snapshot_resync_success_total_.inc(val);
}

void HAMetricManager::inc_p2p_snapshot_resync_failures(int64_t val) {
    p2p_snapshot_resync_failures_total_.inc(val);
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

void HAMetricManager::inc_oplog_applied_noop_entries(int64_t val) {
    oplog_applied_noop_entries_total_.inc(val);
}

int64_t HAMetricManager::get_oplog_applied_noop_entries_total() {
    return static_cast<int64_t>(oplog_applied_noop_entries_total_.value());
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

void HAMetricManager::observe_oplog_write_latency_us(int64_t latency_us) {
    oplog_write_latency_us_.observe(latency_us);
}

void HAMetricManager::observe_oplog_redis_pipeline_batch_size(
    int64_t batch_size) {
    oplog_redis_pipeline_batch_size_.observe(batch_size);
}

void HAMetricManager::observe_election_duration_ms(int64_t duration_ms) {
    election_duration_ms_.observe(duration_ms);
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
    serialize_metric(oplog_async_queue_size_);
    serialize_metric(election_is_leader_);
    serialize_metric(election_last_successful_renew_timestamp_ms_);
    serialize_metric(oplog_async_workers_running_);
    serialize_metric(standby_degraded_);
    serialize_metric(primary_degraded_);
    serialize_metric(oplog_last_successful_poll_timestamp_ms_);
    serialize_metric(oplog_last_successful_write_timestamp_ms_);
    serialize_metric(p2p_snapshot_bootstrap_baseline_sequence_id_);
    serialize_metric(p2p_bootstrap_catchup_target_sequence_id_);
    serialize_metric(master_role_);
    serialize_metric(snapshot_ready_);
    serialize_metric(registry_heartbeat_ok_);
    serialize_metric(status_updated_timestamp_ms_);
    serialize_metric(oplog_consecutive_write_failures_);
    serialize_metric(oplog_snapshot_ready_standby_count_);
    serialize_metric(standby_state_);

    // Counters
    serialize_metric(oplog_skipped_entries_total_);
    serialize_metric(oplog_checksum_failures_total_);
    serialize_metric(oplog_gap_resolve_attempts_total_);
    serialize_metric(oplog_gap_resolve_success_total_);
    serialize_metric(oplog_write_failures_total_);
    serialize_metric(oplog_write_retries_total_);
    serialize_metric(oplog_redis_pipeline_batch_failures_total_);
    serialize_metric(oplog_redis_pipeline_batch_retries_total_);
    serialize_metric(election_attempts_total_);
    serialize_metric(election_failures_total_);
    serialize_metric(election_leadership_lost_total_);
    serialize_metric(election_renew_failures_total_);
    serialize_metric(election_reconnects_total_);
    serialize_metric(election_watch_failures_total_);
    serialize_metric(election_polling_fallbacks_total_);
    serialize_metric(oplog_best_effort_dropped_total_);
    serialize_metric(oplog_queue_rejected_total_);
    serialize_metric(oplog_queue_bypassed_total_);
    serialize_metric(oplog_sync_wait_timeouts_total_);
    serialize_metric(oplog_read_failures_total_);
    serialize_metric(oplog_reader_reconnect_attempts_total_);
    serialize_metric(oplog_reader_reconnect_failures_total_);
    serialize_metric(oplog_reader_reconnects_total_);
    serialize_metric(oplog_apply_failures_total_);
    serialize_metric(oplog_best_effort_apply_skipped_total_);
    serialize_metric(oplog_confirmed_holes_total_);
    serialize_metric(force_promotions_total_);
    serialize_metric(promotion_catchup_incomplete_total_);
    serialize_metric(promotion_restore_failures_total_);
    serialize_metric(promotion_skipped_replicas_total_);
    serialize_metric(promotion_skipped_objects_total_);
    serialize_metric(p2p_snapshot_bootstrap_success_total_);
    serialize_metric(p2p_snapshot_bootstrap_failures_total_);
    serialize_metric(p2p_snapshot_resync_success_total_);
    serialize_metric(p2p_snapshot_resync_failures_total_);
    serialize_metric(oplog_watch_disconnections_total_);
    serialize_metric(oplog_applied_entries_total_);
    serialize_metric(oplog_applied_noop_entries_total_);
    serialize_metric(oplog_dropped_put_end_total_);
    serialize_metric(oplog_batch_commits_total_);
    serialize_metric(oplog_sync_batch_commits_total_);
    serialize_metric(state_transitions_total_);

    // Histograms
    serialize_metric(oplog_write_latency_us_);
    serialize_metric(oplog_redis_pipeline_batch_size_);
    serialize_metric(election_duration_ms_);
    serialize_metric(oplog_apply_latency_us_);

    // Doctor includes time-derived diagnoses, so recompute on every scrape.
    const auto report = HAStatusManager::instance().GetDoctorReport();
    ss << "# HELP ha_doctor_health Current local HA doctor health (one-hot).\n"
          "# TYPE ha_doctor_health gauge\n";
    for (const char* health : {"ok", "degraded", "unknown"}) {
        ss << "ha_doctor_health{status=\"" << health << "\"} "
           << (report.health == health ? 1 : 0) << '\n';
    }
    ss << "# HELP ha_doctor_issues Number of active local HA doctor issues.\n"
          "# TYPE ha_doctor_issues gauge\n"
          "ha_doctor_issues "
       << report.issues.size() << '\n';
    ss << "# HELP ha_doctor_issue_active Active local HA doctor issue.\n"
          "# TYPE ha_doctor_issue_active gauge\n";
    for (const auto& issue : report.issues) {
        ss << "ha_doctor_issue_active{code=\"" << EscapeLabelValue(issue.code)
           << "\",category=\"" << EscapeLabelValue(issue.category)
           << "\",component=\"" << EscapeLabelValue(issue.component)
           << "\",severity=\"" << EscapeLabelValue(issue.severity) << "\"} 1\n";
    }

    return ss.str();
}

std::string HAMetricManager::get_summary_string() {
    std::stringstream ss;
    ss << "HA Metrics Summary: ";
    ss << "last_seq=" << get_oplog_last_sequence_id();
    ss << ", applied_seq=" << get_oplog_applied_sequence_id();
    ss << ", lag=" << get_oplog_standby_lag();
    ss << ", pending=" << get_oplog_pending_entries();
    ss << ", async_queue=" << get_oplog_async_queue_size();
    ss << ", consecutive_write_fail=" << get_oplog_consecutive_write_failures();
    ss << ", snapshot_ready_standby="
       << get_oplog_snapshot_ready_standby_count();
    ss << ", batch_commits=" << get_oplog_batch_commits_total();
    ss << ", sync_commits=" << get_oplog_sync_batch_commits_total();
    ss << ", skipped=" << get_oplog_skipped_entries_total();
    ss << ", checksum_fail=" << get_oplog_checksum_failures_total();
    ss << ", write_fail=" << get_oplog_write_failures_total();
    ss << ", renew_fail=" << get_election_renew_failures_total();
    ss << ", watch_disconn=" << get_oplog_watch_disconnections_total();
    ss << ", state=" << get_standby_state();
    ss << ", role=" << get_master_role();
    ss << ", snapshot_ready=" << get_snapshot_ready();
    ss << ", registry_heartbeat_ok=" << get_registry_heartbeat_ok();
    return ss.str();
}

}  // namespace mooncake
