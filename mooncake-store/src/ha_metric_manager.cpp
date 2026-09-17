#include "ha_metric_manager.h"

#include <glog/logging.h>

#include <iomanip>
#include <sstream>

namespace mooncake {

// --- Singleton Instance ---
HAMetricManager& HAMetricManager::instance() {
    static HAMetricManager static_instance;
    return static_instance;
}

void HAMetricManager::reset_snapshot_runtime(bool enabled) {
    std::lock_guard<std::mutex> lock(snapshot_runtime_mutex_);
    const auto lease_lost = snapshot_runtime_.lease_lost_total;
    const auto floor_advances = snapshot_runtime_.floor_advances_total;
    snapshot_runtime_ = {};
    snapshot_runtime_.lease_lost_total = lease_lost;
    snapshot_runtime_.floor_advances_total = floor_advances;
    snapshot_runtime_.enabled = enabled;
    snapshot_runtime_.skip_reason =
        enabled ? SnapshotSkipReason::None : SnapshotSkipReason::Disabled;
    // Event counters survive service replacement, like writer retry counters.
}

HAMetricManager::SnapshotRuntime HAMetricManager::get_snapshot_runtime() const {
    std::lock_guard<std::mutex> lock(snapshot_runtime_mutex_);
    return snapshot_runtime_;
}

HAMetricManager::SnapshotOperationStats HAMetricManager::get_snapshot_operation(
    SnapshotOperation operation) const {
    std::lock_guard<std::mutex> lock(snapshot_runtime_mutex_);
    return snapshot_operations_[static_cast<size_t>(operation)];
}

void HAMetricManager::record_snapshot_skip(SnapshotSkipReason reason) {
    std::lock_guard<std::mutex> lock(snapshot_runtime_mutex_);
    if (!snapshot_runtime_.enabled) return;
    snapshot_runtime_.skip_reason = reason;
    if (reason != SnapshotSkipReason::None)
        ++snapshot_skips_[static_cast<size_t>(reason)];
}

void HAMetricManager::record_snapshot_operation(
    SnapshotOperation operation, int64_t error,
    std::chrono::steady_clock::time_point start) {
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
                             std::chrono::steady_clock::now() - start)
                             .count();
    std::lock_guard<std::mutex> lock(snapshot_runtime_mutex_);
    if (!snapshot_runtime_.enabled) return;
    auto& stats = snapshot_operations_[static_cast<size_t>(operation)];
    ++stats.total;
    stats.errors += error != 0;
    stats.elapsed_us += elapsed;
}

uint64_t HAMetricManager::activate_writer_runtime(
    const WriterRuntimeSnapshot& snapshot) {
    std::lock_guard<std::mutex> lock(writer_runtime_mutex_);
    writer_retry_base_ = writer_runtime_.retry_count;
    writer_runtime_ = snapshot;
    writer_runtime_.retry_count += writer_retry_base_;
    return ++writer_runtime_owner_;
}

void HAMetricManager::update_writer_runtime(
    uint64_t owner, const WriterRuntimeSnapshot& snapshot) {
    std::lock_guard<std::mutex> lock(writer_runtime_mutex_);
    if (owner == 0 || owner != writer_runtime_owner_) return;
    writer_runtime_ = snapshot;
    writer_runtime_.retry_count += writer_retry_base_;
}

HAMetricManager::WriterRuntimeSnapshot HAMetricManager::get_writer_runtime()
    const {
    std::lock_guard<std::mutex> lock(writer_runtime_mutex_);
    return writer_runtime_;
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

      batch_record_durable_batches_total_(
          "ha_batch_record_durable_batches_total",
          "Total durable batch-record OpLog batches"),
      batch_record_durable_entries_total_(
          "ha_batch_record_durable_entries_total",
          "Total durable entries in batch-record OpLog batches"),
      batch_record_retry_total_("ha_batch_record_retry_total",
                                "Total batch-record backend retries"),
      batch_record_committed_queue_depth_(
          "ha_batch_record_committed_queue_depth",
          "Committed batch-record entries waiting for durability"),
      batch_record_callback_queue_depth_(
          "ha_batch_record_callback_queue_depth",
          "Durable batch-record callbacks waiting to run"),
      batch_record_last_batch_id_("ha_batch_record_last_batch_id",
                                  "Latest durable batch-record batch ID"),
      batch_record_durable_sequence_(
          "ha_batch_record_durable_sequence",
          "Latest durable batch-record OpLog sequence"),
      batch_record_batch_entries_("ha_batch_record_batch_entries",
                                  "Entries per durable batch-record batch",
                                  {1, 8, 32, 64, 128, 256, 512, 1024, 4096}),
      batch_record_batch_bytes_(
          "ha_batch_record_batch_bytes",
          "Encoded bytes per durable batch-record batch",
          {256, 1024, 4096, 16384, 65536, 262144, 1048576, 4194304}),
      batch_record_txn_latency_us_(
          "ha_batch_record_txn_latency_us",
          "Batch-record backend transaction latency in microseconds",
          {100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000}),
      batch_record_commit_to_durable_us_(
          "ha_batch_record_commit_to_durable_us",
          "Batch-record commit-to-durable latency in microseconds",
          {100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000}),
      batch_record_callback_latency_us_(
          "ha_batch_record_callback_latency_us",
          "Batch-record durable-to-callback queue latency in microseconds",
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
    pending_mutation_queue_size_.update(0);
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
    batch_record_committed_queue_depth_.update(0);
    batch_record_callback_queue_depth_.update(0);
    batch_record_last_batch_id_.update(0);
    batch_record_durable_sequence_.update(0);
#endif
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

void HAMetricManager::inc_batch_record_durable_batches(int64_t val) {
    batch_record_durable_batches_total_.inc(val);
}

int64_t HAMetricManager::get_batch_record_durable_batches_total() {
    return static_cast<int64_t>(batch_record_durable_batches_total_.value());
}

void HAMetricManager::inc_batch_record_durable_entries(int64_t val) {
    batch_record_durable_entries_total_.inc(val);
}

int64_t HAMetricManager::get_batch_record_durable_entries_total() {
    return static_cast<int64_t>(batch_record_durable_entries_total_.value());
}

void HAMetricManager::inc_batch_record_retries(int64_t val) {
    batch_record_retry_total_.inc(val);
}

int64_t HAMetricManager::get_batch_record_retries_total() {
    return static_cast<int64_t>(batch_record_retry_total_.value());
}

void HAMetricManager::set_batch_record_committed_queue_depth(int64_t depth) {
    batch_record_committed_queue_depth_.update(depth);
}

int64_t HAMetricManager::get_batch_record_committed_queue_depth() {
    return static_cast<int64_t>(batch_record_committed_queue_depth_.value());
}

void HAMetricManager::set_batch_record_callback_queue_depth(int64_t depth) {
    batch_record_callback_queue_depth_.update(depth);
}

int64_t HAMetricManager::get_batch_record_callback_queue_depth() {
    return static_cast<int64_t>(batch_record_callback_queue_depth_.value());
}

void HAMetricManager::set_batch_record_last_batch_id(int64_t batch_id) {
    batch_record_last_batch_id_.update(batch_id);
}

int64_t HAMetricManager::get_batch_record_last_batch_id() {
    return static_cast<int64_t>(batch_record_last_batch_id_.value());
}

void HAMetricManager::set_batch_record_durable_sequence(int64_t sequence_id) {
    batch_record_durable_sequence_.update(sequence_id);
}

int64_t HAMetricManager::get_batch_record_durable_sequence() {
    return static_cast<int64_t>(batch_record_durable_sequence_.value());
}

void HAMetricManager::observe_batch_record_batch_entries(int64_t entries) {
    batch_record_batch_entries_.observe(entries);
}

void HAMetricManager::observe_batch_record_batch_bytes(int64_t bytes) {
    batch_record_batch_bytes_.observe(bytes);
}

void HAMetricManager::observe_batch_record_txn_latency_us(int64_t latency_us) {
    batch_record_txn_latency_us_.observe(latency_us);
}

void HAMetricManager::observe_batch_record_commit_to_durable_us(
    int64_t latency_us) {
    batch_record_commit_to_durable_us_.observe(latency_us);
}

void HAMetricManager::observe_batch_record_callback_latency_us(
    int64_t latency_us) {
    batch_record_callback_latency_us_.observe(latency_us);
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
    serialize_metric(pending_mutation_queue_size_);
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
    serialize_metric(batch_record_committed_queue_depth_);
    serialize_metric(batch_record_callback_queue_depth_);
    serialize_metric(batch_record_last_batch_id_);
    serialize_metric(batch_record_durable_sequence_);
#endif
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
    serialize_metric(oplog_dropped_put_end_total_);
    serialize_metric(oplog_batch_commits_total_);
    serialize_metric(oplog_sync_batch_commits_total_);
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
    serialize_metric(batch_record_durable_batches_total_);
    serialize_metric(batch_record_durable_entries_total_);
    serialize_metric(batch_record_retry_total_);
#endif
    serialize_metric(state_transitions_total_);

    {
        SnapshotRuntime snapshot;
        decltype(snapshot_operations_) operation_stats;
        decltype(snapshot_skips_) skips;
        {
            std::lock_guard<std::mutex> lock(snapshot_runtime_mutex_);
            snapshot = snapshot_runtime_;
            operation_stats = snapshot_operations_;
            skips = snapshot_skips_;
        }
        auto gauge = [&ss](const char* name, auto value) {
            ss << "# TYPE ha_snapshot_" << name << " gauge\nha_snapshot_"
               << name << " " << value << "\n";
        };
        gauge("enabled", snapshot.enabled ? 1 : 0);
        const auto now_ms =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        const auto age = [now_ms](std::optional<int64_t> created) {
            return created && now_ms > *created ? (now_ms - *created) / 1000
                                                : 0;
        };
        gauge("latest_present",
              snapshot.latest_created_at_ms.has_value() ? 1 : 0);
        gauge("fallback_present",
              snapshot.fallback_created_at_ms.has_value() ? 1 : 0);
        gauge("latest_age_seconds", age(snapshot.latest_created_at_ms));
        gauge("fallback_age_seconds", age(snapshot.fallback_created_at_ms));
        gauge("count", snapshot.latest_created_at_ms.has_value() +
                           snapshot.fallback_created_at_ms.has_value());
        gauge("candidate_floor", snapshot.candidate_floor);
        ss << "# TYPE ha_snapshot_floor_advances_total "
              "counter\nha_snapshot_floor_advances_total "
           << snapshot.floor_advances_total << "\n";
        gauge("bytes", snapshot.snapshot_bytes);
        gauge("chunk_bytes", snapshot.chunk_bytes);
        gauge("chunk_count", snapshot.chunk_count);
        gauge("capture_pause_us", snapshot.capture_pause_us);
        gauge("suffix_batches", snapshot.suffix_batches);
        gauge("catch_up_target_batch", snapshot.catch_up_target_batch);
        gauge("catch_up_target_sequence", snapshot.catch_up_target_sequence);
        gauge("durable_batch", snapshot.durable_batch);
        gauge("applied_batch", snapshot.applied_batch);
        gauge("latest_batch", snapshot.latest_batch);
        gauge("fallback_batch", snapshot.fallback_batch);
        gauge("compaction_floor", snapshot.compaction_floor);
        gauge("gc_orphan_prefixes", snapshot.gc_orphan_prefixes);
        gauge("gc_deleted_prefixes", snapshot.gc_deleted_prefixes);
        gauge("uncompacted_batches",
              snapshot.durable_batch > snapshot.compaction_floor
                  ? snapshot.durable_batch - snapshot.compaction_floor
                  : 0);
        ss << "# TYPE ha_snapshot_lease_lost_total "
              "counter\nha_snapshot_lease_lost_total "
           << snapshot.lease_lost_total << "\n";
        static constexpr const char* reasons[] = {
            "none",         "disabled",
            "interval",     "in_flight",
            "stopped",      "promotion",
            "no_new_batch", "catch_up",
            "lease_busy",   "capture_unavailable",
            "no_fallback",  "invalid_pair",
            "floor_ahead"};
        static_assert(std::size(reasons) ==
                      static_cast<size_t>(SnapshotSkipReason::Count));
        ss << "# TYPE ha_snapshot_skip_reason gauge\n";
        for (size_t i = 0; i < std::size(reasons); ++i)
            ss << "ha_snapshot_skip_reason{reason=\"" << reasons[i] << "\"} "
               << (static_cast<size_t>(snapshot.skip_reason) == i ? 1 : 0)
               << "\n";
        ss << "# TYPE ha_snapshot_skips_total counter\n";
        for (size_t i = 0; i < std::size(reasons); ++i)
            ss << "ha_snapshot_skips_total{reason=\"" << reasons[i] << "\"} "
               << skips[i] << "\n";
        static constexpr const char* operations[] = {
            "schedule", "upload", "bootstrap", "replay",
            "publish",  "gc",     "prune",     "rebootstrap"};
        static_assert(std::size(operations) ==
                      static_cast<size_t>(SnapshotOperation::Count));
        ss << "# TYPE ha_snapshot_operations_total counter\n"
           << "# TYPE ha_snapshot_errors_total counter\n"
           << "# TYPE ha_snapshot_duration_us_total counter\n";
        for (size_t i = 0; i < std::size(operations); ++i) {
            const auto& stats = operation_stats[i];
            const std::string label =
                std::string("{operation=\"") + operations[i] + "\"} ";
            ss << "ha_snapshot_operations_total" << label << stats.total << "\n"
               << "ha_snapshot_errors_total" << label << stats.errors << "\n"
               << "ha_snapshot_duration_us_total" << label << stats.elapsed_us
               << "\n";
        }
    }

    const auto writer = get_writer_runtime();
    ss << "# HELP ha_writer_accepting Whether the batch OpLog writer accepts "
          "new entries\n"
       << "# TYPE ha_writer_accepting gauge\n"
       << "ha_writer_accepting " << (writer.accepting ? 1 : 0) << "\n"
       << "# HELP ha_writer_retry_count Total writer retries\n"
       << "# TYPE ha_writer_retry_count counter\n"
       << "ha_writer_retry_count " << writer.retry_count << "\n"
       << "# HELP ha_writer_retry_delay_ms Current writer retry delay\n"
       << "# TYPE ha_writer_retry_delay_ms gauge\n"
       << "ha_writer_retry_delay_ms " << writer.retry_delay_ms << "\n"
       << "# HELP ha_writer_waiting_slots Current reserved writer slots\n"
       << "# TYPE ha_writer_waiting_slots gauge\n"
       << "ha_writer_waiting_slots " << writer.waiting_slots << "\n"
       << "# HELP ha_writer_committed_queue_depth Committed writer queue "
          "depth\n"
       << "# TYPE ha_writer_committed_queue_depth gauge\n"
       << "ha_writer_committed_queue_depth " << writer.committed_queue_depth
       << "\n"
       << "# HELP ha_writer_callback_queue_depth Callback queue depth\n"
       << "# TYPE ha_writer_callback_queue_depth gauge\n"
       << "ha_writer_callback_queue_depth " << writer.callback_queue_depth
       << "\n"
       << "# HELP ha_writer_last_error Last writer error code\n"
       << "# TYPE ha_writer_last_error gauge\n"
       << "ha_writer_last_error " << writer.last_error << "\n"
       << "# HELP ha_writer_durable_batch_id Last durable batch ID\n"
       << "# TYPE ha_writer_durable_batch_id gauge\n"
       << "ha_writer_durable_batch_id " << writer.durable_batch_id << "\n"
       << "# HELP ha_writer_durable_sequence Last durable sequence\n"
       << "# TYPE ha_writer_durable_sequence gauge\n"
       << "ha_writer_durable_sequence " << writer.durable_sequence << "\n"
       << "# HELP ha_writer_stuck_first_sequence First sequence of the "
          "in-flight batch awaiting durability; 0 when absent, not a timeout "
          "indicator\n"
       << "# TYPE ha_writer_stuck_first_sequence gauge\n"
       << "ha_writer_stuck_first_sequence "
       << (writer.stuck_range ? writer.stuck_range->first : 0) << "\n"
       << "# HELP ha_writer_stuck_last_sequence Last sequence of the "
          "in-flight batch awaiting durability; 0 when absent, not a timeout "
          "indicator\n"
       << "# TYPE ha_writer_stuck_last_sequence gauge\n"
       << "ha_writer_stuck_last_sequence "
       << (writer.stuck_range ? writer.stuck_range->second : 0) << "\n";
    if (!writer.terminal_reason.empty()) {
        ss << "# HELP ha_writer_terminal_reason Current terminal reason\n"
           << "# TYPE ha_writer_terminal_reason gauge\n"
           << "ha_writer_terminal_reason{reason=\"" << writer.terminal_reason
           << "\"} 1\n";
    }

    // Histograms
    serialize_metric(oplog_etcd_write_latency_us_);
    serialize_metric(oplog_apply_latency_us_);
#ifdef MOONCAKE_ENABLE_OPLOG_PERF_METRICS
    serialize_metric(batch_record_batch_entries_);
    serialize_metric(batch_record_batch_bytes_);
    serialize_metric(batch_record_txn_latency_us_);
    serialize_metric(batch_record_commit_to_durable_us_);
    serialize_metric(batch_record_callback_latency_us_);
#endif

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
    const auto writer = get_writer_runtime();
    ss << ", writer_accepting=" << (writer.accepting ? "true" : "false")
       << ", writer_retry_count=" << writer.retry_count
       << ", writer_retry_delay_ms=" << writer.retry_delay_ms
       << ", writer_waiting_slots=" << writer.waiting_slots
       << ", writer_committed_queue=" << writer.committed_queue_depth
       << ", writer_callback_queue=" << writer.callback_queue_depth
       << ", writer_durable_batch=" << writer.durable_batch_id
       << ", writer_durable_seq=" << writer.durable_sequence
       << ", writer_last_error=" << writer.last_error;
    if (!writer.terminal_reason.empty()) {
        ss << ", writer_terminal_reason=" << writer.terminal_reason;
    }
    if (writer.stuck_range) {
        ss << ", writer_stuck_range=" << writer.stuck_range->first << "-"
           << writer.stuck_range->second;
    }
    return ss.str();
}

}  // namespace mooncake
