#pragma once

#include <atomic>
#include <chrono>
#include <mutex>
#include <string>

#include "ylt/metric/counter.hpp"
#include "ylt/metric/gauge.hpp"
#include "ylt/metric/histogram.hpp"

namespace mooncake {

/**
 * @brief Singleton manager for High Availability (HA) related metrics.
 *
 * This class provides metrics for monitoring the health and performance
 * of the OpLog replication system, including:
 * - OpLog sequence tracking
 * - Standby replication lag
 * - Error counters (checksum failures, skipped entries)
 * - Performance histograms (OpLog write/apply latency)
 * - Queue sizes (pending mutations)
 */
class P2PHAMetricManager {
   public:
    // --- Singleton Access ---
    static P2PHAMetricManager& instance();

    P2PHAMetricManager(const P2PHAMetricManager&) = delete;
    P2PHAMetricManager& operator=(const P2PHAMetricManager&) = delete;
    P2PHAMetricManager(P2PHAMetricManager&&) = delete;
    P2PHAMetricManager& operator=(P2PHAMetricManager&&) = delete;

    // ========== OpLog Sequence Metrics (Gauge) ==========

    /**
     * @brief Set the latest OpLog sequence ID on Primary
     */
    void set_oplog_last_sequence_id(int64_t seq_id);
    int64_t get_oplog_last_sequence_id();

    /**
     * @brief Set the Standby's applied sequence ID
     */
    void set_oplog_applied_sequence_id(int64_t seq_id);
    int64_t get_oplog_applied_sequence_id();

    /**
     * @brief Set the replication lag (entries behind Primary)
     */
    void set_oplog_standby_lag(int64_t lag);
    int64_t get_oplog_standby_lag();

    /**
     * @brief Set the number of pending (out-of-order) entries in P2POpLogApplierBase
     */
    void set_oplog_pending_entries(int64_t count);
    int64_t get_oplog_pending_entries();

    /**
     * @brief Set the asynchronous OpLog queue size
     */
    void set_oplog_async_queue_size(int64_t size);
    int64_t get_oplog_async_queue_size();
    void set_election_is_leader(bool value);
    void set_oplog_async_workers_running(bool value);
    void set_standby_degraded(bool value);
    void set_primary_degraded(bool value);
    void set_oplog_last_successful_poll_timestamp_ms(int64_t timestamp_ms);
    void set_p2p_snapshot_bootstrap_baseline_sequence_id(int64_t seq_id);
    void set_p2p_bootstrap_catchup_target_sequence_id(int64_t seq_id);

    // ========== Error Counters ==========

    /**
     * @brief Increment counter for skipped OpLog entries
     */
    void inc_oplog_skipped_entries(int64_t val = 1);
    int64_t get_oplog_skipped_entries_total();

    /**
     * @brief Increment counter for checksum verification failures
     */
    void inc_oplog_checksum_failures(int64_t val = 1);
    int64_t get_oplog_checksum_failures_total();

    /**
     * @brief Increment counter for gap resolve attempts
     */
    void inc_oplog_gap_resolve_attempts(int64_t val = 1);
    int64_t get_oplog_gap_resolve_attempts_total();

    /**
     * @brief Increment counter for successful gap resolves
     */
    void inc_oplog_gap_resolve_success(int64_t val = 1);
    int64_t get_oplog_gap_resolve_success_total();

    /**
     * @brief Increment counter for OpLog persistence failures
     */
    void inc_oplog_write_failures(int64_t val = 1);
    int64_t get_oplog_write_failures_total();

    /**
     * @brief Increment counter for OpLog persistence retries
     */
    void inc_oplog_write_retries(int64_t val = 1);
    int64_t get_oplog_write_retries_total();

    void inc_election_attempts(int64_t val = 1);
    void inc_election_failures(int64_t val = 1);
    void inc_election_leadership_lost(int64_t val = 1);
    void inc_election_reconnects(int64_t val = 1);
    void inc_election_watch_failures(int64_t val = 1);
    void inc_election_polling_fallbacks(int64_t val = 1);

    void inc_oplog_best_effort_dropped(int64_t val = 1);
    void inc_oplog_queue_rejected(int64_t val = 1);
    void inc_oplog_queue_bypassed(int64_t val = 1);
    void inc_oplog_sync_wait_timeouts(int64_t val = 1);
    void inc_oplog_read_failures(int64_t val = 1);
    void inc_oplog_reader_reconnect_attempts(int64_t val = 1);
    void inc_oplog_reader_reconnect_failures(int64_t val = 1);
    void inc_oplog_reader_reconnects(int64_t val = 1);
    void inc_oplog_apply_failures(int64_t val = 1);
    void inc_oplog_best_effort_apply_skipped(int64_t val = 1);
    void inc_oplog_confirmed_holes(int64_t val = 1);

    void inc_force_promotions(int64_t val = 1);
    void inc_promotion_catchup_incomplete(int64_t val = 1);
    void inc_promotion_restore_failures(int64_t val = 1);
    void inc_promotion_skipped_replicas(int64_t val = 1);
    void inc_promotion_skipped_objects(int64_t val = 1);
    void inc_p2p_snapshot_bootstrap_success(int64_t val = 1);
    void inc_p2p_snapshot_bootstrap_failures(int64_t val = 1);
    void inc_p2p_snapshot_resync_success(int64_t val = 1);
    void inc_p2p_snapshot_resync_failures(int64_t val = 1);

    /**
     * @brief Increment counter for watch disconnections
     */
    void inc_oplog_watch_disconnections(int64_t val = 1);
    int64_t get_oplog_watch_disconnections_total();

    /**
     * @brief Increment counter for successfully applied OpLog entries
     */
    void inc_oplog_applied_entries(int64_t val = 1);
    int64_t get_oplog_applied_entries_total();

    /**
     * @brief Increment counter for dropped PUT_END operations (late arrival
     * after skip)
     */
    void inc_oplog_dropped_put_end(int64_t val = 1);
    int64_t get_oplog_dropped_put_end_total();

    /**
     * @brief Increase the total number of OpLog batch commits (Group Commit)
     */
    void inc_oplog_batch_commits(int64_t count = 1);
    int64_t get_oplog_batch_commits_total();

    /**
     * @brief Increase the number of sync batch commits (triggered by
     * DELETE/Sync ops)
     */
    void inc_oplog_sync_batch_commits(int64_t count = 1);
    int64_t get_oplog_sync_batch_commits_total();

    // ========== Latency Histograms ==========

    /**
     * @brief Record OpLog persistence latency in microseconds
     */
    void observe_oplog_write_latency_us(int64_t latency_us);
    void observe_election_duration_ms(int64_t duration_ms);

    /**
     * @brief Record OpLog apply latency in microseconds
     */
    void observe_oplog_apply_latency_us(int64_t latency_us);

    // ========== State Machine Metrics ==========

    /**
     * @brief Set the current Standby state (as integer for Prometheus)
     * @param state_value Integer representation of P2PStandbyState
     */
    void set_standby_state(int64_t state_value);
    int64_t get_standby_state();

    /**
     * @brief Increment state transition counter
     */
    void inc_state_transitions(int64_t val = 1);
    int64_t get_state_transitions_total();

    // ========== Serialization ==========

    /**
     * @brief Serializes all HA metrics into Prometheus text format.
     * @return A string containing the metrics in Prometheus format.
     */
    std::string serialize_metrics();

    /**
     * @brief Generates a concise, human-readable summary of HA metrics.
     * @return A string containing the formatted summary.
     */
    std::string get_summary_string();

   private:
    // --- Private Constructor & Destructor ---
    P2PHAMetricManager();
    ~P2PHAMetricManager() = default;

    // --- Metric Members ---

    // OpLog Sequence Gauges
    ylt::metric::gauge_t oplog_last_sequence_id_;
    ylt::metric::gauge_t oplog_applied_sequence_id_;
    ylt::metric::gauge_t oplog_standby_lag_;
    ylt::metric::gauge_t oplog_pending_entries_;
    ylt::metric::gauge_t oplog_async_queue_size_;
    ylt::metric::gauge_t election_is_leader_;
    ylt::metric::gauge_t oplog_async_workers_running_;
    ylt::metric::gauge_t standby_degraded_;
    ylt::metric::gauge_t primary_degraded_;
    ylt::metric::gauge_t oplog_last_successful_poll_timestamp_ms_;
    ylt::metric::gauge_t p2p_snapshot_bootstrap_baseline_sequence_id_;
    ylt::metric::gauge_t p2p_bootstrap_catchup_target_sequence_id_;

    // Error Counters
    ylt::metric::counter_t oplog_skipped_entries_total_;
    ylt::metric::counter_t oplog_checksum_failures_total_;
    ylt::metric::counter_t oplog_gap_resolve_attempts_total_;
    ylt::metric::counter_t oplog_gap_resolve_success_total_;
    ylt::metric::counter_t oplog_write_failures_total_;
    ylt::metric::counter_t oplog_write_retries_total_;
    ylt::metric::counter_t election_attempts_total_;
    ylt::metric::counter_t election_failures_total_;
    ylt::metric::counter_t election_leadership_lost_total_;
    ylt::metric::counter_t election_reconnects_total_;
    ylt::metric::counter_t election_watch_failures_total_;
    ylt::metric::counter_t election_polling_fallbacks_total_;
    ylt::metric::counter_t oplog_best_effort_dropped_total_;
    ylt::metric::counter_t oplog_queue_rejected_total_;
    ylt::metric::counter_t oplog_queue_bypassed_total_;
    ylt::metric::counter_t oplog_sync_wait_timeouts_total_;
    ylt::metric::counter_t oplog_read_failures_total_;
    ylt::metric::counter_t oplog_reader_reconnect_attempts_total_;
    ylt::metric::counter_t oplog_reader_reconnect_failures_total_;
    ylt::metric::counter_t oplog_reader_reconnects_total_;
    ylt::metric::counter_t oplog_apply_failures_total_;
    ylt::metric::counter_t oplog_best_effort_apply_skipped_total_;
    ylt::metric::counter_t oplog_confirmed_holes_total_;
    ylt::metric::counter_t force_promotions_total_;
    ylt::metric::counter_t promotion_catchup_incomplete_total_;
    ylt::metric::counter_t promotion_restore_failures_total_;
    ylt::metric::counter_t promotion_skipped_replicas_total_;
    ylt::metric::counter_t promotion_skipped_objects_total_;
    ylt::metric::counter_t p2p_snapshot_bootstrap_success_total_;
    ylt::metric::counter_t p2p_snapshot_bootstrap_failures_total_;
    ylt::metric::counter_t p2p_snapshot_resync_success_total_;
    ylt::metric::counter_t p2p_snapshot_resync_failures_total_;
    ylt::metric::counter_t oplog_watch_disconnections_total_;
    ylt::metric::counter_t oplog_applied_entries_total_;
    ylt::metric::counter_t oplog_dropped_put_end_total_;
    ylt::metric::counter_t oplog_batch_commits_total_;
    ylt::metric::counter_t oplog_sync_batch_commits_total_;

    // Latency Histograms (buckets in microseconds: 100us, 500us, 1ms, 5ms,
    // 10ms, 50ms, 100ms, 500ms, 1s)
    ylt::metric::histogram_t oplog_write_latency_us_;
    ylt::metric::histogram_t election_duration_ms_;
    ylt::metric::histogram_t oplog_apply_latency_us_;

    // State Machine
    ylt::metric::gauge_t standby_state_;
    ylt::metric::counter_t state_transitions_total_;
};

}  // namespace mooncake
