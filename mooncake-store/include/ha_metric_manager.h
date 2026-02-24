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
 * - Performance histograms (etcd write latency)
 * - Queue sizes (pending mutations)
 */
class HAMetricManager {
   public:
    // --- Singleton Access ---
    static HAMetricManager& instance();

    /**
     * @brief Explicitly initialize the singleton instance.
     * Use this at startup (e.g. main) to ensure thread-safe initialization
     * of the underlying metric library components.
     */
    static void Init() { instance(); }

    HAMetricManager(const HAMetricManager&) = delete;
    HAMetricManager& operator=(const HAMetricManager&) = delete;
    HAMetricManager(HAMetricManager&&) = delete;
    HAMetricManager& operator=(HAMetricManager&&) = delete;

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
     * @brief Set the number of pending (out-of-order) entries in OpLogApplier
     */
    void set_oplog_pending_entries(int64_t count);
    int64_t get_oplog_pending_entries();

    /**
     * @brief Set the pending mutation queue size (retry queue)
     */
    void set_pending_mutation_queue_size(int64_t size);
    int64_t get_pending_mutation_queue_size();

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
     * @brief Increment counter for etcd write failures
     */
    void inc_oplog_etcd_write_failures(int64_t val = 1);
    int64_t get_oplog_etcd_write_failures_total();

    /**
     * @brief Increment counter for etcd write retries
     */
    void inc_oplog_etcd_write_retries(int64_t val = 1);
    int64_t get_oplog_etcd_write_retries_total();

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
     * @brief Record etcd write latency in microseconds
     */
    void observe_oplog_etcd_write_latency_us(int64_t latency_us);

    /**
     * @brief Record OpLog apply latency in microseconds
     */
    void observe_oplog_apply_latency_us(int64_t latency_us);

    // ========== State Machine Metrics ==========

    /**
     * @brief Set the current Standby state (as integer for Prometheus)
     * @param state_value Integer representation of StandbyState
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
    HAMetricManager();
    ~HAMetricManager() = default;

    // --- Metric Members ---

    // OpLog Sequence Gauges
    ylt::metric::gauge_t oplog_last_sequence_id_;
    ylt::metric::gauge_t oplog_applied_sequence_id_;
    ylt::metric::gauge_t oplog_standby_lag_;
    ylt::metric::gauge_t oplog_pending_entries_;
    ylt::metric::gauge_t pending_mutation_queue_size_;

    // Error Counters
    ylt::metric::counter_t oplog_skipped_entries_total_;
    ylt::metric::counter_t oplog_checksum_failures_total_;
    ylt::metric::counter_t oplog_gap_resolve_attempts_total_;
    ylt::metric::counter_t oplog_gap_resolve_success_total_;
    ylt::metric::counter_t oplog_etcd_write_failures_total_;
    ylt::metric::counter_t oplog_etcd_write_retries_total_;
    ylt::metric::counter_t oplog_watch_disconnections_total_;
    ylt::metric::counter_t oplog_applied_entries_total_;
    ylt::metric::counter_t oplog_dropped_put_end_total_;
    ylt::metric::counter_t oplog_batch_commits_total_;
    ylt::metric::counter_t oplog_sync_batch_commits_total_;

    // Latency Histograms (buckets in microseconds: 100us, 500us, 1ms, 5ms,
    // 10ms, 50ms, 100ms, 500ms, 1s)
    ylt::metric::histogram_t oplog_etcd_write_latency_us_;
    ylt::metric::histogram_t oplog_apply_latency_us_;

    // State Machine
    ylt::metric::gauge_t standby_state_;
    ylt::metric::counter_t state_transitions_total_;
};

}  // namespace mooncake
