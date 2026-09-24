#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

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
    enum class SnapshotOperation {
        Schedule,
        Upload,
        Bootstrap,
        Replay,
        Publish,
        Gc,
        Prune,
        Rebootstrap,
        Count
    };
    enum class SnapshotSkipReason {
        None,
        Disabled,
        Interval,
        InFlight,
        Stopped,
        Promotion,
        NoNewBatch,
        CatchUp,
        LeaseBusy,
        CaptureUnavailable,
        NoFallback,
        InvalidPair,
        FloorAhead,
        Count
    };
    // Records failed/throwing operations too. Set error to zero only after
    // successful completion; the default denotes an exception/incomplete call.
    class SnapshotOperationTimer {
       public:
        explicit SnapshotOperationTimer(SnapshotOperation operation)
            : operation_(operation) {}
        ~SnapshotOperationTimer() {
            HAMetricManager::instance().record_snapshot_operation(
                operation_, error, start_);
        }
        SnapshotOperationTimer(const SnapshotOperationTimer&) = delete;
        SnapshotOperationTimer& operator=(const SnapshotOperationTimer&) =
            delete;
        template <typename Result>
        Result Success(Result result) {
            error = 0;
            return result;
        }
        int64_t error{-1};

       private:
        SnapshotOperation operation_;
        std::chrono::steady_clock::time_point start_{
            std::chrono::steady_clock::now()};
    };
    struct SnapshotRuntime {
        bool enabled{false};
        // Absent means unknown/missing. Timestamps are Unix milliseconds.
        std::optional<int64_t> latest_created_at_ms;
        std::optional<int64_t> fallback_created_at_ms;
        uint64_t snapshot_bytes{0};
        uint64_t chunk_bytes{0};
        uint64_t chunk_count{0};
        uint64_t capture_pause_us{0};
        uint64_t suffix_batches{0};
        uint64_t catch_up_target_batch{0};
        uint64_t catch_up_target_sequence{0};
        uint64_t durable_batch{0};
        uint64_t applied_batch{0};
        uint64_t latest_batch{0};
        uint64_t fallback_batch{0};
        uint64_t compaction_floor{0};
        uint64_t candidate_floor{0};
        uint64_t floor_advances_total{0};
        uint64_t gc_orphan_prefixes{0};
        uint64_t gc_deleted_prefixes{0};
        uint64_t lease_lost_total{0};
        SnapshotSkipReason skip_reason{SnapshotSkipReason::Disabled};
    };
    struct SnapshotOperationStats {
        uint64_t total{0};
        uint64_t errors{0};
        uint64_t elapsed_us{0};
    };
    void reset_snapshot_runtime(bool enabled);
    SnapshotRuntime get_snapshot_runtime() const;
    SnapshotOperationStats get_snapshot_operation(
        SnapshotOperation operation) const;
    // Updates are restricted to observed state transitions; never perform I/O
    // or call back into a service while holding this lock.
    template <typename Update>
    void update_snapshot_runtime(Update&& update) {
        std::lock_guard<std::mutex> lock(snapshot_runtime_mutex_);
        if (snapshot_runtime_.enabled) update(snapshot_runtime_);
    }
    void record_snapshot_skip(SnapshotSkipReason reason);
    void record_snapshot_operation(SnapshotOperation operation, int64_t error,
                                   std::chrono::steady_clock::time_point start);

    struct WriterRuntimeSnapshot {
        bool accepting{false};
        uint64_t retry_count{0};
        uint64_t retry_delay_ms{0};
        uint64_t waiting_slots{0};
        uint64_t committed_queue_depth{0};
        uint64_t callback_queue_depth{0};
        uint64_t durable_batch_id{0};
        uint64_t durable_sequence{0};
        int64_t last_error{0};
        std::string terminal_reason;
        std::optional<std::pair<uint64_t, uint64_t>> stuck_range;
    };

    // Activation replaces the observed writer and invalidates earlier owners.
    // Snapshot retry_count is per writer on input, process cumulative on
    // output.
    uint64_t activate_writer_runtime(const WriterRuntimeSnapshot& snapshot);
    void update_writer_runtime(uint64_t owner,
                               const WriterRuntimeSnapshot& snapshot);
    WriterRuntimeSnapshot get_writer_runtime() const;
    // --- Singleton Access ---
    static HAMetricManager& instance();

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

    // ========== Batch-record OpLog Metrics ==========

    void inc_batch_record_durable_batches(int64_t val = 1);
    int64_t get_batch_record_durable_batches_total();
    void inc_batch_record_durable_entries(int64_t val = 1);
    int64_t get_batch_record_durable_entries_total();
    void inc_batch_record_retries(int64_t val = 1);
    int64_t get_batch_record_retries_total();

    void set_batch_record_committed_queue_depth(int64_t depth);
    int64_t get_batch_record_committed_queue_depth();
    void set_batch_record_callback_queue_depth(int64_t depth);
    int64_t get_batch_record_callback_queue_depth();
    void set_batch_record_last_batch_id(int64_t batch_id);
    int64_t get_batch_record_last_batch_id();
    void set_batch_record_durable_sequence(int64_t sequence_id);
    int64_t get_batch_record_durable_sequence();

    void observe_batch_record_batch_entries(int64_t entries);
    void observe_batch_record_batch_bytes(int64_t bytes);
    void observe_batch_record_txn_latency_us(int64_t latency_us);
    void observe_batch_record_commit_to_durable_us(int64_t latency_us);
    void observe_batch_record_callback_latency_us(int64_t latency_us);

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

    ylt::metric::counter_t batch_record_durable_batches_total_;
    ylt::metric::counter_t batch_record_durable_entries_total_;
    ylt::metric::counter_t batch_record_retry_total_;
    ylt::metric::gauge_t batch_record_committed_queue_depth_;
    ylt::metric::gauge_t batch_record_callback_queue_depth_;
    ylt::metric::gauge_t batch_record_last_batch_id_;
    ylt::metric::gauge_t batch_record_durable_sequence_;
    ylt::metric::histogram_t batch_record_batch_entries_;
    ylt::metric::histogram_t batch_record_batch_bytes_;
    ylt::metric::histogram_t batch_record_txn_latency_us_;
    ylt::metric::histogram_t batch_record_commit_to_durable_us_;
    ylt::metric::histogram_t batch_record_callback_latency_us_;

    // State Machine
    ylt::metric::gauge_t standby_state_;
    ylt::metric::counter_t state_transitions_total_;

    mutable std::mutex snapshot_runtime_mutex_;
    SnapshotRuntime snapshot_runtime_;
    std::array<SnapshotOperationStats,
               static_cast<size_t>(SnapshotOperation::Count)>
        snapshot_operations_{};
    std::array<uint64_t, static_cast<size_t>(SnapshotSkipReason::Count)>
        snapshot_skips_{};

    mutable std::mutex writer_runtime_mutex_;
    WriterRuntimeSnapshot writer_runtime_;
    uint64_t writer_runtime_owner_{0};
    uint64_t writer_retry_base_{0};
};

}  // namespace mooncake
