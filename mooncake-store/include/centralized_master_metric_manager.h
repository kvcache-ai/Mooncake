#pragma once

#include "master_metric_manager.h"

namespace mooncake {

// Centralization-architecture master metrics
class CentralizedMasterMetricManager final : public MasterMetricManager {
   public:
    static CentralizedMasterMetricManager& instance();

    std::string serialize_metrics() override;
    std::string get_summary_string() override;
    void reset_all_metrics() override;

    // Key/Value Metrics
    void inc_soft_pin_key_count(int64_t val = 1);
    void dec_soft_pin_key_count(int64_t val = 1);
    int64_t get_soft_pin_key_count();

    // Operation Statistics (Counters)
    void inc_put_start_requests(int64_t val = 1);
    void inc_put_start_failures(int64_t val = 1);
    void inc_put_end_requests(int64_t val = 1);
    void inc_put_end_failures(int64_t val = 1);
    void inc_put_revoke_requests(int64_t val = 1);
    void inc_put_revoke_failures(int64_t val = 1);

    // Batch Operation Statistics (Counters)
    void inc_batch_replica_clear_requests(int64_t items);
    void inc_batch_replica_clear_failures(int64_t failed_items);
    void inc_batch_replica_clear_partial_success(int64_t failed_items);
    void inc_batch_put_start_requests(int64_t items);
    void inc_batch_put_start_failures(int64_t failed_items);
    void inc_batch_put_start_partial_success(int64_t failed_items);
    void inc_batch_put_end_requests(int64_t items);
    void inc_batch_put_end_failures(int64_t failed_items);
    void inc_batch_put_end_partial_success(int64_t failed_items);
    void inc_batch_put_revoke_requests(int64_t items);
    void inc_batch_put_revoke_failures(int64_t failed_items);
    void inc_batch_put_revoke_partial_success(int64_t failed_items);

    // Operation Statistics Getters
    int64_t get_put_start_requests();
    int64_t get_put_start_failures();
    int64_t get_put_end_requests();
    int64_t get_put_end_failures();
    int64_t get_put_revoke_requests();
    int64_t get_put_revoke_failures();

    // Batch Operation Statistics Getters
    int64_t get_batch_replica_clear_requests();
    int64_t get_batch_replica_clear_failures();
    int64_t get_batch_replica_clear_partial_successes();
    int64_t get_batch_replica_clear_items();
    int64_t get_batch_replica_clear_failed_items();
    int64_t get_batch_put_start_requests();
    int64_t get_batch_put_start_failures();
    int64_t get_batch_put_start_partial_successes();
    int64_t get_batch_put_start_items();
    int64_t get_batch_put_start_failed_items();
    int64_t get_batch_put_end_requests();
    int64_t get_batch_put_end_failures();
    int64_t get_batch_put_end_partial_successes();
    int64_t get_batch_put_end_items();
    int64_t get_batch_put_end_failed_items();
    int64_t get_batch_put_revoke_requests();
    int64_t get_batch_put_revoke_failures();
    int64_t get_batch_put_revoke_partial_successes();
    int64_t get_batch_put_revoke_items();
    int64_t get_batch_put_revoke_failed_items();

    // Eviction Metrics
    void inc_eviction_success(int64_t key_count, int64_t size);
    void inc_eviction_fail();  // not a single object is evicted

    // Eviction Metrics Getters
    int64_t get_eviction_success();
    int64_t get_eviction_attempts();
    int64_t get_evicted_key_count();
    int64_t get_evicted_size();

    // PutStart Discard Metrics
    void inc_put_start_discard_cnt(int64_t count, int64_t size);
    void inc_put_start_release_cnt(int64_t count, int64_t size);

    // PutStart Discard Metrics Getters
    int64_t get_put_start_discard_cnt();
    int64_t get_put_start_release_cnt();
    int64_t get_put_start_discarded_staging_size();

    // CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke Metrics
    void inc_copy_start_requests(int64_t val = 1);
    void inc_copy_start_failures(int64_t val = 1);
    void inc_copy_end_requests(int64_t val = 1);
    void inc_copy_end_failures(int64_t val = 1);
    void inc_copy_revoke_requests(int64_t val = 1);
    void inc_copy_revoke_failures(int64_t val = 1);
    void inc_move_start_requests(int64_t val = 1);
    void inc_move_start_failures(int64_t val = 1);
    void inc_move_end_requests(int64_t val = 1);
    void inc_move_end_failures(int64_t val = 1);
    void inc_move_revoke_requests(int64_t val = 1);
    void inc_move_revoke_failures(int64_t val = 1);

    // CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke Metrics
    // Getters
    int64_t get_copy_start_requests();
    int64_t get_copy_start_failures();
    int64_t get_copy_end_requests();
    int64_t get_copy_end_failures();
    int64_t get_copy_revoke_requests();
    int64_t get_copy_revoke_failures();
    int64_t get_move_start_requests();
    int64_t get_move_start_failures();
    int64_t get_move_end_requests();
    int64_t get_move_end_failures();
    int64_t get_move_revoke_requests();
    int64_t get_move_revoke_failures();

    // Copy, Move, QueryTask, FetchTasks, MarkTaskToComplete Metrics
    void inc_create_copy_task_requests(int64_t val = 1);
    void inc_create_copy_task_failures(int64_t val = 1);
    void inc_create_move_task_requests(int64_t val = 1);
    void inc_create_move_task_failures(int64_t val = 1);
    void inc_query_task_requests(int64_t val = 1);
    void inc_query_task_failures(int64_t val = 1);
    void inc_fetch_tasks_requests(int64_t val = 1);
    void inc_fetch_tasks_failures(int64_t val = 1);
    void inc_update_task_requests(int64_t val = 1);
    void inc_update_task_failures(int64_t val = 1);

    // Copy, Move, QueryTask, FetchTasks, MarkTaskToComplete Metrics Getters
    int64_t get_create_copy_task_requests();
    int64_t get_create_copy_task_failures();
    int64_t get_create_move_task_requests();
    int64_t get_create_move_task_failures();
    int64_t get_query_task_requests();
    int64_t get_query_task_failures();
    int64_t get_fetch_tasks_requests();
    int64_t get_fetch_tasks_failures();
    int64_t get_update_task_requests();
    int64_t get_update_task_failures();

   private:
    CentralizedMasterMetricManager();

    // Marks all arch metrics as changed once so zero values are serialized.
    void update_arch_metrics_for_zero_output();

    // Key/Value Metrics
    ylt::metric::gauge_t soft_pin_key_count_;

    // Put Pipeline Metrics
    ylt::metric::counter_t put_start_requests_;
    ylt::metric::counter_t put_start_failures_;
    ylt::metric::counter_t put_end_requests_;
    ylt::metric::counter_t put_end_failures_;
    ylt::metric::counter_t put_revoke_requests_;
    ylt::metric::counter_t put_revoke_failures_;

    // Batch Operation Statistics
    ylt::metric::counter_t batch_replica_clear_requests_;
    ylt::metric::counter_t batch_replica_clear_failures_;
    ylt::metric::counter_t batch_replica_clear_partial_successes_;
    ylt::metric::counter_t batch_replica_clear_items_;
    ylt::metric::counter_t batch_replica_clear_failed_items_;
    ylt::metric::counter_t batch_put_start_requests_;
    ylt::metric::counter_t batch_put_start_failures_;
    ylt::metric::counter_t batch_put_start_partial_successes_;
    ylt::metric::counter_t batch_put_start_items_;
    ylt::metric::counter_t batch_put_start_failed_items_;
    ylt::metric::counter_t batch_put_end_requests_;
    ylt::metric::counter_t batch_put_end_failures_;
    ylt::metric::counter_t batch_put_end_partial_successes_;
    ylt::metric::counter_t batch_put_end_items_;
    ylt::metric::counter_t batch_put_end_failed_items_;
    ylt::metric::counter_t batch_put_revoke_requests_;
    ylt::metric::counter_t batch_put_revoke_failures_;
    ylt::metric::counter_t batch_put_revoke_partial_successes_;
    ylt::metric::counter_t batch_put_revoke_items_;
    ylt::metric::counter_t batch_put_revoke_failed_items_;

    // Eviction Metrics
    ylt::metric::counter_t eviction_success_;
    ylt::metric::counter_t eviction_attempts_;
    ylt::metric::counter_t evicted_key_count_;
    ylt::metric::counter_t evicted_size_;

    // PutStart Discard Metrics
    ylt::metric::counter_t put_start_discard_cnt_;
    ylt::metric::counter_t put_start_release_cnt_;
    ylt::metric::gauge_t put_start_discarded_staging_size_;

    // CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke Metrics
    ylt::metric::counter_t copy_start_requests_;
    ylt::metric::counter_t copy_start_failures_;
    ylt::metric::counter_t copy_end_requests_;
    ylt::metric::counter_t copy_end_failures_;
    ylt::metric::counter_t copy_revoke_requests_;
    ylt::metric::counter_t copy_revoke_failures_;
    ylt::metric::counter_t move_start_requests_;
    ylt::metric::counter_t move_start_failures_;
    ylt::metric::counter_t move_end_requests_;
    ylt::metric::counter_t move_end_failures_;
    ylt::metric::counter_t move_revoke_requests_;
    ylt::metric::counter_t move_revoke_failures_;

    // Copy and Move, FetchTasks, MarkTaskToComplete Metrics
    ylt::metric::counter_t create_copy_task_requests_;
    ylt::metric::counter_t create_copy_task_failures_;
    ylt::metric::counter_t create_move_task_requests_;
    ylt::metric::counter_t create_move_task_failures_;
    ylt::metric::counter_t query_task_requests_;
    ylt::metric::counter_t query_task_failures_;
    ylt::metric::counter_t fetch_tasks_requests_;
    ylt::metric::counter_t fetch_tasks_failures_;
    ylt::metric::counter_t mark_task_to_complete_requests_;
    ylt::metric::counter_t mark_task_to_complete_failures_;
};

}  // namespace mooncake
