#include "centralized_master_metric_manager.h"

#include <iomanip>  // For std::fixed, std::setprecision
#include <new>
#include <sstream>  // For string building during serialization

#include "utils.h"

namespace mooncake {

CentralizedMasterMetricManager& CentralizedMasterMetricManager::instance() {
    static CentralizedMasterMetricManager static_instance;
    return static_instance;
}

CentralizedMasterMetricManager::CentralizedMasterMetricManager()
    : soft_pin_key_count_(
          "master_soft_pin_key_count",
          "Total number of soft-pinned keys managed by the master"),

      put_start_requests_("master_put_start_requests_total",
                          "Total number of PutStart requests received"),
      put_start_failures_("master_put_start_failures_total",
                          "Total number of failed PutStart requests"),
      put_end_requests_("master_put_end_requests_total",
                        "Total number of PutEnd requests received"),
      put_end_failures_("master_put_end_failures_total",
                        "Total number of failed PutEnd requests"),
      put_revoke_requests_("master_put_revoke_requests_total",
                           "Total number of PutRevoke requests received"),
      put_revoke_failures_("master_put_revoke_failures_total",
                           "Total number of failed PutRevoke requests"),

      batch_replica_clear_requests_(
          "master_batch_replica_clear_requests_total",
          "Total number of BatchReplicaClear requests received"),
      batch_replica_clear_failures_(
          "master_batch_replica_clear_failures_total",
          "Total number of failed BatchReplicaClear requests"),
      batch_replica_clear_partial_successes_(
          "master_batch_replica_clear_partial_successes_total",
          "Total number of partially successful BatchReplicaClear requests"),
      batch_replica_clear_items_(
          "master_batch_replica_clear_items_total",
          "Total number of items processed in BatchReplicaClear requests"),
      batch_replica_clear_failed_items_(
          "master_batch_replica_clear_failed_items_total",
          "Total number of failed items in BatchReplicaClear requests"),
      batch_put_start_requests_(
          "master_batch_put_start_requests_total",
          "Total number of BatchPutStart requests received"),
      batch_put_start_failures_(
          "master_batch_put_start_failures_total",
          "Total number of failed BatchPutStart requests"),
      batch_put_start_partial_successes_(
          "master_batch_put_start_partial_successes_total",
          "Total number of partially successful BatchPutStart requests"),
      batch_put_start_items_(
          "master_batch_put_start_items_total",
          "Total number of items processed in BatchPutStart requests"),
      batch_put_start_failed_items_(
          "master_batch_put_start_failed_items_total",
          "Total number of failed items in BatchPutStart requests"),
      batch_put_end_requests_("master_batch_put_end_requests_total",
                              "Total number of BatchPutEnd requests received"),
      batch_put_end_failures_("master_batch_put_end_failures_total",
                              "Total number of failed BatchPutEnd requests"),
      batch_put_end_partial_successes_(
          "master_batch_put_end_partial_successes_total",
          "Total number of partially successful BatchPutEnd requests"),
      batch_put_end_items_(
          "master_batch_put_end_items_total",
          "Total number of items processed in BatchPutEnd requests"),
      batch_put_end_failed_items_(
          "master_batch_put_end_failed_items_total",
          "Total number of failed items in BatchPutEnd requests"),
      batch_put_revoke_requests_(
          "master_batch_put_revoke_requests_total",
          "Total number of BatchPutRevoke requests received"),
      batch_put_revoke_failures_(
          "master_batch_put_revoke_failures_total",
          "Total number of failed BatchPutRevoke requests"),
      batch_put_revoke_partial_successes_(
          "master_batch_put_revoke_partial_successes_total",
          "Total number of partially successful BatchPutRevoke requests"),
      batch_put_revoke_items_(
          "master_batch_put_revoke_items_total",
          "Total number of items processed in BatchPutRevoke requests"),
      batch_put_revoke_failed_items_(
          "master_batch_put_revoke_failed_items_total",
          "Total number of failed items in BatchPutRevoke requests"),

      eviction_success_("master_successful_evictions_total",
                        "Total number of successful eviction operations"),
      eviction_attempts_("master_attempted_evictions_total",
                         "Total number of attempted eviction operations"),
      evicted_key_count_("master_evicted_key_count",
                         "Total number of keys evicted"),
      evicted_size_("master_evicted_size_bytes",
                    "Total bytes of evicted objects"),

      put_start_discard_cnt_("master_put_start_discard_cnt",
                             "Total number of discarded PutStart operations"),
      put_start_release_cnt_("master_put_start_release_cnt",
                             "Total number of released PutStart operations"),
      put_start_discarded_staging_size_(
          "master_put_start_discarded_staging_size",
          "Total size of memory replicas in discarded but not yet released "
          "PutStart operations"),

      copy_start_requests_("master_copy_start_requests_total",
                           "Total number of CopyStart requests received"),
      copy_start_failures_("master_copy_start_failures_total",
                           "Total number of failed CopyStart requests"),
      copy_end_requests_("master_copy_end_requests_total",
                         "Total number of CopyEnd requests received"),
      copy_end_failures_("master_copy_end_failures_total",
                         "Total number of failed CopyEnd requests"),
      copy_revoke_requests_("master_copy_revoke_requests_total",
                            "Total number of CopyRevoke requests received"),
      copy_revoke_failures_("master_copy_revoke_failures_total",
                            "Total number of failed CopyRevoke requests"),
      move_start_requests_("master_move_start_requests_total",
                           "Total number of MoveStart requests received"),
      move_start_failures_("master_move_start_failures_total",
                           "Total number of failed MoveStart requests"),
      move_end_requests_("master_move_end_requests_total",
                         "Total number of MoveEnd requests received"),
      move_end_failures_("master_move_end_failures_total",
                         "Total number of failed MoveEnd requests"),
      move_revoke_requests_("master_move_revoke_requests_total",
                            "Total number of MoveRevoke requests received"),
      move_revoke_failures_("master_move_revoke_failures_total",
                            "Total number of failed MoveRevoke requests"),

      create_copy_task_requests_("master_create_copy_task_requests_total",
                                 "Total number of Copy requests received"),
      create_copy_task_failures_("master_create_copy_task_failures_total",
                                 "Total number of failed Copy requests"),
      create_move_task_requests_("master_create_move_task_requests_total",
                                 "Total number of Move requests received"),
      create_move_task_failures_("master_create_move_task_failures_total",
                                 "Total number of failed Move requests"),
      query_task_requests_("master_query_task_requests_total",
                           "Total number of QueryTask requests received"),
      query_task_failures_("master_query_task_failures_total",
                           "Total number of failed QueryTask requests"),
      fetch_tasks_requests_("master_fetch_tasks_requests_total",
                            "Total number of FetchTasks requests received"),
      fetch_tasks_failures_("master_fetch_tasks_failures_total",
                            "Total number of failed FetchTasks requests"),
      mark_task_to_complete_requests_(
          "master_update_task_requests_total",
          "Total number of MarkTaskToComplete requests received"),
      mark_task_to_complete_failures_(
          "master_update_task_failures_total",
          "Total number of failed MarkTaskToComplete requests") {
    update_arch_metrics_for_zero_output();
    RegisterInstance(this);
}

void CentralizedMasterMetricManager::reset_all_metrics() {
    this->~CentralizedMasterMetricManager();
    new (this) CentralizedMasterMetricManager();
}

void CentralizedMasterMetricManager::update_arch_metrics_for_zero_output() {
    // update(0)/inc(0) marks metrics changed so zeros serialize.
    soft_pin_key_count_.update(0);
    put_start_discarded_staging_size_.update(0);

    put_start_requests_.inc(0);
    put_start_failures_.inc(0);
    put_end_requests_.inc(0);
    put_end_failures_.inc(0);
    put_revoke_requests_.inc(0);
    put_revoke_failures_.inc(0);

    batch_replica_clear_requests_.inc(0);
    batch_replica_clear_failures_.inc(0);
    batch_replica_clear_partial_successes_.inc(0);
    batch_replica_clear_items_.inc(0);
    batch_replica_clear_failed_items_.inc(0);
    batch_put_start_requests_.inc(0);
    batch_put_start_failures_.inc(0);
    batch_put_start_partial_successes_.inc(0);
    batch_put_start_items_.inc(0);
    batch_put_start_failed_items_.inc(0);
    batch_put_end_requests_.inc(0);
    batch_put_end_failures_.inc(0);
    batch_put_end_partial_successes_.inc(0);
    batch_put_end_items_.inc(0);
    batch_put_end_failed_items_.inc(0);
    batch_put_revoke_requests_.inc(0);
    batch_put_revoke_failures_.inc(0);
    batch_put_revoke_partial_successes_.inc(0);
    batch_put_revoke_items_.inc(0);
    batch_put_revoke_failed_items_.inc(0);

    copy_start_requests_.inc(0);
    copy_start_failures_.inc(0);
    copy_end_requests_.inc(0);
    copy_end_failures_.inc(0);
    copy_revoke_requests_.inc(0);
    copy_revoke_failures_.inc(0);
    move_start_requests_.inc(0);
    move_start_failures_.inc(0);
    move_end_requests_.inc(0);
    move_end_failures_.inc(0);
    move_revoke_requests_.inc(0);
    move_revoke_failures_.inc(0);

    create_copy_task_requests_.inc(0);
    create_copy_task_failures_.inc(0);
    create_move_task_requests_.inc(0);
    create_move_task_failures_.inc(0);
    query_task_requests_.inc(0);
    query_task_failures_.inc(0);
    fetch_tasks_requests_.inc(0);
    fetch_tasks_failures_.inc(0);
    mark_task_to_complete_requests_.inc(0);
    mark_task_to_complete_failures_.inc(0);

    eviction_success_.inc(0);
    eviction_attempts_.inc(0);
    evicted_key_count_.inc(0);
    evicted_size_.inc(0);

    put_start_discard_cnt_.inc(0);
    put_start_release_cnt_.inc(0);
}

// Key/Value Metrics
void CentralizedMasterMetricManager::inc_soft_pin_key_count(int64_t val) {
    soft_pin_key_count_.inc(val);
}

void CentralizedMasterMetricManager::dec_soft_pin_key_count(int64_t val) {
    soft_pin_key_count_.dec(val);
}

int64_t CentralizedMasterMetricManager::get_soft_pin_key_count() {
    return soft_pin_key_count_.value();
}

// Operation Statistics (Counters)
void CentralizedMasterMetricManager::inc_put_start_requests(int64_t val) {
    put_start_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_put_start_failures(int64_t val) {
    put_start_failures_.inc(val);
}

void CentralizedMasterMetricManager::inc_put_end_requests(int64_t val) {
    put_end_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_put_end_failures(int64_t val) {
    put_end_failures_.inc(val);
}

void CentralizedMasterMetricManager::inc_put_revoke_requests(int64_t val) {
    put_revoke_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_put_revoke_failures(int64_t val) {
    put_revoke_failures_.inc(val);
}

// Batch Operation Statistics (Counters)
void CentralizedMasterMetricManager::inc_batch_replica_clear_requests(
    int64_t items) {
    batch_replica_clear_requests_.inc(1);
    batch_replica_clear_items_.inc(items);
}

void CentralizedMasterMetricManager::inc_batch_replica_clear_failures(
    int64_t failed_items) {
    batch_replica_clear_failures_.inc(1);
    batch_replica_clear_failed_items_.inc(failed_items);
}

void CentralizedMasterMetricManager::inc_batch_replica_clear_partial_success(
    int64_t failed_items) {
    batch_replica_clear_partial_successes_.inc(1);
    batch_replica_clear_failed_items_.inc(failed_items);
}

void CentralizedMasterMetricManager::inc_batch_put_start_requests(
    int64_t items) {
    batch_put_start_requests_.inc(1);
    batch_put_start_items_.inc(items);
}

void CentralizedMasterMetricManager::inc_batch_put_start_failures(
    int64_t failed_items) {
    batch_put_start_failures_.inc(1);
    batch_put_start_failed_items_.inc(failed_items);
}

void CentralizedMasterMetricManager::inc_batch_put_start_partial_success(
    int64_t failed_items) {
    batch_put_start_partial_successes_.inc(1);
    batch_put_start_failed_items_.inc(failed_items);
}

void CentralizedMasterMetricManager::inc_batch_put_end_requests(int64_t items) {
    batch_put_end_requests_.inc(1);
    batch_put_end_items_.inc(items);
}

void CentralizedMasterMetricManager::inc_batch_put_end_failures(
    int64_t failed_items) {
    batch_put_end_failures_.inc(1);
    batch_put_end_failed_items_.inc(failed_items);
}

void CentralizedMasterMetricManager::inc_batch_put_end_partial_success(
    int64_t failed_items) {
    batch_put_end_partial_successes_.inc(1);
    batch_put_end_failed_items_.inc(failed_items);
}

void CentralizedMasterMetricManager::inc_batch_put_revoke_requests(
    int64_t items) {
    batch_put_revoke_requests_.inc(1);
    batch_put_revoke_items_.inc(items);
}

void CentralizedMasterMetricManager::inc_batch_put_revoke_failures(
    int64_t failed_items) {
    batch_put_revoke_failures_.inc(1);
    batch_put_revoke_failed_items_.inc(failed_items);
}

void CentralizedMasterMetricManager::inc_batch_put_revoke_partial_success(
    int64_t failed_items) {
    batch_put_revoke_partial_successes_.inc(1);
    batch_put_revoke_failed_items_.inc(failed_items);
}

// Operation Statistics Getters
int64_t CentralizedMasterMetricManager::get_put_start_requests() {
    return put_start_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_put_start_failures() {
    return put_start_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_put_end_requests() {
    return put_end_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_put_end_failures() {
    return put_end_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_put_revoke_requests() {
    return put_revoke_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_put_revoke_failures() {
    return put_revoke_failures_.value();
}

// Batch Operation Statistics Getters
int64_t CentralizedMasterMetricManager::get_batch_replica_clear_requests() {
    return batch_replica_clear_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_replica_clear_failures() {
    return batch_replica_clear_failures_.value();
}

int64_t
CentralizedMasterMetricManager::get_batch_replica_clear_partial_successes() {
    return batch_replica_clear_partial_successes_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_replica_clear_items() {
    return batch_replica_clear_items_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_replica_clear_failed_items() {
    return batch_replica_clear_failed_items_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_start_requests() {
    return batch_put_start_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_start_failures() {
    return batch_put_start_failures_.value();
}

int64_t
CentralizedMasterMetricManager::get_batch_put_start_partial_successes() {
    return batch_put_start_partial_successes_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_start_items() {
    return batch_put_start_items_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_start_failed_items() {
    return batch_put_start_failed_items_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_end_requests() {
    return batch_put_end_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_end_failures() {
    return batch_put_end_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_end_partial_successes() {
    return batch_put_end_partial_successes_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_end_items() {
    return batch_put_end_items_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_end_failed_items() {
    return batch_put_end_failed_items_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_revoke_requests() {
    return batch_put_revoke_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_revoke_failures() {
    return batch_put_revoke_failures_.value();
}

int64_t
CentralizedMasterMetricManager::get_batch_put_revoke_partial_successes() {
    return batch_put_revoke_partial_successes_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_revoke_items() {
    return batch_put_revoke_items_.value();
}

int64_t CentralizedMasterMetricManager::get_batch_put_revoke_failed_items() {
    return batch_put_revoke_failed_items_.value();
}

// Eviction Metrics
void CentralizedMasterMetricManager::inc_eviction_success(int64_t key_count,
                                                          int64_t size) {
    evicted_key_count_.inc(key_count);
    evicted_size_.inc(size);
    eviction_success_.inc();
    eviction_attempts_.inc();
}

void CentralizedMasterMetricManager::inc_eviction_fail() {
    eviction_attempts_.inc();
}

int64_t CentralizedMasterMetricManager::get_eviction_success() {
    return eviction_success_.value();
}

int64_t CentralizedMasterMetricManager::get_eviction_attempts() {
    return eviction_attempts_.value();
}

int64_t CentralizedMasterMetricManager::get_evicted_key_count() {
    return evicted_key_count_.value();
}

int64_t CentralizedMasterMetricManager::get_evicted_size() {
    return evicted_size_.value();
}

// PutStart Discard Metrics
void CentralizedMasterMetricManager::inc_put_start_discard_cnt(int64_t count,
                                                               int64_t size) {
    put_start_discard_cnt_.inc(count);
    put_start_discarded_staging_size_.inc(size);
}

void CentralizedMasterMetricManager::inc_put_start_release_cnt(int64_t count,
                                                               int64_t size) {
    put_start_release_cnt_.inc(count);
    put_start_discarded_staging_size_.dec(size);
}

int64_t CentralizedMasterMetricManager::get_put_start_discard_cnt() {
    return put_start_discard_cnt_.value();
}

int64_t CentralizedMasterMetricManager::get_put_start_release_cnt() {
    return put_start_release_cnt_.value();
}

int64_t CentralizedMasterMetricManager::get_put_start_discarded_staging_size() {
    return put_start_discarded_staging_size_.value();
}

// CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke Metrics
void CentralizedMasterMetricManager::inc_copy_start_requests(int64_t val) {
    copy_start_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_copy_start_failures(int64_t val) {
    copy_start_failures_.inc(val);
}

void CentralizedMasterMetricManager::inc_copy_end_requests(int64_t val) {
    copy_end_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_copy_end_failures(int64_t val) {
    copy_end_failures_.inc(val);
}

void CentralizedMasterMetricManager::inc_copy_revoke_requests(int64_t val) {
    copy_revoke_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_copy_revoke_failures(int64_t val) {
    copy_revoke_failures_.inc(val);
}

void CentralizedMasterMetricManager::inc_move_start_requests(int64_t val) {
    move_start_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_move_start_failures(int64_t val) {
    move_start_failures_.inc(val);
}

void CentralizedMasterMetricManager::inc_move_end_requests(int64_t val) {
    move_end_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_move_end_failures(int64_t val) {
    move_end_failures_.inc(val);
}

void CentralizedMasterMetricManager::inc_move_revoke_requests(int64_t val) {
    move_revoke_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_move_revoke_failures(int64_t val) {
    move_revoke_failures_.inc(val);
}

int64_t CentralizedMasterMetricManager::get_copy_start_requests() {
    return copy_start_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_copy_start_failures() {
    return copy_start_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_copy_end_requests() {
    return copy_end_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_copy_end_failures() {
    return copy_end_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_copy_revoke_requests() {
    return copy_revoke_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_copy_revoke_failures() {
    return copy_revoke_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_move_start_requests() {
    return move_start_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_move_start_failures() {
    return move_start_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_move_end_requests() {
    return move_end_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_move_end_failures() {
    return move_end_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_move_revoke_requests() {
    return move_revoke_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_move_revoke_failures() {
    return move_revoke_failures_.value();
}

// Copy, Move, QueryTask, FetchTasks, MarkTaskToComplete Metrics
void CentralizedMasterMetricManager::inc_create_copy_task_requests(
    int64_t val) {
    create_copy_task_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_create_copy_task_failures(
    int64_t val) {
    create_copy_task_failures_.inc(val);
}

void CentralizedMasterMetricManager::inc_create_move_task_requests(
    int64_t val) {
    create_move_task_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_create_move_task_failures(
    int64_t val) {
    create_move_task_failures_.inc(val);
}

void CentralizedMasterMetricManager::inc_query_task_requests(int64_t val) {
    query_task_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_query_task_failures(int64_t val) {
    query_task_failures_.inc(val);
}

void CentralizedMasterMetricManager::inc_fetch_tasks_requests(int64_t val) {
    fetch_tasks_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_fetch_tasks_failures(int64_t val) {
    fetch_tasks_failures_.inc(val);
}

void CentralizedMasterMetricManager::inc_update_task_requests(int64_t val) {
    mark_task_to_complete_requests_.inc(val);
}

void CentralizedMasterMetricManager::inc_update_task_failures(int64_t val) {
    mark_task_to_complete_failures_.inc(val);
}

// Task create, query, fetch Metrics Getters
int64_t CentralizedMasterMetricManager::get_create_copy_task_requests() {
    return create_copy_task_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_create_copy_task_failures() {
    return create_copy_task_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_create_move_task_requests() {
    return create_move_task_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_create_move_task_failures() {
    return create_move_task_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_query_task_requests() {
    return query_task_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_query_task_failures() {
    return query_task_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_fetch_tasks_requests() {
    return fetch_tasks_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_fetch_tasks_failures() {
    return fetch_tasks_failures_.value();
}

int64_t CentralizedMasterMetricManager::get_update_task_requests() {
    return mark_task_to_complete_requests_.value();
}

int64_t CentralizedMasterMetricManager::get_update_task_failures() {
    return mark_task_to_complete_failures_.value();
}

// --- Serialization ---
std::string CentralizedMasterMetricManager::serialize_metrics() {
    std::string out = MasterMetricManager::serialize_metrics();
    std::stringstream ss;

    auto serialize_metric = [&ss](auto& metric) {
        std::string metric_str;
        metric.serialize(metric_str);
        ss << metric_str;
    };

    serialize_metric(soft_pin_key_count_);

    serialize_metric(put_start_requests_);
    serialize_metric(put_start_failures_);
    serialize_metric(put_end_requests_);
    serialize_metric(put_end_failures_);
    serialize_metric(put_revoke_requests_);
    serialize_metric(put_revoke_failures_);

    serialize_metric(copy_start_requests_);
    serialize_metric(copy_start_failures_);
    serialize_metric(copy_end_requests_);
    serialize_metric(copy_end_failures_);
    serialize_metric(copy_revoke_requests_);
    serialize_metric(copy_revoke_failures_);
    serialize_metric(move_start_requests_);
    serialize_metric(move_start_failures_);
    serialize_metric(move_end_requests_);
    serialize_metric(move_end_failures_);
    serialize_metric(move_revoke_requests_);
    serialize_metric(move_revoke_failures_);

    serialize_metric(create_copy_task_requests_);
    serialize_metric(create_copy_task_failures_);
    serialize_metric(create_move_task_requests_);
    serialize_metric(create_move_task_failures_);
    serialize_metric(mark_task_to_complete_requests_);
    serialize_metric(mark_task_to_complete_failures_);
    serialize_metric(query_task_requests_);
    serialize_metric(query_task_failures_);
    serialize_metric(fetch_tasks_requests_);
    serialize_metric(fetch_tasks_failures_);

    serialize_metric(batch_replica_clear_requests_);
    serialize_metric(batch_replica_clear_failures_);
    serialize_metric(batch_put_start_requests_);
    serialize_metric(batch_put_start_failures_);
    serialize_metric(batch_put_end_requests_);
    serialize_metric(batch_put_end_failures_);
    serialize_metric(batch_put_revoke_requests_);
    serialize_metric(batch_put_revoke_failures_);

    serialize_metric(eviction_success_);
    serialize_metric(eviction_attempts_);
    serialize_metric(evicted_key_count_);
    serialize_metric(evicted_size_);

    serialize_metric(put_start_discard_cnt_);
    serialize_metric(put_start_release_cnt_);
    serialize_metric(put_start_discarded_staging_size_);

    return ss.str();
}

// --- Human-Readable Summary ---
std::string CentralizedMasterMetricManager::get_summary_string() {
    std::string summary = "[Arch: Centralization] ";
    summary += MasterMetricManager::get_summary_string();
    std::stringstream ss;

    int64_t soft_pin_keys = soft_pin_key_count_.value();

    int64_t put_starts = put_start_requests_.value();
    int64_t put_start_fails = put_start_failures_.value();
    int64_t put_ends = put_end_requests_.value();
    int64_t put_end_fails = put_end_failures_.value();
    int64_t put_revoke_requests = put_revoke_requests_.value();
    int64_t put_revoke_fails = put_revoke_failures_.value();
    int64_t create_move_tasks = create_move_task_requests_.value();
    int64_t create_move_task_fails = create_move_task_failures_.value();
    int64_t create_copy_tasks = create_copy_task_requests_.value();
    int64_t create_copy_task_fails = create_copy_task_failures_.value();
    int64_t query_tasks = query_task_requests_.value();
    int64_t query_task_fails = query_task_failures_.value();
    int64_t fetch_tasks = fetch_tasks_requests_.value();
    int64_t fetch_task_fails = fetch_tasks_failures_.value();

    int64_t copy_starts = copy_start_requests_.value();
    int64_t copy_start_fails = copy_start_failures_.value();
    int64_t copy_ends = copy_end_requests_.value();
    int64_t copy_end_fails = copy_end_failures_.value();
    int64_t copy_revokes = copy_revoke_requests_.value();
    int64_t copy_revoke_fails = copy_revoke_failures_.value();
    int64_t move_starts = move_start_requests_.value();
    int64_t move_start_fails = move_start_failures_.value();
    int64_t move_ends = move_end_requests_.value();
    int64_t move_end_fails = move_end_failures_.value();
    int64_t move_revokes = move_revoke_requests_.value();
    int64_t move_revoke_fails = move_revoke_failures_.value();

    int64_t batch_put_start_requests = batch_put_start_requests_.value();
    int64_t batch_put_start_fails = batch_put_start_failures_.value();
    int64_t batch_put_start_partial_successes =
        batch_put_start_partial_successes_.value();
    int64_t batch_put_start_items = batch_put_start_items_.value();
    int64_t batch_put_start_failed_items =
        batch_put_start_failed_items_.value();
    int64_t batch_put_end_requests = batch_put_end_requests_.value();
    int64_t batch_put_end_fails = batch_put_end_failures_.value();
    int64_t batch_put_end_partial_successes =
        batch_put_end_partial_successes_.value();
    int64_t batch_put_end_items = batch_put_end_items_.value();
    int64_t batch_put_end_failed_items = batch_put_end_failed_items_.value();
    int64_t batch_put_revoke_requests = batch_put_revoke_requests_.value();
    int64_t batch_put_revoke_fails = batch_put_revoke_failures_.value();
    int64_t batch_put_revoke_partial_successes =
        batch_put_revoke_partial_successes_.value();
    int64_t batch_put_revoke_items = batch_put_revoke_items_.value();
    int64_t batch_put_revoke_failed_items =
        batch_put_revoke_failed_items_.value();
    int64_t batch_replica_clear_requests =
        batch_replica_clear_requests_.value();
    int64_t batch_replica_clear_fails = batch_replica_clear_failures_.value();
    int64_t batch_replica_clear_partial_successes =
        batch_replica_clear_partial_successes_.value();
    int64_t batch_replica_clear_items = batch_replica_clear_items_.value();
    int64_t batch_replica_clear_failed_items =
        batch_replica_clear_failed_items_.value();

    int64_t eviction_success = eviction_success_.value();
    int64_t eviction_attempts = eviction_attempts_.value();
    int64_t evicted_key_count = evicted_key_count_.value();
    int64_t evicted_size = evicted_size_.value();

    int64_t put_start_discard_cnt = put_start_discard_cnt_.value();
    int64_t put_start_release_cnt = put_start_release_cnt_.value();
    int64_t put_start_discarded_staging_size =
        put_start_discarded_staging_size_.value();

    ss << "Soft-pinned keys: " << soft_pin_keys;

    ss << " | Requests (Success/Total): ";
    ss << "PutStart=" << put_starts - put_start_fails << "/" << put_starts
       << ", ";
    ss << "PutEnd=" << put_ends - put_end_fails << "/" << put_ends << ", ";
    ss << "PutRevoke=" << put_revoke_requests - put_revoke_fails << "/"
       << put_revoke_requests << ", ";
    ss << "CopyStart=" << copy_starts - copy_start_fails << "/" << copy_starts
       << ", ";
    ss << "CopyEnd=" << copy_ends - copy_end_fails << "/" << copy_ends << ", ";
    ss << "CopyRevoke=" << copy_revokes - copy_revoke_fails << "/"
       << copy_revokes << ", ";
    ss << "MoveStart=" << move_starts - move_start_fails << "/" << move_starts
       << ", ";
    ss << "MoveEnd=" << move_ends - move_end_fails << "/" << move_ends << ", ";
    ss << "MoveRevoke=" << move_revokes - move_revoke_fails << "/"
       << move_revokes;

    ss << " | Batch Requests "
          "(Req=Success/PartialSuccess/Total, Item=Success/Total): ";
    ss << "PutStart:(Req="
       << batch_put_start_requests - batch_put_start_fails -
              batch_put_start_partial_successes
       << "/" << batch_put_start_partial_successes << "/"
       << batch_put_start_requests
       << ", Item=" << batch_put_start_items - batch_put_start_failed_items
       << "/" << batch_put_start_items << "), ";
    ss << "PutEnd:(Req="
       << batch_put_end_requests - batch_put_end_fails -
              batch_put_end_partial_successes
       << "/" << batch_put_end_partial_successes << "/"
       << batch_put_end_requests
       << ", Item=" << batch_put_end_items - batch_put_end_failed_items << "/"
       << batch_put_end_items << "), ";
    ss << "PutRevoke:(Req="
       << batch_put_revoke_requests - batch_put_revoke_fails -
              batch_put_revoke_partial_successes
       << "/" << batch_put_revoke_partial_successes << "/"
       << batch_put_revoke_requests
       << ", Item=" << batch_put_revoke_items - batch_put_revoke_failed_items
       << "/" << batch_put_revoke_items << "), ";
    ss << "Clear:(Req="
       << batch_replica_clear_requests - batch_replica_clear_fails -
              batch_replica_clear_partial_successes
       << "/" << batch_replica_clear_partial_successes << "/"
       << batch_replica_clear_requests << ", Item="
       << batch_replica_clear_items - batch_replica_clear_failed_items << "/"
       << batch_replica_clear_items << "), ";

    ss << "CreateMoveTask:(Req=" << create_move_tasks - create_move_task_fails
       << "/" << create_move_tasks << "), ";
    ss << "CreateCopyTask:(Req=" << create_copy_tasks - create_copy_task_fails
       << "/" << create_copy_tasks << "), ";
    ss << "QueryTask=(Req=" << query_tasks - query_task_fails << "/"
       << query_tasks << "), ";
    ss << "FetchTasks=(Req=" << fetch_tasks - fetch_task_fails << "/"
       << fetch_tasks << "), ";
    ss << "MarkTaskToComplete= (Req="
       << mark_task_to_complete_requests_.value() -
              mark_task_to_complete_failures_.value()
       << "/" << mark_task_to_complete_requests_.value() << ")";

    ss << " | Eviction: "
       << "Success/Attempts=" << eviction_success << "/" << eviction_attempts
       << ", "
       << "keys=" << evicted_key_count << ", "
       << "size=" << byte_size_to_string(evicted_size);

    ss << " | Discard: "
       << "Released/Total=" << put_start_release_cnt << "/"
       << put_start_discard_cnt << ", StagingSize="
       << byte_size_to_string(put_start_discarded_staging_size);

    summary += " | ";
    summary += ss.str();
    return summary;
}

}  // namespace mooncake
