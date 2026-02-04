#include "master_metric_manager.h"

#include <glog/logging.h>
#include <iomanip>  // For std::fixed, std::setprecision
#include <sstream>  // For string building during serialization
#include <vector>   // Required by histogram serialization
#include <cmath>

#include "utils.h"

namespace mooncake {

// --- Singleton Instance ---
MasterMetricManager& MasterMetricManager::instance() {
    // Guaranteed to be lazy initialized and thread-safe in C++11+
    static MasterMetricManager static_instance;
    return static_instance;
}

// --- Constructor ---
MasterMetricManager::MasterMetricManager()
    // Initialize Gauges
    : mem_allocated_size_(
          "master_allocated_bytes",
          "Total memory bytes currently allocated across all segments"),
      mem_total_capacity_("master_total_capacity_bytes",
                          "Total memory capacity across all mounted segments"),
      mem_allocated_size_per_segment_(
          "segment_allocated_bytes",
          "Total memory bytes currently allocated of the segment", {"segment"}),
      mem_total_capacity_per_segment_(
          "segment_total_capacity_bytes",
          "Total memory capacity of the mounted segment", {"segment"}),
      file_allocated_size_(
          "master_allocated_file_size_bytes",
          "Total bytes currently allocated for file storage in 3fs/nfs"),
      file_total_capacity_("master_total_file_capacity_bytes",
                           "Total capacity for file storage in 3fs/nfs"),
      key_count_("master_key_count",
                 "Total number of keys managed by the master"),
      soft_pin_key_count_(
          "master_soft_pin_key_count",
          "Total number of soft-pinned keys managed by the master"),
      // Initialize Histogram (4KB, 64KB, 256KB, 1MB, 4MB, 16MB, 64MB)
      value_size_distribution_(
          "master_value_size_bytes", "Distribution of object value sizes",
          {4096, 65536, 262144, 1048576, 4194304, 16777216, 67108864}),
      // Initialize cluster metrics
      active_clients_("master_active_clients",
                      "Total number of active clients"),

      // Initialize Request Counters
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
      get_replica_list_requests_(
          "master_get_replica_list_requests_total",
          "Total number of GetReplicaList requests received"),
      get_replica_list_failures_(
          "master_get_replica_list_failures_total",
          "Total number of failed GetReplicaList requests"),
      get_replica_list_by_regex_requests_(
          "master_get_replica_list_by_regex_requests_total",
          "Total number of GetReplicaListByRegex requests received"),
      get_replica_list_by_regex_failures_(
          "master_get_replica_list_by_regex_failures_total",
          "Total number of failed GetReplicaListByRegex requests"),
      exist_key_requests_("master_exist_key_requests_total",
                          "Total number of ExistKey requests received"),
      exist_key_failures_("master_exist_key_failures_total",
                          "Total number of failed ExistKey requests"),
      remove_requests_("master_remove_requests_total",
                       "Total number of Remove requests received"),
      remove_failures_("master_remove_failures_total",
                       "Total number of failed Remove requests"),
      remove_by_regex_requests_(
          "master_remove_by_regex_requests_total",
          "Total number of RemoveByRegex requests received"),
      remove_by_regex_failures_(
          "master_remove_by_regex_failures_total",
          "Total number of failed RemoveByRegex requests"),
      remove_all_requests_("master_remove_all_requests_total",
                           "Total number of Remove all requests received"),
      remove_all_failures_("master_remove_all_failures_total",
                           "Total number of failed Remove all requests"),

      mount_segment_requests_("master_mount_segment_requests_total",
                              "Total number of MountSegment requests received"),
      mount_segment_failures_("master_mount_segment_failures_total",
                              "Total number of failed MountSegment requests"),
      unmount_segment_requests_(
          "master_unmount_segment_requests_total",
          "Total number of UnmountSegment requests received"),
      unmount_segment_failures_(
          "master_unmount_segment_failures_total",
          "Total number of failed UnmountSegment requests"),
      remount_segment_requests_(
          "master_remount_segment_requests_total",
          "Total number of RemountSegment requests received"),
      remount_segment_failures_(
          "master_remount_segment_failures_total",
          "Total number of failed RemountSegment requests"),
      ping_requests_("master_ping_requests_total",
                     "Total number of ping requests received"),
      ping_failures_("master_ping_failures_total",
                     "Total number of failed ping requests"),

      // Initialize Batch Request Counters
      batch_exist_key_requests_(
          "master_batch_exist_key_requests_total",
          "Total number of BatchExistKey requests received"),
      batch_exist_key_failures_(
          "master_batch_exist_key_failures_total",
          "Total number of failed BatchExistKey requests"),
      batch_exist_key_partial_successes_(
          "master_batch_exist_key_partial_successes_total",
          "Total number of partially successful BatchExistKey requests"),
      batch_exist_key_items_(
          "master_batch_exist_key_items_total",
          "Total number of items processed in BatchExistKey requests"),
      batch_exist_key_failed_items_(
          "master_batch_exist_key_failed_items_total",
          "Total number of failed items in BatchExistKey requests"),
      batch_query_ip_requests_(
          "master_batch_query_ip_requests_total",
          "Total number of BatchQueryIp requests received"),
      batch_query_ip_failures_("master_batch_query_ip_failures_total",
                               "Total number of failed BatchQueryIp requests"),
      batch_query_ip_partial_successes_(
          "master_batch_query_ip_partial_successes_total",
          "Total number of partially successful BatchQueryIp requests"),
      batch_query_ip_items_(
          "master_batch_query_ip_items_total",
          "Total number of items processed in BatchQueryIp requests"),
      batch_query_ip_failed_items_(
          "master_batch_query_ip_failed_items_total",
          "Total number of failed items in BatchQueryIp requests"),
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
      batch_get_replica_list_requests_(
          "master_batch_get_replica_list_requests_total",
          "Total number of BatchGetReplicaList requests received"),
      batch_get_replica_list_failures_(
          "master_batch_get_replica_list_failures_total",
          "Total number of failed BatchGetReplicaList requests"),
      batch_get_replica_list_partial_successes_(
          "master_batch_get_replica_list_partial_successes_total",
          "Total number of partially successful BatchGetReplicaList requests"),
      batch_get_replica_list_items_(
          "master_batch_get_replica_list_items_total",
          "Total number of items processed in BatchGetReplicaList requests"),
      batch_get_replica_list_failed_items_(
          "master_batch_get_replica_list_failed_items_total",
          "Total number of failed items in BatchGetReplicaList requests"),
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

      // Initialize cache hit rate metrics
      mem_cache_hit_nums_("mem_cache_hit_nums_",
                          "Total number of cache hits in the memory pool"),
      file_cache_hit_nums_("file_cache_hit_nums_",
                           "Total number of cache hits in the ssd"),
      mem_cache_nums_("mem_cache_nums_",
                      "Total number of cached values in the memory pool"),
      file_cache_nums_("file_cache_nums_",
                       "Total number of cached values in the ssd"),
      valid_get_nums_("valid_get_nums_",
                      "Total number of valid get operations"),
      total_get_nums_("total_get_nums_", "Total number of get operations"),

      // Initialize Eviction Counters
      eviction_success_("master_successful_evictions_total",
                        "Total number of successful eviction operations"),
      eviction_attempts_("master_attempted_evictions_total",
                         "Total number of attempted eviction operations"),
      evicted_key_count_("master_evicted_key_count",
                         "Total number of keys evicted"),
      evicted_size_("master_evicted_size_bytes",
                    "Total bytes of evicted objects"),

      // Initialize Discarded Replicas Counters
      put_start_discard_cnt_("master_put_start_discard_cnt",
                             "Total number of discarded PutStart operations"),
      put_start_release_cnt_("master_put_start_release_cnt",
                             "Total number of released PutStart operations"),
      put_start_discarded_staging_size_(
          "master_put_start_discarded_staging_size",
          "Total size of memory replicas in discarded but not yet released "
          "PutStart operations"),

      // Initialize CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd,
      // MoveRevoke Counters
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

      /*
       * Initialize CreateMoveTask, CreateCopyTask, QueryTask, FetchTasks,
       * MarkTaskToComplete Counters
       */
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
    // Update all metrics once to ensure zero values are serialized
    update_metrics_for_zero_output();
}

// --- Metric Interface Methods ---

void MasterMetricManager::update_metrics_for_zero_output() {
    // Update Gauges (use update(0) to mark as changed)
    mem_allocated_size_.update(0);
    mem_total_capacity_.update(0);
    file_allocated_size_.update(0);
    file_total_capacity_.update(0);
    key_count_.update(0);
    soft_pin_key_count_.update(0);
    active_clients_.update(0);
    mem_cache_nums_.update(0);
    file_cache_nums_.update(0);
    put_start_discarded_staging_size_.update(0);

    // Update Counters (use inc(0) to mark as changed)
    put_start_requests_.inc(0);
    put_start_failures_.inc(0);
    put_end_requests_.inc(0);
    put_end_failures_.inc(0);
    put_revoke_requests_.inc(0);
    put_revoke_failures_.inc(0);
    get_replica_list_requests_.inc(0);
    get_replica_list_failures_.inc(0);
    get_replica_list_by_regex_requests_.inc(0);
    get_replica_list_by_regex_failures_.inc(0);
    exist_key_requests_.inc(0);
    exist_key_failures_.inc(0);
    remove_requests_.inc(0);
    remove_failures_.inc(0);
    remove_by_regex_requests_.inc(0);
    remove_by_regex_failures_.inc(0);
    remove_all_requests_.inc(0);
    remove_all_failures_.inc(0);
    mount_segment_requests_.inc(0);
    mount_segment_failures_.inc(0);
    unmount_segment_requests_.inc(0);
    unmount_segment_failures_.inc(0);
    remount_segment_requests_.inc(0);
    remount_segment_failures_.inc(0);
    ping_requests_.inc(0);
    ping_failures_.inc(0);
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

    // Update CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke
    // counters
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

    // Update Batch Request Counters
    batch_exist_key_requests_.inc(0);
    batch_exist_key_failures_.inc(0);
    batch_exist_key_partial_successes_.inc(0);
    batch_exist_key_items_.inc(0);
    batch_exist_key_failed_items_.inc(0);
    batch_query_ip_requests_.inc(0);
    batch_query_ip_failures_.inc(0);
    batch_query_ip_partial_successes_.inc(0);
    batch_query_ip_items_.inc(0);
    batch_query_ip_failed_items_.inc(0);
    batch_replica_clear_requests_.inc(0);
    batch_replica_clear_failures_.inc(0);
    batch_replica_clear_partial_successes_.inc(0);
    batch_replica_clear_items_.inc(0);
    batch_replica_clear_failed_items_.inc(0);
    batch_get_replica_list_requests_.inc(0);
    batch_get_replica_list_failures_.inc(0);
    batch_get_replica_list_partial_successes_.inc(0);
    batch_get_replica_list_items_.inc(0);
    batch_get_replica_list_failed_items_.inc(0);
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

    // Update cache hit rate metrics
    mem_cache_hit_nums_.inc(0);
    file_cache_hit_nums_.inc(0);
    valid_get_nums_.inc(0);
    total_get_nums_.inc(0);

    // Update Eviction Counters
    eviction_success_.inc(0);
    eviction_attempts_.inc(0);
    evicted_key_count_.inc(0);
    evicted_size_.inc(0);

    // Update PutStart Discard Metrics
    put_start_discard_cnt_.inc(0);
    put_start_release_cnt_.inc(0);

    // Update Histogram (use observe(0) to mark as changed)
    value_size_distribution_.observe(0);

    // Note: dynamic_gauge_1t (mem_allocated_size_per_segment_ and
    // mem_total_capacity_per_segment_) are not initialized here because they
    // require label values. They will be initialized when first used with
    // actual segment names.
}

// Memory Storage Metrics
void MasterMetricManager::inc_allocated_mem_size(const std::string& segment,
                                                 int64_t val) {
    mem_allocated_size_.inc(val);
    if (!segment.empty()) mem_allocated_size_per_segment_.inc({segment}, val);
}

void MasterMetricManager::dec_allocated_mem_size(const std::string& segment,
                                                 int64_t val) {
    mem_allocated_size_.dec(val);
    if (!segment.empty()) mem_allocated_size_per_segment_.dec({segment}, val);
}

void MasterMetricManager::reset_allocated_mem_size() {
    mem_allocated_size_.reset();
}

void MasterMetricManager::inc_total_mem_capacity(const std::string& segment,
                                                 int64_t val) {
    mem_total_capacity_.inc(val);
    if (!segment.empty()) mem_total_capacity_per_segment_.inc({segment}, val);
}

void MasterMetricManager::dec_total_mem_capacity(const std::string& segment,
                                                 int64_t val) {
    mem_total_capacity_.dec(val);
    if (!segment.empty()) mem_total_capacity_per_segment_.dec({segment}, val);
}

void MasterMetricManager::reset_total_mem_capacity() {
    mem_total_capacity_.reset();
}

int64_t MasterMetricManager::get_allocated_mem_size() {
    return mem_allocated_size_.value();
}

int64_t MasterMetricManager::get_total_mem_capacity() {
    return mem_total_capacity_.value();
}

double MasterMetricManager::get_global_mem_used_ratio(void) {
    double allocated = mem_allocated_size_.value();
    double capacity = mem_total_capacity_.value();
    if (capacity == 0) {
        return 0.0;
    }
    return allocated / capacity;
}

int64_t MasterMetricManager::get_segment_allocated_mem_size(
    const std::string& segment) {
    return mem_allocated_size_per_segment_.value({segment});
}

int64_t MasterMetricManager::get_segment_total_mem_capacity(
    const std::string& segment) {
    return mem_total_capacity_per_segment_.value({segment});
}

double MasterMetricManager::get_segment_mem_used_ratio(
    const std::string& segment) {
    double allocated = get_segment_allocated_mem_size(segment);
    double capacity = get_segment_total_mem_capacity(segment);
    if (capacity == 0) {
        return 0.0;
    }
    return allocated / capacity;
}

// File Storage Metrics
void MasterMetricManager::inc_allocated_file_size(int64_t val) {
    file_allocated_size_.inc(val);
}
void MasterMetricManager::dec_allocated_file_size(int64_t val) {
    file_allocated_size_.dec(val);
}

void MasterMetricManager::inc_total_file_capacity(int64_t val) {
    file_total_capacity_.inc(val);
}
void MasterMetricManager::dec_total_file_capacity(int64_t val) {
    file_total_capacity_.dec(val);
}

int64_t MasterMetricManager::get_allocated_file_size() {
    return file_allocated_size_.value();
}

int64_t MasterMetricManager::get_total_file_capacity() {
    return file_total_capacity_.value();
}

double MasterMetricManager::get_global_file_used_ratio(void) {
    double allocated = file_allocated_size_.value();
    double capacity = file_total_capacity_.value();
    if (capacity == 0) {
        return 0.0;
    }
    return allocated / capacity;
}

// Key/Value Metrics
void MasterMetricManager::inc_key_count(int64_t val) { key_count_.inc(val); }
void MasterMetricManager::dec_key_count(int64_t val) { key_count_.dec(val); }

void MasterMetricManager::inc_soft_pin_key_count(int64_t val) {
    soft_pin_key_count_.inc(val);
}
void MasterMetricManager::dec_soft_pin_key_count(int64_t val) {
    soft_pin_key_count_.dec(val);
}

void MasterMetricManager::observe_value_size(int64_t size) {
    value_size_distribution_.observe(size);
}

int64_t MasterMetricManager::get_key_count() { return key_count_.value(); }

int64_t MasterMetricManager::get_soft_pin_key_count() {
    return soft_pin_key_count_.value();
}

// Cluster Metrics
void MasterMetricManager::inc_active_clients(int64_t val) {
    active_clients_.inc(val);
}

void MasterMetricManager::dec_active_clients(int64_t val) {
    active_clients_.dec(val);
}

int64_t MasterMetricManager::get_active_clients() {
    return active_clients_.value();
}

// cache hit rate metrics
void MasterMetricManager::inc_mem_cache_hit_nums(int64_t val) {
    mem_cache_hit_nums_.inc(val);
}
void MasterMetricManager::inc_file_cache_hit_nums(int64_t val) {
    file_cache_hit_nums_.inc(val);
}
void MasterMetricManager::inc_mem_cache_nums(int64_t val) {
    mem_cache_nums_.inc(val);
}
void MasterMetricManager::inc_file_cache_nums(int64_t val) {
    file_cache_nums_.inc(val);
}
void MasterMetricManager::dec_mem_cache_nums(int64_t val) {
    mem_cache_nums_.dec(val);
}
void MasterMetricManager::dec_file_cache_nums(int64_t val) {
    file_cache_nums_.dec(val);
}
void MasterMetricManager::inc_valid_get_nums(int64_t val) {
    valid_get_nums_.inc(val);
}
void MasterMetricManager::inc_total_get_nums(int64_t val) {
    total_get_nums_.inc(val);
}

// Operation Statistics (Counters)
void MasterMetricManager::inc_exist_key_requests(int64_t val) {
    exist_key_requests_.inc(val);
}
void MasterMetricManager::inc_exist_key_failures(int64_t val) {
    exist_key_failures_.inc(val);
}
void MasterMetricManager::inc_put_start_requests(int64_t val) {
    put_start_requests_.inc(val);
}
void MasterMetricManager::inc_put_start_failures(int64_t val) {
    put_start_failures_.inc(val);
}
void MasterMetricManager::inc_put_end_requests(int64_t val) {
    put_end_requests_.inc(val);
}
void MasterMetricManager::inc_put_end_failures(int64_t val) {
    put_end_failures_.inc(val);
}
void MasterMetricManager::inc_put_revoke_requests(int64_t val) {
    put_revoke_requests_.inc(val);
}
void MasterMetricManager::inc_put_revoke_failures(int64_t val) {
    put_revoke_failures_.inc(val);
}
void MasterMetricManager::inc_get_replica_list_requests(int64_t val) {
    get_replica_list_requests_.inc(val);
}
void MasterMetricManager::inc_get_replica_list_failures(int64_t val) {
    get_replica_list_failures_.inc(val);
}
void MasterMetricManager::inc_get_replica_list_by_regex_requests(int64_t val) {
    get_replica_list_by_regex_requests_.inc(val);
}
void MasterMetricManager::inc_get_replica_list_by_regex_failures(int64_t val) {
    get_replica_list_by_regex_failures_.inc(val);
}
void MasterMetricManager::inc_remove_requests(int64_t val) {
    remove_requests_.inc(val);
}
void MasterMetricManager::inc_remove_failures(int64_t val) {
    remove_failures_.inc(val);
}
void MasterMetricManager::inc_remove_by_regex_requests(int64_t val) {
    remove_by_regex_requests_.inc(val);
}
void MasterMetricManager::inc_remove_by_regex_failures(int64_t val) {
    remove_by_regex_failures_.inc(val);
}
void MasterMetricManager::inc_remove_all_requests(int64_t val) {
    remove_all_requests_.inc(val);
}
void MasterMetricManager::inc_remove_all_failures(int64_t val) {
    remove_all_failures_.inc(val);
}
void MasterMetricManager::inc_mount_segment_requests(int64_t val) {
    mount_segment_requests_.inc(val);
}
void MasterMetricManager::inc_mount_segment_failures(int64_t val) {
    mount_segment_failures_.inc(val);
}
void MasterMetricManager::inc_unmount_segment_requests(int64_t val) {
    unmount_segment_requests_.inc(val);
}
void MasterMetricManager::inc_unmount_segment_failures(int64_t val) {
    unmount_segment_failures_.inc(val);
}
void MasterMetricManager::inc_remount_segment_requests(int64_t val) {
    remount_segment_requests_.inc(val);
}
void MasterMetricManager::inc_remount_segment_failures(int64_t val) {
    remount_segment_failures_.inc(val);
}
void MasterMetricManager::inc_ping_requests(int64_t val) {
    ping_requests_.inc(val);
}
void MasterMetricManager::inc_ping_failures(int64_t val) {
    ping_failures_.inc(val);
}

// Batch Operation Statistics (Counters)
void MasterMetricManager::inc_batch_exist_key_requests(int64_t items) {
    batch_exist_key_requests_.inc(1);
    batch_exist_key_items_.inc(items);
}
void MasterMetricManager::inc_batch_exist_key_failures(int64_t failed_items) {
    batch_exist_key_failures_.inc(1);
    batch_exist_key_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_exist_key_partial_success(
    int64_t failed_items) {
    batch_exist_key_partial_successes_.inc(1);
    batch_exist_key_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_query_ip_requests(int64_t items) {
    batch_query_ip_requests_.inc(1);
    batch_query_ip_items_.inc(items);
}
void MasterMetricManager::inc_batch_query_ip_failures(int64_t failed_items) {
    batch_query_ip_failures_.inc(1);
    batch_query_ip_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_query_ip_partial_success(
    int64_t failed_items) {
    batch_query_ip_partial_successes_.inc(1);
    batch_query_ip_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_replica_clear_requests(int64_t items) {
    batch_replica_clear_requests_.inc(1);
    batch_replica_clear_items_.inc(items);
}
void MasterMetricManager::inc_batch_replica_clear_failures(
    int64_t failed_items) {
    batch_replica_clear_failures_.inc(1);
    batch_replica_clear_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_replica_clear_partial_success(
    int64_t failed_items) {
    batch_replica_clear_partial_successes_.inc(1);
    batch_replica_clear_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_get_replica_list_requests(int64_t items) {
    batch_get_replica_list_requests_.inc(1);
    batch_get_replica_list_items_.inc(items);
}
void MasterMetricManager::inc_batch_get_replica_list_failures(
    int64_t failed_items) {
    batch_get_replica_list_failures_.inc(1);
    batch_get_replica_list_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_get_replica_list_partial_success(
    int64_t failed_items) {
    batch_get_replica_list_partial_successes_.inc(1);
    batch_get_replica_list_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_put_start_requests(int64_t items) {
    batch_put_start_requests_.inc(1);
    batch_put_start_items_.inc(items);
}
void MasterMetricManager::inc_batch_put_start_failures(int64_t failed_items) {
    batch_put_start_failures_.inc(1);
    batch_put_start_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_put_start_partial_success(
    int64_t failed_items) {
    batch_put_start_partial_successes_.inc(1);
    batch_put_start_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_put_end_requests(int64_t items) {
    batch_put_end_requests_.inc(1);
    batch_put_end_items_.inc(items);
}
void MasterMetricManager::inc_batch_put_end_failures(int64_t failed_items) {
    batch_put_end_failures_.inc(1);
    batch_put_end_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_put_end_partial_success(
    int64_t failed_items) {
    batch_put_end_partial_successes_.inc(1);
    batch_put_end_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_put_revoke_requests(int64_t items) {
    batch_put_revoke_requests_.inc(1);
    batch_put_revoke_items_.inc(items);
}
void MasterMetricManager::inc_batch_put_revoke_failures(int64_t failed_items) {
    batch_put_revoke_failures_.inc(1);
    batch_put_revoke_failed_items_.inc(failed_items);
}
void MasterMetricManager::inc_batch_put_revoke_partial_success(
    int64_t failed_items) {
    batch_put_revoke_partial_successes_.inc(1);
    batch_put_revoke_failed_items_.inc(failed_items);
}

// PutStart Discard Metrics
void MasterMetricManager::inc_put_start_discard_cnt(int64_t count,
                                                    int64_t size) {
    put_start_discard_cnt_.inc(count);
    put_start_discarded_staging_size_.inc(size);
}

void MasterMetricManager::inc_put_start_release_cnt(int64_t count,
                                                    int64_t size) {
    put_start_release_cnt_.inc(count);
    put_start_discarded_staging_size_.dec(size);
}

int64_t MasterMetricManager::get_put_start_requests() {
    return put_start_requests_.value();
}

int64_t MasterMetricManager::get_put_start_failures() {
    return put_start_failures_.value();
}

int64_t MasterMetricManager::get_put_end_requests() {
    return put_end_requests_.value();
}

int64_t MasterMetricManager::get_put_end_failures() {
    return put_end_failures_.value();
}

int64_t MasterMetricManager::get_put_revoke_requests() {
    return put_revoke_requests_.value();
}

int64_t MasterMetricManager::get_put_revoke_failures() {
    return put_revoke_failures_.value();
}

int64_t MasterMetricManager::get_get_replica_list_requests() {
    return get_replica_list_requests_.value();
}

int64_t MasterMetricManager::get_get_replica_list_failures() {
    return get_replica_list_failures_.value();
}

int64_t MasterMetricManager::get_get_replica_list_by_regex_requests() {
    return get_replica_list_by_regex_requests_.value();
}

int64_t MasterMetricManager::get_get_replica_list_by_regex_failures() {
    return get_replica_list_by_regex_failures_.value();
}

int64_t MasterMetricManager::get_exist_key_requests() {
    return exist_key_requests_.value();
}

int64_t MasterMetricManager::get_exist_key_failures() {
    return exist_key_failures_.value();
}

int64_t MasterMetricManager::get_remove_by_regex_requests() {
    return remove_by_regex_requests_.value();
}

int64_t MasterMetricManager::get_remove_by_regex_failures() {
    return remove_by_regex_failures_.value();
}

int64_t MasterMetricManager::get_remove_requests() {
    return remove_requests_.value();
}

int64_t MasterMetricManager::get_remove_failures() {
    return remove_failures_.value();
}

int64_t MasterMetricManager::get_remove_all_requests() {
    return remove_all_requests_.value();
}

int64_t MasterMetricManager::get_remove_all_failures() {
    return remove_all_failures_.value();
}

int64_t MasterMetricManager::get_mount_segment_requests() {
    return mount_segment_requests_.value();
}

int64_t MasterMetricManager::get_mount_segment_failures() {
    return mount_segment_failures_.value();
}

int64_t MasterMetricManager::get_unmount_segment_requests() {
    return unmount_segment_requests_.value();
}

int64_t MasterMetricManager::get_unmount_segment_failures() {
    return unmount_segment_failures_.value();
}

int64_t MasterMetricManager::get_remount_segment_requests() {
    return remount_segment_requests_.value();
}

int64_t MasterMetricManager::get_remount_segment_failures() {
    return remount_segment_failures_.value();
}

int64_t MasterMetricManager::get_ping_requests() {
    return ping_requests_.value();
}

int64_t MasterMetricManager::get_ping_failures() {
    return ping_failures_.value();
}

int64_t MasterMetricManager::get_batch_exist_key_requests() {
    return batch_exist_key_requests_.value();
}

int64_t MasterMetricManager::get_batch_exist_key_failures() {
    return batch_exist_key_failures_.value();
}

int64_t MasterMetricManager::get_batch_exist_key_partial_successes() {
    return batch_exist_key_partial_successes_.value();
}

int64_t MasterMetricManager::get_batch_exist_key_items() {
    return batch_exist_key_items_.value();
}

int64_t MasterMetricManager::get_batch_exist_key_failed_items() {
    return batch_exist_key_failed_items_.value();
}

int64_t MasterMetricManager::get_batch_query_ip_requests() {
    return batch_query_ip_requests_.value();
}

int64_t MasterMetricManager::get_batch_query_ip_failures() {
    return batch_query_ip_failures_.value();
}

int64_t MasterMetricManager::get_batch_query_ip_partial_successes() {
    return batch_query_ip_partial_successes_.value();
}

int64_t MasterMetricManager::get_batch_query_ip_items() {
    return batch_query_ip_items_.value();
}

int64_t MasterMetricManager::get_batch_query_ip_failed_items() {
    return batch_query_ip_failed_items_.value();
}

int64_t MasterMetricManager::get_batch_replica_clear_requests() {
    return batch_replica_clear_requests_.value();
}

int64_t MasterMetricManager::get_batch_replica_clear_failures() {
    return batch_replica_clear_failures_.value();
}

int64_t MasterMetricManager::get_batch_replica_clear_partial_successes() {
    return batch_replica_clear_partial_successes_.value();
}

int64_t MasterMetricManager::get_batch_replica_clear_items() {
    return batch_replica_clear_items_.value();
}

int64_t MasterMetricManager::get_batch_replica_clear_failed_items() {
    return batch_replica_clear_failed_items_.value();
}

int64_t MasterMetricManager::get_batch_get_replica_list_requests() {
    return batch_get_replica_list_requests_.value();
}

int64_t MasterMetricManager::get_batch_get_replica_list_failures() {
    return batch_get_replica_list_failures_.value();
}

int64_t MasterMetricManager::get_batch_get_replica_list_partial_successes() {
    return batch_get_replica_list_partial_successes_.value();
}

int64_t MasterMetricManager::get_batch_get_replica_list_items() {
    return batch_get_replica_list_items_.value();
}

int64_t MasterMetricManager::get_batch_get_replica_list_failed_items() {
    return batch_get_replica_list_failed_items_.value();
}

int64_t MasterMetricManager::get_batch_put_start_requests() {
    return batch_put_start_requests_.value();
}

int64_t MasterMetricManager::get_batch_put_start_failures() {
    return batch_put_start_failures_.value();
}

int64_t MasterMetricManager::get_batch_put_start_partial_successes() {
    return batch_put_start_partial_successes_.value();
}

int64_t MasterMetricManager::get_batch_put_start_items() {
    return batch_put_start_items_.value();
}

int64_t MasterMetricManager::get_batch_put_start_failed_items() {
    return batch_put_start_failed_items_.value();
}

int64_t MasterMetricManager::get_batch_put_end_requests() {
    return batch_put_end_requests_.value();
}

int64_t MasterMetricManager::get_batch_put_end_failures() {
    return batch_put_end_failures_.value();
}

int64_t MasterMetricManager::get_batch_put_end_partial_successes() {
    return batch_put_end_partial_successes_.value();
}

int64_t MasterMetricManager::get_batch_put_end_items() {
    return batch_put_end_items_.value();
}

int64_t MasterMetricManager::get_batch_put_end_failed_items() {
    return batch_put_end_failed_items_.value();
}

int64_t MasterMetricManager::get_batch_put_revoke_requests() {
    return batch_put_revoke_requests_.value();
}

int64_t MasterMetricManager::get_batch_put_revoke_failures() {
    return batch_put_revoke_failures_.value();
}

int64_t MasterMetricManager::get_batch_put_revoke_partial_successes() {
    return batch_put_revoke_partial_successes_.value();
}

int64_t MasterMetricManager::get_batch_put_revoke_items() {
    return batch_put_revoke_items_.value();
}

int64_t MasterMetricManager::get_batch_put_revoke_failed_items() {
    return batch_put_revoke_failed_items_.value();
}

// Eviction Metrics
void MasterMetricManager::inc_eviction_success(int64_t key_count,
                                               int64_t size) {
    evicted_key_count_.inc(key_count);
    evicted_size_.inc(size);
    eviction_success_.inc();
    eviction_attempts_.inc();
}

void MasterMetricManager::inc_eviction_fail() { eviction_attempts_.inc(); }

int64_t MasterMetricManager::get_eviction_success() {
    return eviction_success_.value();
}

int64_t MasterMetricManager::get_eviction_attempts() {
    return eviction_attempts_.value();
}

int64_t MasterMetricManager::get_evicted_key_count() {
    return evicted_key_count_.value();
}

int64_t MasterMetricManager::get_evicted_size() {
    return evicted_size_.value();
}

// PutStart Discard Metrics Getters
int64_t MasterMetricManager::get_put_start_discard_cnt() {
    return put_start_discard_cnt_.value();
}

int64_t MasterMetricManager::get_put_start_release_cnt() {
    return put_start_release_cnt_.value();
}

int64_t MasterMetricManager::get_put_start_discarded_staging_size() {
    return put_start_discarded_staging_size_.value();
}

// CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke Metrics
void MasterMetricManager::inc_copy_start_requests(int64_t val) {
    copy_start_requests_.inc(val);
}
void MasterMetricManager::inc_copy_start_failures(int64_t val) {
    copy_start_failures_.inc(val);
}
void MasterMetricManager::inc_copy_end_requests(int64_t val) {
    copy_end_requests_.inc(val);
}
void MasterMetricManager::inc_copy_end_failures(int64_t val) {
    copy_end_failures_.inc(val);
}
void MasterMetricManager::inc_copy_revoke_requests(int64_t val) {
    copy_revoke_requests_.inc(val);
}
void MasterMetricManager::inc_copy_revoke_failures(int64_t val) {
    copy_revoke_failures_.inc(val);
}
void MasterMetricManager::inc_move_start_requests(int64_t val) {
    move_start_requests_.inc(val);
}
void MasterMetricManager::inc_move_start_failures(int64_t val) {
    move_start_failures_.inc(val);
}
void MasterMetricManager::inc_move_end_requests(int64_t val) {
    move_end_requests_.inc(val);
}
void MasterMetricManager::inc_move_end_failures(int64_t val) {
    move_end_failures_.inc(val);
}
void MasterMetricManager::inc_move_revoke_requests(int64_t val) {
    move_revoke_requests_.inc(val);
}
void MasterMetricManager::inc_move_revoke_failures(int64_t val) {
    move_revoke_failures_.inc(val);
}

// CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke Metrics
// Getters
int64_t MasterMetricManager::get_copy_start_requests() {
    return copy_start_requests_.value();
}
int64_t MasterMetricManager::get_copy_start_failures() {
    return copy_start_failures_.value();
}
int64_t MasterMetricManager::get_copy_end_requests() {
    return copy_end_requests_.value();
}
int64_t MasterMetricManager::get_copy_end_failures() {
    return copy_end_failures_.value();
}
int64_t MasterMetricManager::get_copy_revoke_requests() {
    return copy_revoke_requests_.value();
}
int64_t MasterMetricManager::get_copy_revoke_failures() {
    return copy_revoke_failures_.value();
}
int64_t MasterMetricManager::get_move_start_requests() {
    return move_start_requests_.value();
}
int64_t MasterMetricManager::get_move_start_failures() {
    return move_start_failures_.value();
}
int64_t MasterMetricManager::get_move_end_requests() {
    return move_end_requests_.value();
}
int64_t MasterMetricManager::get_move_end_failures() {
    return move_end_failures_.value();
}
int64_t MasterMetricManager::get_move_revoke_requests() {
    return move_revoke_requests_.value();
}
int64_t MasterMetricManager::get_move_revoke_failures() {
    return move_revoke_failures_.value();
}

// Task create, query, fetch Metrics
void MasterMetricManager::inc_create_copy_task_requests(int64_t val) {
    create_copy_task_requests_.inc(val);
}
void MasterMetricManager::inc_create_copy_task_failures(int64_t val) {
    create_copy_task_failures_.inc(val);
}
void MasterMetricManager::inc_create_move_task_requests(int64_t val) {
    create_move_task_requests_.inc(val);
}
void MasterMetricManager::inc_create_move_task_failures(int64_t val) {
    create_move_task_failures_.inc(val);
}
void MasterMetricManager::inc_query_task_requests(int64_t val) {
    query_task_requests_.inc(val);
}
void MasterMetricManager::inc_query_task_failures(int64_t val) {
    query_task_failures_.inc(val);
}
void MasterMetricManager::inc_fetch_tasks_requests(int64_t val) {
    fetch_tasks_requests_.inc(val);
}
void MasterMetricManager::inc_fetch_tasks_failures(int64_t val) {
    fetch_tasks_failures_.inc(val);
}
void MasterMetricManager::inc_update_task_requests(int64_t val) {
    mark_task_to_complete_requests_.inc(val);
}
void MasterMetricManager::inc_update_task_failures(int64_t val) {
    mark_task_to_complete_failures_.inc(val);
}

// Task create, query, fetch Metrics Getters
int64_t MasterMetricManager::get_create_copy_task_requests() {
    return create_copy_task_requests_.value();
}
int64_t MasterMetricManager::get_create_copy_task_failures() {
    return create_copy_task_failures_.value();
}
int64_t MasterMetricManager::get_create_move_task_requests() {
    return create_move_task_requests_.value();
}
int64_t MasterMetricManager::get_create_move_task_failures() {
    return create_move_task_failures_.value();
}
int64_t MasterMetricManager::get_query_task_requests() {
    return query_task_requests_.value();
}
int64_t MasterMetricManager::get_query_task_failures() {
    return query_task_failures_.value();
}
int64_t MasterMetricManager::get_fetch_tasks_requests() {
    return fetch_tasks_requests_.value();
}
int64_t MasterMetricManager::get_fetch_tasks_failures() {
    return fetch_tasks_failures_.value();
}
int64_t MasterMetricManager::get_update_task_requests() {
    return mark_task_to_complete_requests_.value();
}
int64_t MasterMetricManager::get_update_task_failures() {
    return mark_task_to_complete_failures_.value();
}

// --- Serialization ---
std::string MasterMetricManager::serialize_metrics() {
    // Note: Following Prometheus style, metrics with value 0 that haven't
    // changed will not be included in the output. If all metrics are 0 and
    // unchanged, this function will return an empty string.
    std::stringstream ss;

    // Helper function to serialize a metric and append it to the stringstream
    auto serialize_metric = [&ss](auto& metric) {
        std::string metric_str;
        metric.serialize(metric_str);
        ss << metric_str;
    };

    // Serialize Gauges
    serialize_metric(mem_allocated_size_);
    serialize_metric(mem_total_capacity_);
    serialize_metric(mem_allocated_size_per_segment_);
    serialize_metric(mem_total_capacity_per_segment_);
    serialize_metric(file_allocated_size_);
    serialize_metric(file_total_capacity_);
    serialize_metric(key_count_);
    serialize_metric(soft_pin_key_count_);
    serialize_metric(active_clients_);

    // Serialize Histogram
    serialize_metric(value_size_distribution_);

    // Serialize Request Counters
    serialize_metric(exist_key_requests_);
    serialize_metric(exist_key_failures_);
    serialize_metric(put_start_requests_);
    serialize_metric(put_start_failures_);
    serialize_metric(put_end_requests_);
    serialize_metric(put_end_failures_);
    serialize_metric(put_revoke_requests_);
    serialize_metric(put_revoke_failures_);
    serialize_metric(get_replica_list_requests_);
    serialize_metric(get_replica_list_failures_);
    serialize_metric(get_replica_list_by_regex_requests_);
    serialize_metric(get_replica_list_by_regex_failures_);
    serialize_metric(remove_requests_);
    serialize_metric(remove_failures_);
    serialize_metric(remove_by_regex_requests_);
    serialize_metric(remove_by_regex_failures_);
    serialize_metric(remove_all_requests_);
    serialize_metric(remove_all_failures_);
    serialize_metric(mount_segment_requests_);
    serialize_metric(mount_segment_failures_);
    serialize_metric(unmount_segment_requests_);
    serialize_metric(unmount_segment_failures_);
    serialize_metric(remount_segment_requests_);
    serialize_metric(remount_segment_failures_);
    serialize_metric(ping_requests_);
    serialize_metric(ping_failures_);

    // Serialize CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke
    // Counters
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

    // Serialize CreateCopyTask, CreateMoveTask, MarkTaskToComplete, QueryTask,
    // FetchTasks Request Counters
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

    // Serialize Batch Request Counters
    serialize_metric(batch_exist_key_requests_);
    serialize_metric(batch_exist_key_failures_);
    serialize_metric(batch_query_ip_requests_);
    serialize_metric(batch_query_ip_failures_);
    serialize_metric(batch_replica_clear_requests_);
    serialize_metric(batch_replica_clear_failures_);
    serialize_metric(batch_get_replica_list_requests_);
    serialize_metric(batch_get_replica_list_failures_);
    serialize_metric(batch_put_start_requests_);
    serialize_metric(batch_put_start_failures_);
    serialize_metric(batch_put_end_requests_);
    serialize_metric(batch_put_end_failures_);
    serialize_metric(batch_put_revoke_requests_);
    serialize_metric(batch_put_revoke_failures_);

    // Serialize Eviction Counters
    serialize_metric(eviction_success_);
    serialize_metric(eviction_attempts_);
    serialize_metric(evicted_key_count_);
    serialize_metric(evicted_size_);

    // Serialize PutStart Discard Metrics
    serialize_metric(put_start_discard_cnt_);
    serialize_metric(put_start_release_cnt_);
    serialize_metric(put_start_discarded_staging_size_);

    return ss.str();
}

MasterMetricManager::CacheHitStatDict
MasterMetricManager::calculate_cache_stats() {
    MasterMetricManager::CacheHitStatDict stats_dict;
    int64_t mem_cache_hits = mem_cache_hit_nums_.value();
    int64_t ssd_cache_hits = file_cache_hit_nums_.value();
    int64_t mem_total_cache = mem_cache_nums_.value();
    int64_t ssd_total_cache = file_cache_nums_.value();

    int64_t total_hits = mem_cache_hits + ssd_cache_hits;
    int64_t total_cache = mem_total_cache + ssd_total_cache;

    int64_t valid_get_nums = valid_get_nums_.value();
    int64_t total_get_nums = total_get_nums_.value();

    double mem_hit_rate = 0.0;
    if (mem_total_cache > 0) {
        mem_hit_rate = static_cast<double>(mem_cache_hits) /
                       static_cast<double>(mem_total_cache);
        mem_hit_rate = std::round(mem_hit_rate * 100.0) / 100.0;
    }

    double ssd_hit_rate = 0.0;
    if (ssd_total_cache > 0) {
        ssd_hit_rate = static_cast<double>(ssd_cache_hits) /
                       static_cast<double>(ssd_total_cache);
        ssd_hit_rate = std::round(ssd_hit_rate * 100.0) / 100.0;
    }

    double total_hit_rate = 0.0;
    if (total_cache > 0) {
        total_hit_rate =
            static_cast<double>(total_hits) / static_cast<double>(total_cache);
        total_hit_rate = std::round(total_hit_rate * 100.0) / 100.0;
    }

    double valid_get_rate = 0.0;
    if (total_get_nums > 0) {
        valid_get_rate = static_cast<double>(valid_get_nums) /
                         static_cast<double>(total_get_nums);
        valid_get_rate = std::round(valid_get_rate * 100.0) / 100.0;
    }

    add_stat_to_dict(stats_dict, CacheHitStat::MEMORY_HITS, mem_cache_hits);
    add_stat_to_dict(stats_dict, CacheHitStat::SSD_HITS, ssd_cache_hits);
    add_stat_to_dict(stats_dict, CacheHitStat::MEMORY_TOTAL, mem_total_cache);
    add_stat_to_dict(stats_dict, CacheHitStat::SSD_TOTAL, ssd_total_cache);
    add_stat_to_dict(stats_dict, CacheHitStat::MEMORY_HIT_RATE, mem_hit_rate);
    add_stat_to_dict(stats_dict, CacheHitStat::SSD_HIT_RATE, ssd_hit_rate);
    add_stat_to_dict(stats_dict, CacheHitStat::OVERALL_HIT_RATE,
                     total_hit_rate);
    add_stat_to_dict(stats_dict, CacheHitStat::VALID_GET_RATE, valid_get_rate);
    return stats_dict;
}

void MasterMetricManager::add_stat_to_dict(
    MasterMetricManager::CacheHitStatDict& dict,
    MasterMetricManager::CacheHitStat type, double value) {
    auto it = stat_names_.find(type);
    if (it != stat_names_.end()) {
        dict[it->first] = value;
    }
}

// --- Human-Readable Summary ---
std::string MasterMetricManager::get_summary_string() {
    std::stringstream ss;

    // --- Get current values ---
    int64_t mem_allocated = mem_allocated_size_.value();
    int64_t mem_capacity = mem_total_capacity_.value();
    int64_t file_allocated = file_allocated_size_.value();
    int64_t file_capacity = file_total_capacity_.value();
    int64_t keys = key_count_.value();
    int64_t soft_pin_keys = soft_pin_key_count_.value();
    int64_t active_clients = active_clients_.value();

    // Request counters
    int64_t exist_keys = exist_key_requests_.value();
    int64_t exist_key_fails = exist_key_failures_.value();
    int64_t put_starts = put_start_requests_.value();
    int64_t put_start_fails = put_start_failures_.value();
    int64_t put_ends = put_end_requests_.value();
    int64_t put_end_fails = put_end_failures_.value();
    int64_t put_revoke_requests = put_revoke_requests_.value();
    int64_t put_revoke_fails = put_revoke_failures_.value();
    int64_t get_replicas = get_replica_list_requests_.value();
    int64_t get_replica_fails = get_replica_list_failures_.value();
    int64_t removes = remove_requests_.value();
    int64_t remove_fails = remove_failures_.value();
    int64_t remove_all = remove_all_requests_.value();
    int64_t remove_all_fails = remove_all_failures_.value();
    int64_t create_move_tasks = create_move_task_requests_.value();
    int64_t create_move_task_fails = create_move_task_failures_.value();
    int64_t create_copy_tasks = create_copy_task_requests_.value();
    int64_t create_copy_task_fails = create_copy_task_failures_.value();
    int64_t query_tasks = query_task_requests_.value();
    int64_t query_task_fails = query_task_failures_.value();
    int64_t fetch_tasks = fetch_tasks_requests_.value();
    int64_t fetch_task_fails = fetch_tasks_failures_.value();

    // CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke counters
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

    // Batch request counters
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
    int64_t batch_get_replica_list_requests =
        batch_get_replica_list_requests_.value();
    int64_t batch_get_replica_list_fails =
        batch_get_replica_list_failures_.value();
    int64_t batch_get_replica_list_partial_successes =
        batch_get_replica_list_partial_successes_.value();
    int64_t batch_get_replica_list_items =
        batch_get_replica_list_items_.value();
    int64_t batch_get_replica_list_failed_items =
        batch_get_replica_list_failed_items_.value();
    int64_t batch_exist_key_requests = batch_exist_key_requests_.value();
    int64_t batch_exist_key_fails = batch_exist_key_failures_.value();
    int64_t batch_exist_key_partial_successes =
        batch_exist_key_partial_successes_.value();
    int64_t batch_exist_key_items = batch_exist_key_items_.value();
    int64_t batch_exist_key_failed_items =
        batch_exist_key_failed_items_.value();
    int64_t batch_query_ip_requests = batch_query_ip_requests_.value();
    int64_t batch_query_ip_fails = batch_query_ip_failures_.value();
    int64_t batch_query_ip_partial_successes =
        batch_query_ip_partial_successes_.value();
    int64_t batch_query_ip_items = batch_query_ip_items_.value();
    int64_t batch_query_ip_failed_items = batch_query_ip_failed_items_.value();
    int64_t batch_replica_clear_requests =
        batch_replica_clear_requests_.value();
    int64_t batch_replica_clear_fails = batch_replica_clear_failures_.value();
    int64_t batch_replica_clear_partial_successes =
        batch_replica_clear_partial_successes_.value();
    int64_t batch_replica_clear_items = batch_replica_clear_items_.value();
    int64_t batch_replica_clear_failed_items =
        batch_replica_clear_failed_items_.value();

    // Eviction counters
    int64_t eviction_success = eviction_success_.value();
    int64_t eviction_attempts = eviction_attempts_.value();
    int64_t evicted_key_count = evicted_key_count_.value();
    int64_t evicted_size = evicted_size_.value();

    // Ping counters
    int64_t ping = ping_requests_.value();
    int64_t ping_fails = ping_failures_.value();

    // Discard counters
    int64_t put_start_discard_cnt = put_start_discard_cnt_.value();
    int64_t put_start_release_cnt = put_start_release_cnt_.value();
    int64_t put_start_discarded_staging_size =
        put_start_discarded_staging_size_.value();

    // --- Format the summary string ---
    ss << "Mem Storage: " << byte_size_to_string(mem_allocated) << " / "
       << byte_size_to_string(mem_capacity);
    if (mem_capacity > 0) {
        ss << " (" << std::fixed << std::setprecision(1)
           << ((double)mem_allocated / (double)mem_capacity * 100.0) << "%)";
    }
    ss << " | SSD Storage: " << byte_size_to_string(file_allocated) << " / "
       << byte_size_to_string(file_capacity);
    ss << " | Keys: " << keys << " (soft-pinned: " << soft_pin_keys << ")";
    ss << " | Clients: " << active_clients;

    // Request summary - focus on the most important metrics
    ss << " | Requests (Success/Total): ";
    ss << "PutStart=" << put_starts - put_start_fails << "/" << put_starts
       << ", ";
    ss << "PutEnd=" << put_ends - put_end_fails << "/" << put_ends << ", ";
    ss << "PutRevoke=" << put_revoke_requests - put_revoke_fails << "/"
       << put_revoke_requests << ", ";
    ss << "Get=" << get_replicas - get_replica_fails << "/" << get_replicas
       << ", ";
    ss << "Exist=" << exist_keys - exist_key_fails << "/" << exist_keys << ", ";
    ss << "Del=" << removes - remove_fails << "/" << removes << ", ";
    ss << "DelAll=" << remove_all - remove_all_fails << "/" << remove_all
       << ", ";
    ss << "Ping=" << ping - ping_fails << "/" << ping << ", ";
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

    // Batch request summary
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
    ss << "Get:(Req="
       << batch_get_replica_list_requests - batch_get_replica_list_fails -
              batch_get_replica_list_partial_successes
       << "/" << batch_get_replica_list_partial_successes << "/"
       << batch_get_replica_list_requests << ", Item="
       << batch_get_replica_list_items - batch_get_replica_list_failed_items
       << "/" << batch_get_replica_list_items << "), ";
    ss << "ExistKey:(Req="
       << batch_exist_key_requests - batch_exist_key_fails -
              batch_exist_key_partial_successes
       << "/" << batch_exist_key_partial_successes << "/"
       << batch_exist_key_requests
       << ", Item=" << batch_exist_key_items - batch_exist_key_failed_items
       << "/" << batch_exist_key_items << "), ";
    ss << "QueryIp:(Req="
       << batch_query_ip_requests - batch_query_ip_fails -
              batch_query_ip_partial_successes
       << "/" << batch_query_ip_partial_successes << "/"
       << batch_query_ip_requests
       << ", Item=" << batch_query_ip_items - batch_query_ip_failed_items << "/"
       << batch_query_ip_items << "), ";
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
       << "/" << mark_task_to_complete_requests_.value() << "), ";
    // Eviction summary
    ss << " | Eviction: "
       << "Success/Attempts=" << eviction_success << "/" << eviction_attempts
       << ", "
       << "keys=" << evicted_key_count << ", "
       << "size=" << byte_size_to_string(evicted_size);

    // Discard summary
    ss << " | Discard: "
       << "Released/Total=" << put_start_release_cnt << "/"
       << put_start_discard_cnt << ", StagingSize="
       << byte_size_to_string(put_start_discarded_staging_size);

    return ss.str();
}

}  // namespace mooncake
