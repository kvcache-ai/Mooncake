#include "master_metric_manager.h"

#include <iomanip>  // For std::fixed, std::setprecision
#include <sstream>  // For string building during serialization
#include <vector>   // Required by histogram serialization

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
    : allocated_size_("master_allocated_bytes",
                      "Total bytes currently allocated across all segments"),
      total_capacity_("master_total_capacity_bytes",
                      "Total capacity across all mounted segments"),
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

      // Initialize Eviction Counters
      eviction_success_("master_successful_evictions_total",
                        "Total number of successful eviction operations"),
      eviction_attempts_("master_attempted_evictions_total",
                         "Total number of attempted eviction operations"),
      evicted_key_count_("master_evicted_key_count",
                         "Total number of keys evicted"),
      evicted_size_("master_evicted_size_bytes",
                    "Total bytes of evicted objects") {}

// --- Metric Interface Methods ---

// Storage Metrics
void MasterMetricManager::inc_allocated_size(int64_t val) {
    allocated_size_.inc(val);
}
void MasterMetricManager::dec_allocated_size(int64_t val) {
    allocated_size_.dec(val);
}

void MasterMetricManager::inc_total_capacity(int64_t val) {
    total_capacity_.inc(val);
}
void MasterMetricManager::dec_total_capacity(int64_t val) {
    total_capacity_.dec(val);
}

int64_t MasterMetricManager::get_allocated_size() {
    return allocated_size_.value();
}

int64_t MasterMetricManager::get_total_capacity() {
    return total_capacity_.value();
}

double MasterMetricManager::get_global_used_ratio(void) {
    double allocated = allocated_size_.value();
    double capacity = total_capacity_.value();
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

// --- Setters ---
void MasterMetricManager::set_enable_ha(bool enable_ha) {
    enable_ha_ = enable_ha;
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
    serialize_metric(allocated_size_);
    serialize_metric(total_capacity_);
    serialize_metric(key_count_);
    serialize_metric(soft_pin_key_count_);
    if (enable_ha_) {
        serialize_metric(active_clients_);
    }

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
    if (enable_ha_) {
        serialize_metric(ping_requests_);
        serialize_metric(ping_failures_);
    }

    // Serialize Batch Request Counters
    serialize_metric(batch_exist_key_requests_);
    serialize_metric(batch_exist_key_failures_);
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

    return ss.str();
}

// --- Human-Readable Summary ---
std::string MasterMetricManager::get_summary_string() {
    std::stringstream ss;

    // --- Get current values ---
    int64_t allocated = allocated_size_.value();
    int64_t capacity = total_capacity_.value();
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

    // Eviction counters
    int64_t eviction_success = eviction_success_.value();
    int64_t eviction_attempts = eviction_attempts_.value();
    int64_t evicted_key_count = evicted_key_count_.value();
    int64_t evicted_size = evicted_size_.value();

    // Ping counters
    int64_t ping = ping_requests_.value();
    int64_t ping_fails = ping_failures_.value();

    // --- Format the summary string ---
    ss << "Storage: " << byte_size_to_string(allocated) << " / "
       << byte_size_to_string(capacity);
    if (capacity > 0) {
        ss << " (" << std::fixed << std::setprecision(1)
           << ((double)allocated / (double)capacity * 100.0) << "%)";
    }
    ss << " | Keys: " << keys << " (soft-pinned: " << soft_pin_keys << ")";
    if (enable_ha_) {
        ss << " | Clients: " << active_clients;
    }

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
    if (enable_ha_) {
        ss << "Ping=" << ping - ping_fails << "/" << ping << ", ";
    }

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

    // Eviction summary
    ss << " | Eviction: " << "Success/Attempts=" << eviction_success << "/"
       << eviction_attempts << ", " << "keys=" << evicted_key_count << ", "
       << "size=" << byte_size_to_string(evicted_size);

    return ss.str();
}

}  // namespace mooncake
