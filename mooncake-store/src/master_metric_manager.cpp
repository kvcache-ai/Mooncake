#include "master_metric_manager.h"

#include <glog/logging.h>
#include <iomanip>  // For std::fixed, std::setprecision
#include <sstream>  // For string building during serialization
#include <vector>   // Required by histogram serialization
#include <cmath>
#include <new>

#include "utils.h"

namespace mooncake {

MasterMetricManager* MasterMetricManager::instance_ = nullptr;

MasterMetricManager& MasterMetricManager::instance() {
    if (instance_ == nullptr) {
        LOG(ERROR) << "MasterMetricManager::instance() called before any "
                      "architecture metric manager was registered";
    }
    CHECK(instance_ != nullptr);
    return *instance_;
}

void MasterMetricManager::RegisterInstance(MasterMetricManager* instance) {
    if (instance_ == instance) return;  // idempotent (in-place reset)
    if (instance_ != nullptr) {
        LOG(ERROR) << "A MasterMetricManager instance is already registered; "
                      "keeping the first one. A process must run a single "
                      "master architecture.";
        return;
    }
    instance_ = instance;
}

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
      // Initialize Histogram (4KB, 64KB, 256KB, 1MB, 4MB, 16MB, 64MB)
      value_size_distribution_(
          "master_value_size_bytes", "Distribution of object value sizes",
          {4096, 65536, 262144, 1048576, 4194304, 16777216, 67108864}),
      // Initialize cluster metrics
      active_clients_("master_active_clients",
                      "Total number of active clients"),

      // Initialize client RPC counters (requests / failures)
      register_client_requests_("master_register_client_requests",
                                "Total number of RegisterClient requests"),
      register_client_failures_(
          "master_register_client_failures",
          "Total number of failed RegisterClient requests"),
      unregister_client_requests_("master_unregister_client_requests",
                                  "Total number of UnregisterClient requests"),
      unregister_client_failures_(
          "master_unregister_client_failures",
          "Total number of failed UnregisterClient requests"),
      // Initialize client lifecycle counters
      clients_disconnected_total_(
          "master_clients_disconnected_total",
          "Total number of client disconnections detected by heartbeat"),
      clients_recovered_total_(
          "master_clients_recovered_total",
          "Total number of client recoveries detected by heartbeat"),
      clients_crashed_total_(
          "master_clients_crashed_total",
          "Total number of client crashes detected by heartbeat"),

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
      heartbeat_requests_("master_heartbeat_requests_total",
                          "Total number of heartbeat requests received"),
      heartbeat_failures_("master_heartbeat_failures_total",
                          "Total number of failed heartbeat requests"),

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
      total_get_nums_("total_get_nums_", "Total number of get operations") {
    update_metrics_for_zero_output();
}

void MasterMetricManager::update_metrics_for_zero_output() {
    // Update Gauges (use update(0) to mark as changed)
    mem_allocated_size_.update(0);
    mem_total_capacity_.update(0);
    file_allocated_size_.update(0);
    file_total_capacity_.update(0);
    key_count_.update(0);
    active_clients_.update(0);
    mem_cache_nums_.update(0);
    file_cache_nums_.update(0);

    // Update Counters (use inc(0) to mark as changed)
    register_client_requests_.inc(0);
    register_client_failures_.inc(0);
    unregister_client_requests_.inc(0);
    unregister_client_failures_.inc(0);
    clients_disconnected_total_.inc(0);
    clients_recovered_total_.inc(0);
    clients_crashed_total_.inc(0);
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
    heartbeat_requests_.inc(0);
    heartbeat_failures_.inc(0);

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
    batch_get_replica_list_requests_.inc(0);
    batch_get_replica_list_failures_.inc(0);
    batch_get_replica_list_partial_successes_.inc(0);
    batch_get_replica_list_items_.inc(0);
    batch_get_replica_list_failed_items_.inc(0);

    // Update cache hit rate metrics
    mem_cache_hit_nums_.inc(0);
    file_cache_hit_nums_.inc(0);
    valid_get_nums_.inc(0);
    total_get_nums_.inc(0);

    // Update Histogram (use observe(0) to mark as changed)
    value_size_distribution_.observe(0);
}

// Memory Storage Metrics
void MasterMetricManager::inc_allocated_mem_size(const std::string& segment,
                                                 int64_t val) {
    mem_allocated_size_.inc(val);
    if (!segment.empty()) mem_allocated_size_per_segment_.inc({segment}, val);
}

void MasterMetricManager::inc_allocated_mem_size(int64_t val) {
    mem_allocated_size_.inc(val);
}

void MasterMetricManager::dec_allocated_mem_size(const std::string& segment,
                                                 int64_t val) {
    mem_allocated_size_.dec(val);
    if (!segment.empty()) mem_allocated_size_per_segment_.dec({segment}, val);
}

void MasterMetricManager::dec_allocated_mem_size(int64_t val) {
    mem_allocated_size_.dec(val);
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

void MasterMetricManager::observe_value_size(int64_t size) {
    value_size_distribution_.observe(size);
}

int64_t MasterMetricManager::get_key_count() { return key_count_.value(); }

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

// Client RPC Metrics (requests / failures)
void MasterMetricManager::inc_register_client_requests(int64_t val) {
    register_client_requests_.inc(val);
}
void MasterMetricManager::inc_register_client_failures(int64_t val) {
    register_client_failures_.inc(val);
}
void MasterMetricManager::inc_unregister_client_requests(int64_t val) {
    unregister_client_requests_.inc(val);
}
void MasterMetricManager::inc_unregister_client_failures(int64_t val) {
    unregister_client_failures_.inc(val);
}

// Client Lifecycle Metrics
void MasterMetricManager::inc_clients_disconnected_total(int64_t val) {
    clients_disconnected_total_.inc(val);
}
void MasterMetricManager::inc_clients_recovered_total(int64_t val) {
    clients_recovered_total_.inc(val);
}
void MasterMetricManager::inc_clients_crashed_total(int64_t val) {
    clients_crashed_total_.inc(val);
}
int64_t MasterMetricManager::get_register_client_requests() {
    return register_client_requests_.value();
}
int64_t MasterMetricManager::get_register_client_failures() {
    return register_client_failures_.value();
}
int64_t MasterMetricManager::get_unregister_client_requests() {
    return unregister_client_requests_.value();
}
int64_t MasterMetricManager::get_unregister_client_failures() {
    return unregister_client_failures_.value();
}
int64_t MasterMetricManager::get_clients_disconnected_total() {
    return clients_disconnected_total_.value();
}
int64_t MasterMetricManager::get_clients_recovered_total() {
    return clients_recovered_total_.value();
}
int64_t MasterMetricManager::get_clients_crashed_total() {
    return clients_crashed_total_.value();
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

void MasterMetricManager::inc_heartbeat_requests(int64_t val) {
    heartbeat_requests_.inc(val);
}

void MasterMetricManager::inc_heartbeat_failures(int64_t val) {
    heartbeat_failures_.inc(val);
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

// Operation Statistics Getters
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

int64_t MasterMetricManager::get_heartbeat_requests() {
    return heartbeat_requests_.value();
}

int64_t MasterMetricManager::get_heartbeat_failures() {
    return heartbeat_failures_.value();
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
    serialize_metric(active_clients_);
    serialize_metric(register_client_requests_);
    serialize_metric(register_client_failures_);
    serialize_metric(unregister_client_requests_);
    serialize_metric(unregister_client_failures_);
    serialize_metric(clients_disconnected_total_);
    serialize_metric(clients_recovered_total_);
    serialize_metric(clients_crashed_total_);

    // Serialize Histogram
    serialize_metric(value_size_distribution_);

    // Serialize Request Counters
    serialize_metric(exist_key_requests_);
    serialize_metric(exist_key_failures_);
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
    serialize_metric(heartbeat_requests_);
    serialize_metric(heartbeat_failures_);

    // Serialize Batch Request Counters
    serialize_metric(batch_exist_key_requests_);
    serialize_metric(batch_exist_key_failures_);
    serialize_metric(batch_query_ip_requests_);
    serialize_metric(batch_query_ip_failures_);
    serialize_metric(batch_get_replica_list_requests_);
    serialize_metric(batch_get_replica_list_failures_);
    serialize_metric(batch_get_replica_list_partial_successes_);
    serialize_metric(batch_get_replica_list_items_);
    serialize_metric(batch_get_replica_list_failed_items_);

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
    int64_t active_clients = active_clients_.value();

    // Request counters
    int64_t exist_keys = exist_key_requests_.value();
    int64_t exist_key_fails = exist_key_failures_.value();
    int64_t get_replicas = get_replica_list_requests_.value();
    int64_t get_replica_fails = get_replica_list_failures_.value();
    int64_t removes = remove_requests_.value();
    int64_t remove_fails = remove_failures_.value();
    int64_t remove_all = remove_all_requests_.value();
    int64_t remove_all_fails = remove_all_failures_.value();

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

    // Heartbeat counters
    int64_t heartbeat = heartbeat_requests_.value();
    int64_t heartbeat_fails = heartbeat_failures_.value();

    // --- Format the summary string ---
    ss << "Mem Storage: " << byte_size_to_string(mem_allocated) << " / "
       << byte_size_to_string(mem_capacity);
    if (mem_capacity > 0) {
        ss << " (" << std::fixed << std::setprecision(1)
           << ((double)mem_allocated / (double)mem_capacity * 100.0) << "%)";
    }
    ss << " | SSD Storage: " << byte_size_to_string(file_allocated) << " / "
       << byte_size_to_string(file_capacity);
    ss << " | Keys: " << keys;
    ss << " | Clients: " << active_clients;

    // Request summary - focus on the most important metrics
    ss << " | Requests (Success/Total): ";
    ss << "Get=" << get_replicas - get_replica_fails << "/" << get_replicas
       << ", ";
    ss << "Exist=" << exist_keys - exist_key_fails << "/" << exist_keys << ", ";
    ss << "Del=" << removes - remove_fails << "/" << removes << ", ";
    ss << "DelAll=" << remove_all - remove_all_fails << "/" << remove_all
       << ", ";
    ss << "Heartbeat=" << heartbeat - heartbeat_fails << "/" << heartbeat;

    // Batch request summary
    ss << " | Batch Requests "
          "(Req=Success/PartialSuccess/Total, Item=Success/Total): ";
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
       << batch_query_ip_items << ")";

    return ss.str();
}

}  // namespace mooncake
