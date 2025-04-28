#include "master_metric_manager.h"

#include <sstream>  // For string building during serialization
#include <vector>   // Required by histogram serialization

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
      // Initialize Histogram (Example buckets: 1KB, 4KB, 16KB, 64KB, 256KB,
      // 1MB, 4MB) Buckets should be sorted.
      value_size_distribution_(
          "master_value_size_bytes", "Distribution of object value sizes",
          {1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0}),
      // Initialize Counters
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
      remove_requests_("master_remove_requests_total",
                       "Total number of Remove requests received"),
      remove_failures_("master_remove_failures_total",
                       "Total number of failed Remove requests"),
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
      gc_tasks_added_("master_gc_tasks_added_total",
                      "Total number of tasks added to the GC queue"),
      gc_tasks_processed_("master_gc_tasks_processed_total",
                          "Total number of tasks processed by the GC thread"),
      gc_remove_failures_("master_gc_remove_failures_total",
                          "Total number of failures during GC key removal") {}

// --- Metric Interface Methods ---

// Storage Metrics
void MasterMetricManager::inc_allocated_size(int64_t val) {
    allocated_size_.inc(val);
}
void MasterMetricManager::dec_allocated_size(int64_t val) {
    allocated_size_.dec(val);
}
void MasterMetricManager::set_allocated_size(int64_t val) {
    allocated_size_.update(val);
}  // Use update for gauge
void MasterMetricManager::inc_total_capacity(int64_t val) {
    total_capacity_.inc(val);
}
void MasterMetricManager::dec_total_capacity(int64_t val) {
    total_capacity_.dec(val);
}
void MasterMetricManager::set_total_capacity(int64_t val) {
    total_capacity_.update(val);
}  // Use update for gauge

// Key/Value Metrics
void MasterMetricManager::inc_key_count(int64_t val) { key_count_.inc(val); }
void MasterMetricManager::dec_key_count(int64_t val) { key_count_.dec(val); }
void MasterMetricManager::set_key_count(int64_t val) {
    key_count_.update(val);
}  // Use update for gauge
void MasterMetricManager::observe_value_size(int64_t size) {
    value_size_distribution_.observe(size);
}

// Operation Statistics (Counters)
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
void MasterMetricManager::inc_remove_requests(int64_t val) {
    remove_requests_.inc(val);
}
void MasterMetricManager::inc_remove_failures(int64_t val) {
    remove_failures_.inc(val);
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

// --- Serialization ---
std::string MasterMetricManager::serialize_metrics() {
    std::lock_guard<std::mutex> lock(serialization_mutex_);
    std::stringstream ss;
    std::string
        temp_str;  // Temporary string for individual metric serialization

    // Serialize Gauges
    allocated_size_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    total_capacity_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    key_count_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();

    // Serialize Histogram
    value_size_distribution_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();

    // Serialize Counters
    put_start_requests_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    put_start_failures_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    put_end_requests_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    put_end_failures_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    put_revoke_requests_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    put_revoke_failures_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    get_replica_list_requests_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    get_replica_list_failures_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    remove_requests_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    remove_failures_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    mount_segment_requests_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    mount_segment_failures_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    unmount_segment_requests_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    unmount_segment_failures_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    gc_tasks_added_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    gc_tasks_processed_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();
    gc_remove_failures_.serialize(temp_str);
    ss << temp_str;
    temp_str.clear();

    return ss.str();
}

}  // namespace mooncake
