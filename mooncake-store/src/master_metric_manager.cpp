#include "master_metric_manager.h"

#include <iomanip>  // For std::fixed, std::setprecision
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
      // Initialize Histogram (4KB, 64KB, 256KB, 1MB, 4MB, 16MB, 64MB)
      value_size_distribution_("master_value_size_bytes",
                               "Distribution of object value sizes",
                               {4096.0, 65536.0, 262144.0, 1048576.0, 4194304.0,
                                16777216.0, 67108864.0}),
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
          "Total number of failed UnmountSegment requests") {}

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

// Key/Value Metrics
void MasterMetricManager::inc_key_count(int64_t val) { key_count_.inc(val); }
void MasterMetricManager::dec_key_count(int64_t val) { key_count_.dec(val); }

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

    // Serialize Histogram
    serialize_metric(value_size_distribution_);

    // Serialize Counters
    serialize_metric(put_start_requests_);
    serialize_metric(put_start_failures_);
    serialize_metric(put_end_requests_);
    serialize_metric(put_end_failures_);
    serialize_metric(put_revoke_requests_);
    serialize_metric(put_revoke_failures_);
    serialize_metric(get_replica_list_requests_);
    serialize_metric(get_replica_list_failures_);
    serialize_metric(remove_requests_);
    serialize_metric(remove_failures_);
    serialize_metric(mount_segment_requests_);
    serialize_metric(mount_segment_failures_);
    serialize_metric(unmount_segment_requests_);
    serialize_metric(unmount_segment_failures_);

    return ss.str();
}

// --- Human-Readable Summary ---
std::string MasterMetricManager::get_summary_string() {
    std::stringstream ss;

    // --- Helper lambda for formatting bytes ---
    auto format_bytes = [](double bytes) -> std::string {
        std::stringstream byte_ss;
        byte_ss << std::fixed << std::setprecision(2);
        if (bytes >= 1024.0 * 1024.0 * 1024.0) {  // GB
            byte_ss << (bytes / (1024.0 * 1024.0 * 1024.0)) << " GB";
        } else if (bytes >= 1024.0 * 1024.0) {  // MB
            byte_ss << (bytes / (1024.0 * 1024.0)) << " MB";
        } else if (bytes >= 1024.0) {  // KB
            byte_ss << (bytes / 1024.0) << " KB";
        } else {
            byte_ss << bytes << " B";
        }
        return byte_ss.str();
    };

    // --- Get current values ---
    double allocated = allocated_size_.value();
    double capacity = total_capacity_.value();
    double keys = key_count_.value();

    // Request counters
    double put_starts = put_start_requests_.value();
    double put_start_fails = put_start_failures_.value();
    double put_ends = put_end_requests_.value();
    double put_end_fails = put_end_failures_.value();
    double get_replicas = get_replica_list_requests_.value();
    double get_replica_fails = get_replica_list_failures_.value();
    double removes = remove_requests_.value();
    double remove_fails = remove_failures_.value();

    // --- Format the summary string ---
    ss << "Storage: " << format_bytes(allocated) << " / "
       << format_bytes(capacity);
    if (capacity > 0) {
        ss << " (" << std::fixed << std::setprecision(1)
           << (allocated / capacity * 100.0) << "%)";
    }
    ss << " | Keys: " << static_cast<int64_t>(keys);

    // Request summary - focus on the most important metrics
    ss << " | Requests (Success/Total): ";
    ss << "Put="
       << static_cast<int64_t>(put_starts - put_start_fails + put_ends -
                               put_end_fails)
       << "/" << static_cast<int64_t>(put_starts + put_ends) << ", ";
    ss << "Get=" << static_cast<int64_t>(get_replicas - get_replica_fails)
       << "/" << static_cast<int64_t>(get_replicas) << ", ";
    ss << "Del=" << static_cast<int64_t>(removes - remove_fails) << "/"
       << static_cast<int64_t>(removes);

    return ss.str();
}

}  // namespace mooncake
