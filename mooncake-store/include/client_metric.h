#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <iomanip>
#include <limits>
#include <mutex>
#include <optional>
#include <sstream>
#include <thread>
#include <unordered_set>
#include <vector>
#include <ylt/metric/counter.hpp>
#include <ylt/metric/gauge.hpp>
#include <ylt/metric/histogram.hpp>
#include <ylt/metric/summary.hpp>
#include "utils.h"
#include "hybrid_metric.h"

namespace mooncake {

// latency bucket is in microsecond
// Tuned for RDMA: fine-grained in <1ms, with ms-scale tail up to 1s
const std::vector<double> kLatencyBucket = {
    // sub-ms to 1ms region
    50, 75, 125, 150, 200, 250, 300, 400, 500, 750, 1000,
    // ms-level tail for batch/occasional spikes
    1500, 2000, 3000, 5000, 7000, 15000, 20000,
    // safeguards for long tails
    50000, 100000, 200000, 500000, 1000000, 2000000, 5000000, 10000000,
    20000000};

static inline std::string get_env_or_default(
    const char* env_var, const std::string& default_val = "") {
    const char* val = getenv(env_var);
    return val ? val : default_val;
}

// In production mode, more labels are needed for monitoring and troubleshooting
// Static labels include but are not limited to machine address, cluster name,
// etc. These labels remain constant during the lifetime of the application
const std::string kClusterID = get_env_or_default("MC_STORE_CLUSTER_ID");

// Merge static labels with dynamic labels
const inline std::map<std::string, std::string> merge_labels(
    const std::map<std::string, std::string>& labels) {
    std::map<std::string, std::string> merged_labels;
    if (!kClusterID.empty()) {
        merged_labels["cluster_id"] = kClusterID;
    }
    merged_labels.insert(labels.begin(), labels.end());
    return merged_labels;
}

inline std::string format_metric_rate(double value, const char* suffix) {
    const double KB = 1024.0;
    const double MB = KB * 1024.0;
    const double GB = MB * 1024.0;
    const double TB = GB * 1024.0;

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    if (value >= TB) {
        oss << value / TB << " T" << suffix;
    } else if (value >= GB) {
        oss << value / GB << " G" << suffix;
    } else if (value >= MB) {
        oss << value / MB << " M" << suffix;
    } else if (value >= KB) {
        oss << value / KB << " K" << suffix;
    } else {
        oss << value << " " << suffix;
    }
    return oss.str();
}

inline std::string format_metric_bandwidth(uint64_t total_bytes,
                                           double elapsed_seconds) {
    return format_metric_rate(total_bytes / elapsed_seconds, "B/s");
}

inline uint64_t elapsed_us_since(
    std::chrono::steady_clock::time_point start_time) {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - start_time)
            .count());
}

template <typename Result, typename Operation, typename SuccessFn,
          typename ObserveFn>
Result execute_timed_operation(Operation&& operation, SuccessFn&& success_fn,
                               ObserveFn&& observe_fn) {
    const auto start_time = std::chrono::steady_clock::now();
    Result result = std::forward<Operation>(operation)();
    if (std::forward<SuccessFn>(success_fn)(result)) {
        std::forward<ObserveFn>(observe_fn)(elapsed_us_since(start_time),
                                            result);
    }
    return result;
}

enum class TransferOperationKind { kRead, kWrite };

enum class EgmStorePoolStage {
    kPreflight,
    kDiscovery,
    kPlanning,
    kAllocation,
    kAccess,
    kRegistration,
    kMount,
    kRollback,
    kTeardown,
};

inline const char* EgmStorePoolStageName(EgmStorePoolStage stage) {
    switch (stage) {
        case EgmStorePoolStage::kPreflight:
            return "preflight";
        case EgmStorePoolStage::kDiscovery:
            return "discovery";
        case EgmStorePoolStage::kPlanning:
            return "planning";
        case EgmStorePoolStage::kAllocation:
            return "allocation";
        case EgmStorePoolStage::kAccess:
            return "access";
        case EgmStorePoolStage::kRegistration:
            return "registration";
        case EgmStorePoolStage::kMount:
            return "mount";
        case EgmStorePoolStage::kRollback:
            return "rollback";
        case EgmStorePoolStage::kTeardown:
            return "teardown";
    }
    return "unknown";
}

struct EgmStorePoolMetric {
    std::array<std::string, 1> numa_node_label = {"numa_node"};
    std::array<std::string, 1> stage_label = {"stage"};

    explicit EgmStorePoolMetric(std::map<std::string, std::string> = {})
        : requested_capacity_bytes(
              "mooncake_egm_store_pool_requested_capacity_bytes",
              "Requested EGM Store Pool Provider capacity in bytes", {}),
          effective_capacity_bytes(
              "mooncake_egm_store_pool_effective_capacity_bytes",
              "Effective EGM Store Pool Provider capacity in bytes", {}),
          effective_bytes_by_node(
              "mooncake_egm_store_pool_node_effective_bytes",
              "Effective EGM Store Pool Provider bytes by NUMA node",
              numa_node_label),
          chunk_count_by_node(
              "mooncake_egm_store_pool_node_chunks",
              "EGM Store Pool Provider chunk count by NUMA node",
              numa_node_label),
          stage_duration_us(
              "mooncake_egm_store_pool_stage_duration_us",
              "EGM Store Pool setup stage duration in microseconds",
              kLatencyBucket, {}, stage_label),
          initialization_failures(
              "mooncake_egm_store_pool_initialization_failures_total",
              "EGM Store Pool initialization failures by stage", {},
              stage_label),
          rollback_attempts("mooncake_egm_store_pool_rollback_attempts_total",
                            "EGM Store Pool rollback attempts", {}),
          rollback_failures("mooncake_egm_store_pool_rollback_failures_total",
                            "EGM Store Pool rollback failures", {}),
          cleanup_pending("mooncake_egm_store_pool_cleanup_pending",
                          "Whether EGM Store Pool cleanup must be retried", {}),
          cleanup_pending_allocations(
              "mooncake_egm_store_pool_cleanup_pending_allocations",
              "VMM allocations retained by retryable EGM Store Pool cleanup",
              {}),
          cleanup_pending_bytes(
              "mooncake_egm_store_pool_cleanup_pending_bytes",
              "VMM bytes retained by retryable EGM Store Pool cleanup", {}),
          cleanup_attempts(
              "mooncake_egm_store_pool_cleanup_attempts_total",
              "EGM Store Pool rollback and teardown cleanup attempts", {}),
          cleanup_failures(
              "mooncake_egm_store_pool_cleanup_failures_total",
              "EGM Store Pool rollback and teardown cleanup failures", {}),
          process_quarantine_clients(
              "mooncake_egm_store_pool_process_quarantine_clients",
              "Destroyed clients retained in the process EGM quarantine", {}),
          process_quarantine_allocations(
              "mooncake_egm_store_pool_process_quarantine_allocations",
              "VMM allocations retained in the process EGM quarantine", {}),
          process_quarantine_bytes(
              "mooncake_egm_store_pool_process_quarantine_bytes",
              "VMM bytes retained in the process EGM quarantine", {}) {}

    ylt::metric::gauge_t requested_capacity_bytes;
    ylt::metric::gauge_t effective_capacity_bytes;
    ylt::metric::dynamic_gauge_1t effective_bytes_by_node;
    ylt::metric::dynamic_gauge_1t chunk_count_by_node;
    ylt::metric::hybrid_histogram_1t stage_duration_us;
    ylt::metric::hybrid_counter_1t initialization_failures;
    ylt::metric::counter_t rollback_attempts;
    ylt::metric::counter_t rollback_failures;
    ylt::metric::gauge_t cleanup_pending;
    ylt::metric::gauge_t cleanup_pending_allocations;
    ylt::metric::gauge_t cleanup_pending_bytes;
    ylt::metric::counter_t cleanup_attempts;
    ylt::metric::counter_t cleanup_failures;
    ylt::metric::gauge_t process_quarantine_clients;
    ylt::metric::gauge_t process_quarantine_allocations;
    ylt::metric::gauge_t process_quarantine_bytes;

    void SetCapacity(uint64_t requested_bytes, uint64_t effective_bytes) {
        requested_capacity_bytes.update(static_cast<int64_t>(requested_bytes));
        effective_capacity_bytes.update(static_cast<int64_t>(effective_bytes));
    }

    void SetNodeCapacity(int numa_node, uint64_t effective_bytes,
                         uint64_t chunk_count) {
        const std::array<std::string, 1> label = {std::to_string(numa_node)};
        effective_bytes_by_node.update(label,
                                       static_cast<int64_t>(effective_bytes));
        chunk_count_by_node.update(label, static_cast<int64_t>(chunk_count));
    }

    void ObserveStage(EgmStorePoolStage stage, uint64_t duration_us,
                      bool success) {
        const std::array<std::string, 1> label = {EgmStorePoolStageName(stage)};
        // basic_hybrid_histogram omits zero-valued observations during
        // serialization and may clear the caller's output buffer when every
        // observation is zero. Preserve sub-microsecond stages as 1 us so
        // serializing this metric cannot erase metrics appended before it.
        stage_duration_us.observe(label, std::max<uint64_t>(duration_us, 1));
        if (!success) {
            initialization_failures.inc(label);
        }
    }

    void ObserveRollback(bool success) {
        rollback_attempts.inc();
        if (!success) {
            rollback_failures.inc();
        }
    }

    void ObserveCleanup(bool success, uint64_t pending_allocations,
                        uint64_t pending_bytes) {
        cleanup_attempts.inc();
        if (!success) cleanup_failures.inc();
        cleanup_pending.update(success ? 0 : 1);
        cleanup_pending_allocations.update(
            static_cast<int64_t>(std::min<uint64_t>(
                pending_allocations, std::numeric_limits<int64_t>::max())));
        cleanup_pending_bytes.update(static_cast<int64_t>(std::min<uint64_t>(
            pending_bytes, std::numeric_limits<int64_t>::max())));
    }

    void SetProcessQuarantine(uint64_t clients, uint64_t allocations,
                              uint64_t bytes) {
        process_quarantine_clients.update(static_cast<int64_t>(
            std::min<uint64_t>(clients, std::numeric_limits<int64_t>::max())));
        process_quarantine_allocations.update(
            static_cast<int64_t>(std::min<uint64_t>(
                allocations, std::numeric_limits<int64_t>::max())));
        process_quarantine_bytes.update(static_cast<int64_t>(
            std::min<uint64_t>(bytes, std::numeric_limits<int64_t>::max())));
    }

    void serialize(std::string& str) {
        requested_capacity_bytes.serialize(str);
        effective_capacity_bytes.serialize(str);
        effective_bytes_by_node.serialize(str);
        chunk_count_by_node.serialize(str);
        stage_duration_us.serialize(str);
        initialization_failures.serialize(str);
        rollback_attempts.serialize(str);
        rollback_failures.serialize(str);
        cleanup_pending.serialize(str);
        cleanup_pending_allocations.serialize(str);
        cleanup_pending_bytes.serialize(str);
        cleanup_attempts.serialize(str);
        cleanup_failures.serialize(str);
        process_quarantine_clients.serialize(str);
        process_quarantine_allocations.serialize(str);
        process_quarantine_bytes.serialize(str);
    }
};

struct TransferMetric {
    TransferMetric(std::map<std::string, std::string> labels = {})
        : total_read_bytes("mooncake_transfer_read_bytes", "Total bytes read",
                           labels),
          total_write_bytes("mooncake_transfer_write_bytes",
                            "Total bytes written", labels),
          batch_put_latency_us("mooncake_transfer_batch_put_latency",
                               "Batch Put transfer latency (us)",
                               kLatencyBucket, labels),
          batch_get_latency_us("mooncake_transfer_batch_get_latency",
                               "Batch Get transfer latency (us)",
                               kLatencyBucket, labels),
          get_latency_us("mooncake_transfer_get_latency",
                         "Get transfer latency (us)", kLatencyBucket, labels),
          put_latency_us("mooncake_transfer_put_latency",
                         "Put transfer latency (us)", kLatencyBucket, labels),
          start_time_(std::chrono::steady_clock::now()) {}

    ylt::metric::counter_t total_read_bytes;
    ylt::metric::counter_t total_write_bytes;
    ylt::metric::histogram_t batch_put_latency_us;
    ylt::metric::histogram_t batch_get_latency_us;
    ylt::metric::histogram_t get_latency_us;
    ylt::metric::histogram_t put_latency_us;

    void serialize(std::string& str) {
        total_read_bytes.serialize(str);
        total_write_bytes.serialize(str);
        batch_put_latency_us.serialize(str);
        batch_get_latency_us.serialize(str);
        get_latency_us.serialize(str);
        put_latency_us.serialize(str);
    }

    std::string summary_metrics(bool include_bandwidth = true) {
        std::stringstream ss;
        ss << "=== Transfer Metrics Summary ===\n";

        // Bytes transferred
        auto read_bytes = total_read_bytes.value();
        auto write_bytes = total_write_bytes.value();
        ss << "Total Read: " << byte_size_to_string(read_bytes) << "\n";
        ss << "Total Write: " << byte_size_to_string(write_bytes) << "\n";
        if (include_bandwidth) {
            ss << "Average Read Throughput: "
               << format_metric_bandwidth(read_bytes, elapsed_seconds())
               << "\n";
            ss << "Average Write Throughput: "
               << format_metric_bandwidth(write_bytes, elapsed_seconds())
               << "\n";
        }

        // Latency summaries
        ss << "\n=== Latency Summary (microseconds) ===\n";
        ss << "Get: " << format_latency_summary(get_latency_us) << "\n";
        ss << "Put: " << format_latency_summary(put_latency_us) << "\n";
        ss << "Batch Get: " << format_latency_summary(batch_get_latency_us)
           << "\n";
        ss << "Batch Put: " << format_latency_summary(batch_put_latency_us)
           << "\n";

        return ss.str();
    }

   private:
    std::chrono::steady_clock::time_point start_time_;

    double elapsed_seconds() const {
        const auto elapsed = std::chrono::duration<double>(
            std::chrono::steady_clock::now() - start_time_);
        return std::max(elapsed.count(), 1e-9);
    }

    std::string format_latency_summary(ylt::metric::histogram_t& hist) {
        // Access the internal sum and bucket counts
        auto sum_ptr =
            const_cast<ylt::metric::histogram_t&>(hist).get_bucket_counts();
        if (sum_ptr.empty()) {
            return "No data";
        }

        // Calculate total count from all buckets
        int64_t total_count = 0;
        for (auto& bucket : sum_ptr) {
            total_count += bucket->value();
        }

        if (total_count == 0) {
            return "No data";
        }

        // Get sum from the histogram's internal sum gauge
        // Note: We need to access the private sum_ member, which requires
        // friendship or reflection For now, let's use a simpler approach
        // showing just count
        std::stringstream ss;
        ss << "count=" << total_count;

        // Find P95
        int64_t p95_target = (total_count * 95) / 100;
        int64_t cumulative = 0;
        double p95_bucket = 0;

        for (size_t i = 0; i < sum_ptr.size() && i < kLatencyBucket.size();
             i++) {
            cumulative += sum_ptr[i]->value();
            if (cumulative >= p95_target && p95_bucket == 0) {
                p95_bucket = kLatencyBucket[i];
                break;
            }
        }

        if (p95_bucket > 0) {
            ss << ", p95<" << p95_bucket << "μs";
        }

        // Find max bucket (highest bucket with data)
        double max_bucket = 0;
        for (size_t i = sum_ptr.size(); i > 0; i--) {
            size_t idx = i - 1;
            if (idx < kLatencyBucket.size() && sum_ptr[idx]->value() > 0) {
                max_bucket = kLatencyBucket[idx];
                break;
            }
        }

        if (max_bucket > 0) {
            ss << ", max<" << max_bucket << "μs";
        }

        return ss.str();
    }
};

struct MasterClientMetric {
    std::array<std::string, 1> rpc_names = {"rpc_name"};

    MasterClientMetric(std::map<std::string, std::string> labels = {})
        : rpc_count("mooncake_client_rpc_count",
                    "Total number of RPC calls made by the client", labels,
                    rpc_names),
          rpc_latency("mooncake_client_rpc_latency",
                      "Latency of RPC calls made by the client (in us)",
                      kLatencyBucket, labels, rpc_names) {}

    ylt::metric::hybrid_counter_1t rpc_count;
    ylt::metric::hybrid_histogram_1t rpc_latency;
    void serialize(std::string& str) {
        rpc_count.serialize(str);
        rpc_latency.serialize(str);
    }

    std::string summary_metrics() {
        std::stringstream ss;
        ss << "=== RPC Metrics Summary ===\n";

        // For dynamic metrics, we need to check if there are any labels with
        // data
        if (rpc_count.label_value_count() == 0) {
            ss << "No RPC calls recorded\n";
            return ss.str();
        }

        // Get all available RPC names from the dynamic metrics
        // We'll iterate through all possible RPC names instead of using a fixed
        // list
        std::vector<std::string> all_rpc_names = {"GetReplicaList",
                                                  "PutStart",
                                                  "PutEnd",
                                                  "PutRevoke",
                                                  "ExistKey",
                                                  "Remove",
                                                  "RemoveAll",
                                                  "MountSegment",
                                                  "UnmountSegment",
                                                  "GetFsdir",
                                                  "BatchGetReplicaList",
                                                  "BatchPutStart",
                                                  "BatchPutEnd",
                                                  "BatchPutRevoke",
                                                  "MountLocalDiskSegment",
                                                  "OffloadObjectHeartbeat",
                                                  "NotifyOffloadSuccess"};

        bool found_any = false;
        for (const auto& rpc_name : all_rpc_names) {
            std::array<std::string, 1> label_array = {rpc_name};

            // Check if this RPC has any data by trying to access bucket counts
            auto bucket_counts = rpc_latency.get_bucket_counts();
            int64_t total_count = 0;
            for (auto& bucket : bucket_counts) {
                total_count += bucket->value(label_array);
            }

            // Skip RPCs with zero count
            if (total_count == 0) continue;

            found_any = true;
            ss << rpc_name << ": count=" << total_count;

            // Find P95
            int64_t p95_target = (total_count * 95) / 100;
            int64_t cumulative = 0;
            double p95_bucket = 0;

            for (size_t i = 0;
                 i < bucket_counts.size() && i < kLatencyBucket.size(); i++) {
                cumulative += bucket_counts[i]->value(label_array);
                if (cumulative >= p95_target && p95_bucket == 0) {
                    p95_bucket = kLatencyBucket[i];
                    break;
                }
            }

            if (p95_bucket > 0) {
                ss << ", p95<" << p95_bucket << "μs";
            }

            // Find max bucket (highest bucket with data)
            double max_bucket = 0;
            for (size_t i = bucket_counts.size(); i > 0; i--) {
                size_t idx = i - 1;
                if (idx < kLatencyBucket.size() &&
                    bucket_counts[idx]->value(label_array) > 0) {
                    max_bucket = kLatencyBucket[idx];
                    break;
                }
            }

            if (max_bucket > 0) {
                ss << ", max<" << max_bucket << "μs";
            }

            ss << "\n";
        }

        if (!found_any) {
            ss << "No RPC calls recorded\n";
        }

        return ss.str();
    }
};

struct TransferOperationMetric {
    std::array<std::string, 1> op_names = {"op_name"};

    explicit TransferOperationMetric(
        std::map<std::string, std::string> labels = {})
        : read_op_count("mooncake_transfer_read_operation_count",
                        "Total read operations by interface type", labels,
                        op_names),
          read_op_bytes("mooncake_transfer_read_operation_bytes",
                        "Total read bytes by interface type", labels, op_names),
          read_op_latency_us("mooncake_transfer_read_operation_latency",
                             "Read operation latency by interface type (us)",
                             kLatencyBucket, labels, op_names),
          write_op_count("mooncake_transfer_write_operation_count",
                         "Total write operations by interface type", labels,
                         op_names),
          write_op_bytes("mooncake_transfer_write_operation_bytes",
                         "Total write bytes by interface type", labels,
                         op_names),
          write_op_latency_us("mooncake_transfer_write_operation_latency",
                              "Write operation latency by interface type (us)",
                              kLatencyBucket, labels, op_names) {}

    ylt::metric::hybrid_counter_1t read_op_count;
    ylt::metric::hybrid_counter_1t read_op_bytes;
    ylt::metric::hybrid_histogram_1t read_op_latency_us;
    ylt::metric::hybrid_counter_1t write_op_count;
    ylt::metric::hybrid_counter_1t write_op_bytes;
    ylt::metric::hybrid_histogram_1t write_op_latency_us;

    void Observe(TransferOperationKind kind, const std::string& op_name,
                 uint64_t bytes, uint64_t latency_us) {
        const std::array<std::string, 1> label = {op_name};
        {
            std::lock_guard<std::mutex> lock(observed_ops_mutex_);
            if (kind == TransferOperationKind::kRead) {
                observed_read_ops_.insert(op_name);
            } else {
                observed_write_ops_.insert(op_name);
            }
        }

        if (kind == TransferOperationKind::kRead) {
            read_op_count.inc(label);
            read_op_bytes.inc(label, bytes);
            read_op_latency_us.observe(label, latency_us);
        } else {
            write_op_count.inc(label);
            write_op_bytes.inc(label, bytes);
            write_op_latency_us.observe(label, latency_us);
        }
    }

    void serialize(std::string& str) {
        read_op_count.serialize(str);
        read_op_bytes.serialize(str);
        read_op_latency_us.serialize(str);
        write_op_count.serialize(str);
        write_op_bytes.serialize(str);
        write_op_latency_us.serialize(str);
    }

    std::string summary_metrics() {
        std::stringstream ss;
        ss << "=== Interface Operation Metrics Summary ===\n";
        ss << format_operation_group_summary(
                  "Read Interfaces", snapshot_operations(observed_read_ops_),
                  read_op_count, read_op_bytes, read_op_latency_us)
           << "\n";
        ss << format_operation_group_summary(
            "Write Interfaces", snapshot_operations(observed_write_ops_),
            write_op_count, write_op_bytes, write_op_latency_us);
        return ss.str();
    }

   private:
    std::mutex observed_ops_mutex_;
    std::unordered_set<std::string> observed_read_ops_;
    std::unordered_set<std::string> observed_write_ops_;

    std::vector<std::string> snapshot_operations(
        const std::unordered_set<std::string>& source) {
        std::lock_guard<std::mutex> lock(observed_ops_mutex_);
        std::vector<std::string> ops(source.begin(), source.end());
        std::sort(ops.begin(), ops.end());
        return ops;
    }

    std::string format_operation_group_summary(
        const std::string& group_name, const std::vector<std::string>& ops,
        ylt::metric::hybrid_counter_1t& op_count,
        ylt::metric::hybrid_counter_1t& op_bytes,
        ylt::metric::hybrid_histogram_1t& op_latency_us) {
        std::stringstream ss;
        ss << group_name << ":\n";
        if (ops.empty()) {
            ss << "No data";
            return ss.str();
        }

        auto bucket_counts = op_latency_us.get_bucket_counts();
        bool found_any = false;
        for (const auto& op_name : ops) {
            const std::array<std::string, 1> label = {op_name};
            const int64_t total_count = op_count.value(label);
            if (total_count == 0) {
                continue;
            }

            found_any = true;
            ss << op_name << ": count=" << total_count << ", bytes="
               << byte_size_to_string(
                      static_cast<uint64_t>(op_bytes.value(label)));

            int64_t p95_target = (total_count * 95) / 100;
            int64_t cumulative = 0;
            double p95_bucket = 0;
            for (size_t i = 0;
                 i < bucket_counts.size() && i < kLatencyBucket.size(); ++i) {
                cumulative += bucket_counts[i]->value(label);
                if (cumulative >= p95_target && p95_bucket == 0) {
                    p95_bucket = kLatencyBucket[i];
                    break;
                }
            }
            if (p95_bucket > 0) {
                ss << ", p95<" << p95_bucket << "μs";
            }

            double max_bucket = 0;
            for (size_t i = bucket_counts.size(); i > 0; --i) {
                const size_t idx = i - 1;
                if (idx < kLatencyBucket.size() &&
                    bucket_counts[idx]->value(label) > 0) {
                    max_bucket = kLatencyBucket[idx];
                    break;
                }
            }
            if (max_bucket > 0) {
                ss << ", max<" << max_bucket << "μs";
            }
            ss << "\n";
        }

        if (!found_any) {
            ss << "No data";
        }
        return ss.str();
    }
};

// SSD latency bucket: microseconds, tuned for SSD/network storage
// Range: 50us (high-end NVMe) to 30s (3fs/nfs large object batch writes)
inline const std::vector<double> kSsdLatencyBucket = {
    50,       100,     200,                      // <200us (high-end NVMe)
    500,      1000,    2000,    5000,    10000,  // 500us - 10ms
    20000,    50000,   100000,  200000,          // 10ms - 200ms
    500000,   1000000, 2000000, 5000000,         // 500ms - 5s
    10000000, 30000000                           // 10s - 30s (3fs/nfs)
};

struct SsdMetric {
    SsdMetric(std::map<std::string, std::string> labels = {})
        : ssd_read_bytes("mooncake_ssd_read_bytes_total",
                         "Total bytes read from SSD", labels),
          ssd_write_bytes("mooncake_ssd_write_bytes_total",
                          "Total bytes written to SSD", labels),
          ssd_read_ops("mooncake_ssd_read_ops_total",
                       "Total number of SSD read operations (key count)",
                       labels),
          ssd_write_ops("mooncake_ssd_write_ops_total",
                        "Total number of SSD write operations (key count)",
                        labels),
          ssd_read_latency_us("mooncake_ssd_read_latency_us",
                              "SSD BatchLoad latency per batch (us)",
                              kSsdLatencyBucket, labels),
          ssd_write_latency_us("mooncake_ssd_write_latency_us",
                               "SSD BatchOffload latency per batch (us)",
                               kSsdLatencyBucket, labels),
          ssd_total_bytes("mooncake_ssd_total_bytes_total",
                          "Total bytes read and written to SSD", labels),
          ssd_total_ops("mooncake_ssd_total_ops_total",
                        "Total number of SSD operations (key count)", labels),
          ssd_total_latency_us("mooncake_ssd_total_latency_us",
                               "SSD total latency per batch (us)",
                               kSsdLatencyBucket, labels),
          ssd_read_latency_summary("mooncake_ssd_read_latency_summary_us",
                                   "SSD read latency quantiles (us)",
                                   {0.5, 0.9, 0.99}, labels),
          ssd_write_latency_summary("mooncake_ssd_write_latency_summary_us",
                                    "SSD write latency quantiles (us)",
                                    {0.5, 0.9, 0.99}, labels),
          ssd_total_latency_summary("mooncake_ssd_total_latency_summary_us",
                                    "SSD total latency quantiles (us)",
                                    {0.5, 0.9, 0.99}, labels),
          start_time_(std::chrono::steady_clock::now()) {}

    ylt::metric::counter_t ssd_read_bytes;
    ylt::metric::counter_t ssd_write_bytes;
    ylt::metric::counter_t ssd_read_ops;
    ylt::metric::counter_t ssd_write_ops;
    ylt::metric::histogram_t ssd_read_latency_us;
    ylt::metric::histogram_t ssd_write_latency_us;
    ylt::metric::counter_t ssd_total_bytes;
    ylt::metric::counter_t ssd_total_ops;
    ylt::metric::histogram_t ssd_total_latency_us;
    ylt::metric::summary_t ssd_read_latency_summary;
    ylt::metric::summary_t ssd_write_latency_summary;
    ylt::metric::summary_t ssd_total_latency_summary;
    std::chrono::steady_clock::time_point start_time_;

    void serialize(std::string& str) {
        ssd_read_bytes.serialize(str);
        ssd_write_bytes.serialize(str);
        ssd_read_ops.serialize(str);
        ssd_write_ops.serialize(str);
        ssd_read_latency_us.serialize(str);
        ssd_write_latency_us.serialize(str);
        ssd_total_bytes.serialize(str);
        ssd_total_ops.serialize(str);
        ssd_total_latency_us.serialize(str);
        ssd_read_latency_summary.serialize(str);
        ssd_write_latency_summary.serialize(str);
        ssd_total_latency_summary.serialize(str);
    }

    std::string summary_metrics() {
        std::stringstream ss;
        ss << "=== SSD Metrics Summary ===" << "\n";

        auto read_bytes = ssd_read_bytes.value();
        auto write_bytes = ssd_write_bytes.value();
        auto read_ops = ssd_read_ops.value();
        auto write_ops = ssd_write_ops.value();

        auto elapsed_s = std::chrono::duration<double>(
                             std::chrono::steady_clock::now() - start_time_)
                             .count();

        ss << "SSD Read: " << byte_size_to_string(read_bytes)
           << ", ops=" << read_ops;
        if (elapsed_s > 0 && read_bytes > 0) {
            ss << ", throughput="
               << byte_size_to_string(
                      static_cast<int64_t>(read_bytes / elapsed_s))
               << "/s";
            ss << ", IOPS=" << std::fixed << std::setprecision(1)
               << (read_ops / elapsed_s);
        }
        ss << "\n";

        ss << "SSD Write: " << byte_size_to_string(write_bytes)
           << ", ops=" << write_ops;
        if (elapsed_s > 0 && write_bytes > 0) {
            ss << ", throughput="
               << byte_size_to_string(
                      static_cast<int64_t>(write_bytes / elapsed_s))
               << "/s";
            ss << ", IOPS=" << std::fixed << std::setprecision(1)
               << (write_ops / elapsed_s);
        }
        ss << "\n";

        auto total_bytes = ssd_total_bytes.value();
        auto total_ops = ssd_total_ops.value();
        ss << "SSD Total: " << byte_size_to_string(total_bytes)
           << ", ops=" << total_ops;
        if (elapsed_s > 0 && total_bytes > 0) {
            ss << ", throughput="
               << byte_size_to_string(
                      static_cast<int64_t>(total_bytes / elapsed_s))
               << "/s";
            ss << ", IOPS=" << std::fixed << std::setprecision(1)
               << (total_ops / elapsed_s);
        }
        ss << "\n";

        ss << "\n" << "=== SSD Latency Summary (microseconds) ===" << "\n";
        ss << "Read: " << format_summary_percentiles(ssd_read_latency_summary)
           << "\n";
        ss << "Write: " << format_summary_percentiles(ssd_write_latency_summary)
           << "\n";
        ss << "Total: " << format_summary_percentiles(ssd_total_latency_summary)
           << "\n";

        return ss.str();
    }

   private:
    std::string format_summary_percentiles(ylt::metric::summary_t& summary) {
        double sum = 0;
        uint64_t count = 0;
        auto rates = summary.get_rates(sum, count);

        if (count == 0) {
            return "No data";
        }

        std::stringstream ss;
        ss << std::fixed << std::setprecision(1);
        ss << "count=" << count;
        if (rates.size() >= 1) ss << ", p50=" << rates[0] << "us";
        if (rates.size() >= 2) ss << ", p90=" << rates[1] << "us";
        if (rates.size() >= 3) ss << ", p99=" << rates[2] << "us";
        if (count > 0) {
            ss << ", avg=" << (sum / count) << "us";
        }
        return ss.str();
    }
};

struct ClientMetric {
    TransferMetric transfer_metric;
    MasterClientMetric master_client_metric;
    TransferOperationMetric transfer_operation_metric;
    SsdMetric ssd_metric;
    EgmStorePoolMetric egm_store_pool_metric;
    ylt::metric::counter_t mount_segment_compensation_failures;

    /**
     * @brief Creates a ClientMetric instance based on environment variables
     * @return std::unique_ptr<ClientMetric> containing the instance if enabled,
     *         nullptr if disabled
     *
     * Environment variables:
     * - MC_STORE_CLIENT_METRIC: Enable/disable metrics (enabled by default,
     *   set to 0/false to disable)
     * - MC_STORE_CLIENT_METRIC_INTERVAL: Reporting interval in seconds
     *   (default: 0, 0 = collect but don't report)
     */
    static std::unique_ptr<ClientMetric> Create(
        const std::map<std::string, std::string>& labels = {},
        bool master_rpc_metrics_enabled = true);

    void ObserveTransferOperation(TransferOperationKind kind,
                                  const std::string& op_name, uint64_t bytes,
                                  uint64_t latency_us) {
        transfer_operation_metric.Observe(kind, op_name, bytes, latency_us);
    }

    void ObserveEgmStorePoolCapacity(uint64_t requested_bytes,
                                     uint64_t effective_bytes) {
        egm_store_pool_metric.SetCapacity(requested_bytes, effective_bytes);
    }

    void ObserveEgmStorePoolNode(int numa_node, uint64_t effective_bytes,
                                 uint64_t chunk_count) {
        egm_store_pool_metric.SetNodeCapacity(numa_node, effective_bytes,
                                              chunk_count);
    }

    void ObserveEgmStorePoolStage(EgmStorePoolStage stage, uint64_t duration_us,
                                  bool success) {
        egm_store_pool_metric.ObserveStage(stage, duration_us, success);
    }

    void ObserveEgmStorePoolRollback(bool success) {
        egm_store_pool_metric.ObserveRollback(success);
    }

    void ObserveEgmStorePoolCleanup(bool success, uint64_t pending_allocations,
                                    uint64_t pending_bytes) {
        egm_store_pool_metric.ObserveCleanup(success, pending_allocations,
                                             pending_bytes);
    }

    void SetEgmStorePoolProcessQuarantine(uint64_t clients,
                                          uint64_t allocations,
                                          uint64_t bytes) {
        egm_store_pool_metric.SetProcessQuarantine(clients, allocations, bytes);
    }

    void ObserveMountSegmentCompensationFailure() {
        mount_segment_compensation_failures.inc();
    }

    void serialize(std::string& str);
    std::string summary_metrics();

    uint64_t GetReportingInterval() const { return metrics_interval_seconds_; }

    explicit ClientMetric(uint64_t interval_seconds = 0,
                          const std::map<std::string, std::string>& labels = {},
                          bool bandwidth_reporting_enabled = true,
                          bool master_rpc_metrics_enabled = true);
    ~ClientMetric();

   private:
    struct TransferSnapshot {
        uint64_t read_bytes;
        uint64_t write_bytes;
        std::chrono::steady_clock::time_point timestamp;
    };

    // Metrics reporting thread management
    std::jthread metrics_reporting_thread_;
    std::atomic<bool> should_stop_metrics_thread_{false};
    uint64_t metrics_interval_seconds_{0};
    bool bandwidth_reporting_enabled_{true};
    bool master_rpc_metrics_enabled_{true};
    std::mutex snapshot_mutex_;
    std::optional<TransferSnapshot> last_report_snapshot_;

    void StartMetricsReportingThread();
    void StopMetricsReportingThread();
    std::string BuildBandwidthReport();
};
};  // namespace mooncake
