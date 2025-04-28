#pragma once

#include <mutex>
#include <string>

#include "ylt/metric/counter.hpp"
#include "ylt/metric/gauge.hpp"
#include "ylt/metric/histogram.hpp"

namespace mooncake {

class MasterMetricManager {
   public:
    // --- Singleton Access ---
    static MasterMetricManager& instance();

    MasterMetricManager(const MasterMetricManager&) = delete;
    MasterMetricManager& operator=(const MasterMetricManager&) = delete;
    MasterMetricManager(MasterMetricManager&&) = delete;
    MasterMetricManager& operator=(MasterMetricManager&&) = delete;

    // Storage Metrics
    void inc_allocated_size(int64_t val = 1.0);
    void dec_allocated_size(int64_t val = 1.0);
    void inc_total_capacity(int64_t val = 1.0);
    void dec_total_capacity(int64_t val = 1.0);

    // Key/Value Metrics
    void inc_key_count(int64_t val = 1.0);
    void dec_key_count(int64_t val = 1.0);
    void observe_value_size(int64_t size);

    // Operation Statistics (Counters)
    void inc_put_start_requests(int64_t val = 1.0);
    void inc_put_start_failures(int64_t val = 1.0);
    void inc_put_end_requests(int64_t val = 1.0);
    void inc_put_end_failures(int64_t val = 1.0);
    void inc_put_revoke_requests(int64_t val = 1.0);
    void inc_put_revoke_failures(int64_t val = 1.0);
    void inc_get_replica_list_requests(int64_t val = 1.0);
    void inc_get_replica_list_failures(int64_t val = 1.0);
    void inc_remove_requests(int64_t val = 1.0);
    void inc_remove_failures(int64_t val = 1.0);
    void inc_mount_segment_requests(int64_t val = 1.0);
    void inc_mount_segment_failures(int64_t val = 1.0);
    void inc_unmount_segment_requests(int64_t val = 1.0);
    void inc_unmount_segment_failures(int64_t val = 1.0);

    // --- Serialization ---
    /**
     * @brief Serializes all managed metrics into Prometheus text format.
     * @return A string containing the metrics in Prometheus format.
     */
    std::string serialize_metrics();

    /**
     * @brief Generates a concise, human-readable summary of key metrics.
     * @return A string containing the formatted summary.
     */
    std::string get_summary_string();

   private:
    // --- Private Constructor & Destructor ---
    MasterMetricManager();
    ~MasterMetricManager() = default;

    // --- Metric Members ---

    // Storage Metrics
    ylt::metric::gauge_d allocated_size_;  // Use update for gauge
    ylt::metric::gauge_d total_capacity_;  // Use update for gauge

    // Key/Value Metrics
    ylt::metric::gauge_d key_count_;
    ylt::metric::histogram_d value_size_distribution_;

    // Operation Statistics
    ylt::metric::counter_d put_start_requests_;
    ylt::metric::counter_d put_start_failures_;
    ylt::metric::counter_d put_end_requests_;
    ylt::metric::counter_d put_end_failures_;
    ylt::metric::counter_d put_revoke_requests_;
    ylt::metric::counter_d put_revoke_failures_;
    ylt::metric::counter_d get_replica_list_requests_;
    ylt::metric::counter_d get_replica_list_failures_;
    ylt::metric::counter_d remove_requests_;
    ylt::metric::counter_d remove_failures_;
    ylt::metric::counter_d mount_segment_requests_;
    ylt::metric::counter_d mount_segment_failures_;
    ylt::metric::counter_d unmount_segment_requests_;
    ylt::metric::counter_d unmount_segment_failures_;
};

}  // namespace mooncake
