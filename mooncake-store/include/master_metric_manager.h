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
    void inc_allocated_size(int64_t val = 1);
    void dec_allocated_size(int64_t val = 1);
    void inc_total_capacity(int64_t val = 1);
    void dec_total_capacity(int64_t val = 1);
    int64_t get_allocated_size();
    int64_t get_total_capacity();
    double get_global_used_ratio(void);

    // Key/Value Metrics
    void inc_key_count(int64_t val = 1);
    void dec_key_count(int64_t val = 1);
    void inc_soft_pin_key_count(int64_t val = 1);
    void dec_soft_pin_key_count(int64_t val = 1);
    void observe_value_size(int64_t size);
    int64_t get_key_count();
    int64_t get_soft_pin_key_count();

    // Cluster Metrics
    void inc_active_clients(int64_t val = 1);
    void dec_active_clients(int64_t val = 1);
    int64_t get_active_clients();

    // Operation Statistics (Counters)
    void inc_put_start_requests(int64_t val = 1);
    void inc_put_start_failures(int64_t val = 1);
    void inc_put_end_requests(int64_t val = 1);
    void inc_put_end_failures(int64_t val = 1);
    void inc_put_revoke_requests(int64_t val = 1);
    void inc_put_revoke_failures(int64_t val = 1);
    void inc_get_replica_list_by_regex_requests(int64_t val = 1);
    void inc_get_replica_list_by_regex_failures(int64_t val = 1);
    void inc_get_replica_list_requests(int64_t val = 1);
    void inc_get_replica_list_failures(int64_t val = 1);
    void inc_exist_key_requests(int64_t val = 1);
    void inc_exist_key_failures(int64_t val = 1);
    void inc_remove_requests(int64_t val = 1);
    void inc_remove_failures(int64_t val = 1);
    void inc_remove_by_regex_requests(int64_t val = 1);
    void inc_remove_by_regex_failures(int64_t val = 1);
    void inc_remove_all_requests(int64_t val = 1);
    void inc_remove_all_failures(int64_t val = 1);
    void inc_mount_segment_requests(int64_t val = 1);
    void inc_mount_segment_failures(int64_t val = 1);
    void inc_unmount_segment_requests(int64_t val = 1);
    void inc_unmount_segment_failures(int64_t val = 1);
    void inc_remount_segment_requests(int64_t val = 1);
    void inc_remount_segment_failures(int64_t val = 1);
    void inc_ping_requests(int64_t val = 1);
    void inc_ping_failures(int64_t val = 1);

    // Batch Operation Statistics (Counters)
    void inc_batch_exist_key_requests(int64_t items);
    void inc_batch_exist_key_failures(int64_t failed_items);
    void inc_batch_exist_key_partial_success(int64_t failed_items);
    void inc_batch_get_replica_list_requests(int64_t items);
    void inc_batch_get_replica_list_failures(int64_t failed_items);
    void inc_batch_get_replica_list_partial_success(int64_t failed_items);
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
    int64_t get_get_replica_list_requests();
    int64_t get_get_replica_list_failures();
    int64_t get_get_replica_list_by_regex_requests();
    int64_t get_get_replica_list_by_regex_failures();
    int64_t get_exist_key_requests();
    int64_t get_exist_key_failures();
    int64_t get_remove_requests();
    int64_t get_remove_failures();
    int64_t get_remove_by_regex_requests();
    int64_t get_remove_by_regex_failures();
    int64_t get_remove_all_requests();
    int64_t get_remove_all_failures();
    int64_t get_mount_segment_requests();
    int64_t get_mount_segment_failures();
    int64_t get_unmount_segment_requests();
    int64_t get_unmount_segment_failures();
    int64_t get_remount_segment_requests();
    int64_t get_remount_segment_failures();
    int64_t get_ping_requests();
    int64_t get_ping_failures();

    // Batch Operation Statistics Getters
    int64_t get_batch_exist_key_requests();
    int64_t get_batch_exist_key_failures();
    int64_t get_batch_exist_key_partial_successes();
    int64_t get_batch_exist_key_items();
    int64_t get_batch_exist_key_failed_items();
    int64_t get_batch_get_replica_list_requests();
    int64_t get_batch_get_replica_list_failures();
    int64_t get_batch_get_replica_list_partial_successes();
    int64_t get_batch_get_replica_list_items();
    int64_t get_batch_get_replica_list_failed_items();
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

    // --- Setters ---
    void set_enable_ha(bool enable_ha);

   private:
    // --- Private Constructor & Destructor ---
    MasterMetricManager();
    ~MasterMetricManager() = default;

    // --- Metric Members ---

    // Storage Metrics
    ylt::metric::gauge_t allocated_size_;  // Use update for gauge
    ylt::metric::gauge_t total_capacity_;  // Use update for gauge

    // Key/Value Metrics
    ylt::metric::gauge_t key_count_;
    ylt::metric::gauge_t soft_pin_key_count_;
    ylt::metric::histogram_t value_size_distribution_;

    // Cluster Metrics
    ylt::metric::gauge_t active_clients_;

    // Operation Statistics
    ylt::metric::counter_t put_start_requests_;
    ylt::metric::counter_t put_start_failures_;
    ylt::metric::counter_t put_end_requests_;
    ylt::metric::counter_t put_end_failures_;
    ylt::metric::counter_t put_revoke_requests_;
    ylt::metric::counter_t put_revoke_failures_;
    ylt::metric::counter_t get_replica_list_requests_;
    ylt::metric::counter_t get_replica_list_failures_;
    ylt::metric::counter_t get_replica_list_by_regex_requests_;
    ylt::metric::counter_t get_replica_list_by_regex_failures_;
    ylt::metric::counter_t exist_key_requests_;
    ylt::metric::counter_t exist_key_failures_;
    ylt::metric::counter_t remove_requests_;
    ylt::metric::counter_t remove_failures_;
    ylt::metric::counter_t remove_by_regex_requests_;
    ylt::metric::counter_t remove_by_regex_failures_;
    ylt::metric::counter_t remove_all_requests_;
    ylt::metric::counter_t remove_all_failures_;
    ylt::metric::counter_t mount_segment_requests_;
    ylt::metric::counter_t mount_segment_failures_;
    ylt::metric::counter_t unmount_segment_requests_;
    ylt::metric::counter_t unmount_segment_failures_;
    ylt::metric::counter_t remount_segment_requests_;
    ylt::metric::counter_t remount_segment_failures_;
    ylt::metric::counter_t ping_requests_;
    ylt::metric::counter_t ping_failures_;

    // Batch Operation Statistics
    ylt::metric::counter_t batch_exist_key_requests_;
    ylt::metric::counter_t batch_exist_key_failures_;
    ylt::metric::counter_t batch_exist_key_partial_successes_;
    ylt::metric::counter_t batch_exist_key_items_;
    ylt::metric::counter_t batch_exist_key_failed_items_;
    ylt::metric::counter_t batch_get_replica_list_requests_;
    ylt::metric::counter_t batch_get_replica_list_failures_;
    ylt::metric::counter_t batch_get_replica_list_partial_successes_;
    ylt::metric::counter_t batch_get_replica_list_items_;
    ylt::metric::counter_t batch_get_replica_list_failed_items_;
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

    // Some metrics are used only in HA mode. Use a flag to control the output
    // content.
    bool enable_ha_{false};
};

}  // namespace mooncake
