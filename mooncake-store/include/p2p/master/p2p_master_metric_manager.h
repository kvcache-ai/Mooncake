#pragma once

#include <cstdint>
#include <string>

#include "p2p/client/client_metrics_aggregator.h"

#include "ylt/metric/counter.hpp"
#include "ylt/metric/gauge.hpp"
#include "ylt/metric/histogram.hpp"

namespace mooncake {

// P2P-architecture master metrics.
class P2PMasterMetricManager {
   public:
    // Returns the P2P singleton.
    static P2PMasterMetricManager& instance();

    // Resets all P2P metrics to the initial state.
    // (for tests).
    void reset_all_metrics();

    ~P2PMasterMetricManager();

    P2PMasterMetricManager(const P2PMasterMetricManager&) = delete;
    P2PMasterMetricManager& operator=(const P2PMasterMetricManager&) = delete;
    P2PMasterMetricManager(P2PMasterMetricManager&&) = delete;
    P2PMasterMetricManager& operator=(P2PMasterMetricManager&&) = delete;

    // Memory capacity/usage Metrics (global & segment)
    void inc_allocated_mem_size(const std::string& segment, int64_t val = 1);
    void dec_allocated_mem_size(const std::string& segment, int64_t val = 1);
    void reset_allocated_mem_size();
    void inc_total_mem_capacity(const std::string& segment, int64_t val = 1);
    void dec_total_mem_capacity(const std::string& segment, int64_t val = 1);
    void reset_total_mem_capacity();
    double get_global_mem_used_ratio(void);

    // Memory Storage Metrics
    void inc_allocated_mem_size(int64_t val = 1);
    void dec_allocated_mem_size(int64_t val = 1);
    int64_t get_allocated_mem_size();
    int64_t get_total_mem_capacity();
    double get_segment_mem_used_ratio(const std::string& segment);
    int64_t get_segment_allocated_mem_size(const std::string& segment);
    int64_t get_segment_total_mem_capacity(const std::string& segment);

    // File Storage Metrics
    void inc_allocated_file_size(int64_t val = 1);
    void dec_allocated_file_size(int64_t val = 1);
    void inc_total_file_capacity(int64_t val = 1);
    void dec_total_file_capacity(int64_t val = 1);
    int64_t get_allocated_file_size();
    int64_t get_total_file_capacity();
    double get_global_file_used_ratio(void);

    // Key/Value Metrics
    void inc_key_count(int64_t val = 1);
    void dec_key_count(int64_t val = 1);
    void observe_value_size(int64_t size);
    int64_t get_key_count();

    // Cluster Metrics
    void inc_active_clients(int64_t val = 1);
    void dec_active_clients(int64_t val = 1);
    int64_t get_active_clients();

    // Client RPC Metrics (requests / failures, like other RPCs)
    void inc_register_client_requests(int64_t val = 1);
    void inc_register_client_failures(int64_t val = 1);
    void inc_unregister_client_requests(int64_t val = 1);
    void inc_unregister_client_failures(int64_t val = 1);
    int64_t get_register_client_requests();
    int64_t get_register_client_failures();
    int64_t get_unregister_client_requests();
    int64_t get_unregister_client_failures();

    // Client Lifecycle Metrics (Counters)
    void inc_clients_disconnected_total(int64_t val = 1);
    void inc_clients_recovered_total(int64_t val = 1);
    void inc_clients_crashed_total(int64_t val = 1);
    int64_t get_clients_disconnected_total();
    int64_t get_clients_recovered_total();
    int64_t get_clients_crashed_total();

    // Operation Statistics (Counters)
    void inc_get_read_route_by_regex_requests(int64_t val = 1);
    void inc_get_read_route_by_regex_failures(int64_t val = 1);
    void inc_get_read_route_requests(int64_t val = 1);
    void inc_get_read_route_failures(int64_t val = 1);
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
    void inc_heartbeat_requests(int64_t val = 1);
    void inc_heartbeat_failures(int64_t val = 1);

    // Batch Operation Statistics (Counters)
    void inc_batch_exist_key_requests(int64_t items);
    void inc_batch_exist_key_failures(int64_t failed_items);
    void inc_batch_exist_key_partial_success(int64_t failed_items);
    void inc_batch_query_ip_requests(int64_t items);
    void inc_batch_query_ip_failures(int64_t failed_items);
    void inc_batch_query_ip_partial_success(int64_t failed_items);
    void inc_batch_get_read_route_requests(int64_t items);
    void inc_batch_get_read_route_failures(int64_t failed_items);
    void inc_batch_get_read_route_partial_success(int64_t failed_items);

    // Operation Statistics Getters
    int64_t get_get_read_route_requests();
    int64_t get_get_read_route_failures();
    int64_t get_get_read_route_by_regex_requests();
    int64_t get_get_read_route_by_regex_failures();
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
    int64_t get_heartbeat_requests();
    int64_t get_heartbeat_failures();

    // Batch Operation Statistics Getters
    int64_t get_batch_exist_key_requests();
    int64_t get_batch_exist_key_failures();
    int64_t get_batch_exist_key_partial_successes();
    int64_t get_batch_exist_key_items();
    int64_t get_batch_exist_key_failed_items();
    int64_t get_batch_query_ip_requests();
    int64_t get_batch_query_ip_failures();
    int64_t get_batch_query_ip_partial_successes();
    int64_t get_batch_query_ip_items();
    int64_t get_batch_query_ip_failed_items();
    int64_t get_batch_get_read_route_requests();
    int64_t get_batch_get_read_route_failures();
    int64_t get_batch_get_read_route_partial_successes();
    int64_t get_batch_get_read_route_items();
    int64_t get_batch_get_read_route_failed_items();

    // Operation Statistics (Counters)
    void inc_get_write_route_requests(int64_t val = 1);
    void inc_get_write_route_failures(int64_t val = 1);
    void inc_publish_route_requests(int64_t val = 1);
    void inc_publish_route_failures(int64_t val = 1);
    void inc_withdraw_route_requests(int64_t val = 1);
    void inc_withdraw_route_failures(int64_t val = 1);

    // Batch Operation Statistics (Counters)
    void inc_batch_withdraw_route_requests(int64_t items);
    void inc_batch_withdraw_route_failures(int64_t failed_items);
    void inc_batch_withdraw_route_partial_success(int64_t failed_items);
    void inc_batch_get_write_route_requests(int64_t items);
    void inc_batch_get_write_route_failures(int64_t failed_items);
    void inc_batch_get_write_route_partial_success(int64_t failed_items);

    // Operation Statistics Getters
    int64_t get_get_write_route_requests();
    int64_t get_get_write_route_failures();
    int64_t get_publish_route_requests();
    int64_t get_publish_route_failures();
    int64_t get_withdraw_route_requests();
    int64_t get_withdraw_route_failures();

    // Batch Operation Statistics Getters
    int64_t get_batch_withdraw_route_requests();
    int64_t get_batch_withdraw_route_failures();
    int64_t get_batch_withdraw_route_partial_successes();
    int64_t get_batch_withdraw_route_items();
    int64_t get_batch_withdraw_route_failed_items();
    int64_t get_batch_get_write_route_requests();
    int64_t get_batch_get_write_route_failures();
    int64_t get_batch_get_write_route_partial_successes();
    int64_t get_batch_get_write_route_items();
    int64_t get_batch_get_write_route_failed_items();

    void UpdateClientMetrics(const UUID& client_id,
                             const ClientMetricSnapshot& snapshot);
    void OnClientRemoved(const UUID& client_id);

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
    P2PMasterMetricManager();

    // Update all metrics once to ensure zero values are serialized.
    void update_metrics_for_zero_output();
    void update_arch_metrics_for_zero_output();

    // --- Metric Members ---

    // Memory Storage Metrics
    ylt::metric::gauge_t
        mem_allocated_size_;  // Overall memory usage update for gauge
    ylt::metric::gauge_t
        mem_total_capacity_;  // Overall memory capacity update for gauge
    ylt::metric::dynamic_gauge_1t
        mem_allocated_size_per_segment_;  // Segment memory usage update for
                                          // gauge
    ylt::metric::dynamic_gauge_1t
        mem_total_capacity_per_segment_;  // Segment memory capacity update for
                                          // gauge

    // File Storage Metrics
    ylt::metric::gauge_t file_allocated_size_;
    ylt::metric::gauge_t file_total_capacity_;

    // Key/Value Metrics
    ylt::metric::gauge_t key_count_;
    // Histogram (4KB, 64KB, 256KB, 1MB, 4MB, 16MB, 64MB)
    ylt::metric::histogram_t value_size_distribution_;

    // Cluster Metrics
    ylt::metric::gauge_t active_clients_;

    // Client RPC Metrics (register/unregister requests + failures)
    ylt::metric::counter_t register_client_requests_;
    ylt::metric::counter_t register_client_failures_;
    ylt::metric::counter_t unregister_client_requests_;
    ylt::metric::counter_t unregister_client_failures_;

    // Client Lifecycle Metrics
    ylt::metric::counter_t clients_disconnected_total_;
    ylt::metric::counter_t clients_recovered_total_;
    ylt::metric::counter_t clients_crashed_total_;

    // Operation Statistics
    ylt::metric::counter_t get_read_route_requests_;
    ylt::metric::counter_t get_read_route_failures_;
    ylt::metric::counter_t get_read_route_by_regex_requests_;
    ylt::metric::counter_t get_read_route_by_regex_failures_;
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
    ylt::metric::counter_t heartbeat_requests_;
    ylt::metric::counter_t heartbeat_failures_;

    // Batch Operation Statistics
    ylt::metric::counter_t batch_exist_key_requests_;
    ylt::metric::counter_t batch_exist_key_failures_;
    ylt::metric::counter_t batch_exist_key_partial_successes_;
    ylt::metric::counter_t batch_exist_key_items_;
    ylt::metric::counter_t batch_exist_key_failed_items_;
    ylt::metric::counter_t batch_query_ip_requests_;
    ylt::metric::counter_t batch_query_ip_failures_;
    ylt::metric::counter_t batch_query_ip_partial_successes_;
    ylt::metric::counter_t batch_query_ip_items_;
    ylt::metric::counter_t batch_query_ip_failed_items_;
    ylt::metric::counter_t batch_get_read_route_requests_;
    ylt::metric::counter_t batch_get_read_route_failures_;
    ylt::metric::counter_t batch_get_read_route_partial_successes_;
    ylt::metric::counter_t batch_get_read_route_items_;
    ylt::metric::counter_t batch_get_read_route_failed_items_;

    // Operation Statistics
    ylt::metric::counter_t get_write_route_requests_;
    ylt::metric::counter_t get_write_route_failures_;
    ylt::metric::counter_t publish_route_requests_;
    ylt::metric::counter_t publish_route_failures_;
    ylt::metric::counter_t withdraw_route_requests_;
    ylt::metric::counter_t withdraw_route_failures_;

    // Batch Operation Statistics
    ylt::metric::counter_t batch_withdraw_route_requests_;
    ylt::metric::counter_t batch_withdraw_route_failures_;
    ylt::metric::counter_t batch_withdraw_route_partial_successes_;
    ylt::metric::counter_t batch_withdraw_route_items_;
    ylt::metric::counter_t batch_withdraw_route_failed_items_;
    ylt::metric::counter_t batch_get_write_route_requests_;
    ylt::metric::counter_t batch_get_write_route_failures_;
    ylt::metric::counter_t batch_get_write_route_partial_successes_;
    ylt::metric::counter_t batch_get_write_route_items_;
    ylt::metric::counter_t batch_get_write_route_failed_items_;

    // Cluster-wide data-plane metrics aggregated from client heartbeats.
    ClientMetricsAggregator client_metrics_aggregator_;
};

}  // namespace mooncake
