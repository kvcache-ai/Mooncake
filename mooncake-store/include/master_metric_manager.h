#pragma once

#include <string>

#include "ylt/metric/counter.hpp"
#include "ylt/metric/gauge.hpp"
#include "ylt/metric/histogram.hpp"

namespace mooncake {

class MasterMetricManager {
   public:
    // --- Singleton Access ---
    static MasterMetricManager& instance();
    void reset_all_metrics();

    MasterMetricManager(const MasterMetricManager&) = delete;
    MasterMetricManager& operator=(const MasterMetricManager&) = delete;
    MasterMetricManager(MasterMetricManager&&) = delete;
    MasterMetricManager& operator=(MasterMetricManager&&) = delete;

    // Memory Storage Metrics(global & segment)
    void inc_allocated_mem_size(const std::string& segment, int64_t val = 1);
    void dec_allocated_mem_size(const std::string& segment, int64_t val = 1);
    void reset_allocated_mem_size();
    void inc_total_mem_capacity(const std::string& segment, int64_t val = 1);
    void dec_total_mem_capacity(const std::string& segment, int64_t val = 1);
    void reset_total_mem_capacity();
    double get_global_mem_used_ratio(void);

    void inc_mem_cache_hit_nums(int64_t val = 1);
    void inc_file_cache_hit_nums(int64_t val = 1);
    void inc_mem_cache_nums(int64_t val = 1);
    void inc_file_cache_nums(int64_t val = 1);
    void dec_mem_cache_nums(int64_t val = 1);
    void dec_file_cache_nums(int64_t val = 1);

    void inc_valid_get_nums(int64_t val = 1);
    void inc_total_get_nums(int64_t val = 1);

    enum class CacheHitStat {
        MEMORY_HITS,
        SSD_HITS,
        MEMORY_TOTAL,
        SSD_TOTAL,
        MEMORY_HIT_RATE,
        SSD_HIT_RATE,
        OVERALL_HIT_RATE,
        VALID_GET_RATE
    };
    using CacheHitStatDict = std::unordered_map<CacheHitStat, double>;
    void add_stat_to_dict(CacheHitStatDict&, CacheHitStat, double);
    CacheHitStatDict calculate_cache_stats();

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
    void inc_heartbeat_requests(int64_t val = 1);
    void inc_heartbeat_failures(int64_t val = 1);

    // Batch Operation Statistics (Counters)
    void inc_batch_exist_key_requests(int64_t items);
    void inc_batch_exist_key_failures(int64_t failed_items);
    void inc_batch_exist_key_partial_success(int64_t failed_items);
    void inc_batch_query_ip_requests(int64_t items);
    void inc_batch_query_ip_failures(int64_t failed_items);
    void inc_batch_query_ip_partial_success(int64_t failed_items);
    void inc_batch_replica_clear_requests(int64_t items);
    void inc_batch_replica_clear_failures(int64_t failed_items);
    void inc_batch_replica_clear_partial_success(int64_t failed_items);
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
    int64_t get_batch_replica_clear_requests();
    int64_t get_batch_replica_clear_failures();
    int64_t get_batch_replica_clear_partial_successes();
    int64_t get_batch_replica_clear_items();
    int64_t get_batch_replica_clear_failed_items();
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

    // PutStart Discard Metrics
    void inc_put_start_discard_cnt(int64_t count, int64_t size);
    void inc_put_start_release_cnt(int64_t count, int64_t size);

    // PutStart Discard Metrics Getters
    int64_t get_put_start_discard_cnt();
    int64_t get_put_start_release_cnt();
    int64_t get_put_start_discarded_staging_size();

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

    // Update all metrics once to ensure zero values are serialized
    void update_metrics_for_zero_output();

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
    ylt::metric::counter_t batch_replica_clear_requests_;
    ylt::metric::counter_t batch_replica_clear_failures_;
    ylt::metric::counter_t batch_replica_clear_partial_successes_;
    ylt::metric::counter_t batch_replica_clear_items_;
    ylt::metric::counter_t batch_replica_clear_failed_items_;
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

    // cache hit Statistics
    ylt::metric::counter_t mem_cache_hit_nums_;
    ylt::metric::counter_t file_cache_hit_nums_;
    ylt::metric::gauge_t mem_cache_nums_;
    ylt::metric::gauge_t file_cache_nums_;

    ylt::metric::counter_t valid_get_nums_;
    ylt::metric::counter_t total_get_nums_;

    static const inline std::unordered_map<CacheHitStat, std::string>
        stat_names_ = {{CacheHitStat::MEMORY_HITS, "memory_hits"},
                       {CacheHitStat::SSD_HITS, "ssd_hits"},
                       {CacheHitStat::MEMORY_TOTAL, "memory_total"},
                       {CacheHitStat::SSD_TOTAL, "ssd_total"},
                       {CacheHitStat::MEMORY_HIT_RATE, "memory_hit_rate"},
                       {CacheHitStat::SSD_HIT_RATE, "ssd_hit_rate"},
                       {CacheHitStat::OVERALL_HIT_RATE, "overall_hit_rate"},
                       {CacheHitStat::VALID_GET_RATE, "valid_get_rate"}};

    // Eviction Metrics
    ylt::metric::counter_t eviction_success_;
    ylt::metric::counter_t eviction_attempts_;
    ylt::metric::counter_t evicted_key_count_;
    ylt::metric::counter_t evicted_size_;

    // PutStart Discard Metrics
    ylt::metric::counter_t put_start_discard_cnt_;
    ylt::metric::counter_t put_start_release_cnt_;
    ylt::metric::gauge_t put_start_discarded_staging_size_;
};

}  // namespace mooncake
