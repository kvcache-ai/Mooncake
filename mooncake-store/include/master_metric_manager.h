#pragma once

#include <chrono>
#include <mutex>
#include <string>
#include <unordered_map>

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

    struct StorageRatios {
        double total;
        double ram;
        double cxl;
    };

    // Memory Storage Metrics(global & segment)
    void inc_allocated_mem_size(const std::string& segment, int64_t val = 1);
    void dec_allocated_mem_size(const std::string& segment, int64_t val = 1);
    void reset_allocated_mem_size();
    void inc_total_mem_capacity(const std::string& segment, int64_t val = 1);
    void dec_total_mem_capacity(const std::string& segment, int64_t val = 1);
    void reset_total_mem_capacity();
    StorageRatios get_global_used_ratio(void);

    // DRAM Storage Metrics(global & segment)
    void inc_allocated_dram_size(const std::string& segment, int64_t val = 1);
    void dec_allocated_dram_size(const std::string& segment, int64_t val = 1);
    void reset_allocated_dram_size();
    void inc_total_dram_capacity(const std::string& segment, int64_t val = 1);
    void dec_total_dram_capacity(const std::string& segment, int64_t val = 1);
    void reset_total_dram_capacity();

    // CXL Storage Metrics(global & segment)
    void inc_allocated_cxl_size(const std::string& segment, int64_t val = 1);
    void dec_allocated_cxl_size(const std::string& segment, int64_t val = 1);
    void reset_allocated_cxl_size();
    void inc_total_cxl_capacity(const std::string& segment, int64_t val = 1);
    void dec_total_cxl_capacity(const std::string& segment, int64_t val = 1);
    void reset_total_cxl_capacity();

    void inc_mem_cache_hit_nums(int64_t val = 1);
    void inc_file_cache_hit_nums(int64_t val = 1);
    void inc_mem_cache_nums(int64_t val = 1);
    void inc_file_cache_nums(int64_t val = 1);
    void dec_mem_cache_nums(int64_t val = 1);
    void dec_file_cache_nums(int64_t val = 1);

    void inc_valid_get_nums(int64_t val = 1);
    void inc_total_get_nums(int64_t val = 1);

    // NoF segment Metrics
    void inc_allocated_nof_size(const std::string& segment, int64_t val = 1);
    void dec_allocated_nof_size(const std::string& segment, int64_t val = 1);
    void reset_allocated_nof_size();
    void inc_total_nof_capacity(const std::string& segment, int64_t val = 1);
    void dec_total_nof_capacity(const std::string& segment, int64_t val = 1);
    void reset_total_nof_capacity();
    double get_global_nof_used_ratio(void);

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
    void inc_total_mem_capacity(int64_t val = 1);
    void dec_total_mem_capacity(int64_t val = 1);
    int64_t get_allocated_mem_size();
    int64_t get_total_mem_capacity();
    StorageRatios get_segment_used_ratio(const std::string& segment);
    void reset_segment_allocated_mem_size(const std::string& segment);
    void reset_segment_total_mem_capacity(const std::string& segment);
    int64_t get_segment_allocated_mem_size(const std::string& segment);
    int64_t get_segment_total_mem_capacity(const std::string& segment);

    // DRAM Storage Metrics
    void inc_allocated_dram_size(int64_t val = 1);
    void dec_allocated_dram_size(int64_t val = 1);
    void inc_total_dram_capacity(int64_t val = 1);
    void dec_total_dram_capacity(int64_t val = 1);
    int64_t get_allocated_dram_size();
    int64_t get_total_dram_capacity();
    int64_t get_segment_allocated_dram_size(const std::string& segment);
    int64_t get_segment_total_dram_capacity(const std::string& segment);

    // CXL Storage Metrics
    void inc_allocated_cxl_size(int64_t val = 1);
    void dec_allocated_cxl_size(int64_t val = 1);
    void inc_total_cxl_capacity(int64_t val = 1);
    void dec_total_cxl_capacity(int64_t val = 1);
    int64_t get_allocated_cxl_size();
    int64_t get_total_cxl_capacity();
    int64_t get_segment_allocated_cxl_size(const std::string& segment);
    int64_t get_segment_total_cxl_capacity(const std::string& segment);

    // NoF segment Metrics
    void inc_allocated_nof_size(int64_t val = 1);
    void dec_allocated_nof_size(int64_t val = 1);
    void inc_total_nof_capacity(int64_t val = 1);
    void dec_total_nof_capacity(int64_t val = 1);
    int64_t get_allocated_nof_size();
    int64_t get_total_nof_capacity();
    double get_segment_nof_used_ratio(const std::string& segment);
    int64_t get_segment_allocated_nof_size(const std::string& segment);
    int64_t get_segment_total_nof_capacity(const std::string& segment);

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

    // Snapshot Metrics
    void set_snapshot_duration_ms(int64_t size);
    void inc_snapshot_success();
    void inc_snapshot_fail();

    // Operation Statistics (Counters)
    void inc_put_start_requests(int64_t val = 1);
    void inc_put_start_failures(int64_t val = 1);
    void inc_put_start_alloc_failures(int64_t val = 1);
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
    void inc_mount_nof_segment_requests(int64_t val = 1);
    void inc_mount_nof_segment_failures(int64_t val = 1);
    void inc_unmount_segment_requests(int64_t val = 1);
    void inc_unmount_segment_failures(int64_t val = 1);
    void inc_unmount_nof_segment_requests(int64_t val = 1);
    void inc_unmount_nof_segment_failures(int64_t val = 1);
    void inc_remount_segment_requests(int64_t val = 1);
    void inc_remount_segment_failures(int64_t val = 1);
    void inc_remount_nof_segment_requests(int64_t val = 1);
    void inc_remount_nof_segment_failures(int64_t val = 1);
    void inc_ping_requests(int64_t val = 1);
    void inc_ping_failures(int64_t val = 1);
    void inc_nof_heartbeat_success_total(int64_t val = 1);
    void inc_nof_heartbeat_failure_total(int64_t val = 1);
    void inc_nof_heartbeat_timeout_total(int64_t val = 1);
    void inc_nof_segments_unmounted_by_heartbeat_total(int64_t val = 1);
    void observe_nof_heartbeat_probe_latency_ms(int64_t latency_ms);

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
    int64_t get_put_start_alloc_failures();
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
    // total eviction metrics
    void inc_eviction_success(int64_t key_count, int64_t size);
    void inc_eviction_fail();  // not a single object is evicted
    // mem eviction metrics
    void inc_mem_eviction_success(int64_t key_count, int64_t size);
    void inc_mem_eviction_fail();  // not a single object is evicted
    // nof eviction metrics
    void inc_nof_eviction_success(int64_t key_count, int64_t size);
    void inc_nof_eviction_fail();  // not a single object is evicted

    // Eviction Metrics Getters
    // total eviction metrics
    int64_t get_eviction_success();
    int64_t get_eviction_attempts();
    int64_t get_evicted_key_count();
    int64_t get_evicted_size();
    // mem eviction metrics
    int64_t get_mem_eviction_success();
    int64_t get_mem_eviction_attempts();
    int64_t get_mem_evicted_key_count();
    int64_t get_mem_evicted_size();
    // nof eviction metrics
    int64_t get_nof_eviction_success();
    int64_t get_nof_eviction_attempts();
    int64_t get_nof_evicted_key_count();
    int64_t get_nof_evicted_size();

    // PutStart Discard Metrics
    void inc_put_start_discard_cnt(int64_t count, int64_t size);
    void inc_put_start_release_cnt(int64_t count, int64_t size);

    // PutStart Discard Metrics Getters
    int64_t get_put_start_discard_cnt();
    int64_t get_put_start_release_cnt();
    int64_t get_put_start_discarded_staging_size();

    // CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke Metrics
    void inc_copy_start_requests(int64_t val = 1);
    void inc_copy_start_failures(int64_t val = 1);
    void inc_copy_end_requests(int64_t val = 1);
    void inc_copy_end_failures(int64_t val = 1);
    void inc_copy_revoke_requests(int64_t val = 1);
    void inc_copy_revoke_failures(int64_t val = 1);
    void inc_move_start_requests(int64_t val = 1);
    void inc_move_start_failures(int64_t val = 1);
    void inc_move_end_requests(int64_t val = 1);
    void inc_move_end_failures(int64_t val = 1);
    void inc_move_revoke_requests(int64_t val = 1);
    void inc_move_revoke_failures(int64_t val = 1);
    void inc_evict_disk_replica_requests(int64_t val = 1);
    void inc_evict_disk_replica_failures(int64_t val = 1);

    // CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke Metrics
    // Getters
    int64_t get_copy_start_requests();
    int64_t get_copy_start_failures();
    int64_t get_copy_end_requests();
    int64_t get_copy_end_failures();
    int64_t get_copy_revoke_requests();
    int64_t get_copy_revoke_failures();
    int64_t get_move_start_requests();
    int64_t get_move_start_failures();
    int64_t get_move_end_requests();
    int64_t get_move_end_failures();
    int64_t get_move_revoke_requests();
    int64_t get_move_revoke_failures();
    int64_t get_evict_disk_replica_requests();
    int64_t get_evict_disk_replica_failures();

    // Copy, Move, QueryTask, FetchTasks, MarkTaskToComplete Metrics
    void inc_create_copy_task_requests(int64_t val = 1);
    void inc_create_copy_task_failures(int64_t val = 1);
    void inc_create_move_task_requests(int64_t val = 1);
    void inc_create_move_task_failures(int64_t val = 1);
    void inc_query_task_requests(int64_t val = 1);
    void inc_query_task_failures(int64_t val = 1);
    void inc_fetch_tasks_requests(int64_t val = 1);
    void inc_fetch_tasks_failures(int64_t val = 1);
    void inc_update_task_requests(int64_t val = 1);
    void inc_update_task_failures(int64_t val = 1);

    // Copy, Move, QueryTask, FetchTasks, MarkTaskToComplete Metrics Getters
    int64_t get_create_copy_task_requests();
    int64_t get_create_copy_task_failures();
    int64_t get_create_move_task_requests();
    int64_t get_create_move_task_failures();
    int64_t get_query_task_requests();
    int64_t get_query_task_failures();
    int64_t get_fetch_tasks_requests();
    int64_t get_fetch_tasks_failures();
    int64_t get_update_task_requests();
    int64_t get_update_task_failures();

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
    std::string get_summary_string_and_update_snapshot();

   private:
    // --- Private Constructor & Destructor ---
    MasterMetricManager();
    ~MasterMetricManager() = default;

    // Update all metrics once to ensure zero values are serialized
    void update_metrics_for_zero_output();
    std::string get_summary_string(bool update_summary_snapshot);

    struct SummaryCounters {
        int64_t exist_keys = 0;
        int64_t exist_key_fails = 0;
        int64_t put_starts = 0;
        int64_t put_start_fails = 0;
        int64_t put_start_alloc_fails = 0;
        int64_t put_ends = 0;
        int64_t put_end_fails = 0;
        int64_t put_revoke_requests = 0;
        int64_t put_revoke_fails = 0;
        int64_t get_replicas = 0;
        int64_t get_replica_fails = 0;
        int64_t removes = 0;
        int64_t remove_fails = 0;
        int64_t remove_all = 0;
        int64_t remove_all_fails = 0;
        int64_t create_move_tasks = 0;
        int64_t create_move_task_fails = 0;
        int64_t create_copy_tasks = 0;
        int64_t create_copy_task_fails = 0;
        int64_t query_tasks = 0;
        int64_t query_task_fails = 0;
        int64_t fetch_tasks = 0;
        int64_t fetch_task_fails = 0;
        int64_t copy_starts = 0;
        int64_t copy_start_fails = 0;
        int64_t copy_ends = 0;
        int64_t copy_end_fails = 0;
        int64_t copy_revokes = 0;
        int64_t copy_revoke_fails = 0;
        int64_t move_starts = 0;
        int64_t move_start_fails = 0;
        int64_t move_ends = 0;
        int64_t move_end_fails = 0;
        int64_t move_revokes = 0;
        int64_t move_revoke_fails = 0;
        int64_t evict_disk_replicas = 0;
        int64_t evict_disk_replica_fails = 0;
        int64_t batch_put_start_requests = 0;
        int64_t batch_put_start_fails = 0;
        int64_t batch_put_start_partial_successes = 0;
        int64_t batch_put_start_items = 0;
        int64_t batch_put_start_failed_items = 0;
        int64_t batch_put_end_requests = 0;
        int64_t batch_put_end_fails = 0;
        int64_t batch_put_end_partial_successes = 0;
        int64_t batch_put_end_items = 0;
        int64_t batch_put_end_failed_items = 0;
        int64_t batch_put_revoke_requests = 0;
        int64_t batch_put_revoke_fails = 0;
        int64_t batch_put_revoke_partial_successes = 0;
        int64_t batch_put_revoke_items = 0;
        int64_t batch_put_revoke_failed_items = 0;
        int64_t batch_get_replica_list_requests = 0;
        int64_t batch_get_replica_list_fails = 0;
        int64_t batch_get_replica_list_partial_successes = 0;
        int64_t batch_get_replica_list_items = 0;
        int64_t batch_get_replica_list_failed_items = 0;
        int64_t batch_exist_key_requests = 0;
        int64_t batch_exist_key_fails = 0;
        int64_t batch_exist_key_partial_successes = 0;
        int64_t batch_exist_key_items = 0;
        int64_t batch_exist_key_failed_items = 0;
        int64_t batch_query_ip_requests = 0;
        int64_t batch_query_ip_fails = 0;
        int64_t batch_query_ip_partial_successes = 0;
        int64_t batch_query_ip_items = 0;
        int64_t batch_query_ip_failed_items = 0;
        int64_t batch_replica_clear_requests = 0;
        int64_t batch_replica_clear_fails = 0;
        int64_t batch_replica_clear_partial_successes = 0;
        int64_t batch_replica_clear_items = 0;
        int64_t batch_replica_clear_failed_items = 0;
        int64_t eviction_success = 0;
        int64_t eviction_attempts = 0;
        int64_t evicted_key_count = 0;
        int64_t evicted_size = 0;
        int64_t mem_eviction_success = 0;
        int64_t mem_eviction_attempts = 0;
        int64_t mem_evicted_key_count = 0;
        int64_t mem_evicted_size = 0;
        int64_t nof_eviction_success = 0;
        int64_t nof_eviction_attempts = 0;
        int64_t nof_evicted_key_count = 0;
        int64_t nof_evicted_size = 0;
        int64_t ping = 0;
        int64_t ping_fails = 0;
        int64_t mark_task_to_complete_requests = 0;
        int64_t mark_task_to_complete_fails = 0;
    };

    struct SummarySnapshot {
        bool initialized = false;
        std::chrono::steady_clock::time_point timestamp;
        SummaryCounters counters;
    };

    // --- Metric Members ---
    std::mutex summary_snapshot_mutex_;
    SummarySnapshot summary_snapshot_;

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

    // NoF Segment Metrics
    ylt::metric::gauge_t
        nof_allocated_size_;  // Overall NoF SSD usage update for gauge
    ylt::metric::gauge_t
        nof_total_capacity_;  // Overall NoF SSD capacity update for gauge
    ylt::metric::dynamic_gauge_1t
        nof_allocated_size_per_segment_;  // NoF segment usage update for
                                          // gauge
    ylt::metric::dynamic_gauge_1t
        nof_total_capacity_per_segment_;  // NoF segment capacity update for
                                          // gauge

    // File Storage Metrics
    ylt::metric::gauge_t file_allocated_size_;
    ylt::metric::gauge_t file_total_capacity_;

    // DRAM Storage Metrics
    ylt::metric::gauge_t dram_allocated_size_;
    ylt::metric::gauge_t dram_total_capacity_;
    ylt::metric::dynamic_gauge_1t dram_allocated_size_per_segment_;
    ylt::metric::dynamic_gauge_1t dram_total_capacity_per_segment_;

    // CXL Storage Metrics
    ylt::metric::gauge_t cxl_allocated_size_;
    ylt::metric::gauge_t cxl_total_capacity_;
    ylt::metric::dynamic_gauge_1t cxl_allocated_size_per_segment_;
    ylt::metric::dynamic_gauge_1t cxl_total_capacity_per_segment_;

    // Key/Value Metrics
    ylt::metric::gauge_t key_count_;
    ylt::metric::gauge_t soft_pin_key_count_;
    ylt::metric::histogram_t value_size_distribution_;

    // Cluster Metrics
    ylt::metric::gauge_t active_clients_;

    // Operation Statistics
    ylt::metric::counter_t put_start_requests_;
    ylt::metric::counter_t put_start_failures_;
    ylt::metric::counter_t put_start_alloc_failures_;
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
    ylt::metric::counter_t mount_nof_segment_requests_;
    ylt::metric::counter_t mount_nof_segment_failures_;
    ylt::metric::counter_t unmount_nof_segment_requests_;
    ylt::metric::counter_t unmount_nof_segment_failures_;
    ylt::metric::counter_t remount_nof_segment_requests_;
    ylt::metric::counter_t remount_nof_segment_failures_;
    ylt::metric::counter_t ping_requests_;
    ylt::metric::counter_t ping_failures_;
    ylt::metric::counter_t nof_heartbeat_success_total_;
    ylt::metric::counter_t nof_heartbeat_failure_total_;
    ylt::metric::counter_t nof_heartbeat_timeout_total_;
    ylt::metric::counter_t nof_segments_unmounted_by_heartbeat_total_;
    ylt::metric::histogram_t nof_heartbeat_probe_latency_ms_;

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
    // total eviction metrics
    ylt::metric::counter_t eviction_success_;
    ylt::metric::counter_t eviction_attempts_;
    ylt::metric::counter_t evicted_key_count_;
    ylt::metric::counter_t evicted_size_;
    // mem eviction metrics
    ylt::metric::counter_t mem_eviction_success_;
    ylt::metric::counter_t mem_eviction_attempts_;
    ylt::metric::counter_t mem_evicted_key_count_;
    ylt::metric::counter_t mem_evicted_size_;
    // nof eviction metrics
    ylt::metric::counter_t nof_eviction_success_;
    ylt::metric::counter_t nof_eviction_attempts_;
    ylt::metric::counter_t nof_evicted_key_count_;
    ylt::metric::counter_t nof_evicted_size_;

    // PutStart Discard Metrics
    ylt::metric::counter_t put_start_discard_cnt_;
    ylt::metric::counter_t put_start_release_cnt_;
    ylt::metric::gauge_t put_start_discarded_staging_size_;

    // Snapshot Metrics
    ylt::metric::histogram_t snapshot_duration_ms_;
    ylt::metric::counter_t snapshot_success_;
    ylt::metric::counter_t snapshot_fail_;
    // CopyStart, CopyEnd, CopyRevoke, MoveStart, MoveEnd, MoveRevoke Metrics
    ylt::metric::counter_t copy_start_requests_;
    ylt::metric::counter_t copy_start_failures_;
    ylt::metric::counter_t copy_end_requests_;
    ylt::metric::counter_t copy_end_failures_;
    ylt::metric::counter_t copy_revoke_requests_;
    ylt::metric::counter_t copy_revoke_failures_;
    ylt::metric::counter_t move_start_requests_;
    ylt::metric::counter_t move_start_failures_;
    ylt::metric::counter_t move_end_requests_;
    ylt::metric::counter_t move_end_failures_;
    ylt::metric::counter_t move_revoke_requests_;
    ylt::metric::counter_t move_revoke_failures_;
    ylt::metric::counter_t evict_disk_replica_requests_;
    ylt::metric::counter_t evict_disk_replica_failures_;

    // Copy and Move, FetchTasks, MarkTaskToComplete Metrics
    ylt::metric::counter_t create_copy_task_requests_;
    ylt::metric::counter_t create_copy_task_failures_;
    ylt::metric::counter_t create_move_task_requests_;
    ylt::metric::counter_t create_move_task_failures_;
    ylt::metric::counter_t query_task_requests_;
    ylt::metric::counter_t query_task_failures_;
    ylt::metric::counter_t fetch_tasks_requests_;
    ylt::metric::counter_t fetch_tasks_failures_;
    ylt::metric::counter_t mark_task_to_complete_requests_;
    ylt::metric::counter_t mark_task_to_complete_failures_;
};

}  // namespace mooncake
