#pragma once

#include <cstdint>
#include <string>

#include "master_metric_manager.h"

namespace mooncake {

// P2P-architecture master metrics
class P2PMasterMetricManager final : public MasterMetricManager {
   public:
    static P2PMasterMetricManager& instance();

    void reset_all_metrics() override;

    // Operation Statistics (Counters)
    void inc_get_write_route_requests(int64_t val = 1);
    void inc_get_write_route_failures(int64_t val = 1);
    void inc_add_replica_requests(int64_t val = 1);
    void inc_add_replica_failures(int64_t val = 1);
    void inc_remove_replica_requests(int64_t val = 1);
    void inc_remove_replica_failures(int64_t val = 1);

    // Batch Operation Statistics (Counters)
    void inc_batch_remove_replica_requests(int64_t items);
    void inc_batch_remove_replica_failures(int64_t failed_items);
    void inc_batch_remove_replica_partial_success(int64_t failed_items);
    void inc_batch_get_write_route_requests(int64_t items);
    void inc_batch_get_write_route_failures(int64_t failed_items);
    void inc_batch_get_write_route_partial_success(int64_t failed_items);

    // Operation Statistics Getters
    int64_t get_get_write_route_requests();
    int64_t get_get_write_route_failures();
    int64_t get_add_replica_requests();
    int64_t get_add_replica_failures();
    int64_t get_remove_replica_requests();
    int64_t get_remove_replica_failures();

    // Batch Operation Statistics Getters
    int64_t get_batch_remove_replica_requests();
    int64_t get_batch_remove_replica_failures();
    int64_t get_batch_remove_replica_partial_successes();
    int64_t get_batch_remove_replica_items();
    int64_t get_batch_remove_replica_failed_items();
    int64_t get_batch_get_write_route_requests();
    int64_t get_batch_get_write_route_failures();
    int64_t get_batch_get_write_route_partial_successes();
    int64_t get_batch_get_write_route_items();
    int64_t get_batch_get_write_route_failed_items();

   private:
    P2PMasterMetricManager();

    std::string serialize_arch_metrics() override;
    std::string get_arch_summary_string(
        const std::string& shared_summary) override;

    // Marks all arch metrics as changed once so zero values are serialized.
    void update_arch_metrics_for_zero_output();

    // Operation Statistics
    ylt::metric::counter_t get_write_route_requests_;
    ylt::metric::counter_t get_write_route_failures_;
    ylt::metric::counter_t add_replica_requests_;
    ylt::metric::counter_t add_replica_failures_;
    ylt::metric::counter_t remove_replica_requests_;
    ylt::metric::counter_t remove_replica_failures_;

    // Batch Operation Statistics
    ylt::metric::counter_t batch_remove_replica_requests_;
    ylt::metric::counter_t batch_remove_replica_failures_;
    ylt::metric::counter_t batch_remove_replica_partial_successes_;
    ylt::metric::counter_t batch_remove_replica_items_;
    ylt::metric::counter_t batch_remove_replica_failed_items_;
    ylt::metric::counter_t batch_get_write_route_requests_;
    ylt::metric::counter_t batch_get_write_route_failures_;
    ylt::metric::counter_t batch_get_write_route_partial_successes_;
    ylt::metric::counter_t batch_get_write_route_items_;
    ylt::metric::counter_t batch_get_write_route_failed_items_;
};

}  // namespace mooncake
