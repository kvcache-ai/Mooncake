#pragma once

#include "master_metric_manager.h"
#include "p2p/client_metrics_aggregator.h"

namespace mooncake {

// Lightweight singleton that delegates shared metrics to MasterMetricManager
// and owns P2P-only client metric aggregation.
// Usage: P2PMasterMetricManager::instance().UpdateClientMetrics(...)
//        P2PMasterMetricManager::instance().inc_total_mem_capacity(...)
//        (the latter calls MasterMetricManager::instance().inc_total_mem_capacity)
class P2PMasterMetricManager {
   public:
    static P2PMasterMetricManager& instance() {
        static P2PMasterMetricManager inst;
        return inst;
    }

    void UpdateClientMetrics(const UUID& client_id,
                              const ClientMetricSnapshot& snapshot) {
        client_metrics_aggregator_.Update(client_id, snapshot);
    }

    void OnClientRemoved(const UUID& client_id) {
        client_metrics_aggregator_.OnClientRemoved(client_id);
    }

    // Test helper: returns the underlying aggregator for direct test access.
    ClientMetricsAggregator& GetClientMetricsAggregatorForTest() {
        return client_metrics_aggregator_;
    }

    void ResetClientMetricsForTest() {
        client_metrics_aggregator_.ResetAllForTest();
    }

    // Delegate shared metric calls to MasterMetricManager
    void inc_total_mem_capacity(const std::string& segment, int64_t val = 1) {
        MasterMetricManager::instance().inc_total_mem_capacity(segment, val);
    }
    void dec_total_mem_capacity(const std::string& segment, int64_t val = 1) {
        MasterMetricManager::instance().dec_total_mem_capacity(segment, val);
    }
    void inc_allocated_mem_size(const std::string& segment, int64_t val = 1) {
        MasterMetricManager::instance().inc_allocated_mem_size(segment, val);
    }
    void dec_allocated_mem_size(const std::string& segment, int64_t val = 1) {
        MasterMetricManager::instance().dec_allocated_mem_size(segment, val);
    }
    void inc_total_file_capacity(int64_t val = 1) {
        MasterMetricManager::instance().inc_total_file_capacity(val);
    }
    void dec_total_file_capacity(int64_t val = 1) {
        MasterMetricManager::instance().dec_total_file_capacity(val);
    }
    void inc_allocated_file_size(int64_t val = 1) {
        MasterMetricManager::instance().inc_allocated_file_size(val);
    }
    void dec_allocated_file_size(int64_t val = 1) {
        MasterMetricManager::instance().dec_allocated_file_size(val);
    }

    // Serialize cluster-wide client metrics
    void SerializeClientMetrics(std::string& out) {
        client_metrics_aggregator_.Serialize(out);
    }

    std::string SummaryClientMetrics() {
        return client_metrics_aggregator_.Summary();
    }

   private:
    P2PMasterMetricManager() = default;
    ClientMetricsAggregator client_metrics_aggregator_;
};

}  // namespace mooncake
