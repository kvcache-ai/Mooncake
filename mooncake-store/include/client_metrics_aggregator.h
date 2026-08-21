#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/metric/gauge.hpp>

#include "heartbeat_type.h"
#include "types.h"

namespace mooncake {

// Aggregates per-client cumulative metric snapshots into cluster-wide
// master_cluster_* gauges
class ClientMetricsAggregator {
   public:
    ClientMetricsAggregator();

    void Update(const UUID& client_id, const ClientMetricSnapshot& snapshot);
    void OnClientRemoved(const UUID& client_id);

    void Serialize(std::string& out);
    std::string Summary();

   private:
    struct CounterGroup {
        CounterGroup(const std::string& prefix,
                     const std::string& granularity_help);

        ylt::metric::gauge_t get_requests;
        ylt::metric::gauge_t get_hits;
        ylt::metric::gauge_t get_misses;
        ylt::metric::gauge_t get_failures;
        ylt::metric::gauge_t get_bytes;
        ylt::metric::gauge_t put_requests;
        ylt::metric::gauge_t put_failures;
        ylt::metric::gauge_t put_bytes;
    };

    // Applies a signed delta to a gauge (dec for negative deltas).
    static void ApplyDelta(int64_t delta, ylt::metric::gauge_t& gauge);

    void ApplyDataMetricDelta(const DataMetricSnapshot& old_v,
                              const DataMetricSnapshot& new_v,
                              CounterGroup& group);

    // Recomputes retention aggregates from client_snapshots_.
    // Callers must hold mutex_.
    void RefreshRetentionAggregates();

    mutable std::mutex mutex_;
    // Per-client baseline for signed deltas; subtracted on client removal.
    std::unordered_map<UUID, ClientMetricSnapshot> client_snapshots_;

    CounterGroup total_;   // batch request granularity
    CounterGroup local_;   // per-op granularity
    CounterGroup remote_;  // per-op granularity
    ylt::metric::gauge_t remote_read_retries_;
    ylt::metric::gauge_t remote_write_retries_;

    // Cluster-wide key retention. The bucket arrays hold the merged
    // distributions (non-cumulative) over KeyRetentionMetric::LifetimeBuckets()
    // and are rendered as histograms at serialize time.
    ylt::metric::gauge_t key_live_count_;
    ylt::metric::gauge_t key_removed_count_;
    std::vector<int64_t> key_live_age_buckets_;
    std::vector<int64_t> key_removed_age_buckets_;
};

}  // namespace mooncake
