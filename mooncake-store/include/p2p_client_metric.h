#pragma once

#include "client_metric.h"

namespace mooncake {

// P2P client metrics for local storage operations
struct P2PClientMetric {
    // Local Get metrics
    ylt::metric::counter_t local_get_requests;
    ylt::metric::counter_t local_get_hits;
    ylt::metric::counter_t local_get_misses;
    ylt::metric::counter_t local_get_failures;
    ylt::metric::counter_t local_get_bytes;
    ylt::metric::histogram_t local_get_latency;

    // Local Put metrics
    ylt::metric::counter_t local_put_requests;
    ylt::metric::counter_t local_put_failures;
    ylt::metric::counter_t local_put_bytes;
    ylt::metric::histogram_t local_put_latency;

    explicit P2PClientMetric(std::map<std::string, std::string> labels = {});

    void serialize(std::string& str);
    std::string summary_metrics();
};

}  // namespace mooncake
