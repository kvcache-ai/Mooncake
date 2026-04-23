#pragma once

#include "client_metric.h"

namespace mooncake {

// P2P client metrics for local storage operations
struct P2PClientMetric : public ClientMetric {
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

    /**
     * @brief Creates a P2PClientMetric instance based on environment variables
     * @return std::unique_ptr<P2PClientMetric> containing the instance if
     * enabled, nullptr if disabled
     */
    static std::unique_ptr<P2PClientMetric> Create(
        std::map<std::string, std::string> labels) {
        return CreatePtr<P2PClientMetric>(labels);
    }

    explicit P2PClientMetric(uint64_t interval_seconds = 0,
                             std::map<std::string, std::string> labels = {});

    void serialize(std::string& str) override;
    std::string summary_metrics() override;
};

}  // namespace mooncake
