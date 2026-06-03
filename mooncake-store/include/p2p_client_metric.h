#pragma once

#include "client_metric.h"

namespace mooncake {

// Request metrics for Get/Put operations
struct RequestMetric {
    // Get metrics
    ylt::metric::counter_t get_requests;
    ylt::metric::counter_t get_hits;
    ylt::metric::counter_t get_misses;
    ylt::metric::counter_t get_failures;
    ylt::metric::counter_t get_bytes;
    ylt::metric::histogram_t get_latency_success;
    ylt::metric::histogram_t get_latency_failure;

    // Put metrics
    ylt::metric::counter_t put_requests;
    ylt::metric::counter_t put_failures;
    ylt::metric::counter_t put_bytes;
    ylt::metric::histogram_t put_latency_success;
    ylt::metric::histogram_t put_latency_failure;

    explicit RequestMetric(
        const std::string& prefix,
        const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str);
    std::string summary_metrics();
};

// P2P client metrics for local storage operations
struct P2PClientMetric : public ClientMetric {
    // Local request metrics (Get/Put)
    RequestMetric local_request;
    // Peer request metrics (requests from other clients)
    RequestMetric peer_request;

    /**
     * @brief Creates a P2PClientMetric instance based on environment variables
     * @return std::unique_ptr<P2PClientMetric> containing the instance if
     * enabled, nullptr if disabled
     */
    static std::unique_ptr<P2PClientMetric> Create(
        const std::map<std::string, std::string>& labels = {}) {
        return CreatePtr<P2PClientMetric>(labels);
    }

    explicit P2PClientMetric(
        uint64_t interval_seconds = 0,
        const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str) override;
    std::string summary_metrics() override;
};

}  // namespace mooncake
