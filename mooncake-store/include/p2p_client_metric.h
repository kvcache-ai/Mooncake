#pragma once

#include "client_metric.h"

namespace mooncake {

// Local Get/Put metrics (BatchPut / BatchGet).
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

    // Outgoing rollback RPCs (initiator cleanup after transfer failure).
    ylt::metric::counter_t write_revoke_requests;
    ylt::metric::counter_t write_revoke_failures;
    ylt::metric::histogram_t write_revoke_latency_success;
    ylt::metric::histogram_t write_revoke_latency_failure;

    ylt::metric::counter_t unpin_key_requests;
    ylt::metric::counter_t unpin_key_failures;
    ylt::metric::histogram_t unpin_key_latency_success;
    ylt::metric::histogram_t unpin_key_latency_failure;

    explicit RequestMetric(
        const std::string& prefix,
        const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str);
    std::string summary_metrics();
};

// Per-RPC metrics for peer incoming handlers.
struct RpcHandlerMetric {
    ylt::metric::counter_t requests;
    ylt::metric::counter_t hits;
    ylt::metric::counter_t misses;
    ylt::metric::counter_t failures;
    ylt::metric::histogram_t latency_success;
    ylt::metric::histogram_t latency_failure;

    RpcHandlerMetric(const std::string& metric_prefix,
                     const std::string& rpc_name,
                     const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str) const;
    std::string summary_line(const std::string& display_name) const;
};

struct PeerRequestMetrics {
    RpcHandlerMetric read_remote_data;
    RpcHandlerMetric write_remote_data;
    RpcHandlerMetric prewrite;
    RpcHandlerMetric write_commit;
    RpcHandlerMetric write_revoke;
    RpcHandlerMetric pin_key;
    RpcHandlerMetric unpin_key;

    explicit PeerRequestMetrics(
        const std::string& prefix = "mooncake_p2p_peer",
        const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str);
    std::string summary_metrics();
};

struct P2PClientMetric : public ClientMetric {
    RequestMetric local_request;
    PeerRequestMetrics peer_request_metrics;

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
