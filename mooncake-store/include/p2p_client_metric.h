#pragma once

#include <ylt/metric/gauge.hpp>

#include "client_metric.h"
#include "types.h"

namespace mooncake {

// Read/write data metrics (per single-op dimension)
struct DataMetric {
    // Get
    ylt::metric::counter_t get_requests;
    ylt::metric::counter_t get_hits;
    ylt::metric::counter_t get_misses;
    ylt::metric::counter_t get_failures;
    ylt::metric::counter_t get_bytes;
    ylt::metric::histogram_t get_latency_success;
    ylt::metric::histogram_t get_latency_failure;

    // Put
    ylt::metric::counter_t put_requests;
    ylt::metric::counter_t put_failures;
    ylt::metric::counter_t put_bytes;
    ylt::metric::histogram_t put_latency_success;
    ylt::metric::histogram_t put_latency_failure;

    explicit DataMetric(const std::string& prefix,
                        const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str);
    std::string summary_metrics();

    void RecordGet(int64_t elapsed_us, ErrorCode err, uint64_t bytes);

    void RecordPut(int64_t elapsed_us, ErrorCode err, uint64_t bytes);

   protected:
    void append_get_put_summary(std::ostream& ss);
};

// Request metrics = DataMetric + inflight (end-to-end dimension).
struct RequestMetric : public DataMetric {
    ylt::metric::gauge_t inflight;

    explicit RequestMetric(
        const std::string& prefix,
        const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str);
    std::string summary_metrics();
};

// Remote data metrics = DataMetric + retry counters (retries only happen on
// the route-based remote flow).
struct RemoteRequestMetric : public DataMetric {
    ylt::metric::counter_t write_retries;
    ylt::metric::counter_t read_retries;

    explicit RemoteRequestMetric(
        const std::string& prefix,
        const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str);
    std::string summary_metrics();
};

// Rollback metrics (WriteRevoke / UnPinKey).
struct RollbackMetric {
    ylt::metric::counter_t write_revoke_requests;
    ylt::metric::counter_t write_revoke_failures;
    ylt::metric::histogram_t write_revoke_latency_success;
    ylt::metric::histogram_t write_revoke_latency_failure;

    ylt::metric::counter_t unpin_key_requests;
    ylt::metric::counter_t unpin_key_failures;
    ylt::metric::histogram_t unpin_key_latency_success;
    ylt::metric::histogram_t unpin_key_latency_failure;

    explicit RollbackMetric(
        const std::string& prefix,
        const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str);
    std::string summary_metrics();
};

// Per-RPC metrics for peer incoming handlers.
struct RpcHandlerMetric {
    ylt::metric::counter_t requests;
    ylt::metric::counter_t failures;
    ylt::metric::histogram_t latency_success;
    ylt::metric::histogram_t latency_failure;

    RpcHandlerMetric(const std::string& metric_prefix,
                     const std::string& rpc_name,
                     const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str);
    std::string summary_line(const std::string& display_name);
};

// Read-semantics peer RPC metrics (ReadRemoteData, PinKey).
struct ReadRpcHandlerMetric : RpcHandlerMetric {
    ylt::metric::counter_t hits;
    ylt::metric::counter_t misses;

    ReadRpcHandlerMetric(const std::string& metric_prefix,
                         const std::string& rpc_name,
                         const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str);
    std::string summary_line(const std::string& display_name);
};

struct PeerRequestMetrics {
    ReadRpcHandlerMetric read_remote_data;
    RpcHandlerMetric write_remote_data;
    RpcHandlerMetric prewrite;
    RpcHandlerMetric write_commit;
    RpcHandlerMetric write_revoke;
    ReadRpcHandlerMetric pin_key;
    RpcHandlerMetric unpin_key;

    ylt::metric::gauge_t inflight;

    explicit PeerRequestMetrics(
        const std::string& prefix = "mooncake_p2p_peer",
        const std::map<std::string, std::string>& labels = {});

    void serialize(std::string& str);
    std::string summary_metrics();
};

struct P2PClientMetric : public ClientMetric {
    // total_request is recorded at request (Batch) granularity:
    // BatchPut/BatchGet counts as one request, and every key in the batch
    // shares same latency sample (the time cost depend on the slowest key).
    RequestMetric total_request;
    // local_request / remote_request are recorded at per-op granularity:
    // each individual local or remote read/write operation contributes its own
    // latency sample and byte count.
    DataMetric local_request;
    RemoteRequestMetric remote_request;
    RollbackMetric rollback;
    PeerRequestMetrics peer_request_metrics;

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
