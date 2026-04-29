#include "p2p_client_metric.h"

namespace mooncake {

RequestMetric::RequestMetric(const std::string& prefix,
                             const std::map<std::string, std::string>& labels)
    : get_requests(prefix + "_get_requests_total",
                   "Total number of Get requests", labels),
      get_hits(prefix + "_get_hits_total", "Total number of Get hits (found)",
               labels),
      get_misses(prefix + "_get_misses_total",
                 "Total number of Get misses (not found)", labels),
      get_failures(prefix + "_get_failures_total",
                   "Total number of failed Get requests", labels),
      get_bytes(prefix + "_get_bytes_total", "Total bytes read by Get", labels),
      get_latency_success(prefix + "_get_latency_success_us",
                          "Get latency for successful requests (us)",
                          kLatencyBucket, labels),
      get_latency_failure(prefix + "_get_latency_failure_us",
                          "Get latency for failed requests (us)",
                          kLatencyBucket, labels),
      put_requests(prefix + "_put_requests_total",
                   "Total number of Put requests", labels),
      put_failures(prefix + "_put_failures_total",
                   "Total number of failed Put requests", labels),
      put_bytes(prefix + "_put_bytes_total", "Total bytes written by Put",
                labels),
      put_latency_success(prefix + "_put_latency_success_us",
                          "Put latency for successful requests (us)",
                          kLatencyBucket, labels),
      put_latency_failure(prefix + "_put_latency_failure_us",
                          "Put latency for failed requests (us)",
                          kLatencyBucket, labels) {}

void RequestMetric::serialize(std::string& str) {
    get_requests.serialize(str);
    get_hits.serialize(str);
    get_misses.serialize(str);
    get_failures.serialize(str);
    get_bytes.serialize(str);
    get_latency_success.serialize(str);
    get_latency_failure.serialize(str);
    put_requests.serialize(str);
    put_failures.serialize(str);
    put_bytes.serialize(str);
    put_latency_success.serialize(str);
    put_latency_failure.serialize(str);
}

std::string RequestMetric::summary_metrics() {
    std::stringstream ss;
    ss << "Get: " << get_requests.value() << " requests, " << get_hits.value()
       << " hits, " << get_misses.value() << " misses, " << get_failures.value()
       << " failures, " << byte_size_to_string(get_bytes.value()) << " read\n";

    ss << "Put: " << put_requests.value() << " requests, "
       << put_failures.value() << " failures, "
       << byte_size_to_string(put_bytes.value()) << " written\n";

    return ss.str();
}

P2PClientMetric::P2PClientMetric(
    uint64_t interval_seconds, const std::map<std::string, std::string>& labels)
    : ClientMetric(interval_seconds, labels),
      local_request("mooncake_p2p_local", labels),
      peer_request("mooncake_p2p_peer", labels) {}

void P2PClientMetric::serialize(std::string& str) {
    // Call base class serialize first
    ClientMetric::serialize(str);

    // Then serialize P2P-specific metrics
    local_request.serialize(str);
    peer_request.serialize(str);
}

std::string P2PClientMetric::summary_metrics() {
    std::stringstream ss;
    // Include base class metrics first
    ss << ClientMetric::summary_metrics();

    // Then add P2P-specific metrics
    ss << "=== P2P Local Request Metrics ===\n";
    ss << local_request.summary_metrics();

    ss << "=== P2P Peer Request Metrics ===\n";
    ss << peer_request.summary_metrics();

    return ss.str();
}

}  // namespace mooncake
