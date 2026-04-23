#include "p2p_client_metric.h"

namespace mooncake {

P2PClientMetric::P2PClientMetric(uint64_t interval_seconds,
                                 std::map<std::string, std::string> labels)
    : ClientMetric(interval_seconds, labels),
      local_get_requests("mooncake_p2p_local_get_requests_total",
                         "Total number of local Get requests", labels),
      local_get_hits("mooncake_p2p_local_get_hits_total",
                     "Total number of local Get hits (found in local storage)",
                     labels),
      local_get_misses("mooncake_p2p_local_get_misses_total",
                       "Total number of local Get misses (not found locally)",
                       labels),
      local_get_failures("mooncake_p2p_local_get_failures_total",
                         "Total number of failed local Get requests", labels),
      local_get_bytes("mooncake_p2p_local_get_bytes_total",
                      "Total bytes read by local Get", labels),
      local_get_latency("mooncake_p2p_local_get_latency_us",
                        "Local Get latency (us)", kLatencyBucket, labels),
      local_put_requests("mooncake_p2p_local_put_requests_total",
                         "Total number of local Put requests", labels),
      local_put_failures("mooncake_p2p_local_put_failures_total",
                         "Total number of failed local Put requests", labels),
      local_put_bytes("mooncake_p2p_local_put_bytes_total",
                      "Total bytes written by local Put", labels),
      local_put_latency("mooncake_p2p_local_put_latency_us",
                        "Local Put latency (us)", kLatencyBucket, labels) {}

void P2PClientMetric::serialize(std::string& str) {
    // Call base class serialize first
    ClientMetric::serialize(str);

    // Then serialize P2P-specific metrics
    local_get_requests.serialize(str);
    local_get_hits.serialize(str);
    local_get_misses.serialize(str);
    local_get_failures.serialize(str);
    local_get_bytes.serialize(str);
    local_get_latency.serialize(str);
    local_put_requests.serialize(str);
    local_put_failures.serialize(str);
    local_put_bytes.serialize(str);
    local_put_latency.serialize(str);
}

std::string P2PClientMetric::summary_metrics() {
    std::stringstream ss;
    // Include base class metrics first
    ss << ClientMetric::summary_metrics();

    // Then add P2P-specific metrics
    ss << "=== P2P Local Storage Metrics ===\n";

    ss << "Local Get: " << local_get_requests.value() << " requests, "
       << local_get_hits.value() << " hits, " << local_get_misses.value()
       << " misses, " << local_get_failures.value() << " failures, "
       << byte_size_to_string(local_get_bytes.value()) << " read\n";

    ss << "Local Put: " << local_put_requests.value() << " requests, "
       << local_put_failures.value() << " failures, "
       << byte_size_to_string(local_put_bytes.value()) << " written\n";

    return ss.str();
}

}  // namespace mooncake
