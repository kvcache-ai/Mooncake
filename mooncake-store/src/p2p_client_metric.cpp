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
                          kLatencyBucket, labels),
      write_revoke_requests(prefix + "_write_revoke_requests_total",
                            "Total outgoing WriteRevoke rollback RPCs", labels),
      write_revoke_failures(prefix + "_write_revoke_failures_total",
                            "Total failed WriteRevoke rollback RPCs", labels),
      write_revoke_latency_success(
          prefix + "_write_revoke_latency_success_us",
          "WriteRevoke rollback RPC latency for successful requests (us)",
          kLatencyBucket, labels),
      write_revoke_latency_failure(
          prefix + "_write_revoke_latency_failure_us",
          "WriteRevoke rollback RPC latency for failed requests (us)",
          kLatencyBucket, labels),
      unpin_key_requests(prefix + "_unpin_key_requests_total",
                         "Total outgoing UnPinKey rollback RPCs", labels),
      unpin_key_failures(prefix + "_unpin_key_failures_total",
                         "Total failed UnPinKey rollback RPCs", labels),
      unpin_key_latency_success(prefix + "_unpin_key_latency_success_us",
                                "UnPinKey rollback RPC latency for successful "
                                "requests (us)",
                                kLatencyBucket, labels),
      unpin_key_latency_failure(prefix + "_unpin_key_latency_failure_us",
                                "UnPinKey rollback RPC latency for failed "
                                "requests (us)",
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
    write_revoke_requests.serialize(str);
    write_revoke_failures.serialize(str);
    write_revoke_latency_success.serialize(str);
    write_revoke_latency_failure.serialize(str);
    unpin_key_requests.serialize(str);
    unpin_key_failures.serialize(str);
    unpin_key_latency_success.serialize(str);
    unpin_key_latency_failure.serialize(str);
}

std::string RequestMetric::summary_metrics() {
    std::stringstream ss;
    ss << "Get: " << get_requests.value() << " requests, " << get_hits.value()
       << " hits, " << get_misses.value() << " misses, " << get_failures.value()
       << " failures, " << byte_size_to_string(get_bytes.value()) << " read\n";

    ss << "Put: " << put_requests.value() << " requests, "
       << put_failures.value() << " failures, "
       << byte_size_to_string(put_bytes.value()) << " written\n";

    ss << "WriteRevoke rollback: " << write_revoke_requests.value()
       << " requests, " << write_revoke_failures.value() << " failures\n";
    ss << "UnPinKey rollback: " << unpin_key_requests.value() << " requests, "
       << unpin_key_failures.value() << " failures\n";

    return ss.str();
}

RpcHandlerMetric::RpcHandlerMetric(
    const std::string& metric_prefix, const std::string& rpc_name,
    const std::map<std::string, std::string>& labels)
    : requests(metric_prefix + "_" + rpc_name + "_requests_total",
               "Total incoming " + rpc_name + " RPC requests", labels),
      hits(metric_prefix + "_" + rpc_name + "_hits_total",
           "Total successful " + rpc_name + " RPC requests", labels),
      misses(metric_prefix + "_" + rpc_name + "_misses_total",
             "Total " + rpc_name + " RPC misses (not found)", labels),
      failures(metric_prefix + "_" + rpc_name + "_failures_total",
               "Total failed " + rpc_name + " RPC requests", labels),
      latency_success(metric_prefix + "_" + rpc_name + "_latency_success_us",
                      rpc_name + " RPC latency for successful requests (us)",
                      kLatencyBucket, labels),
      latency_failure(metric_prefix + "_" + rpc_name + "_latency_failure_us",
                      rpc_name + " RPC latency for failed requests (us)",
                      kLatencyBucket, labels) {}

void RpcHandlerMetric::serialize(std::string& str) {
    requests.serialize(str);
    hits.serialize(str);
    misses.serialize(str);
    failures.serialize(str);
    latency_success.serialize(str);
    latency_failure.serialize(str);
}

std::string RpcHandlerMetric::summary_line(
    const std::string& display_name) {
    std::stringstream ss;
    ss << display_name << ": " << requests.value() << " requests, "
       << hits.value() << " hits, " << misses.value() << " misses, "
       << failures.value() << " failures\n";
    return ss.str();
}

PeerRequestMetrics::PeerRequestMetrics(
    const std::string& prefix, const std::map<std::string, std::string>& labels)
    : read_remote_data(prefix, "read_remote_data", labels),
      write_remote_data(prefix, "write_remote_data", labels),
      prewrite(prefix, "prewrite", labels),
      write_commit(prefix, "write_commit", labels),
      write_revoke(prefix, "write_revoke", labels),
      pin_key(prefix, "pin_key", labels),
      unpin_key(prefix, "unpin_key", labels) {}

void PeerRequestMetrics::serialize(std::string& str) {
    read_remote_data.serialize(str);
    write_remote_data.serialize(str);
    prewrite.serialize(str);
    write_commit.serialize(str);
    write_revoke.serialize(str);
    pin_key.serialize(str);
    unpin_key.serialize(str);
}

std::string PeerRequestMetrics::summary_metrics() {
    std::stringstream ss;
    ss << read_remote_data.summary_line("ReadRemoteData");
    ss << write_remote_data.summary_line("WriteRemoteData");
    ss << prewrite.summary_line("PreWrite");
    ss << write_commit.summary_line("WriteCommit");
    ss << write_revoke.summary_line("WriteRevoke");
    ss << pin_key.summary_line("PinKey");
    ss << unpin_key.summary_line("UnPinKey");
    return ss.str();
}

P2PClientMetric::P2PClientMetric(
    uint64_t interval_seconds, const std::map<std::string, std::string>& labels)
    : ClientMetric(interval_seconds, labels),
      local_request("mooncake_p2p_local", labels),
      peer_request_metrics("mooncake_p2p_peer", labels) {}

void P2PClientMetric::serialize(std::string& str) {
    // Call base class serialize first
    ClientMetric::serialize(str);

    // Then serialize P2P-specific metrics
    local_request.serialize(str);
    peer_request_metrics.serialize(str);
}

std::string P2PClientMetric::summary_metrics() {
    std::stringstream ss;
    // Include base class metrics first
    ss << ClientMetric::summary_metrics();

    // Then add P2P-specific metrics
    ss << "=== P2P Local Request Metrics ===\n";
    ss << local_request.summary_metrics();

    ss << "=== P2P Peer Request Metrics ===\n";
    ss << peer_request_metrics.summary_metrics();

    return ss.str();
}

}  // namespace mooncake
