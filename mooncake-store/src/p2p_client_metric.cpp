#include "p2p_client_metric.h"

#include <glog/logging.h>

#include <algorithm>
#include <sstream>
#include <vector>

#include "tiered_cache/tiers/cache_tier.h"

namespace mooncake {

// ============================================================================
// DataMetric
// ============================================================================

DataMetric::DataMetric(const std::string& prefix,
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

void DataMetric::serialize(std::string& str) {
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

void DataMetric::RecordGet(int64_t elapsed_us, ErrorCode err, uint64_t bytes) {
    get_requests.inc();
    if (err == ErrorCode::OK) {
        get_hits.inc();
        get_bytes.inc(bytes);
        get_latency_success.observe(elapsed_us);
    } else if (err == ErrorCode::OBJECT_NOT_FOUND) {
        get_misses.inc();
    } else {
        get_failures.inc();
        get_latency_failure.observe(elapsed_us);
    }
}

void DataMetric::RecordPut(int64_t elapsed_us, ErrorCode err, uint64_t bytes) {
    if (IsAlreadyExistsError(err)) {
        // ignore the exist writing
        return;
    }
    put_requests.inc();
    if (err == ErrorCode::OK) {
        put_bytes.inc(bytes);
        put_latency_success.observe(elapsed_us);
    } else {
        put_failures.inc();
        put_latency_failure.observe(elapsed_us);
    }
}

void DataMetric::append_get_put_summary(std::ostream& ss) {
    ss << "Get: " << get_requests.value() << " requests, " << get_hits.value()
       << " hits, " << get_misses.value() << " misses, " << get_failures.value()
       << " failures, " << byte_size_to_string(get_bytes.value()) << " read"
       << " | success: " << format_latency_summary(get_latency_success)
       << " | failure: " << format_latency_summary(get_latency_failure) << "\n";
    ss << "Put: " << put_requests.value() << " requests, "
       << put_failures.value() << " failures, "
       << byte_size_to_string(put_bytes.value()) << " written"
       << " | success: " << format_latency_summary(put_latency_success)
       << " | failure: " << format_latency_summary(put_latency_failure) << "\n";
}

std::string DataMetric::summary_metrics() {
    std::stringstream ss;
    append_get_put_summary(ss);
    return ss.str();
}

// ============================================================================
// RequestMetric
// ============================================================================

RequestMetric::RequestMetric(const std::string& prefix,
                             const std::map<std::string, std::string>& labels)
    : DataMetric(prefix, labels),
      inflight(prefix + "_inflight", "Number of currently in-flight requests",
               labels) {}

void RequestMetric::serialize(std::string& str) {
    DataMetric::serialize(str);
    inflight.serialize(str);
}

std::string RequestMetric::summary_metrics() {
    std::stringstream ss;
    append_get_put_summary(ss);
    ss << "In-flight: " << inflight.value() << " requests\n";
    return ss.str();
}

// ============================================================================
// RemoteRequestMetric
// ============================================================================

RemoteRequestMetric::RemoteRequestMetric(
    const std::string& prefix, const std::map<std::string, std::string>& labels)
    : DataMetric(prefix, labels),
      write_retries(prefix + "_write_retries_total",
                    "Total write attempts beyond the first candidate", labels),
      read_retries(prefix + "_read_retries_total",
                   "Total read attempts beyond the first route", labels) {}

void RemoteRequestMetric::serialize(std::string& str) {
    DataMetric::serialize(str);
    write_retries.serialize(str);
    read_retries.serialize(str);
}

std::string RemoteRequestMetric::summary_metrics() {
    std::stringstream ss;
    append_get_put_summary(ss);
    ss << "Retries: write=" << write_retries.value()
       << ", read=" << read_retries.value() << "\n";
    return ss.str();
}

// ============================================================================
// RollbackMetric
// ============================================================================

RollbackMetric::RollbackMetric(const std::string& prefix,
                               const std::map<std::string, std::string>& labels)
    : write_revoke_requests(prefix + "_write_revoke_requests_total",
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

void RollbackMetric::serialize(std::string& str) {
    write_revoke_requests.serialize(str);
    write_revoke_failures.serialize(str);
    write_revoke_latency_success.serialize(str);
    write_revoke_latency_failure.serialize(str);
    unpin_key_requests.serialize(str);
    unpin_key_failures.serialize(str);
    unpin_key_latency_success.serialize(str);
    unpin_key_latency_failure.serialize(str);
}

std::string RollbackMetric::summary_metrics() {
    std::stringstream ss;
    ss << "WriteRevoke rollback: " << write_revoke_requests.value()
       << " requests, " << write_revoke_failures.value() << " failures\n";
    ss << "UnPinKey rollback: " << unpin_key_requests.value() << " requests, "
       << unpin_key_failures.value() << " failures\n";
    return ss.str();
}

// ============================================================================
// RpcHandlerMetric / ReadRpcHandlerMetric
// ============================================================================

RpcHandlerMetric::RpcHandlerMetric(
    const std::string& metric_prefix, const std::string& rpc_name,
    const std::map<std::string, std::string>& labels)
    : requests(metric_prefix + "_" + rpc_name + "_requests_total",
               "Total incoming " + rpc_name + " RPC requests", labels),
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
    failures.serialize(str);
    latency_success.serialize(str);
    latency_failure.serialize(str);
}

std::string RpcHandlerMetric::summary_line(const std::string& display_name) {
    std::stringstream ss;
    ss << display_name << ": " << requests.value() << " requests, "
       << failures.value() << " failures, "
       << format_latency_summary(latency_success) << "\n";
    return ss.str();
}

ReadRpcHandlerMetric::ReadRpcHandlerMetric(
    const std::string& metric_prefix, const std::string& rpc_name,
    const std::map<std::string, std::string>& labels)
    : RpcHandlerMetric(metric_prefix, rpc_name, labels),
      hits(metric_prefix + "_" + rpc_name + "_hits_total",
           "Total successful " + rpc_name + " RPC requests", labels),
      misses(metric_prefix + "_" + rpc_name + "_misses_total",
             "Total " + rpc_name + " RPC misses (not found)", labels) {}

void ReadRpcHandlerMetric::serialize(std::string& str) {
    RpcHandlerMetric::serialize(str);
    hits.serialize(str);
    misses.serialize(str);
}

std::string ReadRpcHandlerMetric::summary_line(
    const std::string& display_name) {
    std::stringstream ss;
    ss << display_name << ": " << requests.value() << " requests, "
       << hits.value() << " hits, " << misses.value() << " misses, "
       << failures.value() << " failures, "
       << format_latency_summary(latency_success) << "\n";
    return ss.str();
}

// ============================================================================
// PeerRequestMetrics
// ============================================================================

PeerRequestMetrics::PeerRequestMetrics(
    const std::string& prefix, const std::map<std::string, std::string>& labels)
    : read_remote_data(prefix, "read_remote_data", labels),
      write_remote_data(prefix, "write_remote_data", labels),
      prewrite(prefix, "prewrite", labels),
      write_commit(prefix, "write_commit", labels),
      write_revoke(prefix, "write_revoke", labels),
      pin_key(prefix, "pin_key", labels),
      unpin_key(prefix, "unpin_key", labels),
      inflight(prefix + "_inflight",
               "Number of currently in-flight incoming peer RPCs", labels) {}

void PeerRequestMetrics::serialize(std::string& str) {
    read_remote_data.serialize(str);
    write_remote_data.serialize(str);
    prewrite.serialize(str);
    write_commit.serialize(str);
    write_revoke.serialize(str);
    pin_key.serialize(str);
    unpin_key.serialize(str);
    inflight.serialize(str);
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
    ss << "In-flight: " << inflight.value() << " requests\n";
    return ss.str();
}

// ============================================================================
// TierMetric
// ============================================================================

namespace {
std::string TierIdToString(const UUID& tier_id) {
    return std::to_string(tier_id.first) + "_" + std::to_string(tier_id.second);
}
}  // namespace

void TierMetric::RegisterTier(const UUID& tier_id, const std::string& label,
                              const std::shared_ptr<CacheTier>& tier,
                              int priority) {
    if (!tier) {
        LOG(ERROR) << "TierMetric::RegisterTier: null tier, tier_id="
                   << TierIdToString(tier_id);
        return;
    }

    TierEntry entry;
    entry.label_array = {label};
    entry.memory_type = tier->GetMemoryType();
    entry.priority = priority;
    entry.capacity = tier->GetCapacity();
    entry.tier = tier;

    if (!tiers_.try_emplace(tier_id, std::move(entry)).second) {
        LOG(ERROR) << "TierMetric::RegisterTier: tier already registered"
                   << ", tier_id=" << TierIdToString(tier_id)
                   << ", label=" << label;
        return;
    }

    // Initialize all series so an idle tier still shows up in /metrics.
    const std::array<std::string, 1> label_array = {label};
    key_count.inc(label_array, 0);
    capacity_bytes.update(label_array,
                          static_cast<int64_t>(tier->GetCapacity()));
    used_bytes.update(label_array, static_cast<int64_t>(tier->GetUsage()));
    evicted_keys.inc(label_array, 0);
    offloaded_keys.inc(label_array, 0);
    onboarded_keys.inc(label_array, 0);
}

void TierMetric::OnReplicaAdded(const UUID& tier_id) {
    auto it = tiers_.find(tier_id);
    if (it == tiers_.end()) {
        LOG(ERROR) << "TierMetric::OnReplicaAdded: unregistered tier, tier_id="
                   << TierIdToString(tier_id);
        return;
    }
    key_count.inc(it->second.label_array, 1);
}

void TierMetric::OnReplicaRemoved(const UUID& tier_id) {
    auto it = tiers_.find(tier_id);
    if (it == tiers_.end()) {
        LOG(ERROR)
            << "TierMetric::OnReplicaRemoved: unregistered tier, tier_id="
            << TierIdToString(tier_id);
        return;
    }
    key_count.dec(it->second.label_array, 1);
}

void TierMetric::OnEvicted(const UUID& tier_id) {
    auto it = tiers_.find(tier_id);
    if (it == tiers_.end()) {
        LOG(ERROR) << "TierMetric::OnEvicted: unregistered tier, tier_id="
                   << TierIdToString(tier_id);
        return;
    }
    evicted_keys.inc(it->second.label_array, 1);
}

void TierMetric::OnMoved(const UUID& source_tier, const UUID& dest_tier) {
    auto src = tiers_.find(source_tier);
    auto dst = tiers_.find(dest_tier);
    if (src == tiers_.end() || dst == tiers_.end()) {
        LOG(ERROR) << "TierMetric::OnMoved: unregistered tier, source="
                   << TierIdToString(source_tier)
                   << ", dest=" << TierIdToString(dest_tier);
        return;
    }
    if (src->second.priority > dst->second.priority) {
        offloaded_keys.inc(src->second.label_array, 1);
    } else if (src->second.priority < dst->second.priority) {
        onboarded_keys.inc(src->second.label_array, 1);
    }
    // Same-priority movement is neither offload nor onboard; not counted.
}

void TierMetric::RefreshUsage() {
    for (const auto& [tier_id, entry] : tiers_) {
        auto tier = entry.tier.lock();
        if (!tier) {
            // Tier already destroyed; keep the last known value.
            continue;
        }
        used_bytes.update(entry.label_array,
                          static_cast<int64_t>(tier->GetUsage()));
    }
}

void TierMetric::serialize(std::string& str) {
    RefreshUsage();
    key_count.serialize(str);
    capacity_bytes.serialize(str);
    used_bytes.serialize(str);
    evicted_keys.serialize(str);
    offloaded_keys.serialize(str);
    onboarded_keys.serialize(str);
}

std::string TierMetric::summary_metrics() {
    RefreshUsage();

    std::stringstream ss;
    if (tiers_.empty()) {
        ss << "No tiers registered\n";
        return ss.str();
    }

    // Display tiers by priority (descending), then by label, for stable and
    // readable output.
    std::vector<const TierEntry*> sorted;
    sorted.reserve(tiers_.size());
    for (const auto& [tier_id, entry] : tiers_) {
        sorted.push_back(&entry);
    }
    std::sort(sorted.begin(), sorted.end(),
              [](const TierEntry* a, const TierEntry* b) {
                  if (a->priority != b->priority) {
                      return a->priority > b->priority;
                  }
                  return a->label_array[0] < b->label_array[0];
              });

    for (const auto* entry : sorted) {
        const int64_t keys = key_count.value(entry->label_array);
        const int64_t used = used_bytes.value(entry->label_array);
        const int64_t capacity = static_cast<int64_t>(entry->capacity);
        ss << "Tier " << entry->label_array[0] << " ("
           << MemoryTypeToString(entry->memory_type)
           << ", priority=" << entry->priority << "): keys=" << keys
           << ", used=" << byte_size_to_string(used) << "/"
           << byte_size_to_string(capacity);
        if (capacity > 0) {
            ss << " (" << (used * 100 / capacity) << "%)";
        }
        ss << ", evicted_keys=" << evicted_keys.value(entry->label_array)
           << ", offloaded_keys=" << offloaded_keys.value(entry->label_array)
           << ", onboarded_keys=" << onboarded_keys.value(entry->label_array)
           << "\n";
    }
    return ss.str();
}

// ============================================================================
// P2PClientMetric
// ============================================================================

P2PClientMetric::P2PClientMetric(
    uint64_t interval_seconds, const std::map<std::string, std::string>& labels)
    : ClientMetric(interval_seconds, labels),
      total_request("mooncake_p2p_total", labels),
      local_request("mooncake_p2p_local", labels),
      remote_request("mooncake_p2p_remote", labels),
      rollback("mooncake_p2p_rollback", labels),
      peer_request_metrics("mooncake_p2p_peer", labels),
      tier_metric(std::make_shared<TierMetric>()) {}

void P2PClientMetric::serialize(std::string& str) {
    ClientMetric::serialize(str);
    total_request.serialize(str);
    local_request.serialize(str);
    remote_request.serialize(str);
    rollback.serialize(str);
    peer_request_metrics.serialize(str);
    tier_metric->serialize(str);
}

std::string P2PClientMetric::summary_metrics() {
    std::stringstream ss;
    ss << "Client Metrics Summary\n";

    ss << master_client_metric.summary_metrics();

    ss << "=== P2P Total (per-request) ===\n";
    ss << total_request.summary_metrics();

    ss << "=== P2P Local (per-attempt) ===\n";
    ss << local_request.summary_metrics();

    ss << "=== P2P Remote (per-attempt) ===\n";
    ss << remote_request.summary_metrics();

    ss << "=== P2P Rollback ===\n";
    ss << rollback.summary_metrics();

    ss << "=== P2P Peer Request Metrics ===\n";
    ss << peer_request_metrics.summary_metrics();

    ss << "=== P2P Tier Metrics ===\n";
    ss << tier_metric->summary_metrics();

    return ss.str();
}

}  // namespace mooncake
