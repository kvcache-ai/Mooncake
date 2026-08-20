#include "p2p_client_metric.h"

#include <glog/logging.h>

#include <algorithm>
#include <array>
#include <sstream>
#include <utility>
#include <vector>

#include "tiered_cache/tiers/cache_tier.h"

namespace mooncake {

namespace {

DataMetricSnapshot SnapshotDataMetric(DataMetric& m) {
    DataMetricSnapshot s;
    s.get_requests = m.get_requests.value();
    s.get_hits = m.get_hits.value();
    s.get_misses = m.get_misses.value();
    s.get_failures = m.get_failures.value();
    s.get_bytes = m.get_bytes.value();
    s.put_requests = m.put_requests.value();
    s.put_failures = m.put_failures.value();
    s.put_bytes = m.put_bytes.value();
    return s;
}

}  // namespace

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
// KeyRetentionMetric
// ============================================================================

const std::vector<double>& KeyRetentionMetric::LifetimeBuckets() {
    static const std::vector<double> kBoundaries = {
        1, 2, 5, 10, 30, 60, 300, 600, 1800, 3600, 21600, 86400, 604800};
    return kBoundaries;
}

std::vector<int64_t> KeyRetentionMetric::InterpolateQuantiles(
    const std::vector<double>& boundaries,
    const std::vector<int64_t>& bucket_counts,
    const std::vector<double>& quantiles) {
    std::vector<int64_t> result(quantiles.size(), 0);
    const bool valid_qs =
        !quantiles.empty() &&
        std::is_sorted(quantiles.begin(), quantiles.end()) &&
        std::all_of(quantiles.begin(), quantiles.end(),
                    [](double q) { return q > 0.0 && q <= 1.0; });
    if (!valid_qs) {
        LOG(ERROR) << "KeyRetentionMetric::InterpolateQuantiles: quantiles "
                      "must be sorted ascending and within (0, 1]";
        return result;
    }

    int64_t total = 0;
    for (const int64_t count : bucket_counts) {
        total += count;
    }
    if (total <= 0 || boundaries.empty()) {
        return result;
    }

    const double total_d = static_cast<double>(total);
    size_t next = 0;
    int64_t cumulative = 0;
    for (size_t i = 0; i < bucket_counts.size() && next < quantiles.size();
         ++i) {
        cumulative += bucket_counts[i];
        if (static_cast<double>(cumulative) < quantiles[next] * total_d) {
            continue;
        }
        if (i >= boundaries.size()) {
            // Open +Inf bucket: resolve to the largest finite boundary.
            while (next < quantiles.size() && static_cast<double>(cumulative) >=
                                                  quantiles[next] * total_d) {
                result[next++] = static_cast<int64_t>(boundaries.back());
            }
            continue;
        }
        const double lower = (i == 0) ? 0.0 : boundaries[i - 1];
        const double upper = boundaries[i];
        const double before =
            static_cast<double>(cumulative - bucket_counts[i]);
        // bucket_counts[i] > 0 here: a zero-count bucket can never raise
        // the cumulative past a still-unresolved target.
        while (next < quantiles.size() &&
               static_cast<double>(cumulative) >= quantiles[next] * total_d) {
            const double target = quantiles[next] * total_d;
            const double frac =
                (target - before) / static_cast<double>(bucket_counts[i]);
            result[next++] =
                static_cast<int64_t>(lower + (upper - lower) * frac);
        }
    }
    // Targets never reached (numerical edge): clamp like the +Inf bucket.
    while (next < quantiles.size()) {
        result[next++] = static_cast<int64_t>(boundaries.back());
    }
    return result;
}

void KeyRetentionMetric::SerializeBucketHistogram(
    std::string& str, const std::string& name, const std::string& help,
    const std::map<std::string, std::string>& labels,
    const std::vector<double>& boundaries,
    const std::vector<int64_t>& bucket_counts) {
    if (bucket_counts.size() != boundaries.size() + 1) {
        LOG(ERROR) << "KeyRetentionMetric::SerializeBucketHistogram: metric "
                   << name << " expects " << boundaries.size() + 1
                   << " buckets, got " << bucket_counts.size();
        return;
    }
    int64_t total = 0;
    for (const int64_t count : bucket_counts) {
        total += count;
    }
    if (total <= 0) {
        return;  // Consistent with ylt histograms: skip empty distributions.
    }

    str.append("# HELP ").append(name).append(" ").append(help).append("\n");
    str.append("# TYPE ").append(name).append(" histogram\n");

    // Appends the label block: static labels plus, for bucket samples, the
    // le label. Omitted entirely when both are empty.
    const auto append_labels = [&str, &labels](const std::string& le) {
        if (labels.empty() && le.empty()) {
            str.append(" ");
            return;
        }
        str.append("{");
        for (const auto& [k, v] : labels) {
            str.append(k).append("=\"").append(v).append("\",");
        }
        if (!le.empty()) {
            str.append("le=\"").append(le).append("\"");
        } else {
            str.pop_back();  // Drop the trailing comma of the last label.
        }
        str.append("} ");
    };

    double sum = 0.0;
    int64_t cumulative = 0;
    for (size_t i = 0; i < bucket_counts.size(); ++i) {
        cumulative += bucket_counts[i];
        const double lower = (i == 0) ? 0.0 : boundaries[i - 1];
        const double upper =
            (i < boundaries.size()) ? boundaries[i] : boundaries.back();
        sum += static_cast<double>(bucket_counts[i]) * (lower + upper) / 2.0;

        str.append(name).append("_bucket");
        const std::string le =
            (i == boundaries.size()) ? "+Inf" : std::to_string(boundaries[i]);
        append_labels(le);
        str.append(std::to_string(cumulative)).append("\n");
    }
    str.append(name).append("_sum");
    append_labels("");
    str.append(std::to_string(sum)).append("\n");
    str.append(name).append("_count");
    append_labels("");
    str.append(std::to_string(total)).append("\n");
}

KeyRetentionMetric::KeyRetentionMetric(
    const std::string& prefix, const std::map<std::string, std::string>& labels,
    std::chrono::steady_clock::time_point anchor)
    : anchor_(anchor),
      prefix_(prefix),
      labels_(labels),
      live_keys(prefix + "_live_count",
                "Number of keys currently retained on this client", labels),
      removed_keys(prefix + "_removed_total",
                   "Cumulative number of keys removed from this client "
                   "(deleted or evicted)",
                   labels),
      removed_age(prefix + "_removed_age_seconds",
                  "Lifetime distribution of removed keys on this "
                  "client (seconds; deleted, evicted or cleared)",
                  LifetimeBuckets(), labels) {
    // Geometric (~1.3x) birth-offset boundaries for the cohort counters.
    int64_t bound = 1;
    while (bound < kCohortCoverageSeconds) {
        cohort_bounds_.push_back(bound);
        const int64_t next = static_cast<int64_t>(bound * 1.3) + 1;
        bound = next > bound ? next : bound + 1;
    }
    cohort_bounds_.push_back(bound);
    cohorts_.resize(cohort_bounds_.size());
    for (auto& cohort : cohorts_) {
        cohort.store(0, std::memory_order_relaxed);
    }
}

void KeyRetentionMetric::OnKeyCreated(
    std::chrono::steady_clock::time_point birth) {
    live_keys.inc();
    cohorts_[CohortIndex(ToOffsetSeconds(birth))].fetch_add(
        1, std::memory_order_relaxed);
}

void KeyRetentionMetric::OnKeyRemoved(
    std::chrono::steady_clock::time_point birth) {
    const int64_t birth_offset = ToOffsetSeconds(birth);
    const int64_t lifetime = std::max<int64_t>(
        0, ToOffsetSeconds(std::chrono::steady_clock::now()) - birth_offset);

    live_keys.dec();
    cohorts_[CohortIndex(birth_offset)].fetch_sub(1, std::memory_order_relaxed);
    removed_keys.inc();
    removed_age.observe(lifetime);
}

int64_t KeyRetentionMetric::ToOffsetSeconds(
    std::chrono::steady_clock::time_point tp) const {
    return std::chrono::duration_cast<std::chrono::seconds>(tp - anchor_)
        .count();
}

size_t KeyRetentionMetric::CohortIndex(int64_t birth_offset_seconds) const {
    const size_t not_greater = static_cast<size_t>(std::distance(
        cohort_bounds_.begin(),
        std::upper_bound(cohort_bounds_.begin(), cohort_bounds_.end(),
                         birth_offset_seconds)));
    if (not_greater == 0) {
        return 0;  // clamp (e.g. negative offset) into the first slot
    }
    return std::min(not_greater - 1, cohorts_.size() - 1);
}

KeyRetentionSnapshot KeyRetentionMetric::Snapshot() {
    KeyRetentionSnapshot snap;
    snap.live_count = live_keys.value();
    snap.removed_total = removed_keys.value();
    CollectBuckets(snap.live_age_buckets, snap.removed_buckets);
    return snap;
}

void KeyRetentionMetric::serialize(std::string& str) {
    std::vector<int64_t> live_age_buckets;
    std::vector<int64_t> removed_buckets;
    CollectBuckets(live_age_buckets, removed_buckets);
    live_keys.serialize(str);
    removed_keys.serialize(str);
    removed_age.serialize(str);
    SerializeBucketHistogram(
        str, prefix_ + "_live_age_seconds",
        "Current age distribution of live keys on this client (seconds); "
        "approximate (birth cohorts); sum estimated from bucket midpoints",
        labels_, LifetimeBuckets(), live_age_buckets);
    std::vector<int64_t> all_buckets(live_age_buckets.size(), 0);
    for (size_t i = 0; i < all_buckets.size() && i < removed_buckets.size();
         ++i) {
        all_buckets[i] = live_age_buckets[i] + removed_buckets[i];
    }
    SerializeBucketHistogram(
        str, prefix_ + "_all_lifetime_seconds",
        "Lifetime distribution of all keys seen by this client (seconds): "
        "live keys censored at current age + removed keys' exact lifetime; "
        "sum estimated from bucket midpoints",
        labels_, LifetimeBuckets(), all_buckets);
}

std::string KeyRetentionMetric::summary_metrics() {
    std::vector<int64_t> live_age_buckets;
    std::vector<int64_t> removed_buckets;
    CollectBuckets(live_age_buckets, removed_buckets);
    std::vector<int64_t> all_buckets(live_age_buckets.size(), 0);
    for (size_t i = 0; i < all_buckets.size() && i < removed_buckets.size();
         ++i) {
        all_buckets[i] = live_age_buckets[i] + removed_buckets[i];
    }

    const std::vector<double>& boundaries = LifetimeBuckets();
    static const std::vector<double> kQuantiles = {0.30, 0.50, 0.80, 0.95};
    const std::vector<int64_t> live_q =
        InterpolateQuantiles(boundaries, live_age_buckets, kQuantiles);
    const std::vector<int64_t> removed_q =
        InterpolateQuantiles(boundaries, removed_buckets, kQuantiles);
    const std::vector<int64_t> all_q =
        InterpolateQuantiles(boundaries, all_buckets, kQuantiles);

    std::stringstream ss;
    ss << "live=" << live_keys.value() << ", removed=" << removed_keys.value()
       << "\n";
    ss << "live_age: p30=" << live_q[0] << "s, p50=" << live_q[1]
       << "s, p80=" << live_q[2] << "s, p95=" << live_q[3] << "s\n";
    ss << "removed_age: p30=" << removed_q[0] << "s, p50=" << removed_q[1]
       << "s, p80=" << removed_q[2] << "s, p95=" << removed_q[3] << "s\n";
    ss << "all_lifetime: p30=" << all_q[0] << "s, p50=" << all_q[1]
       << "s, p80=" << all_q[2] << "s, p95=" << all_q[3] << "s\n";
    return ss.str();
}

// Builds the two scrape-time distributions: current age of live keys
// (from birth cohorts) and lifetime of removed keys.
void KeyRetentionMetric::CollectBuckets(std::vector<int64_t>& live_age_buckets,
                                        std::vector<int64_t>& removed_buckets) {
    live_age_buckets =
        BuildLiveAgeBuckets(ToOffsetSeconds(std::chrono::steady_clock::now()));
    removed_buckets.clear();
    for (const auto& counter : removed_age.get_bucket_counts()) {
        removed_buckets.push_back(counter->value());
    }
}

std::vector<int64_t> KeyRetentionMetric::BuildLiveAgeBuckets(int64_t t) const {
    const std::vector<double>& boundaries = LifetimeBuckets();
    std::vector<int64_t> buckets(boundaries.size() + 1, 0);
    for (size_t i = 0; i < cohorts_.size(); ++i) {
        const int64_t count = cohorts_[i].load(std::memory_order_relaxed);
        if (count <= 0) {
            if (count < 0) {
                LOG(ERROR) << "KeyRetentionMetric: negative cohort count "
                           << count << " at slot " << i;
            }
            continue;
        }
        const int64_t lo = cohort_bounds_[i];
        // Clamp the slot's upper edge to t (no birth is newer than now) so
        // the frontier slot stays centered on the possible birth range.
        int64_t hi =
            (i + 1 < cohort_bounds_.size()) ? cohort_bounds_[i + 1] : t;
        hi = std::min(hi, std::max(t, lo + 1));
        if (hi <= lo) {
            hi = lo + 1;
        }
        const int64_t mid_age = std::max<int64_t>(0, t - (lo + hi) / 2);
        const size_t bucket = static_cast<size_t>(
            std::distance(boundaries.begin(),
                          std::lower_bound(boundaries.begin(), boundaries.end(),
                                           static_cast<double>(mid_age))));
        buckets[bucket] += count;
    }
    return buckets;
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
      tier_metric(std::make_shared<TierMetric>()),
      key_retention(std::make_shared<KeyRetentionMetric>(
          "mooncake_p2p_key_retention", labels)) {}

ClientMetricSnapshot P2PClientMetric::BuildSyncSnapshot() {
    ClientMetricSnapshot snap;

    snap.total_request = SnapshotDataMetric(total_request);
    snap.local_request = SnapshotDataMetric(local_request);
    snap.remote_request.data = SnapshotDataMetric(remote_request);
    snap.remote_request.read_retries = remote_request.read_retries.value();
    snap.remote_request.write_retries = remote_request.write_retries.value();
    snap.key_retention = key_retention->Snapshot();
    return snap;
}

void P2PClientMetric::serialize(std::string& str) {
    ClientMetric::serialize(str);
    total_request.serialize(str);
    local_request.serialize(str);
    remote_request.serialize(str);
    rollback.serialize(str);
    peer_request_metrics.serialize(str);
    tier_metric->serialize(str);
    key_retention->serialize(str);
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

    ss << "=== P2P Key Retention ===\n";
    ss << key_retention->summary_metrics();

    return ss.str();
}

}  // namespace mooncake
