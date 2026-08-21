#include "client_metrics_aggregator.h"

#include <glog/logging.h>

#include <iomanip>
#include <sstream>

#include "p2p_client_metric.h"
#include "utils.h"

namespace mooncake {

namespace {

const char* kBatchGranularityHelp =
    "at request (batch) granularity: one BatchGet/BatchPut counts as one "
    "request";

const char* kLocalGranularityHelp =
    "at per-operation granularity: each individual local op contributes its "
    "own sample; local + remote op counts may exceed batch-level request "
    "counts due to multi-replica reads or retries";

const char* kRemoteGranularityHelp =
    "at per-operation granularity: each individual remote op contributes its "
    "own sample; local + remote op counts may exceed batch-level request "
    "counts due to multi-replica reads or retries";

const ClientMetricSnapshot kZeroSnapshot{};

}  // namespace

ClientMetricsAggregator::CounterGroup::CounterGroup(
    const std::string& prefix, const std::string& granularity_help)
    : get_requests(prefix + "_get_requests",
                   "Cluster-wide get requests " + granularity_help),
      get_hits(prefix + "_get_hits",
               "Cluster-wide get hits (keys found) " + granularity_help),
      get_misses(
          prefix + "_get_misses",
          "Cluster-wide get misses (keys not found) " + granularity_help),
      get_failures(prefix + "_get_failures",
                   "Cluster-wide failed gets " + granularity_help),
      get_bytes(prefix + "_get_bytes",
                "Cluster-wide bytes read by Get " + granularity_help),
      put_requests(prefix + "_put_requests",
                   "Cluster-wide put requests " + granularity_help),
      put_failures(prefix + "_put_failures",
                   "Cluster-wide failed puts " + granularity_help),
      put_bytes(prefix + "_put_bytes",
                "Cluster-wide bytes written by Put " + granularity_help) {
    // Mark every counter changed once so zero values are serialized.
    get_requests.inc(0);
    get_hits.inc(0);
    get_misses.inc(0);
    get_failures.inc(0);
    get_bytes.inc(0);
    put_requests.inc(0);
    put_failures.inc(0);
    put_bytes.inc(0);
}

ClientMetricsAggregator::ClientMetricsAggregator()
    : total_("master_cluster_total", kBatchGranularityHelp),
      local_("master_cluster_local", kLocalGranularityHelp),
      remote_("master_cluster_remote", kRemoteGranularityHelp),
      remote_read_retries_(
          "master_cluster_remote_read_retries",
          "Cluster-wide remote read retries (route-based remote flow)"),
      remote_write_retries_(
          "master_cluster_remote_write_retries",
          "Cluster-wide remote write retries (route-based remote flow)"),
      key_live_count_(
          "master_cluster_key_retention_live_count",
          "Cluster-wide number of keys currently retained on clients"),
      key_removed_count_("master_cluster_key_retention_removed_count",
                         "Cluster-wide number of keys removed from clients;"
                         "gauge summed from client-reported cumulative counts, "
                         "may decrease when clients restart or rejoin") {
    remote_read_retries_.inc(0);
    remote_write_retries_.inc(0);
    // Mark retention gauges changed once so zero values are serialized.
    key_live_count_.inc(0);
    key_removed_count_.inc(0);
    key_live_age_buckets_.assign(
        KeyRetentionMetric::LifetimeBuckets().size() + 1, 0);
    key_removed_age_buckets_.assign(
        KeyRetentionMetric::LifetimeBuckets().size() + 1, 0);
}

void ClientMetricsAggregator::Update(const UUID& client_id,
                                     const ClientMetricSnapshot& snapshot) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = client_snapshots_.find(client_id);
    const ClientMetricSnapshot& old_v =
        (it != client_snapshots_.end()) ? it->second : kZeroSnapshot;
    ApplyDataMetricDelta(old_v.total_request, snapshot.total_request, total_);
    ApplyDataMetricDelta(old_v.local_request, snapshot.local_request, local_);
    ApplyDataMetricDelta(old_v.remote_request.data,
                         snapshot.remote_request.data, remote_);
    ApplyDelta(snapshot.remote_request.read_retries -
                   old_v.remote_request.read_retries,
               remote_read_retries_);
    ApplyDelta(snapshot.remote_request.write_retries -
                   old_v.remote_request.write_retries,
               remote_write_retries_);
    client_snapshots_[client_id] = snapshot;
    RefreshRetentionAggregates();
}

void ClientMetricsAggregator::ApplyDataMetricDelta(
    const DataMetricSnapshot& old_v, const DataMetricSnapshot& new_v,
    CounterGroup& group) {
    ApplyDelta(new_v.get_requests - old_v.get_requests, group.get_requests);
    ApplyDelta(new_v.get_hits - old_v.get_hits, group.get_hits);
    ApplyDelta(new_v.get_misses - old_v.get_misses, group.get_misses);
    ApplyDelta(new_v.get_failures - old_v.get_failures, group.get_failures);
    ApplyDelta(new_v.get_bytes - old_v.get_bytes, group.get_bytes);

    ApplyDelta(new_v.put_requests - old_v.put_requests, group.put_requests);
    ApplyDelta(new_v.put_failures - old_v.put_failures, group.put_failures);
    ApplyDelta(new_v.put_bytes - old_v.put_bytes, group.put_bytes);
}

void ClientMetricsAggregator::OnClientRemoved(const UUID& client_id) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto it = client_snapshots_.find(client_id);
    if (it == client_snapshots_.end()) {
        return;
    }

    // Subtract the client's last reported values.
    const ClientMetricSnapshot& snap = it->second;
    ApplyDataMetricDelta(snap.total_request, kZeroSnapshot.total_request,
                         total_);
    ApplyDataMetricDelta(snap.local_request, kZeroSnapshot.local_request,
                         local_);
    ApplyDataMetricDelta(snap.remote_request.data,
                         kZeroSnapshot.remote_request.data, remote_);
    ApplyDelta(-snap.remote_request.read_retries, remote_read_retries_);
    ApplyDelta(-snap.remote_request.write_retries, remote_write_retries_);
    client_snapshots_.erase(it);
    RefreshRetentionAggregates();
}

void ClientMetricsAggregator::RefreshRetentionAggregates() {
    const size_t num_buckets = KeyRetentionMetric::LifetimeBuckets().size() + 1;

    int64_t live_sum = 0;
    int64_t removed_sum = 0;
    key_live_age_buckets_.assign(num_buckets, 0);
    key_removed_age_buckets_.assign(num_buckets, 0);

    auto accumulate = [&](std::vector<int64_t>& dst,
                          const std::vector<int64_t>& src,
                          const UUID& client_id, const char* what) {
        if (src.empty()) {
            return;  // client does not report retention data
        }
        if (src.size() != num_buckets) {
            LOG(ERROR) << "ClientMetricsAggregator: key retention " << what
                       << " bucket count mismatch, client_id=" << client_id
                       << ", expected=" << num_buckets << ", got=" << src.size()
                       << "; treated as zero contribution";
            return;
        }
        for (size_t i = 0; i < num_buckets; ++i) {
            dst[i] += src[i];
        }
    };

    for (const auto& [client_id, snap] : client_snapshots_) {
        const KeyRetentionSnapshot& r = snap.key_retention;
        live_sum += r.live_count;
        removed_sum += r.removed_total;
        accumulate(key_live_age_buckets_, r.live_age_buckets, client_id,
                   "live_age");
        accumulate(key_removed_age_buckets_, r.removed_buckets, client_id,
                   "removed");
    }

    key_live_count_.update(live_sum);
    key_removed_count_.update(removed_sum);
}

void ClientMetricsAggregator::ApplyDelta(int64_t delta,
                                         ylt::metric::gauge_t& gauge) {
    if (delta >= 0) {
        gauge.inc(delta);
    } else {
        gauge.dec(-delta);
    }
}

void ClientMetricsAggregator::Serialize(std::string& out) {
    std::lock_guard<std::mutex> lock(mutex_);

    auto serialize_metric = [&out](auto& metric) {
        std::string metric_str;
        metric.serialize(metric_str);
        out += metric_str;
    };

    serialize_metric(total_.get_requests);
    serialize_metric(total_.get_hits);
    serialize_metric(total_.get_misses);
    serialize_metric(total_.get_failures);
    serialize_metric(total_.get_bytes);
    serialize_metric(total_.put_requests);
    serialize_metric(total_.put_failures);
    serialize_metric(total_.put_bytes);

    serialize_metric(local_.get_requests);
    serialize_metric(local_.get_hits);
    serialize_metric(local_.get_misses);
    serialize_metric(local_.get_failures);
    serialize_metric(local_.get_bytes);
    serialize_metric(local_.put_requests);
    serialize_metric(local_.put_failures);
    serialize_metric(local_.put_bytes);

    serialize_metric(remote_.get_requests);
    serialize_metric(remote_.get_hits);
    serialize_metric(remote_.get_misses);
    serialize_metric(remote_.get_failures);
    serialize_metric(remote_.get_bytes);
    serialize_metric(remote_.put_requests);
    serialize_metric(remote_.put_failures);
    serialize_metric(remote_.put_bytes);

    serialize_metric(remote_read_retries_);
    serialize_metric(remote_write_retries_);

    serialize_metric(key_live_count_);
    serialize_metric(key_removed_count_);
    // Merged retention distributions, rendered as scrape-time histograms
    // (quantiles via histogram_quantile() at query time).
    KeyRetentionMetric::SerializeBucketHistogram(
        out, "master_cluster_key_retention_live_age_seconds",
        "Cluster-wide current age distribution of live keys on clients "
        "(seconds; approximate, merged from per-client birth cohorts; sum "
        "estimated from bucket midpoints)",
        {}, KeyRetentionMetric::LifetimeBuckets(), key_live_age_buckets_);
    KeyRetentionMetric::SerializeBucketHistogram(
        out, "master_cluster_key_retention_removed_age_seconds",
        "Cluster-wide lifetime distribution of removed keys on clients "
        "(seconds; sum estimated from bucket midpoints)",
        {}, KeyRetentionMetric::LifetimeBuckets(), key_removed_age_buckets_);
    std::vector<int64_t> all_buckets(key_live_age_buckets_.size(), 0);
    for (size_t i = 0;
         i < all_buckets.size() && i < key_removed_age_buckets_.size(); ++i) {
        all_buckets[i] = key_live_age_buckets_[i] + key_removed_age_buckets_[i];
    }
    KeyRetentionMetric::SerializeBucketHistogram(
        out, "master_cluster_key_retention_all_lifetime_seconds",
        "Cluster-wide lifetime distribution of all keys seen by clients "
        "(seconds): live keys censored at current age + removed keys' "
        "exact lifetime; sum estimated from bucket midpoints)",
        {}, KeyRetentionMetric::LifetimeBuckets(), all_buckets);
}

std::string ClientMetricsAggregator::Summary() {
    std::lock_guard<std::mutex> lock(mutex_);

    std::ostringstream ss;
    ss << "\n Cluster Data Plane: ";

    auto append_get = [&ss](const char* label, CounterGroup& group) {
        ss << label << ": requests=" << group.get_requests.value()
           << ", hits=" << group.get_hits.value()
           << ", misses=" << group.get_misses.value()
           << ", failures=" << group.get_failures.value()
           << ", bytes=" << byte_size_to_string(group.get_bytes.value());
    };
    auto append_put = [&ss](const char* label, CounterGroup& group) {
        ss << label << ": requests=" << group.put_requests.value()
           << ", failures=" << group.put_failures.value()
           << ", bytes=" << byte_size_to_string(group.put_bytes.value());
    };

    append_get("Get(total)", total_);
    const int64_t hit_denominator =
        total_.get_hits.value() + total_.get_misses.value();
    if (hit_denominator > 0) {
        std::ostringstream rate;
        rate << std::fixed << std::setprecision(1)
             << (100.0 * total_.get_hits.value() / hit_denominator);
        ss << " (hit_rate=" << rate.str() << "%)";
    }
    ss << " | ";
    append_get("Get(local)", local_);
    ss << " | ";
    append_get("Get(remote)", remote_);
    ss << " | ";
    append_put("Put(total)", total_);
    ss << " | ";
    append_put("Put(local)", local_);
    ss << " | ";
    append_put("Put(remote)", remote_);
    ss << " | Retries: read=" << remote_read_retries_.value()
       << ", write=" << remote_write_retries_.value();

    const std::vector<double> kQuantiles = {0.30, 0.50, 0.80, 0.95};
    const std::vector<double>& lifetime_buckets =
        KeyRetentionMetric::LifetimeBuckets();
    std::vector<int64_t> all_buckets(key_live_age_buckets_.size(), 0);
    for (size_t i = 0;
         i < all_buckets.size() && i < key_removed_age_buckets_.size(); ++i) {
        all_buckets[i] = key_live_age_buckets_[i] + key_removed_age_buckets_[i];
    }
    const std::vector<int64_t> live_q =
        KeyRetentionMetric::InterpolateQuantiles(
            lifetime_buckets, key_live_age_buckets_, kQuantiles);
    const std::vector<int64_t> removed_q =
        KeyRetentionMetric::InterpolateQuantiles(
            lifetime_buckets, key_removed_age_buckets_, kQuantiles);
    const std::vector<int64_t> all_q = KeyRetentionMetric::InterpolateQuantiles(
        lifetime_buckets, all_buckets, kQuantiles);
    ss << " | Retention: live=" << key_live_count_.value()
       << ", removed=" << key_removed_count_.value()
       << ", live_age p50=" << live_q[1] << "s, p95=" << live_q[3]
       << "s, removed_age p50=" << removed_q[1] << "s, p95=" << removed_q[3]
       << "s, all_lifetime p50=" << all_q[1] << "s, p95=" << all_q[3] << "s";
    return ss.str();
}

}  // namespace mooncake
