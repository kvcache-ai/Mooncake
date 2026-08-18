#include "client_metrics_aggregator.h"

#include <iomanip>
#include <sstream>

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
          "Cluster-wide remote write retries (route-based remote flow)") {
    remote_read_retries_.inc(0);
    remote_write_retries_.inc(0);
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
    return ss.str();
}

}  // namespace mooncake
