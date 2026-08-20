#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <ylt/metric/gauge.hpp>

#include "client_metric.h"
#include "heartbeat_type.h"
#include "types.h"

namespace mooncake {

class CacheTier;  // Defined in tiered_cache/tiers/cache_tier.h

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

// Per-tier storage metrics (key count / capacity / current usage).
struct TierMetric {
    ylt::metric::dynamic_gauge_1t key_count{
        "mooncake_p2p_tier_key_count",
        "Number of committed keys (replicas) on each tier",
        {"tier"}};
    ylt::metric::dynamic_gauge_1t capacity_bytes{
        "mooncake_p2p_tier_capacity_bytes",
        "Total storage capacity of each tier in bytes",
        {"tier"}};
    ylt::metric::dynamic_gauge_1t used_bytes{
        "mooncake_p2p_tier_used_bytes",
        "Currently used bytes of each tier",
        {"tier"}};

    // Scheduler-driven key-movement counters
    ylt::metric::dynamic_counter_1t evicted_keys{
        "mooncake_p2p_tier_evicted_keys_total",
        "Keys whose replica on each tier was evicted by the scheduler",
        {"tier"}};
    ylt::metric::dynamic_counter_1t offloaded_keys{
        "mooncake_p2p_tier_offloaded_keys_total",
        "Keys moved from each tier down to a lower-priority tier",
        {"tier"}};
    ylt::metric::dynamic_counter_1t onboarded_keys{
        "mooncake_p2p_tier_onboarded_keys_total",
        "Keys moved from each tier up to a higher-priority tier",
        {"tier"}};

    // NOT thread-safe
    void RegisterTier(const UUID& tier_id, const std::string& label,
                      const std::shared_ptr<CacheTier>& tier, int priority);

    void OnReplicaAdded(const UUID& tier_id);
    void OnReplicaRemoved(const UUID& tier_id);

    void OnEvicted(const UUID& tier_id);
    // OnMoved classifies by priority: down = offload, up = onboard, both
    // counted on the source tier.
    void OnMoved(const UUID& source_tier, const UUID& dest_tier);

    void serialize(std::string& str);
    std::string summary_metrics();

   private:
    struct TierEntry {
        std::array<std::string, 1> label_array;  // cached gauge label value
        MemoryType memory_type = MemoryType::UNKNOWN;
        int priority = 0;
        size_t capacity = 0;
        std::weak_ptr<CacheTier> tier;
    };

    // Polls CacheTier::GetUsage() of every live tier into used_bytes. Only
    // called from serialize()/summary_metrics(), never from the data path.
    void RefreshUsage();

    std::unordered_map<UUID, TierEntry> tiers_;
};

// record how long keys live on this client
struct KeyRetentionMetric {
   public:
    explicit KeyRetentionMetric(
        const std::string& prefix,
        const std::map<std::string, std::string>& labels = {},
        std::chrono::steady_clock::time_point anchor =
            std::chrono::steady_clock::now());

    // Data path hooks: lock-free, O(1).
    void OnKeyCreated(std::chrono::steady_clock::time_point birth);
    void OnKeyRemoved(std::chrono::steady_clock::time_point birth);

    KeyRetentionSnapshot Snapshot();
    void serialize(std::string& str);
    std::string summary_metrics();

    // Bucket boundaries (seconds) for lifetime / live-age distributions.
    static const std::vector<double>& LifetimeBuckets();

    // Interpolates the quantiles qs (each in (0,1], sorted ascending) from
    // non-cumulative bucket counts in a single pass (histogram semantics:
    // bucket j counts values <= boundaries[j]).
    // Returns one value per q, in input order. Empty distribution -> zeros;
    // quantiles in the open +Inf bucket resolve to the largest finite boundary.
    static std::vector<int64_t> InterpolateQuantiles(
        const std::vector<double>& boundaries,
        const std::vector<int64_t>& bucket_counts,
        const std::vector<double>& quantiles);

    // Renders a classic-format Prometheus histogram from non-cumulative
    // per-bucket counts over `boundaries` (implicit +Inf bucket last).
    // Used for scrape-time distributions that cannot be expressed as
    // cumulative observe() histograms (e.g. the current age of live keys).
    // `_sum` is estimated from bucket midpoints (+Inf resolves to the
    // largest finite boundary).
    static void SerializeBucketHistogram(
        std::string& str, const std::string& name, const std::string& help,
        const std::map<std::string, std::string>& labels,
        const std::vector<double>& boundaries,
        const std::vector<int64_t>& bucket_counts);

   private:
    void CollectBuckets(std::vector<int64_t>& live_age_buckets,
                        std::vector<int64_t>& removed_buckets);
    std::vector<int64_t> BuildLiveAgeBuckets(int64_t t) const;
    int64_t ToOffsetSeconds(std::chrono::steady_clock::time_point tp) const;
    size_t CohortIndex(int64_t birth_offset_seconds) const;

   private:
    const std::chrono::steady_clock::time_point anchor_;
    // Retained to render the scrape-time live-age histogram.
    const std::string prefix_;
    const std::map<std::string, std::string> labels_;

    ylt::metric::gauge_t live_keys;
    ylt::metric::counter_t removed_keys;
    ylt::metric::histogram_t removed_age;

    // Birth cohorts for the live-age distribution. cohorts_[i] counts
    // live keys whose birth offset (seconds since anchor_) falls in
    // [cohort_bounds_[i], cohort_bounds_[i+1]); the last slot extends to
    // +inf (births beyond kCohortCoverageSeconds clamp into it).
    // At scrape time a slot's keys are all estimated at "now - midpoint(slot)",
    // so the error is at most one slot width.
    // Example: a key born 100s after anchor lands in slot [81, 106);
    // at t=1000s its age is estimated as 1000 - 93 = 907s (true: 900s).
    static constexpr int64_t kCohortCoverageSeconds = 365 * 24 * 3600;
    std::vector<int64_t> cohort_bounds_;
    std::vector<std::atomic<int64_t>> cohorts_;
};

struct P2PClientMetric : public ClientMetric {
   public:
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
    // Per-tier storage metrics; initially empty. Shared with TieredBackend.
    std::shared_ptr<TierMetric> tier_metric;
    std::shared_ptr<KeyRetentionMetric> key_retention;

   public:
    static std::unique_ptr<P2PClientMetric> Create(
        const std::map<std::string, std::string>& labels = {}) {
        return CreatePtr<P2PClientMetric>(labels);
    }

    explicit P2PClientMetric(
        uint64_t interval_seconds = 0,
        const std::map<std::string, std::string>& labels = {});

    ClientMetricSnapshot BuildSyncSnapshot();

    void serialize(std::string& str) override;
    std::string summary_metrics() override;
};

}  // namespace mooncake
