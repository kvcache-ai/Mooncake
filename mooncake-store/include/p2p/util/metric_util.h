#pragma once

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <sstream>
#include <map>
#include <string>
#include <vector>

#include <ylt/metric/histogram.hpp>

namespace mooncake::p2p::metric_util {

// Latency bucket boundaries are in microseconds.
const std::vector<double>& LatencyBuckets();

template <typename BucketValueFn>
std::string FormatLatencySummaryFromBuckets(
    size_t bucket_count, BucketValueFn&& bucket_value,
    const std::string& count_key = "count") {
    int64_t total_count = 0;
    for (size_t i = 0; i < bucket_count; ++i) {
        total_count += bucket_value(i);
    }
    if (bucket_count == 0 || total_count == 0) {
        return "No data";
    }

    const auto& boundaries = LatencyBuckets();
    std::stringstream ss;
    ss << count_key << "=" << total_count;

    // Keep a single sample from matching an empty bucket with target == 0.
    int64_t p95_target = std::max<int64_t>(1, (total_count * 95) / 100);
    int64_t cumulative = 0;
    double p95_bucket = 0;
    for (size_t i = 0; i < bucket_count && i < boundaries.size(); ++i) {
        cumulative += bucket_value(i);
        if (cumulative >= p95_target) {
            p95_bucket = boundaries[i];
            break;
        }
    }
    if (p95_bucket > 0) {
        ss << ", p95<" << p95_bucket << "μs";
    }

    // Max bucket: highest bucket boundary that received at least one sample.
    double max_bucket = 0;
    for (size_t i = std::min(bucket_count, boundaries.size()); i > 0; --i) {
        if (bucket_value(i - 1) > 0) {
            max_bucket = boundaries[i - 1];
            break;
        }
    }
    if (max_bucket > 0) {
        ss << ", max<" << max_bucket << "μs";
    }
    return ss.str();
}

inline std::string FormatLatencySummary(ylt::metric::histogram_t& hist) {
    auto counts = hist.get_bucket_counts();
    return FormatLatencySummaryFromBuckets(
        counts.size(), [&](size_t i) { return counts[i]->value(); });
}

// Simple stopwatch for measuring elapsed time in microseconds.
class Stopwatch {
   public:
    Stopwatch() : start_time_(std::chrono::steady_clock::now()) {}

    int64_t elapsed_us() const {
        auto now = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(
                   now - start_time_)
            .count();
    }

   private:
    std::chrono::steady_clock::time_point start_time_;
};

// Bucket boundaries (seconds) for lifetime / live-age distributions.
const std::vector<double>& LifetimeBuckets();

// Interpolates the quantiles qs (each in (0,1], sorted ascending) from
// non-cumulative bucket counts in a single pass (histogram semantics:
// bucket j counts values <= boundaries[j]).
// Returns one value per q, in input order. Empty distribution -> zeros;
// quantiles in the open +Inf bucket resolve to the largest finite boundary.
std::vector<int64_t> InterpolateQuantiles(
    const std::vector<double>& boundaries,
    const std::vector<int64_t>& bucket_counts,
    const std::vector<double>& quantiles);

// Renders a classic-format Prometheus histogram from non-cumulative
// per-bucket counts over `boundaries` (implicit +Inf bucket last).
// Used for scrape-time distributions that cannot be expressed as
// cumulative observe() histograms (e.g. the current age of live keys).
// `_sum` is estimated from bucket midpoints (+Inf resolves to the
// largest finite boundary).
void SerializeBucketHistogram(std::string& str, const std::string& name,
                              const std::string& help,
                              const std::map<std::string, std::string>& labels,
                              const std::vector<double>& boundaries,
                              const std::vector<int64_t>& bucket_counts);

}  // namespace mooncake::p2p::metric_util
