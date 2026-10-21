#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace mooncake::p2p::metric_util {

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
