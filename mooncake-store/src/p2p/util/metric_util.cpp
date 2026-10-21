#include "p2p/util/metric_util.h"

#include <algorithm>
#include <iomanip>
#include <sstream>

#include <glog/logging.h>

namespace mooncake::p2p::metric_util {
namespace {

// Renders a histogram bucket boundary for the `le` label without
// redundant trailing zeros ("1" instead of "1.000000", "2.5" unchanged)
// for cleaner Prometheus exposition.
std::string FormatBucketBoundary(double boundary) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(6) << boundary;
    std::string text = oss.str();
    if (text.find('.') != std::string::npos) {
        while (!text.empty() && text.back() == '0') {
            text.pop_back();
        }
        if (!text.empty() && text.back() == '.') {
            text.pop_back();
        }
    }
    return text;
}

}  // namespace

// Shared histogram calculations and rendering do not own client metric state.
const std::vector<double>& LifetimeBuckets() {
    static const std::vector<double> kBoundaries = {
        1, 2, 5, 10, 30, 60, 300, 600, 1800, 3600, 21600, 86400, 604800};
    return kBoundaries;
}

std::vector<int64_t> InterpolateQuantiles(
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

void SerializeBucketHistogram(std::string& str, const std::string& name,
                              const std::string& help,
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
        const std::string le = (i == boundaries.size())
                                   ? "+Inf"
                                   : FormatBucketBoundary(boundaries[i]);
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

}  // namespace mooncake::p2p::metric_util
