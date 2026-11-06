// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "utils.h"

#include <algorithm>
#include <random>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace tent {
namespace {

// The percentile algorithm before the sort cache: copy, sort, interpolate.
double referencePercentile(const std::vector<double>& samples, double p) {
    if (samples.empty()) return 0.0;
    if (p <= 0) return *std::min_element(samples.begin(), samples.end());
    if (p >= 100) return *std::max_element(samples.begin(), samples.end());
    std::vector<double> sorted = samples;
    std::sort(sorted.begin(), sorted.end());
    double rank = (p / 100.0) * (sorted.size() - 1);
    size_t idx = static_cast<size_t>(rank);
    double frac = rank - idx;
    if (idx + 1 < sorted.size()) {
        return sorted[idx] * (1.0 - frac) + sorted[idx + 1] * frac;
    } else {
        return sorted[idx];
    }
}

const double kPercentiles[] = {-1.0, 0.0,  0.1,  1.0,   25.0,  50.0, 90.0,
                               95.0, 99.0, 99.9, 99.99, 100.0, 150.0};

void expectMatchesReference(const XferMetricStats& stats,
                            const std::vector<double>& samples) {
    ASSERT_EQ(stats.count(), samples.size());
    for (double p : kPercentiles) {
        // Bitwise-equal results, not merely close ones.
        EXPECT_EQ(stats.percentile(p), referencePercentile(samples, p))
            << "p=" << p << " n=" << samples.size();
    }
    EXPECT_EQ(stats.p90(), referencePercentile(samples, 90.0));
    EXPECT_EQ(stats.p95(), referencePercentile(samples, 95.0));
    EXPECT_EQ(stats.p99(), referencePercentile(samples, 99.0));
    EXPECT_EQ(stats.p999(), referencePercentile(samples, 99.9));
}

TEST(XferMetricStatsTest, PercentileMatchesReferenceForSmallSets) {
    const std::vector<std::vector<double>> cases = {
        {},
        {7.0},
        {3.0, 1.0},
        {5.0, 5.0, 5.0},
        {2.0, 9.0, 2.0, 9.0, 4.5},
        {-1.5, 0.0, 1e9, 3.25},
    };
    for (const auto& samples : cases) {
        XferMetricStats stats;
        stats.add(samples);
        expectMatchesReference(stats, samples);
        // A second pass reads the cached sort.
        expectMatchesReference(stats, samples);
    }
}

TEST(XferMetricStatsTest, PercentileMatchesReferenceForRandomSets) {
    std::mt19937_64 rng(5119);
    std::uniform_int_distribution<size_t> size_dist(0, 2000);
    std::uniform_real_distribution<double> value_dist(0.0, 1e4);
    std::uniform_int_distribution<int> coarse_dist(0, 16);
    for (int round = 0; round < 50; ++round) {
        const size_t n = size_dist(rng);
        // Odd rounds use few distinct values so duplicates are common.
        const bool coarse = round % 2 == 1;
        std::vector<double> samples;
        samples.reserve(n);
        for (size_t i = 0; i < n; ++i) {
            samples.push_back(coarse ? coarse_dist(rng) : value_dist(rng));
        }
        XferMetricStats stats;
        stats.add(samples);
        expectMatchesReference(stats, samples);
    }
}

TEST(XferMetricStatsTest, AddInvalidatesSortCache) {
    std::mt19937_64 rng(42);
    std::uniform_real_distribution<double> value_dist(0.0, 100.0);
    XferMetricStats stats;
    std::vector<double> samples;
    expectMatchesReference(stats, samples);

    for (int round = 0; round < 20; ++round) {
        // Alternate the single-value and the bulk add() overloads.
        if (round % 2 == 0) {
            const double value = value_dist(rng);
            stats.add(value);
            samples.push_back(value);
        } else {
            std::vector<double> batch(round * 7);
            for (auto& value : batch) value = value_dist(rng);
            stats.add(batch);
            samples.insert(samples.end(), batch.begin(), batch.end());
        }
        expectMatchesReference(stats, samples);
    }

    // Values below the cached minimum and above the cached maximum.
    stats.add(-1.0);
    samples.push_back(-1.0);
    stats.add(1000.0);
    samples.push_back(1000.0);
    expectMatchesReference(stats, samples);

    stats.clear();
    samples.clear();
    expectMatchesReference(stats, samples);
    stats.add(std::vector<double>{4.0, 2.0});
    samples = {4.0, 2.0};
    expectMatchesReference(stats, samples);
}

TEST(XferMetricStatsTest, CopyKeepsIndependentCache) {
    XferMetricStats stats;
    stats.add(std::vector<double>{1.0, 2.0, 3.0});
    EXPECT_EQ(stats.p99(), referencePercentile({1.0, 2.0, 3.0}, 99.0));
    XferMetricStats copy = stats;
    copy.add(100.0);
    EXPECT_EQ(copy.p99(), referencePercentile({1.0, 2.0, 3.0, 100.0}, 99.0));
    EXPECT_EQ(stats.p99(), referencePercentile({1.0, 2.0, 3.0}, 99.0));
}

TEST(PrintStatsTest, EmptyRunPrintsZerosInsteadOfInfOrNan) {
    XferBenchStats stats;
    testing::internal::CaptureStdout();
    printStats(4096, 1, stats, 4);
    const std::string empty_out = testing::internal::GetCapturedStdout();
    EXPECT_EQ(empty_out.find("inf"), std::string::npos) << empty_out;
    EXPECT_EQ(empty_out.find("nan"), std::string::npos) << empty_out;
    EXPECT_NE(empty_out.find("4096"), std::string::npos) << empty_out;

    // Threads ran for the duration but completed no operation.
    stats.total_duration.add(1e6);
    testing::internal::CaptureStdout();
    printStats(4096, 1, stats, 4);
    const std::string no_ops_out = testing::internal::GetCapturedStdout();
    EXPECT_EQ(no_ops_out.find("inf"), std::string::npos) << no_ops_out;
    EXPECT_EQ(no_ops_out.find("nan"), std::string::npos) << no_ops_out;
}

TEST(PrintStatsTest, NonEmptyRunKeepsFormula) {
    XferBenchStats stats;
    stats.total_duration.add(std::vector<double>{1e6, 1e6});
    stats.transfer_duration.add(std::vector<double>{10.0, 30.0});
    stats.instant_bandwidth.add(std::vector<double>{0.5, 1.5});
    testing::internal::CaptureStdout();
    printStats(1000000, 2, stats, 2);
    const std::string out = testing::internal::GetCapturedStdout();
    // 2 ops x 2 MB over 1 s = 0.004 GB/s; avg latency 1e6 * 2 / 2 us.
    EXPECT_NE(out.find("0.004000"), std::string::npos) << out;
    EXPECT_NE(out.find("1.000000"), std::string::npos) << out;
    EXPECT_NE(out.find("1000000.0"), std::string::npos) << out;
    EXPECT_NE(out.find("20.0"), std::string::npos) << out;
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
