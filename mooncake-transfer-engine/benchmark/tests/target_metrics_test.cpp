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

#include "target_metrics.h"

#include <cstdio>
#include <fstream>

#include <gtest/gtest.h>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

TEST(TargetMetricsTest, ReportsEachTargetAndWritesJsonl) {
    std::vector<TargetBenchStats> stats(2);
    stats[0].segment_name = "target-a";
    stats[0].threads = 2;
    stats[0].transferred_bytes = 6000;
    stats[0].stats.total_duration.add(1000.0);
    stats[0].stats.total_duration.add(1000.0);
    stats[0].stats.transfer_duration.add({10.0, 20.0, 30.0});
    stats[0].stats.instant_bandwidth.add({0.1, 0.2, 0.3});
    stats[1].segment_name = "target-b";

    const auto report =
        calculateTargetMetrics(1000, 2, 2, "tent", "read", &stats);
    ASSERT_EQ(report.targets.size(), 2u);
    EXPECT_EQ(report.aggregate_operations, 3u);
    EXPECT_EQ(report.aggregate_transferred_bytes, 6000u);
    EXPECT_NEAR(report.aggregate_throughput_gbps, 0.006, 1e-12);
    EXPECT_EQ(report.targets[0].threads, 2);
    EXPECT_NEAR(report.targets[0].avg_latency_us, 2000.0 / 3.0, 1e-12);
    EXPECT_DOUBLE_EQ(report.targets[1].throughput_gbps, 0.0);

    const std::string path = "tebench_target_metrics_test.jsonl";
    std::remove(path.c_str());
    std::string error;
    ASSERT_TRUE(appendTargetMetricsJsonl(path, report, &error)) << error;
    std::ifstream input(path);
    nlohmann::json record;
    ASSERT_NO_THROW(input >> record);
    EXPECT_EQ(record["schema_version"], 1);
    EXPECT_EQ(record["record_type"], "target_metrics");
    ASSERT_EQ(record["targets"].size(), 2u);
    EXPECT_EQ(record["targets"][0]["segment_name"], "target-a");
    EXPECT_EQ(record["targets"][1]["operations"], 0);
    std::remove(path.c_str());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
