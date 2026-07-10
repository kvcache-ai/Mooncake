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

#include "qos_metrics.h"

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

TEST(QosMetricsTest, SupportsBulkSamples) {
    XferMetricStats stats;
    stats.add(std::vector<double>{3.0, 1.0, 2.0});

    EXPECT_EQ(stats.count(), 3u);
    EXPECT_DOUBLE_EQ(stats.min(), 1.0);
    EXPECT_DOUBLE_EQ(stats.max(), 3.0);
    EXPECT_DOUBLE_EQ(stats.avg(), 2.0);
}

TEST(QosMetricsTest, ParsesClassContract) {
    std::vector<QosClassConfig> classes;
    std::string error;
    ASSERT_TRUE(parseQosClasses("fg:4:1000:2:12.5,bg:12:0:1", &classes, &error))
        << error;
    ASSERT_EQ(classes.size(), 2);
    EXPECT_EQ(classes[0].name, "fg");
    EXPECT_EQ(classes[0].threads, 4);
    EXPECT_EQ(classes[0].slo_us, 1000);
    EXPECT_DOUBLE_EQ(classes[0].weight, 2.0);
    ASSERT_TRUE(classes[0].isolated_throughput_gbps);
    EXPECT_DOUBLE_EQ(*classes[0].isolated_throughput_gbps, 12.5);
    EXPECT_FALSE(classes[1].isolated_throughput_gbps);
    EXPECT_TRUE(validateQosClasses(classes, 16, &error));
    EXPECT_EQ(qosClassForThread(classes, 0), 0);
    EXPECT_EQ(qosClassForThread(classes, 3), 0);
    EXPECT_EQ(qosClassForThread(classes, 4), 1);
    EXPECT_EQ(qosClassForThread(classes, 15), 1);
}

TEST(QosMetricsTest, RejectsAmbiguousOrInvalidContracts) {
    std::vector<QosClassConfig> classes;
    std::string error;
    EXPECT_FALSE(parseQosClasses("fg:1:100:1,fg:1:0:1", &classes, &error));
    EXPECT_FALSE(parseQosClasses("fg:0:100:1", &classes, &error));
    EXPECT_FALSE(parseQosClasses("fg:1::1", &classes, &error));
    EXPECT_FALSE(parseQosClasses("fg:1:-1:1", &classes, &error));
    EXPECT_FALSE(parseQosClasses("fg:1:100:0", &classes, &error));
    EXPECT_FALSE(parseQosClasses("fg:1:100:1:0", &classes, &error));
    ASSERT_TRUE(parseQosClasses("fg:1:100:1", &classes, &error));
    EXPECT_FALSE(validateQosClasses(classes, 2, &error));
}

TEST(QosMetricsTest, CalculatesSloFairnessIsolationAndUtilization) {
    std::vector<QosClassConfig> classes = {
        {"foreground", 1, 100, 2.0, 0.006},
        {"background", 1, 0, 1.0, 0.004},
    };
    std::vector<XferBenchStats> stats(2);
    stats[0].total_duration.add(1000.0);
    stats[0].transfer_duration.add(50.0);
    stats[0].transfer_duration.add(100.0);
    stats[0].transfer_duration.add(150.0);
    stats[1].total_duration.add(1000.0);
    stats[1].transfer_duration.add(100.0);
    stats[1].transfer_duration.add(100.0);

    const auto report = calculateQosMetrics(1000, 1, 2, classes, &stats, 0.01);
    ASSERT_EQ(report.classes.size(), 2);
    EXPECT_NEAR(report.aggregate_throughput_gbps, 0.005, 1e-12);
    EXPECT_NEAR(report.weighted_goodput_gbps, 0.006, 1e-12);
    EXPECT_NEAR(report.jain_fairness, 0.98, 1e-12);
    ASSERT_TRUE(report.max_isolation_leakage);
    EXPECT_NEAR(*report.max_isolation_leakage, 0.5, 1e-12);
    ASSERT_TRUE(report.total_utilization);
    EXPECT_NEAR(*report.total_utilization, 0.5, 1e-12);
    ASSERT_TRUE(report.link_capacity_gbps);
    EXPECT_NEAR(*report.link_capacity_gbps, 0.01, 1e-12);

    const auto& foreground = report.classes[0];
    EXPECT_EQ(foreground.operations, 3);
    EXPECT_NEAR(foreground.p99_us, 149.0, 1e-12);
    ASSERT_TRUE(foreground.slo_attainment);
    EXPECT_NEAR(*foreground.slo_attainment, 2.0 / 3.0, 1e-12);
    EXPECT_NEAR(foreground.goodput_gbps, 0.002, 1e-12);
    EXPECT_NEAR(foreground.weighted_goodput_gbps, 0.004, 1e-12);
    ASSERT_TRUE(foreground.isolated_throughput_gbps);
    EXPECT_NEAR(*foreground.isolated_throughput_gbps, 0.006, 1e-12);

    EXPECT_FALSE(report.classes[1].slo_attainment);
}

TEST(QosMetricsTest, UsesNullForUnavailableJsonMetrics) {
    std::vector<QosClassConfig> classes = {
        {"best_effort", 1, 0, 1.0, std::nullopt}};
    std::vector<XferBenchStats> stats(1);
    stats[0].total_duration.add(1000.0);
    stats[0].transfer_duration.add(100.0);
    const auto report = calculateQosMetrics(1000, 1, 1, classes, &stats, 0.0);

    const std::string path = "tebench_qos_metrics_test.jsonl";
    std::remove(path.c_str());
    std::string error;
    ASSERT_TRUE(appendQosMetricsJsonl(path, report, &error)) << error;

    std::ifstream input(path);
    nlohmann::json record;
    ASSERT_NO_THROW(input >> record);
    EXPECT_EQ(record["schema_version"], 1);
    EXPECT_TRUE(record["total_utilization"].is_null());
    EXPECT_TRUE(record["link_capacity_gbps"].is_null());
    EXPECT_TRUE(record["max_isolation_leakage"].is_null());
    EXPECT_TRUE(record["classes"][0]["slo_attainment"].is_null());
    EXPECT_TRUE(record["classes"][0]["isolation_leakage"].is_null());
    EXPECT_TRUE(record["classes"][0]["isolated_throughput_gbps"].is_null());
    std::remove(path.c_str());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
