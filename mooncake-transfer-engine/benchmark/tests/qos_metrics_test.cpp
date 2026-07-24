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

#include "qos_metrics_adapter.h"

#include <cstdio>
#include <fstream>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "tent/thirdparty/nlohmann/json.h"
#include "workload_config.h"

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

TEST(QosMetricsTest, ParsesJsonClassContract) {
    std::vector<QosClassConfig> classes;
    std::string error;
    ASSERT_TRUE(parseQosClassesJson(
        R"json([
          {"name":"fg","threads":4,"slo_us":1000,"weight":2,"isolated_gbps":12.5},
          {"name":"bg","threads":12,"slo_us":0,"weight":1}
        ])json",
        &classes, &error))
        << error;
    ASSERT_EQ(classes.size(), 2);
    EXPECT_EQ(classes[0].name, "fg");
    EXPECT_EQ(classes[0].threads, 4);
    EXPECT_EQ(classes[0].slo_us, 1000);
    EXPECT_DOUBLE_EQ(classes[0].weight, 2.0);
    ASSERT_TRUE(classes[0].isolated_throughput_gbps);
    EXPECT_DOUBLE_EQ(*classes[0].isolated_throughput_gbps, 12.5);
    EXPECT_FALSE(classes[1].isolated_throughput_gbps);
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

TEST(QosMetricsTest, ParsesMixedWorkloadClasses) {
    std::vector<WorkloadClassConfig> classes;
    std::string error;
    ASSERT_TRUE(parseWorkloadClassesJson(
        R"json([
          {"name":"foreground","threads":2,"block_size":4096,"batch_size":1,
           "intent_type":"foreground_get","deadline_us":250,
           "slo_us":300,"weight":4},
          {"name":"migration","threads":6,"block_size":4194304,"batch_size":2,
           "intent_type":"migration","slo_us":0,"weight":1}
        ])json",
        &classes, &error))
        << error;
    ASSERT_EQ(classes.size(), 2);
    EXPECT_EQ(classes[0].qos.name, "foreground");
    EXPECT_EQ(classes[0].block_size, 4096u);
    EXPECT_EQ(classes[0].deadline_us, 250u);
    EXPECT_EQ(classes[0].intent_type, IntentType::FOREGROUND_GET);
    EXPECT_EQ(classes[1].intent_type, IntentType::MIGRATION);
    EXPECT_TRUE(validateWorkloadClasses(classes, 8, 1UL << 30, &error))
        << error;
    const auto qos_classes = qosClassesFromWorkload(classes);
    ASSERT_EQ(qos_classes.size(), 2);
    EXPECT_EQ(qos_classes[1].threads, 6);
}

TEST(QosMetricsTest, RejectsInvalidMixedWorkloadClasses) {
    std::vector<WorkloadClassConfig> classes;
    std::string error;
    EXPECT_FALSE(parseWorkloadClassesJson(
        R"json([{"name":"fg","threads":1,"block_size":0,"batch_size":1,
                 "intent_type":"foreground_get","slo_us":100,"weight":1}])json",
        &classes, &error));
    EXPECT_FALSE(parseWorkloadClassesJson(
        R"json([{"name":"fg","threads":1,"block_size":4096,"batch_size":1,
                 "intent_type":"unknown","slo_us":100,"weight":1}])json",
        &classes, &error));
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

    const auto report =
        calculateQosMetricsFromBenchStats(1000, 1, 2, classes, &stats, 0.01);
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

TEST(QosMetricsTest, UsesPerClassTransferredBytes) {
    std::vector<QosClassConfig> classes = {
        {"foreground", 1, 0, 1.0, std::nullopt},
        {"migration", 1, 0, 1.0, std::nullopt},
    };
    std::vector<XferBenchStats> stats(2);
    for (auto& class_stats : stats) {
        class_stats.total_duration.add(1000.0);
        class_stats.transfer_duration.add(100.0);
    }

    const auto report = calculateQosMetricsFromBenchStats(
        0, 0, 2, classes, &stats, 0.0, {4096, 4UL << 20});
    ASSERT_EQ(report.classes.size(), 2);
    EXPECT_EQ(report.classes[0].transferred_bytes, 4096u);
    EXPECT_EQ(report.classes[1].transferred_bytes, 4UL << 20);
    EXPECT_NEAR(report.classes[0].throughput_gbps, 0.004096, 1e-12);
    EXPECT_NEAR(report.classes[1].throughput_gbps, 4.194304, 1e-12);
}

TEST(QosMetricsTest, UsesNullForUnavailableJsonMetrics) {
    std::vector<QosClassConfig> classes = {
        {"best_effort", 1, 0, 1.0, std::nullopt}};
    std::vector<XferBenchStats> stats(1);
    stats[0].total_duration.add(1000.0);
    stats[0].transfer_duration.add(100.0);
    const auto report =
        calculateQosMetricsFromBenchStats(1000, 1, 1, classes, &stats, 0.0);

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
