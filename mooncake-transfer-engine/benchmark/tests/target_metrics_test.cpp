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

#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sys/resource.h>
#include <unistd.h>

#include <gtest/gtest.h>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

TEST(TargetMetricsDeathTest, ReportsBufferedWriteFailure) {
    std::string path = testing::TempDir() + "tebench-target-jsonl-XXXXXX";
    const int fd = mkstemp(path.data());
    ASSERT_GE(fd, 0);
    close(fd);

    // Limit only the child: opening succeeds, but even a buffered write fails
    // when the stream is flushed or closed.
    EXPECT_EXIT(
        {
            if (std::signal(SIGXFSZ, SIG_IGN) == SIG_ERR) std::_Exit(2);
            struct rlimit limit{};
            if (setrlimit(RLIMIT_FSIZE, &limit) != 0) std::_Exit(3);
            TargetMetricsReport report;
            std::string error;
            const bool ok = appendTargetMetricsJsonl(path, report, &error);
            std::_Exit(!ok && error == "failed to write target JSONL output: " +
                                           path
                           ? 0
                           : 1);
        },
        testing::ExitedWithCode(0), "");
    std::remove(path.c_str());
}

TEST(TargetMetricsTest, ReportsOpenFailure) {
    TargetMetricsReport report;
    std::string error;
    const auto path = testing::TempDir();
    EXPECT_FALSE(appendTargetMetricsJsonl(path, report, &error));
    EXPECT_EQ(error, "failed to open target JSONL output: " + path);
}

TEST(TargetMetricsTest, NonConstantPayloadOnlyForHpTcpConsistencyChecks) {
    const auto saved_transport = XferBenchConfig::xport_type;
    const bool saved_check = XferBenchConfig::check_consistency;
    std::vector<uint8_t> data(4096);
    for (const auto* transport : {"hp_tcp", "tcp"}) {
        XferBenchConfig::xport_type = transport;
        for (bool check : {false, true}) {
            XferBenchConfig::check_consistency = check;
            fillData(data.data(), data.size(), 37);
            const bool constant = std::all_of(
                data.begin(), data.end(), [](uint8_t b) { return b == 37; });
            EXPECT_EQ(constant,
                      !(check && XferBenchConfig::xport_type == "hp_tcp"));
            verifyData(data.data(), data.size(), 37);
        }
    }
    XferBenchConfig::xport_type = saved_transport;
    XferBenchConfig::check_consistency = saved_check;
}

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
    auto next_report = report;
    next_report.batch_size = 3;
    ASSERT_TRUE(appendTargetMetricsJsonl(path, next_report, &error)) << error;
    std::ifstream input(path);
    nlohmann::json record;
    ASSERT_NO_THROW(input >> record);
    EXPECT_EQ(record["schema_version"], 1);
    EXPECT_EQ(record["record_type"], "target_metrics");
    ASSERT_EQ(record["targets"].size(), 2u);
    EXPECT_EQ(record["targets"][0]["segment_name"], "target-a");
    EXPECT_EQ(record["targets"][1]["operations"], 0);
    EXPECT_EQ(record["batch_size"], 2);
    ASSERT_NO_THROW(input >> record);
    EXPECT_EQ(record["batch_size"], 3);
    std::remove(path.c_str());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
