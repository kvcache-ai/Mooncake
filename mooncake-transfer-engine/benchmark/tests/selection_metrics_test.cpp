// Copyright 2026 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

#include "selection_metrics.h"

#include <cstdio>
#include <fstream>

#include <gtest/gtest.h>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

constexpr uint64_t kMiB = 1ull << 20;

TEST(RdmaSelectionMetricsTest, WritesSnapshotDeltaAsJsonl) {
    SelectionStats start;
    start.allocations = 10;
    start.single_path_allocations = 4;
    start.multi_path_allocations = 6;
    start.probe_allocations = 1;
    start.devices = {{0, true, 20, 20 * kMiB, "mlx5_0", 0},
                     {1, true, 10, 10 * kMiB, "mlx5_1", 1}};

    auto end = start;
    end.allocations += 5;
    end.single_path_allocations += 2;
    end.multi_path_allocations += 3;
    end.probe_allocations += 1;
    end.devices[0].selected_slices += 6;
    end.devices[0].selected_bytes += 6 * kMiB;
    end.devices[1].selected_slices += 4;
    end.devices[1].selected_bytes += 4 * kMiB;

    const auto report = calculateRdmaSelectionMetrics(2 * kMiB, 32, 4, "tent",
                                                      "write", start, end);
    EXPECT_EQ(report.selection.allocations, 5u);
    EXPECT_EQ(report.selection.single_path_allocations, 2u);
    EXPECT_EQ(report.selection.multi_path_allocations, 3u);
    EXPECT_EQ(report.selection.probe_allocations, 1u);
    ASSERT_EQ(report.selection.devices.size(), 2u);
    EXPECT_EQ(report.selection.devices[0].selected_bytes, 6 * kMiB);
    EXPECT_EQ(report.selection.devices[1].selected_slices, 4u);

    const std::string path = "tebench_selection_metrics_test.jsonl";
    std::remove(path.c_str());
    std::string error;
    ASSERT_TRUE(appendRdmaSelectionMetricsJsonl(path, report, &error)) << error;

    std::ifstream input(path);
    nlohmann::json record;
    ASSERT_NO_THROW(input >> record);
    EXPECT_EQ(record["schema_version"], 1);
    EXPECT_EQ(record["record_type"], "rdma_selection_metrics");
    EXPECT_EQ(record["measurement_window"], "snapshot_delta");
    EXPECT_EQ(record["selection"]["allocations"], 5);
    EXPECT_EQ(record["selection"]["devices"][0]["dev_id"], 0);
    EXPECT_EQ(record["selection"]["devices"][0]["device_name"], "mlx5_0");
    EXPECT_EQ(record["selection"]["devices"][0]["numa_node"], 0);
    EXPECT_EQ(record["selection"]["devices"][1]["numa_node"], 1);
    EXPECT_EQ(record["selection"]["devices"][1]["selected_bytes"], 4 * kMiB);
    std::remove(path.c_str());
}

}  // namespace
}  // namespace tent
}  // namespace mooncake
