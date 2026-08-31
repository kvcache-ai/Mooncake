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

#include <fstream>
#include <iomanip>
#include <iostream>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {

TargetMetricsReport calculateTargetMetrics(
    size_t block_size, size_t batch_size, int num_threads,
    const std::string& backend, const std::string& op_type,
    std::vector<TargetBenchStats>* stats) {
    TargetMetricsReport report;
    report.block_size = block_size;
    report.batch_size = batch_size;
    report.num_threads = num_threads;
    report.backend = backend;
    report.op_type = op_type;
    report.targets.reserve(stats->size());
    double aggregate_duration_sum_us = 0.0;
    int assigned_threads = 0;

    for (size_t i = 0; i < stats->size(); ++i) {
        auto& input = (*stats)[i];
        TargetMetrics metrics;
        metrics.index = i;
        metrics.segment_name = input.segment_name;
        metrics.threads = input.threads;
        metrics.operations = input.stats.transfer_duration.count();
        metrics.transferred_bytes = input.transferred_bytes;
        metrics.total_duration_us = input.stats.total_duration.avg();
        metrics.avg_transfer_us = input.stats.transfer_duration.avg();
        metrics.p99_us = input.stats.transfer_duration.p99();
        metrics.p999_us = input.stats.transfer_duration.p999();
        metrics.avg_instant_gbps = input.stats.instant_bandwidth.avg();
        if (metrics.operations != 0) {
            metrics.avg_latency_us = metrics.total_duration_us *
                                     metrics.threads / metrics.operations;
        }
        if (metrics.total_duration_us > 0.0) {
            metrics.throughput_gbps =
                static_cast<double>(metrics.transferred_bytes) / 1000.0 /
                metrics.total_duration_us;
        }
        report.aggregate_operations =
            checkedAdd(report.aggregate_operations, metrics.operations,
                       "aggregate operations");
        report.aggregate_transferred_bytes = checkedAdd(
            report.aggregate_transferred_bytes, metrics.transferred_bytes,
            "aggregate transferred bytes");
        aggregate_duration_sum_us +=
            metrics.total_duration_us * metrics.threads;
        assigned_threads += metrics.threads;
        report.targets.push_back(std::move(metrics));
    }
    if (assigned_threads > 0 && aggregate_duration_sum_us > 0.0) {
        const double aggregate_duration_us =
            aggregate_duration_sum_us / assigned_threads;
        report.aggregate_throughput_gbps =
            static_cast<double>(report.aggregate_transferred_bytes) / 1000.0 /
            aggregate_duration_us;
    }
    return report;
}

void printTargetMetrics(const TargetMetricsReport& report) {
    for (const auto& metrics : report.targets) {
        std::cout << "  [target-summary] index=" << metrics.index
                  << " name=" << metrics.segment_name
                  << " threads=" << metrics.threads
                  << " operations=" << metrics.operations
                  << " transferred_bytes=" << metrics.transferred_bytes
                  << " throughput=" << std::fixed << std::setprecision(6)
                  << metrics.throughput_gbps
                  << " GB/s p99_us=" << std::setprecision(1) << metrics.p99_us
                  << std::endl;
    }
}

bool appendTargetMetricsJsonl(const std::string& path,
                              const TargetMetricsReport& report,
                              std::string* error) {
    nlohmann::json root = {
        {"schema_version", 1},
        {"record_type", "target_metrics"},
        {"backend", report.backend},
        {"op_type", report.op_type},
        {"block_size", report.block_size},
        {"batch_size", report.batch_size},
        {"num_threads", report.num_threads},
        {"aggregate_operations", report.aggregate_operations},
        {"aggregate_transferred_bytes", report.aggregate_transferred_bytes},
        {"aggregate_throughput_gbps", report.aggregate_throughput_gbps},
        {"targets", nlohmann::json::array()},
    };
    for (const auto& metrics : report.targets) {
        root["targets"].push_back({
            {"index", metrics.index},
            {"segment_name", metrics.segment_name},
            {"threads", metrics.threads},
            {"operations", metrics.operations},
            {"transferred_bytes", metrics.transferred_bytes},
            {"total_duration_us", metrics.total_duration_us},
            {"throughput_gbps", metrics.throughput_gbps},
            {"avg_latency_us", metrics.avg_latency_us},
            {"avg_transfer_us", metrics.avg_transfer_us},
            {"p99_us", metrics.p99_us},
            {"p999_us", metrics.p999_us},
            {"avg_instant_gbps", metrics.avg_instant_gbps},
        });
    }

    std::ofstream output(path, std::ios::app);
    if (!output) {
        *error = "failed to open target JSONL output: " + path;
        return false;
    }
    output << root.dump() << '\n';
    if (!output) {
        *error = "failed to write target JSONL output: " + path;
        return false;
    }
    return true;
}

}  // namespace tent
}  // namespace mooncake
