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

#ifndef TEBENCH_TARGET_METRICS_H
#define TEBENCH_TARGET_METRICS_H

#include <cstdint>
#include <string>
#include <vector>

#include "utils.h"

namespace mooncake {
namespace tent {

struct TargetBenchStats {
    std::string segment_name;
    int threads = 0;
    uint64_t transferred_bytes = 0;
    XferBenchStats stats;
};

struct TargetMetrics {
    size_t index = 0;
    std::string segment_name;
    int threads = 0;
    uint64_t operations = 0;
    uint64_t transferred_bytes = 0;
    double total_duration_us = 0.0;
    double throughput_gbps = 0.0;
    double avg_latency_us = 0.0;
    double avg_transfer_us = 0.0;
    double p99_us = 0.0;
    double p999_us = 0.0;
    double avg_instant_gbps = 0.0;
};

struct TargetMetricsReport {
    size_t block_size = 0;
    size_t batch_size = 0;
    int num_threads = 0;
    std::string backend;
    std::string op_type;
    uint64_t aggregate_operations = 0;
    uint64_t aggregate_transferred_bytes = 0;
    double aggregate_throughput_gbps = 0.0;
    std::vector<TargetMetrics> targets;
};

TargetMetricsReport calculateTargetMetrics(
    size_t block_size, size_t batch_size, int num_threads,
    const std::string& backend, const std::string& op_type,
    std::vector<TargetBenchStats>* stats);

void printTargetMetrics(const TargetMetricsReport& report);

bool appendTargetMetricsJsonl(const std::string& path,
                              const TargetMetricsReport& report,
                              std::string* error);

}  // namespace tent
}  // namespace mooncake

#endif  // TEBENCH_TARGET_METRICS_H
