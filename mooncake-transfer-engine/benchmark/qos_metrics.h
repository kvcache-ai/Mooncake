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

#ifndef TEBENCH_QOS_METRICS_H
#define TEBENCH_QOS_METRICS_H

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "utils.h"

namespace mooncake {
namespace tent {

// One entry in --qos_classes:
//   name:threads:slo_us:weight[:isolated_gbps]
// isolated_gbps is an optional result from a matching isolated run. It is
// deliberately an input: isolation loss cannot be inferred from a mixed run.
struct QosClassConfig {
    std::string name;
    int threads = 0;
    uint64_t slo_us = 0;
    double weight = 1.0;
    std::optional<double> isolated_throughput_gbps;
};

struct QosClassMetrics {
    std::string name;
    int threads = 0;
    uint64_t slo_us = 0;
    double weight = 1.0;
    size_t operations = 0;
    double throughput_gbps = 0.0;
    double p99_us = 0.0;
    std::optional<double> slo_attainment;
    double goodput_gbps = 0.0;
    double weighted_goodput_gbps = 0.0;
    std::optional<double> isolation_leakage;
};

struct QosMetricsReport {
    size_t block_size = 0;
    size_t batch_size = 0;
    int num_threads = 0;
    double aggregate_throughput_gbps = 0.0;
    double weighted_goodput_gbps = 0.0;
    double jain_fairness = 0.0;
    std::optional<double> max_isolation_leakage;
    std::optional<double> total_utilization;
    std::vector<QosClassMetrics> classes;
};

bool parseQosClasses(const std::string& spec,
                     std::vector<QosClassConfig>* classes, std::string* error);

bool validateQosClasses(const std::vector<QosClassConfig>& classes,
                        int num_threads, std::string* error);

size_t qosClassForThread(const std::vector<QosClassConfig>& classes,
                         int thread_id);

QosMetricsReport calculateQosMetrics(size_t block_size, size_t batch_size,
                                     int num_threads,
                                     const std::vector<QosClassConfig>& classes,
                                     std::vector<XferBenchStats>* stats,
                                     double link_capacity_gbps);

void printQosMetrics(const QosMetricsReport& report);

bool appendQosMetricsJsonl(const std::string& path,
                           const QosMetricsReport& report, std::string* error);

}  // namespace tent
}  // namespace mooncake

#endif  // TEBENCH_QOS_METRICS_H
