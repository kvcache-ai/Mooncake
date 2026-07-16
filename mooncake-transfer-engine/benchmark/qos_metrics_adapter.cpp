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

namespace mooncake {
namespace tent {

std::vector<QosClassSample> makeQosClassSamples(
    const std::vector<QosClassConfig>& classes,
    std::vector<XferBenchStats>* stats) {
    std::vector<QosClassSample> samples(classes.size());
    for (size_t i = 0; i < classes.size(); ++i) {
        auto& class_stats = (*stats)[i];
        auto& sample = samples[i];
        sample.operations = class_stats.transfer_duration.count();
        sample.total_duration_us = class_stats.total_duration.avg();
        sample.p99_us = class_stats.transfer_duration.p99();
        if (classes[i].slo_us != 0) {
            sample.slo_attainment =
                class_stats.transfer_duration.fractionAtOrBelow(
                    static_cast<double>(classes[i].slo_us));
        }
    }
    return samples;
}

QosMetricsReport calculateQosMetricsFromBenchStats(
    size_t block_size, size_t batch_size, int num_threads,
    const std::vector<QosClassConfig>& classes,
    std::vector<XferBenchStats>* stats, double link_capacity_gbps) {
    return calculateQosMetrics(block_size, batch_size, num_threads, classes,
                               makeQosClassSamples(classes, stats),
                               link_capacity_gbps);
}

}  // namespace tent
}  // namespace mooncake
