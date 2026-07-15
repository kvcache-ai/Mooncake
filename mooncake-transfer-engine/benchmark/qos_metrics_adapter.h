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

#ifndef TEBENCH_QOS_METRICS_ADAPTER_H
#define TEBENCH_QOS_METRICS_ADAPTER_H

#include <vector>

#include "tent/common/qos_metrics.h"
#include "utils.h"

namespace mooncake {
namespace tent {

std::vector<QosClassSample> makeQosClassSamples(
    const std::vector<QosClassConfig>& classes,
    std::vector<XferBenchStats>* stats);

QosMetricsReport calculateQosMetricsFromBenchStats(
    size_t block_size, size_t batch_size, int num_threads,
    const std::vector<QosClassConfig>& classes,
    std::vector<XferBenchStats>* stats, double link_capacity_gbps);

}  // namespace tent
}  // namespace mooncake

#endif  // TEBENCH_QOS_METRICS_ADAPTER_H
