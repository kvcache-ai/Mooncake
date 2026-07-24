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

#ifndef TEBENCH_WORKLOAD_CONFIG_H
#define TEBENCH_WORKLOAD_CONFIG_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "tent/common/qos_metrics.h"
#include "tent/common/types.h"

namespace mooncake {
namespace tent {

struct WorkloadClassConfig {
    QosClassConfig qos;
    size_t block_size = 0;
    size_t batch_size = 0;
    uint64_t deadline_us = 0;
    IntentType intent_type = IntentType::INTENT_UNSPEC;
};

bool parseWorkloadClassesJson(const std::string& spec,
                              std::vector<WorkloadClassConfig>* classes,
                              std::string* error);

bool validateWorkloadClasses(const std::vector<WorkloadClassConfig>& classes,
                             int num_threads, size_t total_buffer_size,
                             std::string* error);

std::vector<QosClassConfig> qosClassesFromWorkload(
    const std::vector<WorkloadClassConfig>& classes);

}  // namespace tent
}  // namespace mooncake

#endif  // TEBENCH_WORKLOAD_CONFIG_H
