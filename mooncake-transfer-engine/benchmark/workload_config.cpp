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

#include "workload_config.h"

#include <algorithm>
#include <limits>
#include <unordered_map>

#include "tent/thirdparty/nlohmann/json.h"

namespace mooncake {
namespace tent {
namespace {

bool parseIntentType(const std::string& value, IntentType* intent_type) {
    static const std::unordered_map<std::string, IntentType> kIntentTypes = {
        {"unspec", IntentType::INTENT_UNSPEC},
        {"intent_unspec", IntentType::INTENT_UNSPEC},
        {"foreground_get", IntentType::FOREGROUND_GET},
        {"background_prefetch", IntentType::BACKGROUND_PREFETCH},
        {"migration", IntentType::MIGRATION},
        {"checkpoint", IntentType::CHECKPOINT},
        {"weight_loading", IntentType::WEIGHT_LOADING},
        {"staging_internal", IntentType::STAGING_INTERNAL},
    };
    const auto it = kIntentTypes.find(value);
    if (it == kIntentTypes.end()) return false;
    *intent_type = it->second;
    return true;
}

bool parsePositiveSize(const nlohmann::json& node, const char* field,
                       const std::string& path, size_t* result,
                       std::string* error) {
    if (!node.contains(field) || !node[field].is_number_unsigned()) {
        *error = path + "." + field + " must be an unsigned integer";
        return false;
    }
    const uint64_t value = node[field].get<uint64_t>();
    if (value == 0 || value > std::numeric_limits<size_t>::max()) {
        *error = path + "." + field + " must be positive and fit in size_t";
        return false;
    }
    *result = static_cast<size_t>(value);
    return true;
}

}  // namespace

bool parseWorkloadClassesJson(const std::string& spec,
                              std::vector<WorkloadClassConfig>* classes,
                              std::string* error) {
    classes->clear();
    try {
        const auto root = nlohmann::json::parse(spec);
        if (!root.is_array() || root.empty()) {
            *error = "workload_classes_json must be a non-empty array";
            return false;
        }
        std::vector<QosClassConfig> qos_classes;
        if (!parseQosClassesJson(spec, &qos_classes, error)) return false;

        for (size_t i = 0; i < root.size(); ++i) {
            const auto& node = root[i];
            const std::string path =
                "workload_classes_json[" + std::to_string(i) + "]";

            WorkloadClassConfig config;
            config.qos = qos_classes[i];
            if (!parsePositiveSize(node, "block_size", path, &config.block_size,
                                   error) ||
                !parsePositiveSize(node, "batch_size", path, &config.batch_size,
                                   error)) {
                return false;
            }
            if (config.block_size >
                std::numeric_limits<size_t>::max() / config.batch_size) {
                *error = path + " block_size * batch_size overflows size_t";
                return false;
            }

            if (node.contains("deadline_us")) {
                if (!node["deadline_us"].is_number_unsigned()) {
                    *error = path + ".deadline_us must be an unsigned integer";
                    return false;
                }
                config.deadline_us = node["deadline_us"].get<uint64_t>();
            }
            if (!node.contains("intent_type") ||
                !node["intent_type"].is_string() ||
                !parseIntentType(node["intent_type"].get<std::string>(),
                                 &config.intent_type)) {
                *error = path + ".intent_type is invalid";
                return false;
            }
            classes->push_back(std::move(config));
        }
        return true;
    } catch (const std::exception& e) {
        *error =
            std::string("failed to parse workload_classes_json: ") + e.what();
        return false;
    }
}

bool validateWorkloadClasses(const std::vector<WorkloadClassConfig>& classes,
                             int num_threads, size_t total_buffer_size,
                             std::string* error) {
    size_t configured_threads = 0;
    size_t max_bytes_per_thread = 0;
    for (const auto& config : classes) {
        configured_threads += static_cast<size_t>(config.qos.threads);
        const size_t bytes = config.block_size * config.batch_size;
        max_bytes_per_thread = std::max(max_bytes_per_thread, bytes);
    }
    if (configured_threads != static_cast<size_t>(num_threads)) {
        *error = "workload class thread count must equal benchmark threads";
        return false;
    }
    if (max_bytes_per_thread > total_buffer_size ||
        configured_threads > total_buffer_size / max_bytes_per_thread) {
        *error = "mixed workload address stride exceeds total_buffer_size";
        return false;
    }
    return true;
}

std::vector<QosClassConfig> qosClassesFromWorkload(
    const std::vector<WorkloadClassConfig>& classes) {
    std::vector<QosClassConfig> qos_classes;
    qos_classes.reserve(classes.size());
    for (const auto& config : classes) qos_classes.push_back(config.qos);
    return qos_classes;
}

}  // namespace tent
}  // namespace mooncake
