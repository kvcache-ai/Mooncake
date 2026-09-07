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

#include "tent/common/config_lifecycle.h"

namespace mooncake {
namespace tent {
namespace {

// Only runtime candidates need explicit entries. Everything else is
// bootstrap-only, including new transport namespaces and unknown keys.
constexpr ConfigFieldSpec kConfigFields[] = {
    // Fields that can be represented by an immutable runtime generation.
    {"log_level", ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"merge_requests", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"max_failover_attempts", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"enable_auto_failover_on_poll", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"runtime_queue", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kSubtree},
    {"policy", ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kSubtree},
    {"qos", ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kSubtree},
    {"metrics/report_interval_seconds", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},

    // RDMA policy and health knobs do not allocate devices, CQs, or QPs and
    // are candidates for a later staged apply handler.
    {"transports/rdma/enable_smart_scheduling",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/numa_penalties", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"transports/rdma/strict_local_numa", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"transports/rdma/bandwidth_learning_rate",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/ewma_min_bandwidth_multiplier",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/ewma_max_bandwidth_multiplier",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/score_jitter_range", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"transports/rdma/score_epsilon", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"transports/rdma/enable_priority_filtering",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/local_rotation_interval_us",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/priority_promotion_timeout_us",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/deadline_bw_arbitration",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/priority_promotion_per_entry",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/slot_rotation_interval_ms",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/default_bandwidth_gbps",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/min_bandwidth_gbps", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"transports/rdma/max_bandwidth_gbps", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"transports/rdma/rail_error_threshold", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"transports/rdma/rail_error_window_secs",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/rail_cooldown_secs", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"transports/rdma/gdr_error_threshold", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
    {"transports/rdma/gdr_error_window_secs",
     ConfigLifecycle::kRuntimeCandidate, ConfigFieldMatch::kExact},
    {"transports/rdma/gdr_cooldown_secs", ConfigLifecycle::kRuntimeCandidate,
     ConfigFieldMatch::kExact},
};

bool matches(const ConfigFieldSpec& spec, std::string_view path) {
    if (path == spec.path) return true;
    return spec.match == ConfigFieldMatch::kSubtree &&
           path.size() > spec.path.size() &&
           path.compare(0, spec.path.size(), spec.path) == 0 &&
           path[spec.path.size()] == '/';
}

}  // namespace

std::span<const ConfigFieldSpec> configFieldInventory() {
    return kConfigFields;
}

ConfigLifecycle classifyConfigPath(std::string_view path) {
    const ConfigFieldSpec* best_match = nullptr;
    for (const auto& field : kConfigFields) {
        if (!matches(field, path)) continue;
        if (!best_match || field.path.size() > best_match->path.size()) {
            best_match = &field;
        }
    }
    return best_match ? best_match->lifecycle : ConfigLifecycle::kBootstrapOnly;
}

const char* configLifecycleName(ConfigLifecycle lifecycle) {
    switch (lifecycle) {
        case ConfigLifecycle::kBootstrapOnly:
            return "bootstrap-only";
        case ConfigLifecycle::kRuntimeCandidate:
            return "runtime-candidate";
    }
    return "unknown";
}

bool LifecycleConfigView::allows(std::string_view key_path) const {
    return classifyConfigPath(key_path) == lifecycle_;
}

bool LifecycleConfigView::canRead(std::string_view key_path) const {
    if (!values_ || !allows(key_path)) return false;

    for (const auto& configured_path : configured_paths_) {
        if (configured_path.size() <= key_path.size() ||
            configured_path.compare(0, key_path.size(), key_path) != 0 ||
            configured_path[key_path.size()] != '/') {
            continue;
        }
        if (!allows(configured_path)) return false;
    }
    return true;
}

bool LifecycleConfigView::contains(const std::string& key_path) const {
    return canRead(key_path) && values_->contains(key_path);
}

bool LifecycleConfigView::dumpSubtree(const std::string& key_path,
                                      std::string* out) const {
    return canRead(key_path) && values_->dumpSubtree(key_path, out);
}

TentConfigBundle buildTentConfigBundle(const Config& effective_config,
                                       uint64_t generation) {
    TentConfigBundle bundle;
    auto frozen = effective_config.freeze();
    bundle.bootstrap = std::make_shared<const BootstrapConfig>(frozen);

    auto runtime_config = std::make_shared<const RuntimeConfig>(frozen);
    bundle.runtime = std::make_shared<const RuntimeConfigSnapshot>(
        RuntimeConfigSnapshot{generation, std::move(runtime_config)});

    for (const auto& path : frozen->paths()) {
        if (path.empty()) {
            bundle.diagnostics.push_back(
                {ConfigDiagnosticCode::kInvalidRoot, "$",
                 "TENT configuration root must be a JSON object"});
        }
    }
    return bundle;
}

}  // namespace tent
}  // namespace mooncake
