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

// Transport namespaces are extension points and therefore use subtree
// entries. Runtime-eligible exceptions below them are listed as exact fields;
// classifyConfigPath() resolves those using longest-match semantics.
constexpr ConfigFieldSpec kConfigFields[] = {
    // Bootstrap identity and process-level services.
    {"local_segment_name", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"metadata_type", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"metadata_servers", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"rpc_server_hostname", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"rpc_server_port", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"rpc_server_threads", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"verbose", ConfigLifecycle::kBootstrapOnly, ConfigFieldMatch::kExact},
    {"use_legacy_transport_selection", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"enable_progress_worker", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"enable_runtime_queue", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"staging", ConfigLifecycle::kBootstrapOnly, ConfigFieldMatch::kSubtree},

    // Discovery and resource construction.
    {"topology", ConfigLifecycle::kBootstrapOnly, ConfigFieldMatch::kSubtree},
    {"transports", ConfigLifecycle::kBootstrapOnly, ConfigFieldMatch::kExact},
    {"transports/rdma", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/tcp", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/hp_tcp", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/shm", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/nvlink", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/mnnvl", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/gds", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/io_uring", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/ub", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/ascend_direct", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/sunrise_link", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/tpu", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},
    {"transports/mpcomm", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kSubtree},

    // Metrics listener construction is bootstrap-only. Its reporting cadence
    // is a runtime candidate and is overridden below.
    {"metrics", ConfigLifecycle::kBootstrapOnly, ConfigFieldMatch::kExact},
    {"metrics/enabled", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"metrics/http_port", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"metrics/http_host", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},
    {"metrics/http_server_threads", ConfigLifecycle::kBootstrapOnly,
     ConfigFieldMatch::kExact},

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
    return best_match ? best_match->lifecycle : ConfigLifecycle::kUnsupported;
}

const char* configLifecycleName(ConfigLifecycle lifecycle) {
    switch (lifecycle) {
        case ConfigLifecycle::kBootstrapOnly:
            return "bootstrap-only";
        case ConfigLifecycle::kRuntimeCandidate:
            return "runtime-candidate";
        case ConfigLifecycle::kDerived:
            return "derived";
        case ConfigLifecycle::kUnsupported:
            return "unsupported";
    }
    return "unsupported";
}

bool LifecycleConfigView::allows(std::string_view key_path) const {
    return classifyConfigPath(key_path) == lifecycle_;
}

bool LifecycleConfigView::contains(const std::string& key_path) const {
    return allows(key_path) && values_ && values_->contains(key_path);
}

bool LifecycleConfigView::dumpSubtree(const std::string& key_path,
                                      std::string* out) const {
    return allows(key_path) && values_ && values_->dumpSubtree(key_path, out);
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
            continue;
        }
        if (classifyConfigPath(path) == ConfigLifecycle::kUnsupported) {
            bundle.diagnostics.push_back(
                {ConfigDiagnosticCode::kUnsupportedField, path,
                 "Unsupported TENT configuration field: " + path});
        }
    }
    return bundle;
}

}  // namespace tent
}  // namespace mooncake
