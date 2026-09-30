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

#include "tent/common/config_validation.h"

#include <algorithm>
#include <cmath>
#include <map>
#include <set>
#include <thread>

#include "tent/common/config_parser.h"
#include "tent/runtime/admission_queue.h"

namespace mooncake {
namespace tent {
namespace {

using Values = std::map<std::string, json>;

// Unknown consumers may distinguish integer/float representations, including
// inside arrays. JSON's operator== alone considers 1 and 1.0 equal.
bool sameValue(const json& lhs, const json& rhs) {
    if (lhs.type() != rhs.type()) return false;
    if (!lhs.is_structured()) return lhs == rhs;
    if (lhs.size() != rhs.size()) return false;
    auto right = rhs.begin();
    for (auto left = lhs.begin(); left != lhs.end(); ++left, ++right) {
        if (lhs.is_object() && left.key() != right.key()) return false;
        if (!sameValue(*left, *right)) return false;
    }
    return true;
}

constexpr std::string_view kTransports[] = {
    "tcp",          "hp_tcp", "shm",   "rdma", "ub",
    "io_uring",     "nvlink", "mnnvl", "gds",  "ascend_direct",
    "sunrise_link", "tpu",    "mpcomm"};

Status invalidConfig(const std::string& path, const std::string& message) {
    // Never echo an oversized/control-character-containing path or raw values.
    const bool safe =
        path.size() <= 256 &&
        std::none_of(path.begin(), path.end(),
                     [](unsigned char c) { return c < 32 || c == 127; });
    return Status::InvalidArgument((safe ? path : "$") + ": " + message +
                                   LOC_MARK);
}

struct Analysis {
    // Raw entries point into this owned document. Keep containers until their
    // consumers check them, so an object cannot hide a mistyped scalar field.
    json document;
    std::map<std::string, const json*> inputs;
    Values values;
    std::set<std::string> validated_paths;
    size_t visited{0};

    Status collect(const json& node, const std::string& path,
                   size_t depth = 0) {
        if (++visited > 4096 || depth > 32 || path.size() > 256 ||
            std::count(path.begin(), path.end(), '/') >= 32) {
            return invalidConfig(
                "$", "Configuration path, depth or value limit exceeded");
        }
        if (!path.empty()) {
            auto [it, inserted] = inputs.emplace(path, &node);
            if (!inserted && !sameValue(*it->second, node)) {
                return invalidConfig(
                    path, "Conflicting flat and nested configuration values");
            }
        }
        if (!node.is_object()) return Status::OK();
        for (auto it = node.begin(); it != node.end(); ++it) {
            const auto& key = it.key();
            const auto child = path.empty() ? key : path + "/" + key;
            if (key.empty() || key.front() == '/' || key.back() == '/' ||
                key.find("//") != std::string::npos ||
                (!path.empty() && key.find('/') != std::string::npos) ||
                std::any_of(key.begin(), key.end(), [](unsigned char c) {
                    return c < 32 || c == 127;
                })) {
                return invalidConfig(child,
                                     "Invalid canonical configuration path");
            }
            if (path.empty() && key == "transports/hp_tcp/enable") {
                return invalidConfig(
                    child, "HP TCP enable must be inside its transport object");
            }
            if (key.find('/') != std::string::npos && it->is_object()) {
                return invalidConfig(child,
                                     "Flat object aliases are not supported");
            }
            CHECK_STATUS(collect(*it, child, depth + 1));
        }
        return Status::OK();
    }

    const json* input(const std::string& path) const {
        const auto it = inputs.find(path);
        return it == inputs.end() ? nullptr : it->second;
    }

    Status readGroup(const std::string& path) {
        const auto* node = input(path);
        if (node && !node->is_null() && !node->is_object()) {
            return invalidConfig(path, "Expected a JSON object");
        }
        inputs.erase(path);
        return Status::OK();
    }

    void remember(const std::string& path, json value) {
        inputs.erase(path);
        values[path] = std::move(value);
        validated_paths.insert(path);
    }

    Status readBool(const std::string& path, bool fallback,
                    bool* output = nullptr) {
        bool value = fallback;
        const auto* node = input(path);
        if (node && !node->is_null()) {
            CHECK_STATUS(parseBoolConfigValue(*node, path, &value));
        }
        remember(path, value);
        if (output) *output = value;
        return Status::OK();
    }

    Status readString(const std::string& path, const std::string& fallback) {
        const auto* node = input(path);
        if (node && !node->is_null() && !node->is_string()) {
            return invalidConfig(path, "must be a string");
        }
        remember(path, node && !node->is_null() ? *node : json(fallback));
        return Status::OK();
    }

    template <typename T>
    Status readUnsigned(const std::string& path, T fallback,
                        T* output = nullptr) {
        T value = fallback;
        const auto* node = input(path);
        if (node && !node->is_null()) {
            CHECK_STATUS(parseUnsignedConfigValue(*node, path, &value));
        }
        remember(path, static_cast<uint64_t>(value));
        if (output) *output = value;
        return Status::OK();
    }

    Status readNumber(const std::string& path, double fallback) {
        double value = fallback;
        const auto* node = input(path);
        if (node && !node->is_null()) {
            if (!node->is_number() || !std::isfinite(node->get<double>())) {
                return invalidConfig(path, "must be a finite number");
            }
            value = node->get<double>();
        }
        remember(path, value);
        return Status::OK();
    }

    bool flag(const std::string& path, bool fallback) const {
        const auto& value = values.at(path);
        return value.is_null() ? fallback : value.get<bool>();
    }

    Status readTransportFlags() {
        CHECK_STATUS(readGroup("transports"));
        for (auto transport : kTransports) {
            const auto group = "transports/" + std::string(transport);
            CHECK_STATUS(readGroup(group));
            const auto path = group + "/enable";
            const auto* node = input(path);
            if (node && (!node->is_null() || transport == "hp_tcp")) {
                bool enabled = false;
                CHECK_STATUS(parseBoolConfigValue(*node, path, &enabled));
                remember(path, enabled);
            } else {
                // Preserve absence: device/environment selection is outside
                // preflight. HP TCP's parser rejects explicit null.
                remember(path, nullptr);
            }
        }
        return Status::OK();
    }

    Status validateRpc(const Config& config,
                       const ConfigValidationContext& context) {
        CHECK_STATUS(readString("rpc_server_hostname", ""));
        uint16_t port = 0;
        CHECK_STATUS(getRpcServerPortFromConfig(config, 0, port));
        remember("rpc_server_port", static_cast<uint64_t>(port));
        const size_t fallback =
            getDefaultRpcServerThreads(config, context.hardware_concurrency);
        size_t threads = 0;
        CHECK_STATUS(getRpcServerThreadsFromConfig(config, fallback, threads));
        remember("rpc_server_threads", static_cast<uint64_t>(threads));
        return Status::OK();
    }

    Status validateRuntime() {
        CHECK_STATUS(readBool("merge_requests", true));
        CHECK_STATUS(
            readUnsigned("max_failover_attempts", kDefaultMaxFailoverAttempts));
        CHECK_STATUS(readBool("enable_auto_failover_on_poll",
                              kDefaultAutoFailoverOnPoll));
        CHECK_STATUS(readBool("enable_progress_worker", false));
        CHECK_STATUS(readBool("enable_runtime_queue", false));
        CHECK_STATUS(readGroup("runtime_queue"));

        QueueLimits limits;
        CHECK_STATUS(readUnsigned("runtime_queue/max_outstanding_owners",
                                  size_t{1024},
                                  &limits.max_outstanding_owners));
        CHECK_STATUS(readUnsigned("runtime_queue/max_outstanding_bytes",
                                  size_t{1} << 30,
                                  &limits.max_outstanding_bytes));
        CHECK_STATUS(readUnsigned("runtime_queue/staging_owner_reserve",
                                  limits.staging_owner_reserve,
                                  &limits.staging_owner_reserve));
        CHECK_STATUS(readUnsigned("runtime_queue/staging_byte_reserve",
                                  limits.staging_byte_reserve,
                                  &limits.staging_byte_reserve));
        CHECK_STATUS(
            readBool("runtime_queue/deadline_aware", limits.deadline_aware));
        // Nonpositive thresholds disable degradation and remain valid.
        CHECK_STATUS(readNumber("runtime_queue/mlu_local_threshold",
                                limits.mlu_local_threshold));
        CHECK_STATUS(readUnsigned("runtime_queue/promotion_slack_ns",
                                  limits.promotion_slack_ns));
        CHECK_STATUS(limits.validate());

        size_t max_owners = 0, max_bytes = 0;
        CHECK_STATUS(readUnsigned("runtime_queue/max_dispatch_owners",
                                  size_t{64}, &max_owners));
        CHECK_STATUS(readUnsigned("runtime_queue/max_dispatch_bytes",
                                  size_t{64} << 20, &max_bytes));
        CHECK_STATUS(readUnsigned("runtime_queue/progress_fallback_interval_us",
                                  int64_t{50000}));
        const bool enabled = flag("enable_runtime_queue", false);
        CHECK_STATUS(
            validateRuntimeQueueDispatchWindow(enabled, max_owners, max_bytes));
        if (enabled) values["enable_progress_worker"] = true;
        return Status::OK();
    }

    Status validateTransports(const ConfigValidationContext& context) {
        CHECK_STATUS(validateTcpTransportSelection(
            flag("transports/tcp/enable", true),
            flag("transports/hp_tcp/enable", false)));
        for (auto transport : kTransports) {
            const auto path =
                "transports/" + std::string(transport) + "/enable";
            if (flag(path, false) &&
                std::find(context.compiled_transports.begin(),
                          context.compiled_transports.end(),
                          transport) == context.compiled_transports.end()) {
                return invalidConfig(
                    path, "explicitly enabled transport is not compiled in");
            }
        }
        return Status::OK();
    }

    Status run(const Config& config, const ConfigValidationContext& context) {
        document = config.toJson();
        if (!document.is_null() && !document.is_object()) {
            return invalidConfig("$",
                                 "Configuration root must be a JSON object");
        }
        CHECK_STATUS(collect(document, ""));
        CHECK_STATUS(readTransportFlags());
        CHECK_STATUS(validateRpc(config, context));
        CHECK_STATUS(validateRuntime());
        CHECK_STATUS(validateTransports(context));
        // Covered fields were consumed above. Keep unknown leaves and empty
        // objects for structural diff; traversed containers are not changes.
        for (const auto& [path, node] : inputs) {
            if (!node->is_object() || node->empty())
                values.emplace(path, *node);
        }
        return Status::OK();
    }
};

}  // namespace

ConfigValidationContext currentConfigValidationContext() {
    ConfigValidationContext context{{"tcp", "hp_tcp", "shm"},
                                    std::thread::hardware_concurrency()};
    // Match runtime/transport_loader.cpp without probing devices or services.
#ifdef USE_RDMA
    context.compiled_transports.push_back("rdma");
#endif
#ifdef USE_UB
    context.compiled_transports.push_back("ub");
#endif
#ifdef USE_URING
    context.compiled_transports.push_back("io_uring");
#endif
#ifdef USE_CUDA
    context.compiled_transports.push_back("nvlink");
    context.compiled_transports.push_back("mnnvl");
#endif
#ifdef USE_GDS
    context.compiled_transports.push_back("gds");
#endif
#if defined(USE_ASCEND) || defined(USE_ASCEND_DIRECT)
    context.compiled_transports.push_back("ascend_direct");
#endif
#ifdef USE_SUNRISE
    context.compiled_transports.push_back("sunrise_link");
#endif
#ifdef USE_TPU
    context.compiled_transports.push_back("tpu");
#endif
#ifdef USE_MPCOMM
    context.compiled_transports.push_back("mpcomm");
#endif
    return context;
}

ConfigChangePlan planTentConfigChange(const Config& current,
                                      const Config& candidate,
                                      const ConfigValidationContext& context) {
    ConfigChangePlan plan;
    plan.current = current.freeze();
    plan.candidate = &current == &candidate ? plan.current : candidate.freeze();
    Analysis before, after;
    plan.current_status = before.run(*plan.current, context);
    plan.candidate_status = after.run(*plan.candidate, context);
    if (!plan.valid()) return plan;
    std::set<std::string> paths;
    for (const auto& [path, value] : before.values) paths.insert(path);
    for (const auto& [path, value] : after.values) paths.insert(path);
    for (const auto& path : paths) {
        const auto old = before.values.find(path);
        const auto next = after.values.find(path);
        if (old != before.values.end() && next != after.values.end() &&
            sameValue(old->second, next->second))
            continue;
        if (plan.changes.size() == 128) {
            plan.candidate_status =
                invalidConfig("$", "Configuration change limit exceeded");
            plan.changes.clear();
            return plan;
        }
        auto disposition = ConfigChangeDisposition::kUnsupported;
        if (after.validated_paths.contains(path)) {
            disposition =
                classifyConfigPath(path) == ConfigLifecycle::kRuntimeCandidate
                    ? ConfigChangeDisposition::kRuntimeCandidate
                    : ConfigChangeDisposition::kRestartRequired;
        }
        plan.changes.push_back({path, disposition});
    }
    return plan;
}

}  // namespace tent
}  // namespace mooncake
