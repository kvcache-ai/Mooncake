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
#include <limits>
#include <map>
#include <set>
#include <stdexcept>
#include <thread>

namespace mooncake {
namespace tent {
namespace {

using Values = std::map<std::string, json>;
using Code = ConfigDiagnosticCode;
enum class Kind { kBool, kString, kUnsigned, kRpcInteger, kNumber };

struct Rule {
    std::string path;
    Kind kind;
    json default_value;
    uint64_t maximum{0};
    uint64_t minimum{0};
};

// Unknown consumers may distinguish integer/float representations. JSON's
// operator== considers 1 and 1.0 equal, including inside arrays/objects.
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

// Defaults/ranges follow construct(), getRpcServer{Port,Threads}FromConfig(),
// QueueLimits, and the shared failover defaults introduced by #3934. Keep these
// checks outside the legacy loader: it intentionally defaults on type errors.
std::vector<Rule> rules() {
    constexpr auto size_max = std::numeric_limits<size_t>::max();
    std::vector<Rule> result = {
        {"rpc_server_hostname", Kind::kString, ""},
        {"rpc_server_port", Kind::kRpcInteger, 0, 65535},
        {"rpc_server_threads", Kind::kRpcInteger, 1, 1024, 1},
        {"merge_requests", Kind::kBool, true},
        {"max_failover_attempts", Kind::kUnsigned, kDefaultMaxFailoverAttempts,
         std::numeric_limits<int>::max()},
        {"enable_auto_failover_on_poll", Kind::kBool,
         kDefaultAutoFailoverOnPoll},
        {"enable_progress_worker", Kind::kBool, false},
        {"enable_runtime_queue", Kind::kBool, false},
        {"runtime_queue/max_outstanding_owners", Kind::kUnsigned, 1024,
         size_max},
        {"runtime_queue/max_outstanding_bytes", Kind::kUnsigned, 1ULL << 30,
         size_max},
        {"runtime_queue/staging_owner_reserve", Kind::kUnsigned, 0, size_max},
        {"runtime_queue/staging_byte_reserve", Kind::kUnsigned, 0, size_max},
        {"runtime_queue/deadline_aware", Kind::kBool, false},
        // Nonpositive MLU thresholds disable degradation; do not reject them.
        {"runtime_queue/mlu_local_threshold", Kind::kNumber, 0.0},
        {"runtime_queue/promotion_slack_ns", Kind::kUnsigned, 0,
         std::numeric_limits<uint64_t>::max()},
        {"runtime_queue/max_dispatch_owners", Kind::kUnsigned, 64, size_max},
        {"runtime_queue/max_dispatch_bytes", Kind::kUnsigned, 64ULL << 20,
         size_max},
        {"runtime_queue/progress_fallback_interval_us", Kind::kUnsigned, 50000,
         static_cast<uint64_t>(std::numeric_limits<int64_t>::max())},
    };
    for (auto transport : kTransports) {
        result.push_back({"transports/" + std::string(transport) + "/enable",
                          Kind::kBool, nullptr});
    }
    return result;
}

void diagnose(std::vector<ConfigDiagnostic>& out, Code code,
              const std::string& path, const std::string& message) {
    if (out.size() >= 64) return;
    if (out.size() == 63) {
        out.push_back({Code::kAnalysisLimitExceeded, "$",
                       "Configuration diagnostic limit exceeded"});
        return;
    }
    // Do not echo oversized or control-character-containing paths.
    const bool safe =
        path.size() <= 256 &&
        std::none_of(path.begin(), path.end(),
                     [](unsigned char c) { return c < 32 || c == 127; });
    out.push_back({code, safe ? path : "$", message});
}

struct Analysis {
    Values values;
    std::vector<ConfigDiagnostic>& diagnostics;
    const std::vector<Rule>& fields;
    size_t visited{0};

    const Rule* rule(const std::string& path) const {
        auto it = std::find_if(fields.begin(), fields.end(),
                               [&](const Rule& r) { return r.path == path; });
        return it == fields.end() ? nullptr : &*it;
    }

    bool container(const std::string& path) const {
        return std::any_of(fields.begin(), fields.end(), [&](const Rule& r) {
            return r.path.starts_with(path + "/");
        });
    }

    void collect(const json& node, const std::string& path, size_t depth = 0) {
        if (++visited > 4096 || depth > 32 || path.size() > 256 ||
            std::count(path.begin(), path.end(), '/') >= 32) {
            diagnose(diagnostics, Code::kAnalysisLimitExceeded, "$",
                     "Configuration path, depth or value limit exceeded");
            return;
        }
        const bool group = path.empty() || container(path);
        if (group && node.is_null()) return;  // legacy missing/default values
        if (group && !node.is_object()) {
            diagnose(diagnostics,
                     path.empty() ? Code::kInvalidRoot : Code::kInvalidType,
                     path.empty() ? "$" : path, "Expected a JSON object");
            return;
        }
        if (!rule(path) && node.is_object() && (group || !node.empty())) {
            for (auto it = node.begin(); it != node.end(); ++it) {
                if (visited > 4096) break;
                const auto& key = it.key();
                const auto child = path.empty() ? key : path + "/" + key;
                // Config only supports slash-containing flat aliases at root.
                if (key.empty() || key.front() == '/' || key.back() == '/' ||
                    key.find("//") != std::string::npos ||
                    (!path.empty() && key.find('/') != std::string::npos) ||
                    std::any_of(key.begin(), key.end(), [](unsigned char c) {
                        return c < 32 || c == 127;
                    })) {
                    diagnose(diagnostics, Code::kInvalidPath, child,
                             "Invalid canonical configuration path");
                    continue;
                }
                // HP TCP consumes the entire transport object through its
                // dedicated parser, not Config::get() on individual flat keys.
                if (path.empty() && key == "transports/hp_tcp/enable") {
                    diagnose(
                        diagnostics, Code::kInvalidPath, child,
                        "HP TCP enable must be inside its transport object");
                    continue;
                }
                // A flat object alias does not expose its children through
                // Config::get("a/b/c"); reject that misleading representation.
                if (key.find('/') != std::string::npos && it->is_object() &&
                    !rule(child)) {
                    diagnose(diagnostics, Code::kInvalidPath, child,
                             "Flat object aliases are not supported");
                    continue;
                }
                collect(*it, child, depth + 1);
            }
            return;
        }
        auto [it, inserted] = values.emplace(path, node);
        if (!inserted && !sameValue(it->second, node)) {
            diagnose(diagnostics, Code::kAmbiguousPath, path,
                     "Conflicting flat and nested configuration values");
        }
    }

    void validate(const Rule& field) {
        if (field.path == "transports/hp_tcp/enable" &&
            values.contains(field.path) && values.at(field.path).is_null()) {
            diagnose(diagnostics, Code::kInvalidType, field.path,
                     "HP TCP enable must be a boolean");
            return;
        }
        auto& value =
            values.try_emplace(field.path, field.default_value).first->second;
        if (value.is_null()) value = field.default_value;
        if (value.is_null()) return;  // unspecified transport enable flag
        const auto range_message = [&] {
            return "Expected an integer in range [" +
                   std::to_string(field.minimum) + ", " +
                   std::to_string(field.maximum) + "]";
        };
        if (field.kind == Kind::kRpcInteger && value.is_string()) {
            try {
                const auto text = value.get<std::string>();
                size_t consumed = 0;
                const auto number = std::stoll(text, &consumed);
                if (consumed == text.size()) value = number;
            } catch (const std::out_of_range&) {
                diagnose(diagnostics, Code::kOutOfRange, field.path,
                         range_message());
                return;
            } catch (const std::invalid_argument&) {
            }
        }
        bool type_ok = false;
        const char* type_message = "Expected an integer";
        switch (field.kind) {
            case Kind::kBool:
                type_ok = value.is_boolean();
                type_message = "Expected a boolean";
                break;
            case Kind::kString:
                type_ok = value.is_string();
                type_message = "Expected a string";
                break;
            case Kind::kNumber:
                type_ok = value.is_number();
                type_message = "Expected a finite number";
                break;
            case Kind::kUnsigned:
                type_ok = value.is_number_integer();
                break;
            case Kind::kRpcInteger:
                type_ok = value.is_number_integer();
                type_message = "Expected an integer or integer string";
                break;
        }
        if (!type_ok) {
            diagnose(diagnostics, Code::kInvalidType, field.path, type_message);
            return;
        }
        if (field.kind == Kind::kUnsigned || field.kind == Kind::kRpcInteger) {
            // Check signedness/range BEFORE narrowing. json::get<T>() alone can
            // silently wrap an oversized unsigned or a negative integer.
            if ((!value.is_number_unsigned() && value.get<int64_t>() < 0) ||
                value.get<uint64_t>() < field.minimum ||
                value.get<uint64_t>() > field.maximum) {
                diagnose(diagnostics, Code::kOutOfRange, field.path,
                         range_message());
                return;
            }
            value = value.get<uint64_t>();
        } else if (field.kind == Kind::kNumber) {
            if (!std::isfinite(value.get<double>())) {
                diagnose(diagnostics, Code::kOutOfRange, field.path,
                         "Expected a finite number");
            }
            value = value.get<double>();
        }
    }

    void run(const Config& config, const ConfigValidationContext& context) {
        // One frozen capture, not a sequence of individually locked get()s.
        collect(config.toJson(), "");
        if (!diagnostics.empty()) return;
        for (const auto& field : fields) validate(field);
        if (!diagnostics.empty()) return;
        auto flag = [&](const std::string& path, bool fallback) {
            const auto& value = values.at(path);
            return value.is_null() ? fallback : value.get<bool>();
        };
        // construct() uses an explicit TCP enable to choose RPC thread
        // defaults, whereas loadTransports() defaults TCP itself to enabled.
        // Preserve both.
        if (!config.contains("rpc_server_threads")) {
            values["rpc_server_threads"] =
                flag("transports/tcp/enable", false)
                    ? std::min(8U, std::max(4U, context.hardware_concurrency))
                    : 1U;
        }
        if (flag("enable_runtime_queue", false)) {
            values["enable_progress_worker"] = true;
            for (const auto* path : {"runtime_queue/max_dispatch_owners",
                                     "runtime_queue/max_dispatch_bytes"}) {
                if (values.at(path).get<uint64_t>() == 0) {
                    diagnose(diagnostics, Code::kCrossFieldConstraint, path,
                             "Enabled runtime queue requires a nonzero "
                             "dispatch window");
                }
            }
        }
        // Same two invariants as admission_queue.cpp::validateLimits(). Zero
        // total capacity and negative MLU (disabled) remain legal.
        for (const auto& [reserve, total] :
             {std::pair{"runtime_queue/staging_owner_reserve",
                        "runtime_queue/max_outstanding_owners"},
              std::pair{"runtime_queue/staging_byte_reserve",
                        "runtime_queue/max_outstanding_bytes"}}) {
            if (values.at(reserve).get<uint64_t>() >
                values.at(total).get<uint64_t>()) {
                diagnose(diagnostics, Code::kCrossFieldConstraint, reserve,
                         "Staging reserve exceeds total queue capacity");
            }
        }
        if (flag("transports/hp_tcp/enable", false) &&
            flag("transports/tcp/enable", true)) {
            diagnose(diagnostics, Code::kCrossFieldConstraint,
                     "transports/hp_tcp/enable",
                     "HP TCP and TCP cannot both be enabled");
        }
        for (auto transport : kTransports) {
            const auto path =
                "transports/" + std::string(transport) + "/enable";
            if (flag(path, false) &&
                std::find(context.compiled_transports.begin(),
                          context.compiled_transports.end(),
                          transport) == context.compiled_transports.end()) {
                diagnose(diagnostics, Code::kBackendUnavailable, path,
                         "Explicitly enabled transport is not compiled in");
            }
        }
    }
};

}  // namespace

ConfigValidationContext currentConfigValidationContext() {
    ConfigValidationContext context{{"tcp", "hp_tcp", "shm"},
                                    std::thread::hardware_concurrency()};
    // Match compile guards in runtime/transport_loader.cpp, without probing
    // topology or initializing process-wide platform/metrics singletons.
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
    const auto fields = rules();
    Analysis before{{}, plan.current_diagnostics, fields};
    Analysis after{{}, plan.candidate_diagnostics, fields};
    before.run(*plan.current, context);
    after.run(*plan.candidate, context);
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
            diagnose(plan.candidate_diagnostics, Code::kAnalysisLimitExceeded,
                     "$", "Configuration change limit exceeded");
            plan.changes.clear();
            return plan;
        }
        auto disposition = ConfigChangeDisposition::kUnsupported;
        if (after.rule(path)) {
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
