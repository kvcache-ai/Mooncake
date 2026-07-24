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

#include "tent/runtime/qos_contract.h"

#include <algorithm>
#include <cctype>
#include <limits>

namespace mooncake {
namespace tent {
namespace {

bool isValidPriority(int priority) {
    return priority == PRIO_HIGH || priority == PRIO_MEDIUM ||
           priority == PRIO_LOW;
}

bool isKnownDegradedAction(const std::string& action) {
    static const std::vector<std::string> kActions = {
        "delay", "fallback_transport", "local_recompute", "compress", "reject"};
    return std::find(kActions.begin(), kActions.end(), action) !=
           kActions.end();
}

std::string trim(const std::string& input) {
    size_t begin = 0;
    while (begin < input.size() &&
           std::isspace(static_cast<unsigned char>(input[begin]))) {
        ++begin;
    }
    size_t end = input.size();
    while (end > begin &&
           std::isspace(static_cast<unsigned char>(input[end - 1]))) {
        --end;
    }
    return input.substr(begin, end - begin);
}

bool containsKey(const std::vector<std::string>& allowed,
                 const std::string& key) {
    return std::find(allowed.begin(), allowed.end(), key) != allowed.end();
}

Status validateKnownKeys(const json& node,
                         const std::vector<std::string>& allowed,
                         const std::string& path) {
    for (auto it = node.begin(); it != node.end(); ++it) {
        if (!containsKey(allowed, it.key())) {
            return Status::InvalidArgument(
                path + " contains unknown key: " + it.key());
        }
    }
    return Status::OK();
}

}  // namespace

std::string QosContractResolver::normalizeKey(const std::string& value) {
    std::string out = trim(value);
    std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    return out;
}

std::string QosContractResolver::intentTypeName(IntentType intent) {
    switch (intent) {
        case IntentType::INTENT_UNSPEC:
            return "unspec";
        case IntentType::FOREGROUND_GET:
            return "foreground_get";
        case IntentType::BACKGROUND_PREFETCH:
            return "background_prefetch";
        case IntentType::MIGRATION:
            return "migration";
        case IntentType::CHECKPOINT:
            return "checkpoint";
        case IntentType::WEIGHT_LOADING:
            return "weight_loading";
        case IntentType::STAGING_INTERNAL:
            return "staging_internal";
    }
    return "unspec";
}

Status QosContractResolver::parsePriority(const json& node, int* out,
                                          const std::string& path) {
    if (!out) return Status::InvalidArgument("priority output is null");
    if (node.is_number_integer()) {
        int value = node.get<int>();
        if (!isValidPriority(value)) {
            return Status::InvalidArgument(path + " priority out of range");
        }
        *out = value;
        return Status::OK();
    }
    if (!node.is_string()) {
        return Status::InvalidArgument(path +
                                       " priority must be string or int");
    }
    auto value = normalizeKey(node.get<std::string>());
    if (value == "high" || value == "0") {
        *out = PRIO_HIGH;
    } else if (value == "medium" || value == "1") {
        *out = PRIO_MEDIUM;
    } else if (value == "low" || value == "2") {
        *out = PRIO_LOW;
    } else {
        return Status::InvalidArgument(path + " priority is unknown: " + value);
    }
    return Status::OK();
}

Status QosContractResolver::parseBytes(const json& node, uint64_t* out,
                                       const std::string& path) {
    if (!out) return Status::InvalidArgument("bytes output is null");
    if (node.is_number_unsigned()) {
        *out = node.get<uint64_t>();
        return Status::OK();
    }
    if (node.is_number_integer()) {
        auto value = node.get<int64_t>();
        if (value < 0) return Status::InvalidArgument(path + " must be >= 0");
        *out = static_cast<uint64_t>(value);
        return Status::OK();
    }
    if (!node.is_string()) {
        return Status::InvalidArgument(path + " must be bytes string or int");
    }

    std::string text = normalizeKey(node.get<std::string>());
    if (text.empty()) return Status::InvalidArgument(path + " is empty");

    size_t pos = 0;
    while (pos < text.size() &&
           std::isdigit(static_cast<unsigned char>(text[pos]))) {
        ++pos;
    }
    if (pos == 0) return Status::InvalidArgument(path + " missing number");

    uint64_t base = 0;
    try {
        base = std::stoull(text.substr(0, pos));
    } catch (const std::exception&) {
        return Status::InvalidArgument(path + " invalid number");
    }

    std::string unit = trim(text.substr(pos));
    uint64_t mul = 1;
    if (unit.empty() || unit == "b") {
        mul = 1;
    } else if (unit == "kib" || unit == "kb") {
        mul = 1024ull;
    } else if (unit == "mib" || unit == "mb") {
        mul = 1024ull * 1024ull;
    } else if (unit == "gib" || unit == "gb") {
        mul = 1024ull * 1024ull * 1024ull;
    } else if (unit == "tib" || unit == "tb") {
        mul = 1024ull * 1024ull * 1024ull * 1024ull;
    } else {
        return Status::InvalidArgument(path + " unsupported unit: " + unit);
    }
    if (base > std::numeric_limits<uint64_t>::max() / mul) {
        return Status::InvalidArgument(path + " overflows uint64");
    }
    *out = base * mul;
    return Status::OK();
}

Status QosContractResolver::parseUint64(const json& node, uint64_t* out,
                                        const std::string& path) {
    if (!out) return Status::InvalidArgument("uint64 output is null");
    if (node.is_number_unsigned()) {
        *out = node.get<uint64_t>();
        return Status::OK();
    }
    if (node.is_number_integer()) {
        auto value = node.get<int64_t>();
        if (value < 0) return Status::InvalidArgument(path + " must be >= 0");
        *out = static_cast<uint64_t>(value);
        return Status::OK();
    }
    return Status::InvalidArgument(path + " must be unsigned integer");
}

Status QosContractResolver::parsePolicyFields(const json& node,
                                              QosPolicyFields* out,
                                              const std::string& path) {
    if (!out) return Status::InvalidArgument("policy fields output is null");
    if (!node.is_object()) {
        return Status::InvalidArgument(path + " must be an object");
    }
    static const std::vector<std::string> kAllowedPolicyKeys = {
        "priority", "max_inflight_bytes", "max_inflight_requests",
        "allowed_degraded_actions"};
    CHECK_STATUS(validateKnownKeys(node, kAllowedPolicyKeys, path));

    if (node.contains("priority")) {
        int priority = PRIO_LOW;
        CHECK_STATUS(
            parsePriority(node["priority"], &priority, path + ".priority"));
        out->priority = priority;
    }

    auto parse_bytes_field = [&](const char* key,
                                 std::optional<uint64_t>* dst) -> Status {
        if (!node.contains(key)) return Status::OK();
        uint64_t bytes = 0;
        CHECK_STATUS(parseBytes(node[key], &bytes, path + "." + key));
        *dst = bytes;
        return Status::OK();
    };
    auto parse_uint64_field = [&](const char* key,
                                  std::optional<uint64_t>* dst) -> Status {
        if (!node.contains(key)) return Status::OK();
        uint64_t value = 0;
        CHECK_STATUS(parseUint64(node[key], &value, path + "." + key));
        *dst = value;
        return Status::OK();
    };
    CHECK_STATUS(
        parse_bytes_field("max_inflight_bytes", &out->max_inflight_bytes));
    CHECK_STATUS(parse_uint64_field("max_inflight_requests",
                                    &out->max_inflight_requests));

    if (node.contains("allowed_degraded_actions")) {
        if (!node["allowed_degraded_actions"].is_array()) {
            return Status::InvalidArgument(
                path + ".allowed_degraded_actions must be array");
        }
        std::vector<std::string> actions;
        for (const auto& action_node : node["allowed_degraded_actions"]) {
            if (!action_node.is_string()) {
                return Status::InvalidArgument(
                    path + ".allowed_degraded_actions entry must be string");
            }
            auto action = normalizeKey(action_node.get<std::string>());
            if (!isKnownDegradedAction(action)) {
                return Status::InvalidArgument(
                    path + " unknown degraded action: " + action);
            }
            actions.push_back(action);
        }
        out->allowed_degraded_actions = std::move(actions);
    }

    return Status::OK();
}

void QosContractResolver::mergeFields(QosPolicyFields* dst,
                                      const QosPolicyFields& src) {
    if (src.priority) dst->priority = src.priority;
    if (src.max_inflight_bytes)
        dst->max_inflight_bytes = src.max_inflight_bytes;
    if (src.max_inflight_requests)
        dst->max_inflight_requests = src.max_inflight_requests;
    if (src.allowed_degraded_actions) {
        dst->allowed_degraded_actions = src.allowed_degraded_actions;
    }
}

Status QosContractResolver::loadFromConfig(const Config& config) {
    enabled_ = false;
    global_defaults_ = QosPolicyFields{};
    tenants_.clear();

    try {
        json qos = config.get<json>("qos", json{});
        if (qos.is_null() || qos.empty()) return Status::OK();
        if (!qos.is_object()) {
            return Status::InvalidArgument("qos must be an object");
        }
        static const std::vector<std::string> kAllowedQosKeys = {
            "version", "defaults", "tenants"};
        CHECK_STATUS(validateKnownKeys(qos, kAllowedQosKeys, "qos"));

        if (qos.contains("version") && !qos["version"].is_number_integer()) {
            return Status::InvalidArgument("qos.version must be an integer");
        }
        int version = qos.value("version", 1);
        if (version != 1) {
            return Status::InvalidArgument("unsupported qos.version: " +
                                           std::to_string(version));
        }
        if (qos.contains("defaults")) {
            CHECK_STATUS(parsePolicyFields(qos["defaults"], &global_defaults_,
                                           "qos.defaults"));
        }

        if (qos.contains("tenants")) {
            if (!qos["tenants"].is_array()) {
                return Status::InvalidArgument("qos.tenants must be an array");
            }
            static const std::vector<std::string> kAllowedTenantKeys = {
                "name", "defaults", "intents"};
            for (size_t i = 0; i < qos["tenants"].size(); ++i) {
                const auto& tenant_node = qos["tenants"][i];
                const std::string path =
                    "qos.tenants[" + std::to_string(i) + "]";
                if (!tenant_node.is_object()) {
                    return Status::InvalidArgument(path + " must be an object");
                }
                CHECK_STATUS(
                    validateKnownKeys(tenant_node, kAllowedTenantKeys, path));
                if (!tenant_node.contains("name") ||
                    !tenant_node["name"].is_string()) {
                    return Status::InvalidArgument(path + ".name is required");
                }
                auto tenant_name =
                    normalizeKey(tenant_node["name"].get<std::string>());
                if (tenant_name.empty()) {
                    return Status::InvalidArgument(path + ".name is empty");
                }
                if (tenants_.contains(tenant_name)) {
                    return Status::InvalidArgument("duplicate qos tenant: " +
                                                   tenant_name);
                }

                TenantContract tenant;
                if (tenant_node.contains("defaults")) {
                    CHECK_STATUS(parsePolicyFields(tenant_node["defaults"],
                                                   &tenant.defaults,
                                                   path + ".defaults"));
                }
                if (tenant_node.contains("intents")) {
                    if (!tenant_node["intents"].is_object()) {
                        return Status::InvalidArgument(
                            path + ".intents must be an object");
                    }
                    for (auto it = tenant_node["intents"].begin();
                         it != tenant_node["intents"].end(); ++it) {
                        QosPolicyFields fields;
                        const auto intent = normalizeKey(it.key());
                        CHECK_STATUS(
                            parsePolicyFields(it.value(), &fields,
                                              path + ".intents." + it.key()));
                        tenant.intents[intent] = std::move(fields);
                    }
                }
                tenants_[tenant_name] = std::move(tenant);
            }
        }

        enabled_ = qos.contains("defaults") || qos.contains("tenants");
        return Status::OK();
    } catch (const std::exception& e) {
        enabled_ = false;
        global_defaults_ = QosPolicyFields{};
        tenants_.clear();
        return Status::InvalidArgument(
            std::string("failed to parse qos config: ") + e.what());
    }
}

Status QosContractResolver::resolve(const QosRequestContext& context,
                                    EffectiveQosPolicy* out) const {
    if (!out) return Status::InvalidArgument("effective qos output is null");
    *out = EffectiveQosPolicy{};
    out->tenant_id =
        normalizeKey(context.tenant_id.empty() ? "default" : context.tenant_id);
    out->intent =
        normalizeKey(context.intent.empty() ? "unspec" : context.intent);
    out->requested_priority = context.requested_priority;
    out->effective_priority = isValidPriority(context.requested_priority)
                                  ? context.requested_priority
                                  : PRIO_LOW;

    if (!enabled_) return Status::OK();

    out->enabled = true;
    QosPolicyFields fields = global_defaults_;
    out->matched_contract = "global_default";

    auto tenant_it = tenants_.find(out->tenant_id);
    if (tenant_it != tenants_.end()) {
        mergeFields(&fields, tenant_it->second.defaults);
        out->matched_contract = out->tenant_id + ".default";
        auto intent_it = tenant_it->second.intents.find(out->intent);
        if (intent_it != tenant_it->second.intents.end()) {
            mergeFields(&fields, intent_it->second);
            out->matched = true;
            out->matched_contract = out->tenant_id + "." + out->intent;
        }
    }

    static_cast<QosPolicyFields&>(*out) = fields;
    if (out->priority) out->effective_priority = *out->priority;
    return Status::OK();
}

void QosContractResolver::fieldsToJson(json* out,
                                       const QosPolicyFields& fields) {
    if (fields.priority) (*out)["priority"] = *fields.priority;
    if (fields.max_inflight_bytes) {
        (*out)["max_inflight_bytes"] = *fields.max_inflight_bytes;
    }
    if (fields.max_inflight_requests) {
        (*out)["max_inflight_requests"] = *fields.max_inflight_requests;
    }
    if (fields.allowed_degraded_actions) {
        (*out)["allowed_degraded_actions"] = *fields.allowed_degraded_actions;
    }
}

std::string QosContractResolver::explainJson(const EffectiveQosPolicy& policy,
                                             int indent) const {
    json out;
    out["enabled"] = policy.enabled;
    out["matched"] = policy.matched;
    out["tenant_id"] = policy.tenant_id;
    out["intent"] = policy.intent;
    out["matched_contract"] = policy.matched_contract;
    out["requested_priority"] = policy.requested_priority;
    out["effective_priority"] = policy.effective_priority;
    out["diagnostic_scope"] = "resolution_only";
    fieldsToJson(&out, policy);
    return out.dump(indent);
}

}  // namespace tent
}  // namespace mooncake
