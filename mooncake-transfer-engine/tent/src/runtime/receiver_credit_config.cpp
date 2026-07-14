// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_config.h"

#include <exception>
#include <initializer_list>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

namespace mooncake::tent {
namespace {

constexpr size_t kMaxPeers = 65536;
constexpr size_t kMaxDispatchOwners = 65536;
constexpr size_t kDataBytesIndex =
    static_cast<size_t>(CreditResource::DataBytes) - 1;
constexpr size_t kRequestSlotsIndex =
    static_cast<size_t>(CreditResource::RequestSlots) - 1;

Status invalidConfig(const std::string& path, std::string_view reason) {
    return Status::InvalidArgument("invalid receiver-credit config at " + path +
                                   ": " + std::string(reason) + LOC_MARK);
}

bool isKnownKey(std::string_view key,
                std::initializer_list<std::string_view> known) {
    for (auto candidate : known) {
        if (key == candidate) return true;
    }
    return false;
}

Status rejectUnknownKeys(const json& object, const std::string& path,
                         std::initializer_list<std::string_view> known) {
    for (auto it = object.begin(); it != object.end(); ++it) {
        if (!isKnownKey(it.key(), known))
            return invalidConfig(path + "." + it.key(), "unknown key");
    }
    return Status::OK();
}

Status requireObject(const json& value, const std::string& path) {
    if (!value.is_object()) return invalidConfig(path, "expected object");
    return Status::OK();
}

Status readPositiveUint64(const json& object, const char* key,
                          const std::string& path, uint64_t& output) {
    auto it = object.find(key);
    if (it == object.end())
        return invalidConfig(path + "." + key, "missing required key");
    if (!it->is_number_unsigned())
        return invalidConfig(path + "." + key, "expected a positive integer");
    uint64_t value = it->get<uint64_t>();
    if (value == 0)
        return invalidConfig(path + "." + key, "must be greater than zero");
    output = value;
    return Status::OK();
}

Status readPositiveUint32(const json& object, const char* key,
                          const std::string& path, uint32_t& output) {
    auto it = object.find(key);
    if (it == object.end()) return Status::OK();
    if (!it->is_number_unsigned())
        return invalidConfig(path + "." + key, "expected a positive integer");
    uint64_t value = it->get<uint64_t>();
    if (value == 0 || value > std::numeric_limits<uint32_t>::max())
        return invalidConfig(path + "." + key, "out of range");
    output = static_cast<uint32_t>(value);
    return Status::OK();
}

Status readPositiveSize(const json& object, const char* key,
                        const std::string& path, size_t maximum,
                        size_t& output) {
    auto it = object.find(key);
    if (it == object.end()) return Status::OK();
    if (!it->is_number_unsigned())
        return invalidConfig(path + "." + key, "expected a positive integer");
    uint64_t value = it->get<uint64_t>();
    if (value == 0 || value > maximum)
        return invalidConfig(path + "." + key, "out of range");
    output = static_cast<size_t>(value);
    return Status::OK();
}

Status readMode(const json& value, CreditRolloutMode& output) {
    if (!value.is_string())
        return invalidConfig("receiver_credit.mode", "expected string");
    const auto& mode = value.get_ref<const std::string&>();
    if (mode == "disabled") {
        output = CreditRolloutMode::Disabled;
    } else if (mode == "optional") {
        output = CreditRolloutMode::Optional;
    } else if (mode == "required") {
        output = CreditRolloutMode::Required;
    } else {
        return invalidConfig("receiver_credit.mode",
                             "expected disabled, optional, or required");
    }
    return Status::OK();
}

CreditRolloutMode legacyMode(bool enabled) {
    return enabled ? CreditRolloutMode::Required : CreditRolloutMode::Disabled;
}

Status mergeLegacyMode(std::optional<CreditRolloutMode>& result,
                       CreditRolloutMode candidate,
                       const std::string& source_path) {
    if (result && *result != candidate)
        return invalidConfig(source_path,
                             "conflicts with another rollout setting");
    result = candidate;
    return Status::OK();
}

Status parseResourcePair(const json& section, const char* key,
                         const std::string& path,
                         std::array<uint64_t, kCreditResourceCount>& output,
                         bool required, bool& present) {
    auto it = section.find(key);
    present = it != section.end();
    if (!present) {
        if (required) return invalidConfig(path, "missing required object");
        return Status::OK();
    }
    CHECK_STATUS(requireObject(*it, path));
    CHECK_STATUS(rejectUnknownKeys(*it, path, {"data_bytes", "request_slots"}));
    CHECK_STATUS(
        readPositiveUint64(*it, "data_bytes", path, output[kDataBytesIndex]));
    CHECK_STATUS(readPositiveUint64(*it, "request_slots", path,
                                    output[kRequestSlotsIndex]));
    return Status::OK();
}

}  // namespace

Status loadReceiverCreditConfig(const Config& config,
                                bool runtime_queue_enabled,
                                ReceiverCreditRuntimeConfig& output) {
    ReceiverCreditRuntimeConfig parsed;
    json root;
    try {
        root = json::parse(config.dump());
    } catch (const std::exception& e) {
        return invalidConfig("<root>", e.what());
    }

    // A default-constructed Config is null and is equivalent to an empty
    // object. Other non-object roots are configuration errors.
    if (root.is_null()) root = json::object();
    if (!root.is_object()) return invalidConfig("<root>", "expected object");

    const json* section = nullptr;
    auto section_it = root.find("receiver_credit");
    if (section_it != root.end()) {
        CHECK_STATUS(requireObject(*section_it, "receiver_credit"));
        section = &*section_it;
        CHECK_STATUS(rejectUnknownKeys(
            *section, "receiver_credit",
            {"mode", "enabled", "default_qos_class", "capacity", "grant_batch",
             "control", "limits"}));
    }

    std::optional<CreditRolloutMode> selected_mode;
    if (section) {
        auto mode_it = section->find("mode");
        if (mode_it != section->end()) {
            CreditRolloutMode explicit_mode;
            CHECK_STATUS(readMode(*mode_it, explicit_mode));
            selected_mode = explicit_mode;
        }

        auto enabled_it = section->find("enabled");
        if (enabled_it != section->end()) {
            if (!enabled_it->is_boolean())
                return invalidConfig("receiver_credit.enabled",
                                     "expected boolean");
            CHECK_STATUS(mergeLegacyMode(selected_mode,
                                         legacyMode(enabled_it->get<bool>()),
                                         "receiver_credit.enabled"));
        }
    }

    // Config supports historical top-level keys containing '/'. Validate the
    // JSON type before using Config::get so a malformed alias cannot silently
    // fall back to the supplied default.
    auto flat_enabled_it = root.find("receiver_credit/enabled");
    if (flat_enabled_it != root.end()) {
        if (!flat_enabled_it->is_boolean())
            return invalidConfig("receiver_credit/enabled", "expected boolean");
        bool enabled = flat_enabled_it->get<bool>();
        if (!section || section->find("enabled") == section->end()) {
            if (!config.contains("receiver_credit/enabled"))
                return invalidConfig("receiver_credit/enabled",
                                     "alias lookup failed");
            enabled = config.get("receiver_credit/enabled", !enabled);
        }
        CHECK_STATUS(mergeLegacyMode(selected_mode, legacyMode(enabled),
                                     "receiver_credit/enabled"));
    }

    parsed.mode = selected_mode.value_or(CreditRolloutMode::Disabled);

    if (parsed.mode != CreditRolloutMode::Disabled && !section)
        return invalidConfig("receiver_credit.capacity",
                             "missing required object");

    if (section) {
        auto qos_it = section->find("default_qos_class");
        if (qos_it != section->end()) {
            if (!qos_it->is_number_unsigned())
                return invalidConfig("receiver_credit.default_qos_class",
                                     "expected integer zero");
            auto qos_class = qos_it->get<uint64_t>();
            if (qos_class != 0)
                return invalidConfig("receiver_credit.default_qos_class",
                                     "only QoS class 0 is supported");
            parsed.default_qos_class = 0;
        }

        const bool active = parsed.mode != CreditRolloutMode::Disabled;
        bool capacity_present = false;
        bool grant_present = false;
        CHECK_STATUS(
            parseResourcePair(*section, "capacity", "receiver_credit.capacity",
                              parsed.capacity, active, capacity_present));
        CHECK_STATUS(parseResourcePair(
            *section, "grant_batch", "receiver_credit.grant_batch",
            parsed.max_grant_per_pull, active, grant_present));
        if (capacity_present != grant_present)
            return invalidConfig(
                "receiver_credit",
                "capacity and grant_batch must be specified together");
        if (capacity_present) {
            for (size_t i : {kDataBytesIndex, kRequestSlotsIndex}) {
                if (parsed.max_grant_per_pull[i] > parsed.capacity[i])
                    return invalidConfig(
                        "receiver_credit.grant_batch",
                        "resource grant must not exceed capacity");
            }
        }

        auto control_it = section->find("control");
        if (control_it != section->end()) {
            CHECK_STATUS(requireObject(*control_it, "receiver_credit.control"));
            CHECK_STATUS(
                rejectUnknownKeys(*control_it, "receiver_credit.control",
                                  {"freshness_ttl_ms", "retry_after_us",
                                   "poll_interval_us", "adaptive_dispatch"}));
            CHECK_STATUS(readPositiveUint32(*control_it, "freshness_ttl_ms",
                                            "receiver_credit.control",
                                            parsed.freshness_ttl_ms));
            CHECK_STATUS(readPositiveUint32(*control_it, "retry_after_us",
                                            "receiver_credit.control",
                                            parsed.retry_after_us));
            CHECK_STATUS(readPositiveUint32(*control_it, "poll_interval_us",
                                            "receiver_credit.control",
                                            parsed.progress_interval_us));

            auto adaptive_it = control_it->find("adaptive_dispatch");
            if (adaptive_it != control_it->end()) {
                const std::string path =
                    "receiver_credit.control.adaptive_dispatch";
                CHECK_STATUS(requireObject(*adaptive_it, path));
                CHECK_STATUS(rejectUnknownKeys(
                    *adaptive_it, path,
                    {"enabled", "min_owners", "initial_owners", "max_owners",
                     "slow_rtt_us", "healthy_pulls_per_increase"}));
                auto enabled_it = adaptive_it->find("enabled");
                if (enabled_it != adaptive_it->end()) {
                    if (!enabled_it->is_boolean())
                        return invalidConfig(path + ".enabled",
                                             "expected boolean");
                    parsed.adaptive_dispatch_enabled = enabled_it->get<bool>();
                }
                CHECK_STATUS(readPositiveSize(
                    *adaptive_it, "min_owners", path, kMaxDispatchOwners,
                    parsed.adaptive_dispatch_min_owners));
                CHECK_STATUS(readPositiveSize(
                    *adaptive_it, "initial_owners", path, kMaxDispatchOwners,
                    parsed.adaptive_dispatch_initial_owners));
                CHECK_STATUS(readPositiveSize(
                    *adaptive_it, "max_owners", path, kMaxDispatchOwners,
                    parsed.adaptive_dispatch_max_owners));
                CHECK_STATUS(
                    readPositiveUint32(*adaptive_it, "slow_rtt_us", path,
                                       parsed.adaptive_dispatch_slow_rtt_us));
                CHECK_STATUS(readPositiveUint32(
                    *adaptive_it, "healthy_pulls_per_increase", path,
                    parsed.adaptive_dispatch_healthy_pulls));
                if (parsed.adaptive_dispatch_min_owners >
                        parsed.adaptive_dispatch_initial_owners ||
                    parsed.adaptive_dispatch_initial_owners >
                        parsed.adaptive_dispatch_max_owners)
                    return invalidConfig(
                        path,
                        "expected min_owners <= initial_owners <= max_owners");
            }
        }

        auto limits_it = section->find("limits");
        if (limits_it != section->end()) {
            CHECK_STATUS(requireObject(*limits_it, "receiver_credit.limits"));
            CHECK_STATUS(rejectUnknownKeys(*limits_it, "receiver_credit.limits",
                                           {"max_peers"}));
            auto max_peers_it = limits_it->find("max_peers");
            if (max_peers_it != limits_it->end()) {
                if (!max_peers_it->is_number_unsigned())
                    return invalidConfig("receiver_credit.limits.max_peers",
                                         "expected a positive integer");
                uint64_t max_peers = max_peers_it->get<uint64_t>();
                if (max_peers == 0 || max_peers > kMaxPeers)
                    return invalidConfig("receiver_credit.limits.max_peers",
                                         "must be in range [1, 65536]");
                parsed.max_peers = static_cast<size_t>(max_peers);
            }
        }
    }

    if (parsed.mode != CreditRolloutMode::Disabled && !runtime_queue_enabled)
        return invalidConfig("receiver_credit.mode",
                             "runtime queue must be enabled");

    const uint64_t ttl_us =
        static_cast<uint64_t>(parsed.freshness_ttl_ms) * 1000;
    if (ttl_us <= parsed.progress_interval_us)
        return invalidConfig("receiver_credit.control.freshness_ttl_ms",
                             "must be longer than control.poll_interval_us");

    output = parsed;
    return Status::OK();
}

}  // namespace mooncake::tent
