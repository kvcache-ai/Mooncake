// Copyright 2026 KVCache.AI
#include "tent/runtime/hp_tcp_transport_config.h"
#include "tent/common/config_parser.h"

#include <string_view>

namespace mooncake::tent {
namespace {

Status Invalid(const std::string& path, const std::string& detail) {
    return Status::InvalidArgument(path + " " + detail + LOC_MARK);
}

template <typename T>
Status ReadUnsigned(const json& object, std::string_view key, T* output) {
    auto it = object.find(std::string(key));
    if (it == object.end()) return Status::OK();
    return parseUnsignedConfigValue(
        *it, "transports/hp_tcp/" + std::string(key), output);
}

Status ReadString(const json& object, std::string_view key,
                  std::string* output) {
    auto it = object.find(std::string(key));
    if (it == object.end()) return Status::OK();
    if (!it->is_string()) {
        return Invalid("transports/hp_tcp/" + std::string(key),
                       "must be a string");
    }
    *output = it->get<std::string>();
    return Status::OK();
}

Status ReadStringList(const json& object, std::string_view key,
                      std::vector<std::string>* output) {
    auto it = object.find(std::string(key));
    if (it == object.end()) return Status::OK();
    if (!it->is_array()) {
        return Invalid("transports/hp_tcp/" + std::string(key),
                       "must be an array of strings");
    }
    for (const auto& item : *it) {
        if (!item.is_string()) {
            return Invalid("transports/hp_tcp/" + std::string(key),
                           "must be an array of strings");
        }
        output->push_back(item.get<std::string>());
    }
    return Status::OK();
}

}  // namespace

Status ParseHpTcpTransportConfig(const Config& config,
                                 HpTcpTransportConfig* out) {
    if (out == nullptr) {
        return Status::InvalidArgument(
            "HP TCP configuration output is null" LOC_MARK);
    }

    HpTcpTransportConfig parsed;
    std::string subtree;
    if (!config.dumpSubtree("transports/hp_tcp", &subtree)) {
        *out = std::move(parsed);
        return Status::OK();
    }

    json hp_tcp;
    try {
        hp_tcp = json::parse(subtree);
    } catch (const std::exception& error) {
        return Status::MalformedJson(
            std::string("Invalid transports/hp_tcp configuration: ") +
            error.what() + LOC_MARK);
    }
    if (!hp_tcp.is_object()) {
        return Invalid("transports/hp_tcp", "must be an object");
    }

    if (auto it = hp_tcp.find("enable"); it != hp_tcp.end()) {
        CHECK_STATUS(parseBoolConfigValue(*it, "transports/hp_tcp/enable",
                                          &parsed.enabled));
    }
    CHECK_STATUS(
        ReadString(hp_tcp, "bind_address", &parsed.params.bind_address));
    CHECK_STATUS(ReadString(hp_tcp, "advertise_address",
                            &parsed.params.advertise_address));
    CHECK_STATUS(ReadStringList(hp_tcp, "rail_addresses",
                                &parsed.params.rail_addresses));
    CHECK_STATUS(ReadUnsigned(hp_tcp, "port", &parsed.params.port));
    CHECK_STATUS(
        ReadUnsigned(hp_tcp, "worker_count", &parsed.params.worker_count));
    CHECK_STATUS(ReadUnsigned(hp_tcp, "connections_per_peer",
                              &parsed.params.connections_per_peer));
    CHECK_STATUS(ReadUnsigned(hp_tcp, "max_outstanding_tasks",
                              &parsed.params.max_outstanding_tasks));
    CHECK_STATUS(ReadUnsigned(hp_tcp, "max_outstanding_bytes",
                              &parsed.params.max_outstanding_bytes));
    CHECK_STATUS(ReadUnsigned(hp_tcp, "max_transfer_bytes",
                              &parsed.params.max_transfer_bytes));
    CHECK_STATUS(ReadUnsigned(hp_tcp, "connect_timeout_ms",
                              &parsed.params.connect_timeout_ms));
    CHECK_STATUS(ReadUnsigned(hp_tcp, "progress_timeout_ms",
                              &parsed.params.progress_timeout_ms));
    CHECK_STATUS(ReadUnsigned(hp_tcp, "idle_connection_timeout_ms",
                              &parsed.params.idle_connection_timeout_ms));

    const auto& hp = parsed.params;
    if (parsed.enabled &&
        (hp.worker_count == 0 || hp.connections_per_peer == 0 ||
         hp.max_outstanding_tasks == 0 || hp.max_outstanding_bytes == 0 ||
         hp.max_transfer_bytes == 0 || hp.connect_timeout_ms == 0 ||
         hp.progress_timeout_ms == 0 || hp.idle_connection_timeout_ms == 0)) {
        return Invalid("transports/hp_tcp",
                       "contains zero or inconsistent limits");
    }
    CHECK_STATUS(validateTcpTransportSelection(
        config.get("transports/tcp/enable", true), parsed.enabled));

    *out = std::move(parsed);
    return Status::OK();
}

}  // namespace mooncake::tent
