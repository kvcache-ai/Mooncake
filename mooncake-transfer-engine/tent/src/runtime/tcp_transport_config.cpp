// Copyright 2026 KVCache.AI
#include "tent/runtime/tcp_transport_config.h"

#include <limits>
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
    if (!it->is_number_unsigned() && !it->is_number_integer()) {
        return Invalid("transports/tcp/high_performance/" + std::string(key),
                       "must be an integer");
    }
    if (it->is_number_integer() && it->get<int64_t>() < 0) {
        return Invalid("transports/tcp/high_performance/" + std::string(key),
                       "must be non-negative");
    }
    const auto value = it->get<uint64_t>();
    if (value > std::numeric_limits<T>::max()) {
        return Invalid("transports/tcp/high_performance/" + std::string(key),
                       "is out of range");
    }
    *output = static_cast<T>(value);
    return Status::OK();
}

Status ReadString(const json& object, std::string_view key,
                  std::string* output) {
    auto it = object.find(std::string(key));
    if (it == object.end()) return Status::OK();
    if (!it->is_string()) {
        return Invalid("transports/tcp/high_performance/" + std::string(key),
                       "must be a string");
    }
    *output = it->get<std::string>();
    return Status::OK();
}

}  // namespace

Status ParseTcpTransportConfig(const Config& config, TcpTransportConfig* out) {
    if (out == nullptr) {
        return Status::InvalidArgument(
            "TCP configuration output is null" LOC_MARK);
    }

    TcpTransportConfig parsed;
    std::string subtree;
    if (!config.dumpSubtree("transports/tcp", &subtree)) {
        *out = std::move(parsed);
        return Status::OK();
    }

    json tcp;
    try {
        tcp = json::parse(subtree);
    } catch (const std::exception& error) {
        return Status::MalformedJson(
            std::string("Invalid transports/tcp configuration: ") +
            error.what() + LOC_MARK);
    }
    if (!tcp.is_object()) {
        return Invalid("transports/tcp", "must be an object");
    }

    if (auto it = tcp.find("enable"); it != tcp.end()) {
        if (!it->is_boolean())
            return Invalid("transports/tcp/enable", "must be a boolean");
        parsed.enabled = it->get<bool>();
    }
    if (auto it = tcp.find("implementation"); it != tcp.end()) {
        if (!it->is_string()) {
            return Invalid("transports/tcp/implementation", "must be a string");
        }
        const auto implementation = it->get<std::string>();
        if (implementation == "standard") {
            parsed.implementation = TcpImplementation::kStandard;
        } else if (implementation == "high_performance") {
            parsed.implementation = TcpImplementation::kHighPerformance;
        } else {
            return Invalid("transports/tcp/implementation",
                           "must be standard or high_performance");
        }
    }

    if (auto it = tcp.find("high_performance"); it != tcp.end()) {
        if (!it->is_object()) {
            return Invalid("transports/tcp/high_performance",
                           "must be an object");
        }
        const json& hp = *it;
        CHECK_STATUS(ReadString(hp, "bind_address",
                                &parsed.high_performance.bind_address));
        CHECK_STATUS(ReadString(hp, "advertise_address",
                                &parsed.high_performance.advertise_address));
        CHECK_STATUS(ReadUnsigned(hp, "port", &parsed.high_performance.port));
        CHECK_STATUS(ReadUnsigned(hp, "worker_count",
                                  &parsed.high_performance.worker_count));
        CHECK_STATUS(
            ReadUnsigned(hp, "queue_capacity_per_worker",
                         &parsed.high_performance.queue_capacity_per_worker));
        CHECK_STATUS(
            ReadUnsigned(hp, "connections_per_peer",
                         &parsed.high_performance.connections_per_peer));
        CHECK_STATUS(
            ReadUnsigned(hp, "max_outstanding_tasks",
                         &parsed.high_performance.max_outstanding_tasks));
        CHECK_STATUS(
            ReadUnsigned(hp, "max_outstanding_bytes",
                         &parsed.high_performance.max_outstanding_bytes));
        CHECK_STATUS(ReadUnsigned(hp, "max_transfer_bytes",
                                  &parsed.high_performance.max_transfer_bytes));
        CHECK_STATUS(ReadUnsigned(hp, "chunk_size",
                                  &parsed.high_performance.chunk_size));
        CHECK_STATUS(ReadUnsigned(hp, "connect_timeout_ms",
                                  &parsed.high_performance.connect_timeout_ms));
        CHECK_STATUS(
            ReadUnsigned(hp, "progress_timeout_ms",
                         &parsed.high_performance.progress_timeout_ms));
    }

    const auto& hp = parsed.high_performance;
    if (parsed.enabled &&
        parsed.implementation == TcpImplementation::kHighPerformance &&
        (hp.worker_count == 0 || hp.queue_capacity_per_worker == 0 ||
         hp.connections_per_peer == 0 || hp.max_outstanding_tasks == 0 ||
         hp.max_outstanding_bytes == 0 || hp.max_transfer_bytes == 0 ||
         hp.chunk_size == 0 || hp.chunk_size > hp.max_transfer_bytes ||
         hp.connect_timeout_ms == 0 || hp.progress_timeout_ms == 0)) {
        return Invalid("transports/tcp/high_performance",
                       "contains zero or inconsistent limits");
    }
    *out = std::move(parsed);
    return Status::OK();
}

}  // namespace mooncake::tent
