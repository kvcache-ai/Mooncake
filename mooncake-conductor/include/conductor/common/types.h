#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

namespace conductor {
namespace common {

enum class PublisherKind { kVllm, kMooncake };

inline constexpr std::string_view PublisherKindName(PublisherKind kind) {
    switch (kind) {
        case PublisherKind::kVllm:
            return "vLLM";
        case PublisherKind::kMooncake:
            return "Mooncake";
    }
    return "unknown";
}

inline std::optional<PublisherKind> ParsePublisherKind(std::string_view value) {
    if (value == "vLLM") {
        return PublisherKind::kVllm;
    }
    if (value == "Mooncake") {
        return PublisherKind::kMooncake;
    }
    return std::nullopt;
}

struct HashProfileConfig {
    std::string strategy;
    std::string algorithm;
    std::string root_digest;
    std::string index_projection;

    bool operator==(const HashProfileConfig&) const = default;
};

struct ServiceConfig {
    std::string endpoint;         // kv publisher endpoint
    std::string replay_endpoint;  // replay publisher endpoint
    PublisherKind publisher_kind = PublisherKind::kVllm;
    std::string model_name;  // Model name hosted by the service
    std::string lora_name;
    std::string tenant_id;    // (optional), default use 'default'
    std::string instance_id;  // required
    int64_t block_size = 0;
    int dp_rank = 0;
    std::optional<int64_t> cache_group;
    HashProfileConfig hash_profile;

    bool operator==(const ServiceConfig&) const = default;
};

}  // namespace common
}  // namespace conductor
