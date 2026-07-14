#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace conductor {
namespace common {

inline constexpr const char* kServiceTypeVLLM = "vLLM";
inline constexpr const char* kServiceTypeMooncake = "Mooncake";

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
    std::string type;             // kv publisher type, support: vLLM,Mooncake
    std::string model_name;       // Model name hosted by the service
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
