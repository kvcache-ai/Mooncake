#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace conductor {
namespace common {

inline constexpr const char* kServiceTypeVLLM = "vLLM";
inline constexpr const char* kServiceTypeMooncake = "Mooncake";

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
    std::string additional_salt;  // (optional), default use empty string
};

struct StoredEvent {
    std::vector<uint64_t> block_hashes;
    int64_t block_size = 0;
    std::string model_name;
    std::string lora_name;
    std::string instance_id;
    uint64_t parent_block_hash = 0;
    std::vector<int32_t> token_ids;
    std::string medium;
};

struct RemovedEvent {
    std::vector<uint64_t> block_hashes;
    std::string model_name;
    std::string lora_name;
    std::string instance_id;
    int64_t block_size = 0;
    std::string medium;
};

}  // namespace common
}  // namespace conductor
