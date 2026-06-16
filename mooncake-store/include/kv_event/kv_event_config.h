#pragma once

#include <cstdint>
#include <string>

namespace mooncake {

// Configuration for the optional KV Events publisher (RFC #1527).
struct KvEventConfig {
    bool enabled{false};
    // ZMQ PUB bind address, e.g. "tcp://0.0.0.0:5557".
    std::string bind_endpoint;
    std::string model_name;
    std::string backend_id;
    std::string tenant_id{"default"};
    std::string additional_salt;
    std::string lora_name;
    uint32_t block_size{0};
    uint32_t dp_rank{0};
    // Emit legacy vLLM/SGLang field names alongside RFC #1527 fields.
    bool emit_legacy_compat_fields{true};
    // Emit Mooncake object_key for consumers that match on store key format.
    bool emit_object_key{true};
};

}  // namespace mooncake
