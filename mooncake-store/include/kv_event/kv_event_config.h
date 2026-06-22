#pragma once

#include <cstdint>
#include <string>

namespace mooncake {

// Publisher transport/identity for the optional KV Events ZMQ socket (RFC
// #1527). Semantic block fields (model_name, block_size, lora_name,
// parent_hash, token_ids, dp_rank) belong on each event payload, not here — see
// https://docs.nvidia.com/dynamo/kv-managers/kv-events-for-custom-engines
struct KvEventConfig {
    bool enabled{false};
    // ZMQ PUB bind address, e.g. "tcp://0.0.0.0:5557".
    std::string bind_endpoint;
    // Identifies the cache owner stream (storage daemon / pool node).
    std::string backend_id;
    // Emit legacy vLLM/SGLang field names alongside RFC #1527 fields.
    bool emit_legacy_compat_fields{true};
    // Emit Mooncake object_key for consumers that match on store key format.
    bool emit_object_key{true};

    // Deprecated: not stamped on events. Indexer registration supplies model,
    // block_size, dp_rank, and hash namespace for the Mooncake publisher.
    std::string model_name;
    std::string tenant_id{"default"};
    std::string additional_salt;
    std::string lora_name;
    uint32_t block_size{0};
    uint32_t dp_rank{0};
};

}  // namespace mooncake
