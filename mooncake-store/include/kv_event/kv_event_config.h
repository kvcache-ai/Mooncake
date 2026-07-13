#pragma once

#include <cstdint>
#include <string>

namespace mooncake {

// Publisher transport, identity, and fixed model context for the optional KV
// Events ZMQ socket (RFC #1527). One publisher serves one model/block-size/
// LoRA/DP context. Per-object fields such as tenant_id, parent_hash, and
// token_ids are still derived from each operation when available.
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
    // Max pending events in the async publisher queue; oldest dropped when full.
    uint32_t queue_capacity{65536};

    // Fixed publisher context. A model parsed from an object key takes
    // precedence over model_name; model_name is also used for keys without a
    // recognized connector format and for cleared events.
    std::string model_name;
    // Retained for config compatibility. Published tenant_id comes from each
    // object operation so one publisher can preserve tenant isolation.
    std::string tenant_id{"default"};
    std::string additional_salt;
    // Empty means the base model.
    std::string lora_name;
    // Zero means unknown and is emitted as nil.
    uint32_t block_size{0};
    uint32_t dp_rank{0};
};

}  // namespace mooncake
