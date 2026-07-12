#pragma once

#include <cstdint>
#include <string>
#include <variant>
#include <vector>

namespace conductor {
namespace zmq {

// Event type string constants — these values appear verbatim in error
// messages such as "unhandled event: BlockUpdate".
inline constexpr const char* kEventTypeBlockStored = "BlockStored";
inline constexpr const char* kEventTypeBlockRemoved = "BlockRemoved";
inline constexpr const char* kEventTypeBlockUpdate = "BlockUpdate";
inline constexpr const char* kEventTypeAllCleared = "AllBlocksCleared";

inline constexpr const char* kSourceMooncake = "mooncake";
inline constexpr const char* kSourceVLLM = "vllm";

struct BlockStoredEvent {
    std::string type = kEventTypeBlockStored;
    // Unix microseconds; 0 == no timestamp. Only the vLLM BlockStored
    // parser assigns it (Mooncake parser and BlockRemoved never do).
    int64_t timestamp_unix_micro = 0;
    std::vector<uint64_t> block_hashes;
    std::vector<int32_t> token_ids;
    uint64_t parent_block_hash = 0;
    int64_t block_size = 0;
    std::string mooncake_key;
    std::vector<std::vector<std::string>> replica_list;
    std::string model_name;
    int64_t lora_id = 0;
    std::string lora_name;
    std::string pod_name;
    std::string medium;
};

struct BlockRemovedEvent {
    std::string type = kEventTypeBlockRemoved;
    int64_t timestamp_unix_micro = 0;
    std::vector<uint64_t> block_hashes;
    std::string model_name;
    std::string pod_name;
    std::string medium;
};

// KVEvent is the sum type for all KV cache events dispatched through the
// ZMQ event pipeline; handlers switch over BlockStoredEvent and
// BlockRemovedEvent (the only concrete types).
using KVEvent = std::variant<BlockStoredEvent, BlockRemovedEvent>;

struct EventBatch {
    std::string source;  // origin of the event batch: "vllm" | "mooncake"
    std::vector<KVEvent> events;
    int64_t data_parallel_rank = -1;
};

}  // namespace zmq
}  // namespace conductor
