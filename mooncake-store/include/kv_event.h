#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace mooncake {

/**
 * @brief Type of a Dynamo-compatible KV cache event published by Mooncake.
 */
enum class KvEventType {
    kBlockStored,
    kBlockRemoved,
};

/**
 * @brief A single Dynamo-compatible KV cache event.
 *
 * Field semantics follow the Dynamo ZMQ relay "map event" wire format plus the
 * Mooncake extension fields (source / tenant_id / group_id / worker_id /
 * event_id). The encoder in kv_event.cpp serializes this into the msgpack map
 * that the Dynamo Mooncake adapter consumes.
 *
 * BlockStored uses all fields; BlockRemoved only needs the identity fields plus
 * block_hashes and medium (token_ids/block_size/parent_block_hash are ignored).
 */
struct KvEvent {
    KvEventType type{KvEventType::kBlockStored};

    // Identity / Mooncake extension fields.
    std::string event_id;
    std::string source{"mooncake"};
    std::string tenant_id;
    std::string group_id;
    std::string worker_id;

    // Dynamo core fields.
    std::vector<uint64_t> block_hashes;
    std::string medium{"EXTERNAL"};

    // BlockStored-only fields.
    std::optional<uint64_t> parent_block_hash;
    std::vector<uint32_t> token_ids;
    uint32_t block_size{0};
    std::string lora_name;
    std::string model_name;
    std::optional<uint32_t> dp_rank;

    bool operator==(const KvEvent& other) const {
        return type == other.type && event_id == other.event_id &&
               source == other.source && tenant_id == other.tenant_id &&
               group_id == other.group_id && worker_id == other.worker_id &&
               block_hashes == other.block_hashes && medium == other.medium &&
               parent_block_hash == other.parent_block_hash &&
               token_ids == other.token_ids && block_size == other.block_size &&
               lora_name == other.lora_name && model_name == other.model_name &&
               dp_rank == other.dp_rank;
    }
};

/**
 * @brief Abstract sink for Dynamo-compatible KV events.
 *
 * The master calls Publish() when a logical KV block transitions between
 * complete/incomplete. Implementations decide the transport (e.g. a single ZMQ
 * PUB stream). Tests use an in-memory mock. A null publisher means events are
 * disabled and Mooncake behavior is unchanged.
 */
class KvEventPublisher {
   public:
    virtual ~KvEventPublisher() = default;
    virtual void Publish(const KvEvent& event) = 0;
};

// --- Encoding / decoding (msgpack) -----------------------------------------
//
// These produce / consume the Dynamo ZMQ relay wire format. Returning byte
// strings keeps msgpack out of widely-included headers.

/**
 * @brief Encode one event into the Dynamo "map event" msgpack representation.
 */
std::string EncodeKvEventMap(const KvEvent& event);

/**
 * @brief Encode a batch of events into the ZMQ relay payload:
 * [timestamp (f64), [events], dp_rank (i32, optional)].
 */
std::string EncodeKvEventBatchPayload(const std::vector<KvEvent>& events,
                                      double timestamp,
                                      std::optional<int32_t> dp_rank);

/**
 * @brief Decode one msgpack "map event". Returns std::nullopt on malformed
 * input or unsupported event type. Intended for tests / adapters.
 */
std::optional<KvEvent> DecodeKvEventMap(const std::string& bytes);

/**
 * @brief Result of decoding a ZMQ relay batch payload.
 */
struct KvEventBatch {
    double timestamp{0.0};
    std::vector<KvEvent> events;
    std::optional<int32_t> dp_rank;
};

/**
 * @brief Decode a ZMQ relay payload produced by EncodeKvEventBatchPayload.
 */
std::optional<KvEventBatch> DecodeKvEventBatchPayload(const std::string& bytes);

}  // namespace mooncake
