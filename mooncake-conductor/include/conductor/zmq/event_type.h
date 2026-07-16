#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include "conductor/common/types.h"

namespace conductor {
namespace zmq {

using ExternalHash = std::variant<uint64_t, std::vector<uint8_t>>;

struct VllmStoredEvent {
    std::vector<ExternalHash> block_hashes;
    std::optional<ExternalHash> parent_block_hash;
    std::optional<std::vector<int32_t>> token_ids;
    int64_t block_size = 0;
    std::optional<int64_t> lora_id;
    std::optional<std::string> medium;
    std::optional<std::string> lora_name;
    bool extra_keys_present = false;
    std::optional<int64_t> group_idx;
    std::optional<std::string> kv_cache_spec_kind;
    std::optional<int64_t> kv_cache_spec_sliding_window;
};

struct VllmRemovedEvent {
    std::vector<ExternalHash> block_hashes;
    std::optional<std::string> medium;
    std::optional<int64_t> group_idx;
};

struct VllmClearedEvent {};

using VllmEvent =
    std::variant<VllmStoredEvent, VllmRemovedEvent, VllmClearedEvent>;

struct MooncakeEventFields {
    uint64_t event_id = 0;
    int64_t timestamp_milliseconds = 0;
    std::optional<std::string> model_name;
    std::optional<int64_t> block_size;
    std::optional<std::string> additional_salt;
    std::optional<std::string> lora_name;
    std::string tenant_id;
    std::string backend_id;
    std::optional<std::string> medium;
    int64_t data_parallel_rank = 0;
};

struct MooncakeObjectFields {
    std::optional<std::string> group_id;
    std::optional<std::string> object_key;
    std::optional<std::string> connector_block_hash;
    std::optional<std::string> cache_prefix;
    std::optional<int64_t> tp_rank;
    std::optional<int64_t> head_or_tp_rank;
    std::optional<int64_t> pcp_rank;
    std::optional<int64_t> dcp_rank;
    std::optional<int64_t> pp_rank;
    std::optional<int64_t> layer_id;
    std::vector<uint64_t> seq_hashes;
    std::optional<std::vector<uint64_t>> legacy_block_hashes;
    std::optional<int64_t> base_block_idx;
};

struct MooncakeStoredEvent {
    MooncakeEventFields fields;
    MooncakeObjectFields object;
    std::optional<uint64_t> parent_hash;
    std::optional<std::vector<int32_t>> token_ids;
};

struct MooncakeRemovedEvent {
    MooncakeEventFields fields;
    MooncakeObjectFields object;
};

struct MooncakeClearedEvent {
    MooncakeEventFields fields;
};

using MooncakeEvent = std::variant<MooncakeStoredEvent, MooncakeRemovedEvent,
                                   MooncakeClearedEvent>;

template <typename Event>
struct DecodedEvent {
    std::optional<Event> event;
    std::string error;

    bool ok() const { return event.has_value(); }
};

struct VllmEventBatch {
    double timestamp_seconds = 0;
    std::vector<DecodedEvent<VllmEvent>> events;
    std::optional<int64_t> data_parallel_rank;
};

struct MooncakeEventBatch {
    int64_t timestamp_milliseconds = 0;
    std::vector<DecodedEvent<MooncakeEvent>> events;
    std::optional<int64_t> data_parallel_rank;
};

using DecodedBatch = std::variant<VllmEventBatch, MooncakeEventBatch>;

struct MessageMetadata {
    common::PublisherKind publisher_kind = common::PublisherKind::kVllm;
    std::string endpoint;
    std::string topic;
    int64_t sequence = -1;
};

}  // namespace zmq
}  // namespace conductor
