#include "kv_event.h"

#include <glog/logging.h>
#include <msgpack.hpp>

#include <cstring>
#include <string>

namespace mooncake {

namespace {

constexpr const char* kBlockStoredTag = "BlockStored";
constexpr const char* kBlockRemovedTag = "BlockRemoved";

// Packs a single event as a Dynamo "map event" into the given packer. Only the
// fields relevant to the event type are emitted.
template <typename Packer>
void PackEventMap(Packer& packer, const KvEvent& event) {
    const bool is_stored = event.type == KvEventType::kBlockStored;

    // Common fields shared by both event types.
    // type, block_hashes, medium, event_id, source, tenant_id, group_id,
    // worker_id => 8 fields. Stored adds parent_block_hash, token_ids,
    // block_size, lora_name, model_name, and optionally dp_rank.
    uint32_t field_count = 8;
    if (is_stored) {
        field_count += 5;  // parent_block_hash, token_ids, block_size,
                           // lora_name, model_name
        if (event.dp_rank.has_value()) {
            field_count += 1;
        }
    }

    packer.pack_map(field_count);

    packer.pack(std::string("type"));
    packer.pack(std::string(is_stored ? kBlockStoredTag : kBlockRemovedTag));

    packer.pack(std::string("block_hashes"));
    packer.pack(event.block_hashes);

    packer.pack(std::string("medium"));
    packer.pack(event.medium);

    packer.pack(std::string("event_id"));
    packer.pack(event.event_id);

    packer.pack(std::string("source"));
    packer.pack(event.source);

    packer.pack(std::string("tenant_id"));
    packer.pack(event.tenant_id);

    packer.pack(std::string("group_id"));
    packer.pack(event.group_id);

    packer.pack(std::string("worker_id"));
    packer.pack(event.worker_id);

    if (is_stored) {
        packer.pack(std::string("parent_block_hash"));
        if (event.parent_block_hash.has_value()) {
            packer.pack(event.parent_block_hash.value());
        } else {
            packer.pack_nil();
        }

        packer.pack(std::string("token_ids"));
        packer.pack(event.token_ids);

        packer.pack(std::string("block_size"));
        packer.pack(event.block_size);

        packer.pack(std::string("lora_name"));
        packer.pack(event.lora_name);

        packer.pack(std::string("model_name"));
        packer.pack(event.model_name);

        if (event.dp_rank.has_value()) {
            packer.pack(std::string("dp_rank"));
            packer.pack(event.dp_rank.value());
        }
    }
}

bool ObjectToString(const msgpack::object& obj, std::string& out) {
    if (obj.type != msgpack::type::STR) {
        return false;
    }
    out.assign(obj.via.str.ptr, obj.via.str.size);
    return true;
}

// Parses a single msgpack map object into a KvEvent. Unknown keys are ignored,
// mirroring the Dynamo decoder's tolerant behavior.
bool DecodeEventObject(const msgpack::object& obj, KvEvent& out) {
    if (obj.type != msgpack::type::MAP) {
        return false;
    }

    std::string type_tag;
    bool has_type = false;
    KvEvent event;

    for (uint32_t i = 0; i < obj.via.map.size; ++i) {
        const msgpack::object& key_obj = obj.via.map.ptr[i].key;
        const msgpack::object& val_obj = obj.via.map.ptr[i].val;
        std::string key;
        if (!ObjectToString(key_obj, key)) {
            continue;
        }

        try {
            if (key == "type") {
                has_type = ObjectToString(val_obj, type_tag);
            } else if (key == "block_hashes") {
                if (val_obj.type == msgpack::type::ARRAY) {
                    val_obj.convert(event.block_hashes);
                }
            } else if (key == "medium") {
                ObjectToString(val_obj, event.medium);
            } else if (key == "event_id") {
                ObjectToString(val_obj, event.event_id);
            } else if (key == "source") {
                ObjectToString(val_obj, event.source);
            } else if (key == "tenant_id") {
                ObjectToString(val_obj, event.tenant_id);
            } else if (key == "group_id") {
                ObjectToString(val_obj, event.group_id);
            } else if (key == "worker_id") {
                ObjectToString(val_obj, event.worker_id);
            } else if (key == "parent_block_hash") {
                if (val_obj.type == msgpack::type::NIL) {
                    event.parent_block_hash = std::nullopt;
                } else {
                    event.parent_block_hash = val_obj.as<uint64_t>();
                }
            } else if (key == "token_ids") {
                if (val_obj.type == msgpack::type::ARRAY) {
                    val_obj.convert(event.token_ids);
                }
            } else if (key == "block_size") {
                event.block_size = val_obj.as<uint32_t>();
            } else if (key == "lora_name") {
                ObjectToString(val_obj, event.lora_name);
            } else if (key == "model_name") {
                ObjectToString(val_obj, event.model_name);
            } else if (key == "dp_rank") {
                if (val_obj.type != msgpack::type::NIL) {
                    event.dp_rank = val_obj.as<uint32_t>();
                }
            }
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to decode kv event field '" << key
                       << "': " << e.what();
            return false;
        }
    }

    if (!has_type) {
        return false;
    }
    if (type_tag == kBlockStoredTag) {
        event.type = KvEventType::kBlockStored;
    } else if (type_tag == kBlockRemovedTag) {
        event.type = KvEventType::kBlockRemoved;
    } else {
        return false;
    }

    out = std::move(event);
    return true;
}

}  // namespace

std::string EncodeKvEventMap(const KvEvent& event) {
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);
    PackEventMap(packer, event);
    return std::string(sbuf.data(), sbuf.size());
}

std::string EncodeKvEventBatchPayload(const std::vector<KvEvent>& events,
                                      double timestamp,
                                      std::optional<int32_t> dp_rank) {
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);

    packer.pack_array(3);
    packer.pack(timestamp);

    packer.pack_array(static_cast<uint32_t>(events.size()));
    for (const auto& event : events) {
        PackEventMap(packer, event);
    }

    if (dp_rank.has_value()) {
        packer.pack(dp_rank.value());
    } else {
        packer.pack_nil();
    }

    return std::string(sbuf.data(), sbuf.size());
}

std::optional<KvEvent> DecodeKvEventMap(const std::string& bytes) {
    try {
        msgpack::object_handle oh = msgpack::unpack(bytes.data(), bytes.size());
        KvEvent event;
        if (!DecodeEventObject(oh.get(), event)) {
            return std::nullopt;
        }
        return event;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to unpack kv event map: " << e.what();
        return std::nullopt;
    }
}

std::optional<KvEventBatch> DecodeKvEventBatchPayload(const std::string& bytes) {
    try {
        msgpack::object_handle oh = msgpack::unpack(bytes.data(), bytes.size());
        const msgpack::object& obj = oh.get();
        if (obj.type != msgpack::type::ARRAY || obj.via.array.size != 3) {
            return std::nullopt;
        }

        KvEventBatch batch;
        const msgpack::object& ts_obj = obj.via.array.ptr[0];
        batch.timestamp = ts_obj.as<double>();

        const msgpack::object& events_obj = obj.via.array.ptr[1];
        if (events_obj.type != msgpack::type::ARRAY) {
            return std::nullopt;
        }
        for (uint32_t i = 0; i < events_obj.via.array.size; ++i) {
            KvEvent event;
            if (!DecodeEventObject(events_obj.via.array.ptr[i], event)) {
                return std::nullopt;
            }
            batch.events.push_back(std::move(event));
        }

        const msgpack::object& dp_obj = obj.via.array.ptr[2];
        if (dp_obj.type != msgpack::type::NIL) {
            batch.dp_rank = dp_obj.as<int32_t>();
        }

        return batch;
    } catch (const std::exception& e) {
        LOG(ERROR) << "Failed to unpack kv event batch: " << e.what();
        return std::nullopt;
    }
}

}  // namespace mooncake
