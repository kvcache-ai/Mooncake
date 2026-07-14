#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <limits>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "kv_event/key_util.h"
#include "kv_event/kv_event_publisher.h"

#if defined(MOONCAKE_ENABLE_KV_EVENTS) && MOONCAKE_ENABLE_KV_EVENTS
#include <msgpack.hpp>
#include <zmq.h>
#endif

namespace mooncake {
namespace {

TEST(KvEventKeyUtilTest, ParseSeqHashFromObjectKey) {
    EXPECT_EQ(ParseSeqHashFromObjectKey("12345"), 12345u);
    EXPECT_EQ(ParseSeqHashFromObjectKey("0x2a"), 42u);
    EXPECT_EQ(ParseSeqHashFromObjectKey("0XFF"), 255u);
    EXPECT_FALSE(ParseSeqHashFromObjectKey("").has_value());
    EXPECT_FALSE(ParseSeqHashFromObjectKey("not-a-hash").has_value());
    EXPECT_FALSE(ParseSeqHashFromObjectKey("123abc").has_value());
    EXPECT_FALSE(ParseSeqHashFromObjectKey("-1").has_value());
    EXPECT_FALSE(ParseSeqHashFromObjectKey("0x").has_value());
    EXPECT_FALSE(ParseSeqHashFromObjectKey("18446744073709551616").has_value());
    EXPECT_EQ(KvEventPublisher::ParseSeqHashFromObjectKey("99"), 99u);
}

TEST(KvEventKeyUtilTest, ParsesVllmAndVllmAscendConnectorKeys) {
    const auto vllm = ParseKvEventKey(
        "deployment-a@model-a@tp_rank:2@pcp1@dcp0@pp_rank:3@group:4@"
        "0123456789abcdef000000000000002a");
    ASSERT_TRUE(vllm.has_value());
    EXPECT_EQ(vllm->cache_prefix, "deployment-a");
    EXPECT_EQ(vllm->model_name, "model-a");
    EXPECT_EQ(vllm->block_hash,
              "0123456789abcdef000000000000002a");
    EXPECT_EQ(vllm->seq_hash, 42u);
    EXPECT_EQ(vllm->group_id, 4);
    EXPECT_EQ(vllm->tp_rank, 2);
    EXPECT_EQ(vllm->pcp_rank, 1);
    EXPECT_EQ(vllm->dcp_rank, 0);
    EXPECT_EQ(vllm->pp_rank, 3);
    EXPECT_FALSE(vllm->head_or_tp_rank.has_value());
    EXPECT_FALSE(vllm->layer_id.has_value());

    const auto historical = ParseKvEventKey(
        "model-b@tp_rank:0@pcp0@dcp0@pp_rank:0@000000000000002b");
    ASSERT_TRUE(historical.has_value());
    EXPECT_EQ(historical->model_name, "model-b");
    EXPECT_EQ(historical->block_hash, "000000000000002b");
    EXPECT_EQ(historical->seq_hash, 43u);
    EXPECT_FALSE(historical->group_id.has_value());

    const auto ascend = ParseKvEventKey(
        "model-c@pcp0@dcp1@head_or_tp_rank:2@pp_rank:3@"
        "0123456789abcdef000000000000002c");
    ASSERT_TRUE(ascend.has_value());
    EXPECT_EQ(ascend->model_name, "model-c");
    EXPECT_EQ(ascend->block_hash,
              "0123456789abcdef000000000000002c");
    EXPECT_EQ(ascend->seq_hash, 44u);
    EXPECT_EQ(ascend->head_or_tp_rank, 2);
    EXPECT_EQ(ascend->pcp_rank, 0);
    EXPECT_EQ(ascend->dcp_rank, 1);
    EXPECT_EQ(ascend->pp_rank, 3);
    EXPECT_FALSE(ascend->tp_rank.has_value());
    EXPECT_FALSE(ascend->layer_id.has_value());

    const auto layerwise = ParseKvEventKey(
        "model-d@pcp0@dcp0@head_or_tp_rank:1@"
        "0123456789abcdef000000000000002d@7");
    ASSERT_TRUE(layerwise.has_value());
    EXPECT_EQ(layerwise->model_name, "model-d");
    EXPECT_EQ(layerwise->block_hash,
              "0123456789abcdef000000000000002d");
    EXPECT_EQ(layerwise->seq_hash, 45u);
    EXPECT_EQ(layerwise->head_or_tp_rank, 1);
    EXPECT_EQ(layerwise->layer_id, 7);
    EXPECT_FALSE(layerwise->pp_rank.has_value());

    const auto opaque_hash = ParseKvEventKey(
        "model-e@pcp0@dcp0@head_or_tp_rank:0@pp_rank:0@not-a-hash");
    ASSERT_TRUE(opaque_hash.has_value());
    EXPECT_FALSE(opaque_hash->seq_hash.has_value());
    EXPECT_FALSE(ParseKvEventKey("unstructured-key").has_value());
    EXPECT_FALSE(ParseKvEventKey(
                     "@pcp0@dcp0@head_or_tp_rank:0@pp_rank:0@1")
                     .has_value());

    const auto same_low64 = ParseKvEventKey(
        "model-f@tp_rank:0@pcp0@dcp0@pp_rank:0@"
        "fedcba9876543210000000000000002a");
    ASSERT_TRUE(same_low64.has_value());
    EXPECT_EQ(same_low64->seq_hash, vllm->seq_hash);
    EXPECT_NE(same_low64->block_hash, vllm->block_hash);
}

TEST(KvEventPublisherTest, DisabledPublisherIsNoop) {
    KvEventConfig config;
    config.enabled = false;
    KvEventPublisher publisher(config);
    EXPECT_FALSE(publisher.enabled());
    publisher.PublishStored("42", "cpu");
    publisher.PublishRemoved("42", "cpu");
    const auto stats = publisher.GetStats();
    EXPECT_EQ(stats.published_events, 0u);
    EXPECT_EQ(stats.dropped_events, 0u);
}

#if defined(MOONCAKE_ENABLE_KV_EVENTS) && MOONCAKE_ENABLE_KV_EVENTS

namespace {

std::string MakeIpcEndpoint() {
    return "ipc:///tmp/kv_event_test_" + std::to_string(getpid()) + "_" +
           std::to_string(
               std::chrono::steady_clock::now().time_since_epoch().count());
}

bool ReceiveZmqMultipart(void* socket, std::vector<std::string>& frames) {
    frames.clear();
    while (true) {
        zmq_msg_t msg;
        if (zmq_msg_init(&msg) != 0) {
            return false;
        }
        const int rc = zmq_msg_recv(&msg, socket, 0);
        if (rc < 0) {
            zmq_msg_close(&msg);
            return false;
        }
        const char* data = static_cast<const char*>(zmq_msg_data(&msg));
        frames.emplace_back(data, data + zmq_msg_size(&msg));
        const int more = zmq_msg_more(&msg);
        zmq_msg_close(&msg);
        if (!more) {
            break;
        }
    }
    return true;
}

const msgpack::object* FindMapValue(const msgpack::object& map,
                                    const std::string& field) {
    if (map.type != msgpack::type::MAP) {
        return nullptr;
    }
    for (uint32_t i = 0; i < map.via.map.size; ++i) {
        const auto& key = map.via.map.ptr[i].key;
        if (key.type == msgpack::type::STR &&
            std::string(key.via.str.ptr, key.via.str.size) == field) {
            return &map.via.map.ptr[i].val;
        }
    }
    return nullptr;
}

std::string MsgpackString(const msgpack::object& value) {
    EXPECT_EQ(value.type, msgpack::type::STR);
    return value.type == msgpack::type::STR
               ? std::string(value.via.str.ptr, value.via.str.size)
               : std::string{};
}

}  // namespace

TEST(KvEventPublisherTest, PublishesSglangObjectKeyOverZmq) {
    const std::string endpoint = MakeIpcEndpoint();
    const std::string object_key =
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855_0_k";
    const std::string group_id =
        "sglang-hicache:"
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = endpoint;
    config.backend_id = "mooncake-test";
    config.model_name = "configured-model";
    config.block_size = 128;
    config.additional_salt = "salt-a";
    config.lora_name = "adapter-a";
    config.dp_rank = 5;
    config.emit_object_key = true;
    config.emit_legacy_compat_fields = true;
    config.queue_capacity = 64;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());

    void* ctx = zmq_ctx_new();
    ASSERT_NE(ctx, nullptr);
    void* sub = zmq_socket(ctx, ZMQ_SUB);
    ASSERT_NE(sub, nullptr);
    const int timeout_ms = 2000;
    ASSERT_EQ(
        zmq_setsockopt(sub, ZMQ_RCVTIMEO, &timeout_ms, sizeof(timeout_ms)), 0);
    ASSERT_EQ(zmq_connect(sub, endpoint.c_str()), 0);
    ASSERT_EQ(zmq_setsockopt(sub, ZMQ_SUBSCRIBE, "", 0), 0);

    // Allow SUB connect before first publish propagates.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    publisher.PublishStored(object_key, "cpu", "tenant-a", group_id);

    std::vector<std::string> frames;
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames)) << zmq_strerror(zmq_errno());
    ASSERT_EQ(frames.size(), 3u);
    EXPECT_TRUE(frames[0].empty());
    ASSERT_EQ(frames[1].size(), sizeof(uint64_t));

    const auto object_handle =
        msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& root = object_handle.get();
    ASSERT_EQ(root.type, msgpack::type::ARRAY);
    ASSERT_EQ(root.via.array.size, 3u);

    const auto& events = root.via.array.ptr[1];
    ASSERT_EQ(events.type, msgpack::type::ARRAY);
    ASSERT_EQ(events.via.array.size, 1u);

    const auto& event = events.via.array.ptr[0];
    ASSERT_EQ(event.type, msgpack::type::MAP);

    bool has_object_key = false;
    bool has_group_id = false;
    bool has_empty_seq_hashes = false;
    std::string event_type;
    std::string backend_id;
    std::string tenant_id;
    for (uint32_t i = 0; i < event.via.map.size; ++i) {
        const auto& key = event.via.map.ptr[i].key;
        const auto& val = event.via.map.ptr[i].val;
        ASSERT_EQ(key.type, msgpack::type::STR);
        const std::string field(key.via.str.ptr, key.via.str.size);
        if (field == "object_key") {
            ASSERT_EQ(val.type, msgpack::type::STR);
            EXPECT_EQ(std::string(val.via.str.ptr, val.via.str.size),
                      object_key);
            has_object_key = true;
        } else if (field == "group_id") {
            ASSERT_EQ(val.type, msgpack::type::STR);
            EXPECT_EQ(std::string(val.via.str.ptr, val.via.str.size), group_id);
            has_group_id = true;
        } else if (field == "seq_hashes") {
            ASSERT_EQ(val.type, msgpack::type::ARRAY);
            EXPECT_EQ(val.via.array.size, 0u);
            has_empty_seq_hashes = true;
        } else if (field == "event_type") {
            ASSERT_EQ(val.type, msgpack::type::STR);
            event_type = std::string(val.via.str.ptr, val.via.str.size);
        } else if (field == "backend_id") {
            ASSERT_EQ(val.type, msgpack::type::STR);
            backend_id = std::string(val.via.str.ptr, val.via.str.size);
        } else if (field == "tenant_id") {
            ASSERT_EQ(val.type, msgpack::type::STR);
            tenant_id = std::string(val.via.str.ptr, val.via.str.size);
        }
    }

    EXPECT_EQ(event_type, "stored");
    EXPECT_EQ(backend_id, "mooncake-test");
    EXPECT_EQ(tenant_id, "tenant-a");
    EXPECT_TRUE(has_object_key);
    EXPECT_TRUE(has_group_id);
    EXPECT_TRUE(has_empty_seq_hashes);
    const auto* model_name = FindMapValue(event, "model_name");
    const auto* block_size = FindMapValue(event, "block_size");
    const auto* additional_salt = FindMapValue(event, "additional_salt");
    const auto* lora_name = FindMapValue(event, "lora_name");
    const auto* dp_rank = FindMapValue(event, "dp_rank");
    ASSERT_NE(model_name, nullptr);
    ASSERT_NE(block_size, nullptr);
    ASSERT_NE(additional_salt, nullptr);
    ASSERT_NE(lora_name, nullptr);
    ASSERT_NE(dp_rank, nullptr);
    EXPECT_EQ(MsgpackString(*model_name), "configured-model");
    EXPECT_EQ(block_size->as<uint32_t>(), 128u);
    EXPECT_EQ(MsgpackString(*additional_salt), "salt-a");
    EXPECT_EQ(MsgpackString(*lora_name), "adapter-a");
    EXPECT_EQ(dp_rank->as<uint32_t>(), 5u);
    EXPECT_EQ(root.via.array.ptr[2].as<uint32_t>(), 5u);

    // Wait for async worker to finish publishing.
    for (int i = 0; i < 50; ++i) {
        const auto stats = publisher.GetStats();
        if (stats.published_events == 1 && stats.published_batches == 1) {
            EXPECT_EQ(stats.dropped_events, 0u);
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const auto stats = publisher.GetStats();
    EXPECT_EQ(stats.published_events, 1u);
    EXPECT_EQ(stats.published_batches, 1u);
    EXPECT_EQ(stats.dropped_events, 0u);

    zmq_close(sub);
    zmq_ctx_destroy(ctx);
}

TEST(KvEventPublisherTest, PublishesStoreMetadataAndMediumDelta) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = endpoint;
    config.backend_id = "mooncake-test";
    config.model_name = "model-a";
    config.block_size = 64;
    config.additional_salt = "salt-a";
    config.lora_name = "adapter-a";
    config.dp_rank = 4;
    config.emit_object_key = false;
    config.emit_legacy_compat_fields = true;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());

    void* ctx = zmq_ctx_new();
    ASSERT_NE(ctx, nullptr);
    void* sub = zmq_socket(ctx, ZMQ_SUB);
    ASSERT_NE(sub, nullptr);
    const int timeout_ms = 2000;
    ASSERT_EQ(
        zmq_setsockopt(sub, ZMQ_RCVTIMEO, &timeout_ms, sizeof(timeout_ms)), 0);
    ASSERT_EQ(zmq_connect(sub, endpoint.c_str()), 0);
    ASSERT_EQ(zmq_setsockopt(sub, ZMQ_SUBSCRIBE, "", 0), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    const std::string object_key =
        "deployment-a@model-a@tp_rank:2@pcp1@dcp0@pp_rank:3@group:7@"
        "0123456789abcdef000000000000002a";
    publisher.PublishCommitted(object_key, {"cpu"}, "tenant-a", "group-a");

    std::vector<std::string> frames;
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    ASSERT_EQ(frames.size(), 3u);
    auto first_handle = msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& first_events = first_handle.get().via.array.ptr[1];
    ASSERT_EQ(first_events.via.array.size, 1u);
    const auto& stored = first_events.via.array.ptr[0];
    const auto* stored_event_type = FindMapValue(stored, "event_type");
    ASSERT_NE(stored_event_type, nullptr);
    EXPECT_EQ(MsgpackString(*stored_event_type), "stored");
    const auto* stored_model_name = FindMapValue(stored, "model_name");
    ASSERT_NE(stored_model_name, nullptr);
    EXPECT_EQ(MsgpackString(*stored_model_name), "model-a");
    const auto* stored_block_size = FindMapValue(stored, "block_size");
    ASSERT_NE(stored_block_size, nullptr);
    EXPECT_EQ(stored_block_size->as<uint32_t>(), 64u);
    const auto* stored_additional_salt =
        FindMapValue(stored, "additional_salt");
    const auto* stored_lora_name = FindMapValue(stored, "lora_name");
    const auto* stored_dp_rank = FindMapValue(stored, "dp_rank");
    ASSERT_NE(stored_additional_salt, nullptr);
    ASSERT_NE(stored_lora_name, nullptr);
    ASSERT_NE(stored_dp_rank, nullptr);
    EXPECT_EQ(MsgpackString(*stored_additional_salt), "salt-a");
    EXPECT_EQ(MsgpackString(*stored_lora_name), "adapter-a");
    EXPECT_EQ(stored_dp_rank->as<uint32_t>(), 4u);
    EXPECT_EQ(first_handle.get().via.array.ptr[2].as<uint32_t>(), 4u);
    const auto* connector_block_hash =
        FindMapValue(stored, "connector_block_hash");
    ASSERT_NE(connector_block_hash, nullptr);
    EXPECT_EQ(MsgpackString(*connector_block_hash),
              "0123456789abcdef000000000000002a");
    EXPECT_EQ(FindMapValue(stored, "object_key"), nullptr);
    const auto* cache_prefix = FindMapValue(stored, "cache_prefix");
    const auto* tp_rank = FindMapValue(stored, "tp_rank");
    const auto* pcp_rank = FindMapValue(stored, "pcp_rank");
    const auto* dcp_rank = FindMapValue(stored, "dcp_rank");
    const auto* pp_rank = FindMapValue(stored, "pp_rank");
    ASSERT_NE(cache_prefix, nullptr);
    ASSERT_NE(tp_rank, nullptr);
    ASSERT_NE(pcp_rank, nullptr);
    ASSERT_NE(dcp_rank, nullptr);
    ASSERT_NE(pp_rank, nullptr);
    EXPECT_EQ(MsgpackString(*cache_prefix), "deployment-a");
    EXPECT_EQ(tp_rank->as<int64_t>(), 2);
    EXPECT_EQ(pcp_rank->as<int64_t>(), 1);
    EXPECT_EQ(dcp_rank->as<int64_t>(), 0);
    EXPECT_EQ(pp_rank->as<int64_t>(), 3);
    EXPECT_EQ(FindMapValue(stored, "head_or_tp_rank"), nullptr);
    EXPECT_EQ(FindMapValue(stored, "layer_id"), nullptr);
    const auto* seq_hashes = FindMapValue(stored, "seq_hashes");
    ASSERT_NE(seq_hashes, nullptr);
    ASSERT_EQ(seq_hashes->via.array.size, 1u);
    EXPECT_EQ(seq_hashes->via.array.ptr[0].as<uint64_t>(), 42u);
    const auto* base_block_idx = FindMapValue(stored, "base_block_idx");
    ASSERT_NE(base_block_idx, nullptr);
    EXPECT_EQ(base_block_idx->type, msgpack::type::NIL);
    const auto* stored_parent_hash = FindMapValue(stored, "parent_hash");
    ASSERT_NE(stored_parent_hash, nullptr);
    EXPECT_EQ(stored_parent_hash->type, msgpack::type::NIL);
    const auto* token_ids = FindMapValue(stored, "token_ids");
    ASSERT_NE(token_ids, nullptr);
    EXPECT_EQ(token_ids->type, msgpack::type::NIL);
    const auto* stored_group_id = FindMapValue(stored, "group_id");
    ASSERT_NE(stored_group_id, nullptr);
    EXPECT_EQ(MsgpackString(*stored_group_id), "7");

    // A repeated commit represents Put/Upsert replacement and must refresh
    // every currently available medium with another stored event.
    publisher.PublishCommitted(object_key, {"cpu"}, "tenant-a", "group-a");
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto repeated_handle = msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& repeated_events = repeated_handle.get().via.array.ptr[1];
    ASSERT_EQ(repeated_events.via.array.size, 1u);
    const auto* repeated_type =
        FindMapValue(repeated_events.via.array.ptr[0], "event_type");
    ASSERT_NE(repeated_type, nullptr);
    EXPECT_EQ(MsgpackString(*repeated_type), "stored");

    publisher.SyncObjectState(object_key, {}, "tenant-a", "group-a", {"cpu"});
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto unavailable_handle =
        msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& unavailable_events = unavailable_handle.get().via.array.ptr[1];
    ASSERT_EQ(unavailable_events.via.array.size, 1u);
    const auto* unavailable_type =
        FindMapValue(unavailable_events.via.array.ptr[0], "event_type");
    const auto* unavailable_hash = FindMapValue(
        unavailable_events.via.array.ptr[0], "connector_block_hash");
    ASSERT_NE(unavailable_type, nullptr);
    ASSERT_NE(unavailable_hash, nullptr);
    EXPECT_EQ(MsgpackString(*unavailable_type), "removed");
    EXPECT_EQ(MsgpackString(*unavailable_hash),
              "0123456789abcdef000000000000002a");

    // A transient zero-medium state keeps compact metadata for the Upsert
    // commit, but token IDs are not retained.
    publisher.PublishCommitted(object_key, {"cpu"}, "tenant-a", "group-a");
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto recommitted_handle =
        msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& recommitted =
        recommitted_handle.get().via.array.ptr[1].via.array.ptr[0];
    const auto* recommitted_model = FindMapValue(recommitted, "model_name");
    const auto* recommitted_parent = FindMapValue(recommitted, "parent_hash");
    const auto* recommitted_tokens = FindMapValue(recommitted, "token_ids");
    ASSERT_NE(recommitted_model, nullptr);
    ASSERT_NE(recommitted_parent, nullptr);
    ASSERT_NE(recommitted_tokens, nullptr);
    EXPECT_EQ(MsgpackString(*recommitted_model), "model-a");
    EXPECT_EQ(recommitted_parent->type, msgpack::type::NIL);
    EXPECT_EQ(recommitted_tokens->type, msgpack::type::NIL);

    publisher.SyncObjectState(object_key, {"disk"}, "tenant-a", "group-a",
                              {"cpu"});
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto delta_handle = msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& delta_events = delta_handle.get().via.array.ptr[1];
    ASSERT_EQ(delta_events.via.array.size, 2u);
    const auto& removed = delta_events.via.array.ptr[0];
    const auto& added = delta_events.via.array.ptr[1];
    const auto* removed_type = FindMapValue(removed, "event_type");
    const auto* removed_medium = FindMapValue(removed, "medium");
    const auto* added_type = FindMapValue(added, "event_type");
    const auto* added_medium = FindMapValue(added, "medium");
    const auto* added_model_name = FindMapValue(added, "model_name");
    const auto* added_parent_hash = FindMapValue(added, "parent_hash");
    const auto* added_token_ids = FindMapValue(added, "token_ids");
    const auto* removed_hash = FindMapValue(removed, "connector_block_hash");
    const auto* added_hash = FindMapValue(added, "connector_block_hash");
    ASSERT_NE(removed_type, nullptr);
    ASSERT_NE(removed_medium, nullptr);
    ASSERT_NE(added_type, nullptr);
    ASSERT_NE(added_medium, nullptr);
    ASSERT_NE(added_model_name, nullptr);
    ASSERT_NE(added_parent_hash, nullptr);
    ASSERT_NE(added_token_ids, nullptr);
    ASSERT_NE(removed_hash, nullptr);
    ASSERT_NE(added_hash, nullptr);
    EXPECT_EQ(MsgpackString(*removed_type), "removed");
    EXPECT_EQ(MsgpackString(*removed_medium), "cpu");
    EXPECT_EQ(MsgpackString(*added_type), "stored");
    EXPECT_EQ(MsgpackString(*added_medium), "disk");
    EXPECT_EQ(MsgpackString(*added_model_name), "model-a");
    EXPECT_EQ(added_parent_hash->type, msgpack::type::NIL);
    EXPECT_EQ(added_token_ids->type, msgpack::type::NIL);
    EXPECT_EQ(MsgpackString(*removed_hash),
              "0123456789abcdef000000000000002a");
    EXPECT_EQ(MsgpackString(*added_hash),
              "0123456789abcdef000000000000002a");

    publisher.PublishObjectRemoved(object_key, "tenant-a", "group-a",
                                   {"disk"});
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto deleted_handle = msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& deleted =
        deleted_handle.get().via.array.ptr[1].via.array.ptr[0];
    const auto* deleted_type = FindMapValue(deleted, "event_type");
    const auto* deleted_medium = FindMapValue(deleted, "medium");
    const auto* deleted_hash = FindMapValue(deleted, "connector_block_hash");
    ASSERT_NE(deleted_type, nullptr);
    ASSERT_NE(deleted_medium, nullptr);
    ASSERT_NE(deleted_hash, nullptr);
    EXPECT_EQ(MsgpackString(*deleted_type), "removed");
    EXPECT_EQ(MsgpackString(*deleted_medium), "disk");
    EXPECT_EQ(MsgpackString(*deleted_hash),
              "0123456789abcdef000000000000002a");

    const std::string layerwise_key =
        "model-b@pcp0@dcp1@head_or_tp_rank:2@"
        "0123456789abcdef000000000000002b@7";
    publisher.PublishStored(layerwise_key, "cpu", "tenant-a");
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto layerwise_handle =
        msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& layerwise_event =
        layerwise_handle.get().via.array.ptr[1].via.array.ptr[0];
    const auto* head_or_tp_rank =
        FindMapValue(layerwise_event, "head_or_tp_rank");
    const auto* layer_id = FindMapValue(layerwise_event, "layer_id");
    const auto* layer_hash =
        FindMapValue(layerwise_event, "connector_block_hash");
    ASSERT_NE(head_or_tp_rank, nullptr);
    ASSERT_NE(layer_id, nullptr);
    ASSERT_NE(layer_hash, nullptr);
    EXPECT_EQ(head_or_tp_rank->as<int64_t>(), 2);
    EXPECT_EQ(layer_id->as<int64_t>(), 7);
    EXPECT_EQ(MsgpackString(*layer_hash),
              "0123456789abcdef000000000000002b");
    EXPECT_EQ(FindMapValue(layerwise_event, "pp_rank"), nullptr);

    const std::string high_bit_hash_key =
        "model-c@tp_rank:0@pcp0@dcp0@pp_rank:0@"
        "0123456789abcdefffffffffffffffff";
    publisher.PublishStored(high_bit_hash_key, "cpu", "tenant-a");
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto high_bit_handle =
        msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& high_bit_event =
        high_bit_handle.get().via.array.ptr[1].via.array.ptr[0];
    const auto* high_seq_hashes = FindMapValue(high_bit_event, "seq_hashes");
    const auto* high_block_hashes =
        FindMapValue(high_bit_event, "block_hashes");
    ASSERT_NE(high_seq_hashes, nullptr);
    ASSERT_NE(high_block_hashes, nullptr);
    ASSERT_EQ(high_seq_hashes->via.array.size, 1u);
    ASSERT_EQ(high_block_hashes->via.array.size, 1u);
    EXPECT_EQ(high_seq_hashes->via.array.ptr[0].type,
              msgpack::type::POSITIVE_INTEGER);
    EXPECT_EQ(high_block_hashes->via.array.ptr[0].type,
              msgpack::type::POSITIVE_INTEGER);
    EXPECT_EQ(high_seq_hashes->via.array.ptr[0].as<uint64_t>(),
              std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(high_block_hashes->via.array.ptr[0].as<uint64_t>(),
              std::numeric_limits<uint64_t>::max());

    zmq_close(sub);
    zmq_ctx_destroy(ctx);
}

TEST(KvEventPublisherTest, InvalidHashesDegradeAndClearedIsTenantScoped) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = endpoint;
    config.backend_id = "mooncake-test";
    config.model_name = "model-a";
    config.block_size = 32;
    config.additional_salt = "salt-a";
    config.lora_name = "adapter-a";
    config.dp_rank = 6;
    config.emit_object_key = true;
    config.emit_legacy_compat_fields = true;
    KvEventPublisher publisher(config);

    void* ctx = zmq_ctx_new();
    ASSERT_NE(ctx, nullptr);
    void* sub = zmq_socket(ctx, ZMQ_SUB);
    ASSERT_NE(sub, nullptr);
    const int timeout_ms = 2000;
    ASSERT_EQ(
        zmq_setsockopt(sub, ZMQ_RCVTIMEO, &timeout_ms, sizeof(timeout_ms)), 0);
    ASSERT_EQ(zmq_connect(sub, endpoint.c_str()), 0);
    ASSERT_EQ(zmq_setsockopt(sub, ZMQ_SUBSCRIBE, "", 0), 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    publisher.PublishStored(
        "model-a@pcp0@dcp0@head_or_tp_rank:0@pp_rank:0@not-a-hash",
        "cpu", "tenant-a");

    std::vector<std::string> frames;
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto stored_handle = msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& stored = stored_handle.get().via.array.ptr[1].via.array.ptr[0];
    const auto* seq_hashes = FindMapValue(stored, "seq_hashes");
    ASSERT_NE(seq_hashes, nullptr);
    EXPECT_EQ(seq_hashes->via.array.size, 0u);
    EXPECT_EQ(FindMapValue(stored, "connector_block_hash"), nullptr);
    EXPECT_EQ(publisher.GetStats().invalid_event_hashes, 1u);

    publisher.PublishCleared("tenant-a");
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto cleared_handle = msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& cleared =
        cleared_handle.get().via.array.ptr[1].via.array.ptr[0];
    const auto* cleared_type = FindMapValue(cleared, "event_type");
    const auto* cleared_legacy_type = FindMapValue(cleared, "type");
    const auto* cleared_tenant = FindMapValue(cleared, "tenant_id");
    const auto* cleared_medium = FindMapValue(cleared, "medium");
    const auto* cleared_model = FindMapValue(cleared, "model_name");
    const auto* cleared_block_size = FindMapValue(cleared, "block_size");
    const auto* cleared_additional_salt =
        FindMapValue(cleared, "additional_salt");
    const auto* cleared_lora_name = FindMapValue(cleared, "lora_name");
    const auto* cleared_dp_rank = FindMapValue(cleared, "dp_rank");
    ASSERT_NE(cleared_type, nullptr);
    ASSERT_NE(cleared_legacy_type, nullptr);
    ASSERT_NE(cleared_tenant, nullptr);
    ASSERT_NE(cleared_medium, nullptr);
    ASSERT_NE(cleared_model, nullptr);
    ASSERT_NE(cleared_block_size, nullptr);
    ASSERT_NE(cleared_additional_salt, nullptr);
    ASSERT_NE(cleared_lora_name, nullptr);
    ASSERT_NE(cleared_dp_rank, nullptr);
    EXPECT_EQ(MsgpackString(*cleared_type), "cleared");
    EXPECT_EQ(MsgpackString(*cleared_legacy_type), "AllBlocksCleared");
    EXPECT_EQ(MsgpackString(*cleared_tenant), "tenant-a");
    EXPECT_EQ(cleared_medium->type, msgpack::type::NIL);
    EXPECT_EQ(MsgpackString(*cleared_model), "model-a");
    EXPECT_EQ(cleared_block_size->as<uint32_t>(), 32u);
    EXPECT_EQ(MsgpackString(*cleared_additional_salt), "salt-a");
    EXPECT_EQ(MsgpackString(*cleared_lora_name), "adapter-a");
    EXPECT_EQ(cleared_dp_rank->as<uint32_t>(), 6u);
    EXPECT_EQ(cleared_handle.get().via.array.ptr[2].as<uint32_t>(), 6u);
    EXPECT_EQ(FindMapValue(cleared, "object_key"), nullptr);

    zmq_close(sub);
    zmq_ctx_destroy(ctx);
}

TEST(KvEventPublisherTest, DropsOldestWhenQueueFull) {
    const std::string endpoint = MakeIpcEndpoint();

    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = endpoint;
    config.backend_id = "mooncake-test";
    config.emit_object_key = true;
    config.queue_capacity = 2;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());

    for (int i = 0; i < 100; ++i) {
        publisher.PublishStored(std::to_string(i), "cpu");
    }

    for (int i = 0; i < 50; ++i) {
        const auto stats = publisher.GetStats();
        if (stats.dropped_events >= 1) {
            EXPECT_GE(stats.dropped_events, 1u);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    FAIL() << "expected dropped_events after queue overflow";
}

#endif  // MOONCAKE_ENABLE_KV_EVENTS

}  // namespace
}  // namespace mooncake
