#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
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
    config.emit_object_key = true;
    config.emit_legacy_compat_fields = true;
    config.queue_capacity = 64;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());

    void* ctx = zmq_ctx_new();
    ASSERT_NE(ctx, nullptr);
    void* sub = zmq_socket(ctx, ZMQ_SUB);
    ASSERT_NE(sub, nullptr);
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
    config.emit_object_key = true;
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

    StoreEventInfo info;
    info.model_name = "model-a";
    info.block_size = 64;
    info.block_hash = "0x2a";
    info.parent_block_hash = "41";
    info.token_ids = {1, 2, 3};
    publisher.PublishCommitted("opaque-key", {"cpu"}, "tenant-a", "group-a",
                               &info);

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
    const auto* seq_hashes = FindMapValue(stored, "seq_hashes");
    ASSERT_NE(seq_hashes, nullptr);
    ASSERT_EQ(seq_hashes->via.array.size, 1u);
    EXPECT_EQ(seq_hashes->via.array.ptr[0].as<uint64_t>(), 42u);
    const auto* stored_parent_hash = FindMapValue(stored, "parent_hash");
    ASSERT_NE(stored_parent_hash, nullptr);
    EXPECT_EQ(stored_parent_hash->as<uint64_t>(), 41u);
    const auto* token_ids = FindMapValue(stored, "token_ids");
    ASSERT_NE(token_ids, nullptr);
    ASSERT_EQ(token_ids->via.array.size, 3u);

    // A repeated commit represents Put/Upsert replacement and must refresh
    // every currently available medium with another stored event.
    publisher.PublishCommitted("opaque-key", {"cpu"}, "tenant-a", "group-a",
                               &info);
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto repeated_handle = msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& repeated_events = repeated_handle.get().via.array.ptr[1];
    ASSERT_EQ(repeated_events.via.array.size, 1u);
    const auto* repeated_type =
        FindMapValue(repeated_events.via.array.ptr[0], "event_type");
    ASSERT_NE(repeated_type, nullptr);
    EXPECT_EQ(MsgpackString(*repeated_type), "stored");

    publisher.SyncObjectState("opaque-key", {}, "tenant-a", "group-a", {"cpu"});
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto unavailable_handle =
        msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& unavailable_events = unavailable_handle.get().via.array.ptr[1];
    ASSERT_EQ(unavailable_events.via.array.size, 1u);
    const auto* unavailable_type =
        FindMapValue(unavailable_events.via.array.ptr[0], "event_type");
    ASSERT_NE(unavailable_type, nullptr);
    EXPECT_EQ(MsgpackString(*unavailable_type), "removed");

    // A transient zero-medium state keeps compact metadata for the Upsert
    // commit, but token IDs are not retained.
    publisher.PublishCommitted("opaque-key", {"cpu"}, "tenant-a", "group-a");
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
    EXPECT_EQ(recommitted_parent->as<uint64_t>(), 41u);
    EXPECT_EQ(recommitted_tokens->type, msgpack::type::NIL);

    publisher.SyncObjectState("opaque-key", {"disk"}, "tenant-a", "group-a",
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
    ASSERT_NE(removed_type, nullptr);
    ASSERT_NE(removed_medium, nullptr);
    ASSERT_NE(added_type, nullptr);
    ASSERT_NE(added_medium, nullptr);
    ASSERT_NE(added_model_name, nullptr);
    ASSERT_NE(added_parent_hash, nullptr);
    ASSERT_NE(added_token_ids, nullptr);
    EXPECT_EQ(MsgpackString(*removed_type), "removed");
    EXPECT_EQ(MsgpackString(*removed_medium), "cpu");
    EXPECT_EQ(MsgpackString(*added_type), "stored");
    EXPECT_EQ(MsgpackString(*added_medium), "disk");
    EXPECT_EQ(MsgpackString(*added_model_name), "model-a");
    EXPECT_EQ(added_parent_hash->as<uint64_t>(), 41u);
    EXPECT_EQ(added_token_ids->type, msgpack::type::NIL);

    publisher.PublishObjectRemoved("opaque-key", "tenant-a", "group-a",
                                   {"disk"});
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto deleted_handle = msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& deleted =
        deleted_handle.get().via.array.ptr[1].via.array.ptr[0];
    const auto* deleted_type = FindMapValue(deleted, "event_type");
    const auto* deleted_medium = FindMapValue(deleted, "medium");
    ASSERT_NE(deleted_type, nullptr);
    ASSERT_NE(deleted_medium, nullptr);
    EXPECT_EQ(MsgpackString(*deleted_type), "removed");
    EXPECT_EQ(MsgpackString(*deleted_medium), "disk");

    zmq_close(sub);
    zmq_ctx_destroy(ctx);
}

TEST(KvEventPublisherTest, InvalidHashesDegradeAndClearedIsTenantScoped) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = endpoint;
    config.backend_id = "mooncake-test";
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

    StoreEventInfo info;
    info.block_hash = "sha256:not-a-u64";
    info.parent_block_hash = "parent:not-a-u64";
    publisher.PublishStored("opaque-key", "cpu", "tenant-a", "", info);

    std::vector<std::string> frames;
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto stored_handle = msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& stored = stored_handle.get().via.array.ptr[1].via.array.ptr[0];
    const auto* seq_hashes = FindMapValue(stored, "seq_hashes");
    ASSERT_NE(seq_hashes, nullptr);
    EXPECT_EQ(seq_hashes->via.array.size, 0u);
    EXPECT_EQ(publisher.GetStats().invalid_event_hashes, 2u);

    publisher.PublishCleared("tenant-a");
    ASSERT_TRUE(ReceiveZmqMultipart(sub, frames));
    auto cleared_handle = msgpack::unpack(frames[2].data(), frames[2].size());
    const auto& cleared =
        cleared_handle.get().via.array.ptr[1].via.array.ptr[0];
    const auto* cleared_type = FindMapValue(cleared, "event_type");
    const auto* cleared_legacy_type = FindMapValue(cleared, "type");
    const auto* cleared_tenant = FindMapValue(cleared, "tenant_id");
    const auto* cleared_medium = FindMapValue(cleared, "medium");
    ASSERT_NE(cleared_type, nullptr);
    ASSERT_NE(cleared_legacy_type, nullptr);
    ASSERT_NE(cleared_tenant, nullptr);
    ASSERT_NE(cleared_medium, nullptr);
    EXPECT_EQ(MsgpackString(*cleared_type), "cleared");
    EXPECT_EQ(MsgpackString(*cleared_legacy_type), "AllBlocksCleared");
    EXPECT_EQ(MsgpackString(*cleared_tenant), "tenant-a");
    EXPECT_EQ(cleared_medium->type, msgpack::type::NIL);
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
