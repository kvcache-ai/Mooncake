#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include "kv_event/kv_event_publisher.h"

#if defined(MOONCAKE_ENABLE_KV_EVENTS) && MOONCAKE_ENABLE_KV_EVENTS
#include <msgpack.hpp>
#include <zmq.h>
#endif

namespace mooncake {
namespace {

TEST(KvEventPublisherTest, DisabledPublisherIsNoop) {
    KvEventConfig config;
    config.enabled = false;
    KvEventPublisher publisher(config);
    EXPECT_FALSE(publisher.enabled());
    publisher.PublishStored("42", "cpu");
    publisher.PublishRemoved("42", "cpu");
    publisher.PublishCleared("tenant-a");
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

bool ReceivePayload(void* socket, msgpack::object_handle* payload) {
    std::vector<std::string> frames;
    if (!ReceiveZmqMultipart(socket, frames) || frames.size() != 3) {
        return false;
    }
    *payload = msgpack::unpack(frames[2].data(), frames[2].size());
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
    return value.type == msgpack::type::STR
               ? std::string(value.via.str.ptr, value.via.str.size)
               : std::string{};
}

const msgpack::object& FirstEvent(const msgpack::object_handle& payload) {
    return payload.get().via.array.ptr[1].via.array.ptr[0];
}

void ExpectObjectEvent(const msgpack::object& event, const std::string& type,
                       const std::string& object_key) {
    ASSERT_EQ(event.type, msgpack::type::MAP);
    const auto* event_type = FindMapValue(event, "event_type");
    const auto* actual_key = FindMapValue(event, "object_key");
    const auto* seq_hashes = FindMapValue(event, "seq_hashes");
    ASSERT_NE(event_type, nullptr);
    ASSERT_NE(actual_key, nullptr);
    ASSERT_NE(seq_hashes, nullptr);
    EXPECT_EQ(MsgpackString(*event_type), type);
    EXPECT_EQ(MsgpackString(*actual_key), object_key);
    ASSERT_EQ(seq_hashes->type, msgpack::type::ARRAY);
    EXPECT_EQ(seq_hashes->via.array.size, 0u);
    EXPECT_EQ(FindMapValue(event, "connector_block_hash"), nullptr);
    EXPECT_EQ(FindMapValue(event, "cache_prefix"), nullptr);
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
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    publisher.PublishStored(object_key, "cpu", "tenant-a", group_id);
    msgpack::object_handle payload;
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ASSERT_EQ(payload.get().type, msgpack::type::ARRAY);
    ASSERT_EQ(payload.get().via.array.size, 3u);
    ExpectObjectEvent(FirstEvent(payload), "stored", object_key);
    const auto& event = FirstEvent(payload);
    EXPECT_EQ(MsgpackString(*FindMapValue(event, "group_id")), group_id);
    EXPECT_EQ(MsgpackString(*FindMapValue(event, "backend_id")),
              "mooncake-test");
    EXPECT_EQ(MsgpackString(*FindMapValue(event, "tenant_id")), "tenant-a");
    EXPECT_EQ(FindMapValue(event, "model_name")->as<std::string>(),
              "configured-model");
    EXPECT_EQ(FindMapValue(event, "block_size")->as<uint32_t>(), 128u);
    EXPECT_EQ(FindMapValue(event, "dp_rank")->as<uint32_t>(), 5u);

    publisher.PublishRemoved(object_key, "cpu", "tenant-a", group_id);
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "removed", object_key);

    publisher.PublishCleared("tenant-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    const auto& cleared = FirstEvent(payload);
    EXPECT_EQ(MsgpackString(*FindMapValue(cleared, "event_type")), "cleared");
    EXPECT_EQ(MsgpackString(*FindMapValue(cleared, "tenant_id")), "tenant-a");
    EXPECT_EQ(FindMapValue(cleared, "object_key"), nullptr);

    for (int i = 0; i < 50; ++i) {
        const auto stats = publisher.GetStats();
        if (stats.published_events == 3 && stats.published_batches == 3) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const auto stats = publisher.GetStats();
    EXPECT_EQ(stats.published_events, 3u);
    EXPECT_EQ(stats.published_batches, 3u);
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

    const std::string object_key =
        "deployment-a@model-a@tp_rank:2@pcp1@dcp0@pp_rank:3@group:7@"
        "0123456789abcdef000000000000002a";
    msgpack::object_handle payload;
    publisher.PublishCommitted(object_key, {"cpu"}, "tenant-a", "group-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "stored", object_key);

    publisher.PublishCommitted(object_key, {"cpu"}, "tenant-a", "group-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "stored", object_key);

    publisher.SyncObjectState(object_key, {}, "tenant-a", "group-a", {"cpu"});
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "removed", object_key);

    publisher.PublishCommitted(object_key, {"cpu"}, "tenant-a", "group-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "stored", object_key);

    publisher.SyncObjectState(object_key, {"disk"}, "tenant-a", "group-a",
                              {"cpu"});
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ASSERT_EQ(payload.get().via.array.ptr[1].via.array.size, 2u);
    ExpectObjectEvent(payload.get().via.array.ptr[1].via.array.ptr[0],
                      "removed", object_key);
    ExpectObjectEvent(payload.get().via.array.ptr[1].via.array.ptr[1],
                      "stored", object_key);

    publisher.PublishObjectRemoved(object_key, "tenant-a", "group-a", {"disk"});
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    ExpectObjectEvent(FirstEvent(payload), "removed", object_key);

    zmq_close(sub);
    zmq_ctx_destroy(ctx);
}

TEST(KvEventPublisherTest, ClearIsTenantScoped) {
    const std::string endpoint = MakeIpcEndpoint();
    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = endpoint;
    config.backend_id = "mooncake-test";
    config.model_name = "model-a";
    config.block_size = 64;
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

    msgpack::object_handle payload;
    publisher.PublishStored("tenant-a-key", "cpu", "tenant-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    publisher.PublishStored("tenant-b-key", "cpu", "tenant-b");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    publisher.PublishCleared("tenant-a");
    ASSERT_TRUE(ReceivePayload(sub, &payload));
    const auto& event = FirstEvent(payload);
    EXPECT_EQ(MsgpackString(*FindMapValue(event, "event_type")), "cleared");
    EXPECT_EQ(MsgpackString(*FindMapValue(event, "tenant_id")), "tenant-a");

    zmq_close(sub);
    zmq_ctx_destroy(ctx);
}

TEST(KvEventPublisherTest, DisabledObjectKeySkipsObjectEventsButKeepsClear) {
    KvEventConfig config;
    config.enabled = true;
    config.bind_endpoint = MakeIpcEndpoint();
    config.backend_id = "mooncake-test";
    config.emit_object_key = false;
    KvEventPublisher publisher(config);
    ASSERT_TRUE(publisher.enabled());

    publisher.PublishStored("object-a", "cpu", "tenant-a");
    publisher.PublishRemoved("object-a", "cpu", "tenant-a");
    publisher.PublishCleared("tenant-a");

    for (int i = 0; i < 50; ++i) {
        const auto stats = publisher.GetStats();
        if (stats.skipped_unparsed_keys == 2 && stats.published_events == 1) {
            EXPECT_EQ(stats.published_batches, 1u);
            EXPECT_EQ(stats.dropped_events, 0u);
            return;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    const auto stats = publisher.GetStats();
    FAIL() << "unexpected publisher stats: skipped="
           << stats.skipped_unparsed_keys
           << " published_events=" << stats.published_events
           << " published_batches=" << stats.published_batches;
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
