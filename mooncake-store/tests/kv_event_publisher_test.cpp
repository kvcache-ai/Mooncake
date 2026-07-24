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

    publisher.PublishStored(object_key, "cpu", TenantId("tenant-a"), group_id);

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
