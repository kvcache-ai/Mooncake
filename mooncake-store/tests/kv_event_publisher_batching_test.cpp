#include <gtest/gtest.h>

#include "kv_event/kv_event_publisher.h"

#if defined(MOONCAKE_ENABLE_KV_EVENTS) && MOONCAKE_ENABLE_KV_EVENTS

#include <msgpack.hpp>
#include <zmq.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <endian.h>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <vector>

namespace mooncake {
namespace {

using Clock = std::chrono::steady_clock;

struct Event {
    uint64_t id;
    std::string type, key, tenant, medium, group, session;
};

struct Batch {
    uint64_t sequence;
    size_t bytes = 0;
    std::vector<Event> events;
};

class Subscriber {
   public:
    ~Subscriber() {
        if (socket_) zmq_close(socket_);
        if (context_) zmq_ctx_term(context_);
    }

    bool Connect(const std::string& endpoint) {
        context_ = zmq_ctx_new();
        if (!context_) return false;
        socket_ = zmq_socket(context_, ZMQ_SUB);
        if (!socket_) return false;
        const int timeout = 2000;
        const int hwm = 100000;
        if (zmq_setsockopt(socket_, ZMQ_RCVTIMEO, &timeout, sizeof(timeout)) !=
                0 ||
            zmq_setsockopt(socket_, ZMQ_RCVHWM, &hwm, sizeof(hwm)) != 0 ||
            zmq_setsockopt(socket_, ZMQ_SUBSCRIBE, "", 0) != 0 ||
            zmq_connect(socket_, endpoint.c_str()) != 0) {
            return false;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        return true;
    }

    std::optional<Batch> Receive() {
        std::vector<std::string> frames;
        while (true) {
            zmq_msg_t message;
            zmq_msg_init(&message);
            if (zmq_msg_recv(&message, socket_, 0) < 0) {
                zmq_msg_close(&message);
                return std::nullopt;
            }
            frames.emplace_back(
                static_cast<const char*>(zmq_msg_data(&message)),
                zmq_msg_size(&message));
            const bool more = zmq_msg_more(&message);
            zmq_msg_close(&message);
            if (!more) break;
        }
        if (frames.size() != 3 || frames[1].size() != 8) return std::nullopt;
        Batch batch;
        std::memcpy(&batch.sequence, frames[1].data(), 8);
        batch.sequence = be64toh(batch.sequence);
        for (const auto& frame : frames) batch.bytes += frame.size();
        auto payload = msgpack::unpack(frames[2].data(), frames[2].size());
        using Fields = std::map<std::string, msgpack::object>;
        auto envelope =
            payload.get()
                .as<std::tuple<int64_t, std::vector<Fields>, uint32_t>>();
        for (const auto& fields : std::get<1>(envelope)) {
            auto text = [&fields](const char* name) {
                const auto it = fields.find(name);
                return it == fields.end() || it->second.is_nil()
                           ? std::string{}
                           : it->second.as<std::string>();
            };
            batch.events.push_back({fields.at("event_id").as<uint64_t>(),
                                    text("event_type"), text("object_key"),
                                    text("tenant_id"), text("medium"),
                                    text("group_id"), text("session_id")});
        }
        return batch;
    }

   private:
    void* context_ = nullptr;
    void* socket_ = nullptr;
};

KvEventConfig Config(uint32_t timeout_us = 1000) {
    static std::atomic<unsigned> next{0};
    KvEventConfig config;
    config.enabled = true;
    config.backend_id = "batching-test";
    config.bind_endpoint = "ipc:///tmp/mooncake-kv-batch-" +
                           std::to_string(getpid()) + "-" +
                           std::to_string(next.fetch_add(1));
    config.removal_batch_timeout_us = timeout_us;
    return config;
}

TEST(KvEventPublisherBatchingTest, PacedRemovalsFlushWhenFull) {
    auto config = Config(5000000);
    KvEventPublisher publisher(config);
    Subscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(config.bind_endpoint));
    std::jthread producer([&] {
        for (size_t i = 0; i < 64; ++i) {
            publisher.PublishRemoved(std::to_string(i), "cpu", "tenant-a", "g");
            std::this_thread::sleep_for(std::chrono::microseconds(100));
        }
    });
    const auto batch = subscriber.Receive();
    ASSERT_TRUE(batch);
    ASSERT_EQ(batch->events.size(), 64u);
    EXPECT_EQ(batch->sequence, 1u);
    for (size_t i = 0; i < 64; ++i) {
        const auto& event = batch->events[i];
        EXPECT_EQ(event.id, i + 1);
        EXPECT_EQ(event.key, std::to_string(i));
        EXPECT_EQ(event.type, "removed");
        EXPECT_EQ(event.tenant, "tenant-a");
        EXPECT_EQ(event.medium, "cpu");
        EXPECT_EQ(event.group, "g");
    }
}

TEST(KvEventPublisherBatchingTest, PartialRemovalBatchFlushesAtDeadline) {
    const auto config = Config();
    KvEventPublisher publisher(config);
    Subscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(config.bind_endpoint));
    publisher.PublishRemoved("a", "disk");
    const auto batch = subscriber.Receive();
    ASSERT_TRUE(batch);
    ASSERT_EQ(batch->events.size(), 1u);
    EXPECT_EQ(batch->events[0].key, "a");
    EXPECT_EQ(batch->events[0].medium, "disk");
}

TEST(KvEventPublisherBatchingTest, ContinuousArrivalsDoNotResetDeadline) {
    const auto config = Config(20000);
    KvEventPublisher publisher(config);
    Subscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(config.bind_endpoint));
    std::jthread producer([&] {
        for (unsigned i = 0; i < 100; ++i) {
            publisher.PublishRemoved(std::to_string(i), "cpu");
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });
    const auto batch = subscriber.Receive();
    ASSERT_TRUE(batch);
    EXPECT_GT(batch->events.size(), 1u);
    EXPECT_LT(batch->events.size(), 64u);
}

TEST(KvEventPublisherBatchingTest, StoresAndClearsFlushAndKeepMutationOrder) {
    const auto config = Config(5000000);
    KvEventPublisher publisher(config);
    Subscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(config.bind_endpoint));
    publisher.PublishRemoved("a", "cpu", "tenant-a");
    publisher.PublishStored("a", "cpu", "tenant-a", "", "new-a");
    publisher.PublishCleared("tenant-a");
    publisher.PublishRemoved("b", "disk", "tenant-b");
    publisher.PublishStored("b", "disk", "tenant-b", "", "new-b");
    const std::vector<std::string> types{"removed", "stored", "cleared",
                                         "removed", "stored"};
    const std::vector<std::string> keys{"a", "a", "", "b", "b"};
    size_t seen = 0;
    uint64_t sequence = 0;
    while (seen < types.size()) {
        const auto batch = subscriber.Receive();
        ASSERT_TRUE(batch);
        EXPECT_EQ(batch->sequence, ++sequence);
        for (const auto& event : batch->events) {
            ASSERT_LT(seen, types.size());
            EXPECT_EQ(event.id, seen + 1);
            EXPECT_EQ(event.type, types[seen]);
            EXPECT_EQ(event.key, keys[seen]);
            EXPECT_EQ(event.tenant, seen < 3 ? "tenant-a" : "tenant-b");
            if (seen == 1) {
                EXPECT_EQ(event.session, "new-a");
            }
            if (seen == 4) {
                EXPECT_EQ(event.session, "new-b");
            }
            ++seen;
        }
    }
}

TEST(KvEventPublisherBatchingTest, StoreAloneDoesNotWaitForRemovalDeadline) {
    const auto config = Config(5000000);
    KvEventPublisher publisher(config);
    Subscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(config.bind_endpoint));
    publisher.PublishStored("a", "cpu");
    const auto batch = subscriber.Receive();
    ASSERT_TRUE(batch);
    ASSERT_EQ(batch->events.size(), 1u);
    EXPECT_EQ(batch->events[0].type, "stored");
}

TEST(KvEventPublisherBatchingTest, ZeroTimeoutKeepsImmediatePartialFlush) {
    const auto config = Config(0);
    KvEventPublisher publisher(config);
    Subscriber subscriber;
    ASSERT_TRUE(subscriber.Connect(config.bind_endpoint));
    publisher.PublishRemoved("a", "cpu");
    ASSERT_TRUE(subscriber.Receive());
}

TEST(KvEventPublisherBatchingTest, ShutdownInterruptsRemovalDeadline) {
    const auto config = Config(5000000);
    auto publisher = std::make_unique<KvEventPublisher>(config);
    publisher->PublishRemoved("a", "cpu");
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const auto before = Clock::now();
    publisher.reset();
    EXPECT_LT(Clock::now() - before, std::chrono::seconds(1));
}

// CPU-only reproduction. Run explicitly with --gtest_also_run_disabled_tests
// --gtest_filter=KvEventPublisherBatchingTest.DISABLED_PacedRemovalBurstBenchmark
// Zero is the previous immediate-drain policy. Every case checks the complete
// object stream; the reported reduction is in wire batches, not object events.
TEST(KvEventPublisherBatchingTest, DISABLED_PacedRemovalBurstBenchmark) {
    constexpr size_t count = 15000;
    const char* interval = std::getenv("MOONCAKE_KV_EVENT_BENCH_INTERVAL_US");
    const auto interval_us = interval ? std::stoul(interval) : 5u;
    std::vector<std::string> keys;
    for (size_t i = 0; i < count; ++i) {
        std::string hash(64, '0');
        const auto suffix = std::to_string(i);
        hash.replace(hash.size() - suffix.size(), suffix.size(), suffix);
        keys.push_back("sglang-rkv-v4:dsa:pp0:" + hash);
    }
    for (uint32_t timeout_us : {0u, 100u, 250u, 500u, 1000u}) {
        const auto config = Config(timeout_us);
        KvEventPublisher publisher(config);
        Subscriber subscriber;
        ASSERT_TRUE(subscriber.Connect(config.bind_endpoint));
        std::vector<std::atomic<int64_t>> enqueued(count);
        std::jthread producer([&] {
            for (size_t i = 0; i < count; ++i) {
                const auto now = Clock::now();
                enqueued[i].store(now.time_since_epoch().count(),
                                  std::memory_order_release);
                publisher.PublishRemoved(keys[i], "cpu", "default", "evict");
                // Model work between consecutive evictions without making
                // publication depend on a subscriber acknowledgment.
                while (Clock::now() - now <
                       std::chrono::microseconds(interval_us)) {
                }
            }
        });
        size_t seen = 0, batches = 0, bytes = 0;
        std::vector<double> latency_us;
        while (seen < count) {
            const auto batch = subscriber.Receive();
            ASSERT_TRUE(batch);
            EXPECT_EQ(batch->sequence, ++batches);
            ASSERT_LE(batch->events.size(), 64u);
            bytes += batch->bytes;
            const auto received = Clock::now();
            for (const auto& event : batch->events) {
                ASSERT_LT(seen, count);
                EXPECT_EQ(event.id, seen + 1);
                EXPECT_EQ(event.key, keys[seen]);
                EXPECT_EQ(event.type, "removed");
                EXPECT_EQ(event.medium, "cpu");
                EXPECT_EQ(event.tenant, "default");
                EXPECT_EQ(event.group, "evict");
                const auto sent = Clock::time_point(Clock::duration(
                    enqueued[seen].load(std::memory_order_acquire)));
                latency_us.push_back(
                    std::chrono::duration<double, std::micro>(received - sent)
                        .count());
                ++seen;
            }
        }
        producer.join();
        EXPECT_EQ(publisher.GetStats().dropped_events, 0u);
        std::sort(latency_us.begin(), latency_us.end());
        std::cout << "{\"interval_us\":" << interval_us
                  << ",\"window_us\":" << timeout_us << ",\"events\":" << seen
                  << ",\"batches\":" << batches
                  << ",\"payload_bytes\":" << bytes
                  << ",\"p50_us\":" << latency_us[count / 2]
                  << ",\"p99_us\":" << latency_us[count * 99 / 100] << "}\n";
    }
}

}  // namespace
}  // namespace mooncake

#endif  // MOONCAKE_ENABLE_KV_EVENTS
