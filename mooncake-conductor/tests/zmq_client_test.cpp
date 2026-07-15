// Tests for ZMQClient source routing, metadata propagation, lifecycle,
// sequence tracking, gap handling, and replay-after-reconnect.

#include <gtest/gtest.h>
#include <msgpack.hpp>
#include <zmq.hpp>
#include <zmq_addon.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <future>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "conductor/zmq/zmq_client.h"

namespace conductor {
namespace zmq {

class ZMQClientTestPeer {
   public:
    static void SetEndpoint(ZMQClient& client, std::string endpoint) {
        std::unique_lock lock(client.mu_);
        client.config_.endpoint = std::move(endpoint);
    }

    static void SetLastSequence(ZMQClient& client, int64_t sequence) {
        std::unique_lock lock(client.mu_);
        client.last_seq_ = sequence;
    }

    static void HandleReconnect(ZMQClient& client) { client.HandleReconnect(); }

    static void MarkDisconnected(ZMQClient& client) {
        client.MarkDisconnected();
    }

    static bool IsConnected(ZMQClient& client) { return client.IsConnected(); }

    static bool HasReplaySocket(ZMQClient& client) {
        std::shared_lock lock(client.mu_);
        return client.replay_socket_ != nullptr;
    }
};

}  // namespace zmq
}  // namespace conductor

namespace {

using conductor::common::PublisherKind;
using conductor::zmq::DecodedBatch;
using conductor::zmq::EventHandler;
using conductor::zmq::MessageMetadata;
using conductor::zmq::MooncakeEventBatch;
using conductor::zmq::MooncakeStoredEvent;
using conductor::zmq::ValidateConfig;
using conductor::zmq::VllmEventBatch;
using conductor::zmq::VllmStoredEvent;
using conductor::zmq::ZMQClient;
using conductor::zmq::ZMQClientConfig;
using conductor::zmq::ZMQClientTestPeer;

struct HandledBatch {
    DecodedBatch batch;
    MessageMetadata metadata;
};

class MockEventHandler : public EventHandler {
   public:
    std::string HandleBatch(const DecodedBatch& batch,
                            const MessageMetadata& metadata) override {
        std::lock_guard<std::mutex> lock(mu_);
        batches_.push_back({batch, metadata});
        return "";
    }

    std::optional<HandledBatch> FindBatch(int64_t sequence,
                                          const std::string& endpoint) {
        std::lock_guard<std::mutex> lock(mu_);
        for (const auto& handled : batches_) {
            if (handled.metadata.sequence == sequence &&
                handled.metadata.endpoint == endpoint) {
                return handled;
            }
        }
        return std::nullopt;
    }

    bool WaitForBatch(int64_t sequence, const std::string& endpoint,
                      std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (FindBatch(sequence, endpoint).has_value()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return FindBatch(sequence, endpoint).has_value();
    }

   private:
    std::mutex mu_;
    std::vector<HandledBatch> batches_;
};

std::string PackVllmStoredBatch(uint64_t hash, int64_t dp_rank = 3) {
    std::stringstream buf;
    msgpack::packer<std::stringstream> pk(buf);
    pk.pack_array(3);
    pk.pack_double(1.25);
    pk.pack_array(1);
    pk.pack_map(9);
    pk.pack(std::string("type"));
    pk.pack(std::string("BlockStored"));
    pk.pack(std::string("block_hashes"));
    pk.pack_array(1);
    pk.pack_uint64(hash);
    pk.pack(std::string("parent_block_hash"));
    pk.pack_nil();
    pk.pack(std::string("token_ids"));
    pk.pack_array(2);
    pk.pack_int32(1);
    pk.pack_int32(2);
    pk.pack(std::string("block_size"));
    pk.pack_int64(2);
    pk.pack(std::string("lora_id"));
    pk.pack_nil();
    pk.pack(std::string("medium"));
    pk.pack(std::string("GPU"));
    pk.pack(std::string("lora_name"));
    pk.pack_nil();
    pk.pack(std::string("group_idx"));
    pk.pack_int64(0);
    pk.pack_int64(dp_rank);
    return buf.str();
}

std::string PackMooncakeStoredBatch(uint64_t event_id, uint64_t hash,
                                    int64_t batch_dp_rank = 7) {
    constexpr int64_t kTimestampMilliseconds = 1700000000123;
    std::stringstream buf;
    msgpack::packer<std::stringstream> pk(buf);
    pk.pack_array(3);
    pk.pack_int64(kTimestampMilliseconds);
    pk.pack_array(1);
    pk.pack_map(24);
    pk.pack(std::string("event_id"));
    pk.pack_uint64(event_id);
    pk.pack(std::string("timestamp"));
    pk.pack_int64(kTimestampMilliseconds);
    pk.pack(std::string("event_type"));
    pk.pack(std::string("stored"));
    pk.pack(std::string("type"));
    pk.pack(std::string("BlockStored"));
    pk.pack(std::string("model_name"));
    pk.pack(std::string("test-model"));
    pk.pack(std::string("block_size"));
    pk.pack_int64(2);
    pk.pack(std::string("additional_salt"));
    pk.pack_nil();
    pk.pack(std::string("lora_name"));
    pk.pack_nil();
    pk.pack(std::string("tenant_id"));
    pk.pack(std::string("tenant-a"));
    pk.pack(std::string("backend_id"));
    pk.pack(std::string("backend-a"));
    pk.pack(std::string("medium"));
    pk.pack(std::string("cpu"));
    pk.pack(std::string("dp_rank"));
    pk.pack_int64(7);
    pk.pack(std::string("group_id"));
    pk.pack_nil();
    pk.pack(std::string("object_key"));
    pk.pack(std::string("object-42"));
    pk.pack(std::string("connector_block_hash"));
    pk.pack(std::string("0001020304050607000000000000002a"));
    pk.pack(std::string("cache_prefix"));
    pk.pack(std::string("prefix"));
    pk.pack(std::string("tp_rank"));
    pk.pack_int64(1);
    pk.pack(std::string("head_or_tp_rank"));
    pk.pack_nil();
    pk.pack(std::string("pp_rank"));
    pk.pack_int64(0);
    pk.pack(std::string("seq_hashes"));
    pk.pack_array(1);
    pk.pack_uint64(hash);
    pk.pack(std::string("base_block_idx"));
    pk.pack_int64(0);
    pk.pack(std::string("parent_hash"));
    pk.pack_nil();
    pk.pack(std::string("parent_block_hash"));
    pk.pack_nil();
    pk.pack(std::string("token_ids"));
    pk.pack_nil();
    pk.pack_int64(batch_dp_rank);
    return buf.str();
}

class MockPublisher {
   public:
    MockPublisher()
        : ctx_(1),
          pub_(ctx_, ::zmq::socket_type::pub),
          router_(ctx_, ::zmq::socket_type::router) {
        pub_.set(::zmq::sockopt::ipv6, 1);
        pub_.bind("tcp://127.0.0.1:*");
        router_.set(::zmq::sockopt::ipv6, 1);
        router_.bind("tcp://127.0.0.1:*");
        replay_thread_ = std::thread([this] { HandleReplay(); });
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ~MockPublisher() { Close(); }

    void Close() {
        if (!closed_.exchange(true)) {
            if (replay_thread_.joinable()) replay_thread_.join();
            pub_.close();
            router_.close();
        }
    }

    std::string PubEndpoint() {
        return pub_.get(::zmq::sockopt::last_endpoint);
    }

    std::string RouterEndpoint() {
        return router_.get(::zmq::sockopt::last_endpoint);
    }

    void Publish(const std::string& topic, const std::string& payload,
                 uint64_t sequence) {
        unsigned char sequence_bytes[8];
        for (int index = 7; index >= 0; --index) {
            sequence_bytes[index] = static_cast<unsigned char>(sequence & 0xFF);
            sequence >>= 8;
        }
        std::array<::zmq::const_buffer, 3> frames = {
            ::zmq::buffer(topic),
            ::zmq::buffer(sequence_bytes, sizeof(sequence_bytes)),
            ::zmq::buffer(payload),
        };
        ::zmq::send_multipart(pub_, frames);
    }

    void PublishFrames(const std::vector<std::string>& frames) {
        std::vector<::zmq::const_buffer> buffers;
        buffers.reserve(frames.size());
        for (const auto& frame : frames) {
            buffers.push_back(::zmq::buffer(frame));
        }
        ::zmq::send_multipart(pub_, buffers);
    }

    size_t ReplayRequestCount() const { return replay_requests_.load(); }

    uint64_t LastReplayFromSequence() const {
        return last_replay_from_sequence_.load();
    }

    bool WaitForReplayRequests(size_t count,
                               std::chrono::milliseconds timeout) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (ReplayRequestCount() < count &&
               std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
        return ReplayRequestCount() >= count;
    }

   private:
    void HandleReplay() {
        router_.set(::zmq::sockopt::rcvtimeo, 100);
        while (!closed_.load()) {
            std::vector<::zmq::message_t> frames;
            const auto count = ::zmq::recv_multipart(
                router_, std::back_inserter(frames), ::zmq::recv_flags::none);
            if (!count) continue;
            if (frames.size() != 2 || frames[1].size() != 8) continue;

            const auto* bytes =
                static_cast<const unsigned char*>(frames[1].data());
            uint64_t from_sequence = 0;
            for (int index = 0; index < 8; ++index) {
                from_sequence = (from_sequence << 8) | bytes[index];
            }
            last_replay_from_sequence_.store(from_sequence);
            replay_requests_.fetch_add(1);

            std::array<::zmq::const_buffer, 2> reply = {
                ::zmq::buffer(frames[0].data(), frames[0].size()),
                ::zmq::buffer(std::string("OK")),
            };
            ::zmq::send_multipart(router_, reply);
        }
    }

    ::zmq::context_t ctx_;
    ::zmq::socket_t pub_;
    ::zmq::socket_t router_;
    std::atomic<bool> closed_{false};
    std::atomic<size_t> replay_requests_{0};
    std::atomic<uint64_t> last_replay_from_sequence_{0};
    std::thread replay_thread_;
};

ZMQClientConfig TestConfig(MockPublisher& publisher) {
    ZMQClientConfig config;
    config.cache_pool_key = "test-pod";
    config.endpoint = publisher.PubEndpoint();
    config.replay_endpoint = publisher.RouterEndpoint();
    config.model_name = "test-model";
    config.publisher_kind = PublisherKind::kVllm;
    config.poll_timeout = std::chrono::milliseconds(100);
    config.replay_timeout = std::chrono::milliseconds(1000);
    config.reconnect_delay = std::chrono::milliseconds(100);
    return config;
}

template <typename Publish>
bool PublishUntilHandled(MockEventHandler& handler, int64_t sequence,
                         const std::string& endpoint, Publish publish) {
    for (int attempt = 0; attempt < 20; ++attempt) {
        publish();
        if (handler.WaitForBatch(sequence, endpoint,
                                 std::chrono::milliseconds(200))) {
            return true;
        }
    }
    return false;
}

const VllmStoredEvent* GetVllmStored(const HandledBatch& handled) {
    const auto* batch = std::get_if<VllmEventBatch>(&handled.batch);
    if (batch == nullptr || batch->events.size() != 1 ||
        !batch->events[0].event.has_value()) {
        return nullptr;
    }
    return std::get_if<VllmStoredEvent>(&*batch->events[0].event);
}

const MooncakeStoredEvent* GetMooncakeStored(const HandledBatch& handled) {
    const auto* batch = std::get_if<MooncakeEventBatch>(&handled.batch);
    if (batch == nullptr || batch->events.size() != 1 ||
        !batch->events[0].event.has_value()) {
        return nullptr;
    }
    return std::get_if<MooncakeStoredEvent>(&*batch->events[0].event);
}

TEST(ValidateConfig, RequiresOnlyLiveEndpoint) {
    ZMQClientConfig config;
    EXPECT_FALSE(ValidateConfig(config).empty());
    config.endpoint = "tcp://127.0.0.1:5557";
    config.replay_endpoint.clear();
    EXPECT_TRUE(ValidateConfig(config).empty());
}

TEST(ZMQClient, ConnectSuccessWithReplaySocket) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    EXPECT_EQ(client.Connect(), "");
    EXPECT_TRUE(ZMQClientTestPeer::HasReplaySocket(client));
    client.Stop();
}

TEST(ZMQClient, ConnectAlreadyConnectedIsNoop) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    EXPECT_EQ(client.Connect(), "");
    EXPECT_EQ(client.Connect(), "");
    EXPECT_TRUE(ZMQClientTestPeer::HasReplaySocket(client));
    client.Stop();
}

TEST(ZMQClient, StartStopGracefulWithReplayConfigured) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    EXPECT_EQ(client.Start(), "");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    const auto start = std::chrono::steady_clock::now();
    client.Stop();
    EXPECT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::seconds(2));
}

TEST(ZMQClient, StopIsIdempotentWithReplayConfigured) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    ASSERT_EQ(client.Start(), "");
    client.Stop();
    client.Stop();
}

TEST(ZMQClient, TruncatedMultipartDoesNotBlockStop) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    ASSERT_EQ(client.Start(), "");

    ASSERT_TRUE(PublishUntilHandled(*handler, 76, publisher.PubEndpoint(), [&] {
        publisher.Publish("", PackVllmStoredBatch(76), 76);
    }));

    publisher.PublishFrames({"truncated"});
    std::this_thread::sleep_for(std::chrono::milliseconds(250));

    auto stopped = std::async(std::launch::async, [&] { client.Stop(); });
    const auto status = stopped.wait_for(std::chrono::milliseconds(500));
    if (status != std::future_status::ready) {
        // Release an old three-recv implementation so a regression reports a
        // failure instead of hanging the entire test process.
        publisher.PublishFrames({"12345678", "rescue"});
    }
    EXPECT_EQ(status, std::future_status::ready);
    stopped.get();
}

TEST(ZMQClient, ExtraMultipartFramesAreDrainedBeforeNextMessage) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    ASSERT_EQ(client.Start(), "");

    for (int attempt = 0; attempt < 10; ++attempt) {
        publisher.PublishFrames({"topic", "12345678", "payload", "extra"});
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(PublishUntilHandled(*handler, 77, publisher.PubEndpoint(), [&] {
        publisher.Publish("", PackVllmStoredBatch(77), 77);
    }));

    EXPECT_TRUE(handler->FindBatch(77, publisher.PubEndpoint()).has_value());
    client.Stop();
}

TEST(ZMQClient, VllmKindRoutesEmptyAndMisleadingTopics) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    const auto config = TestConfig(publisher);
    const std::string endpoint = config.endpoint;
    ZMQClient client(config, handler);
    ASSERT_EQ(client.Start(), "");

    const auto empty_topic_payload = PackVllmStoredBatch(42);
    ASSERT_TRUE(PublishUntilHandled(*handler, 41, endpoint, [&] {
        publisher.Publish("", empty_topic_payload, 41);
    }));
    const auto misleading_topic_payload = PackVllmStoredBatch(43);
    ASSERT_TRUE(PublishUntilHandled(*handler, 42, endpoint, [&] {
        publisher.Publish("mooncake", misleading_topic_payload, 42);
    }));

    const auto empty_topic = handler->FindBatch(41, endpoint);
    ASSERT_TRUE(empty_topic.has_value());
    EXPECT_EQ(empty_topic->metadata.publisher_kind, PublisherKind::kVllm);
    EXPECT_EQ(empty_topic->metadata.endpoint, endpoint);
    EXPECT_EQ(empty_topic->metadata.topic, "");
    EXPECT_EQ(empty_topic->metadata.sequence, 41);
    const auto* empty_batch = std::get_if<VllmEventBatch>(&empty_topic->batch);
    ASSERT_NE(empty_batch, nullptr);
    EXPECT_DOUBLE_EQ(empty_batch->timestamp_seconds, 1.25);
    EXPECT_EQ(empty_batch->data_parallel_rank, std::optional<int64_t>(3));
    const auto* empty_stored = GetVllmStored(*empty_topic);
    ASSERT_NE(empty_stored, nullptr);
    ASSERT_EQ(empty_stored->block_hashes.size(), 1u);
    EXPECT_EQ(std::get<uint64_t>(empty_stored->block_hashes[0]), 42u);

    const auto misleading_topic = handler->FindBatch(42, endpoint);
    ASSERT_TRUE(misleading_topic.has_value());
    EXPECT_EQ(misleading_topic->metadata.publisher_kind, PublisherKind::kVllm);
    EXPECT_EQ(misleading_topic->metadata.endpoint, endpoint);
    EXPECT_EQ(misleading_topic->metadata.topic, "mooncake");
    EXPECT_EQ(misleading_topic->metadata.sequence, 42);
    const auto* misleading_stored = GetVllmStored(*misleading_topic);
    ASSERT_NE(misleading_stored, nullptr);
    ASSERT_EQ(misleading_stored->block_hashes.size(), 1u);
    EXPECT_EQ(std::get<uint64_t>(misleading_stored->block_hashes[0]), 43u);
    client.Stop();
}

TEST(ZMQClient, EqualTopicsOnDistinctEndpointsPreserveProvenance) {
    MockPublisher publisher_a;
    MockPublisher publisher_b;
    auto handler = std::make_shared<MockEventHandler>();
    auto config_a = TestConfig(publisher_a);
    auto config_b = TestConfig(publisher_b);
    config_a.cache_pool_key = "instance-a";
    config_b.cache_pool_key = "instance-b";
    const std::string endpoint_a = config_a.endpoint;
    const std::string endpoint_b = config_b.endpoint;
    ASSERT_NE(endpoint_a, endpoint_b);
    ZMQClient client_a(config_a, handler);
    ZMQClient client_b(config_b, handler);
    ASSERT_EQ(client_a.Start(), "");
    ASSERT_EQ(client_b.Start(), "");

    const auto payload_a = PackVllmStoredBatch(101);
    const auto payload_b = PackVllmStoredBatch(202);
    ASSERT_TRUE(PublishUntilHandled(*handler, 77, endpoint_a, [&] {
        publisher_a.Publish("shared-topic", payload_a, 77);
    }));
    ASSERT_TRUE(PublishUntilHandled(*handler, 77, endpoint_b, [&] {
        publisher_b.Publish("shared-topic", payload_b, 77);
    }));

    const auto handled_a = handler->FindBatch(77, endpoint_a);
    const auto handled_b = handler->FindBatch(77, endpoint_b);
    ASSERT_TRUE(handled_a.has_value());
    ASSERT_TRUE(handled_b.has_value());
    EXPECT_EQ(handled_a->metadata.topic, "shared-topic");
    EXPECT_EQ(handled_b->metadata.topic, "shared-topic");
    EXPECT_EQ(handled_a->metadata.publisher_kind, PublisherKind::kVllm);
    EXPECT_EQ(handled_b->metadata.publisher_kind, PublisherKind::kVllm);
    const auto* stored_a = GetVllmStored(*handled_a);
    const auto* stored_b = GetVllmStored(*handled_b);
    ASSERT_NE(stored_a, nullptr);
    ASSERT_NE(stored_b, nullptr);
    EXPECT_EQ(std::get<uint64_t>(stored_a->block_hashes[0]), 101u);
    EXPECT_EQ(std::get<uint64_t>(stored_b->block_hashes[0]), 202u);
    client_a.Stop();
    client_b.Stop();
}

TEST(ZMQClient, MooncakeLiveOnlyConsumesEmptyAndMisleadingTopics) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    auto config = TestConfig(publisher);
    config.publisher_kind = PublisherKind::kMooncake;
    config.replay_endpoint.clear();
    const std::string endpoint = config.endpoint;
    ZMQClient client(config, handler);
    ASSERT_EQ(client.Start(), "");
    EXPECT_FALSE(ZMQClientTestPeer::HasReplaySocket(client));

    const auto empty_topic_payload = PackMooncakeStoredBatch(9001, 42);
    ASSERT_TRUE(PublishUntilHandled(*handler, 101, endpoint, [&] {
        publisher.Publish("", empty_topic_payload, 101);
    }));
    const auto misleading_topic_payload = PackMooncakeStoredBatch(9002, 43);
    ASSERT_TRUE(PublishUntilHandled(*handler, 102, endpoint, [&] {
        publisher.Publish("vllm", misleading_topic_payload, 102);
    }));

    const auto empty_topic = handler->FindBatch(101, endpoint);
    ASSERT_TRUE(empty_topic.has_value());
    EXPECT_EQ(empty_topic->metadata.publisher_kind, PublisherKind::kMooncake);
    EXPECT_EQ(empty_topic->metadata.endpoint, endpoint);
    EXPECT_EQ(empty_topic->metadata.topic, "");
    EXPECT_EQ(empty_topic->metadata.sequence, 101);
    const auto* empty_batch =
        std::get_if<MooncakeEventBatch>(&empty_topic->batch);
    ASSERT_NE(empty_batch, nullptr);
    EXPECT_EQ(empty_batch->timestamp_milliseconds, 1700000000123);
    EXPECT_EQ(empty_batch->data_parallel_rank, std::optional<int64_t>(7));
    const auto* empty_stored = GetMooncakeStored(*empty_topic);
    ASSERT_NE(empty_stored, nullptr);
    EXPECT_EQ(empty_stored->fields.event_id, 9001u);
    EXPECT_EQ(empty_stored->fields.backend_id, "backend-a");
    EXPECT_EQ(empty_stored->object.object_key,
              std::optional<std::string>("object-42"));
    EXPECT_EQ(empty_stored->object.seq_hashes, (std::vector<uint64_t>{42}));
    EXPECT_FALSE(empty_stored->parent_hash.has_value());
    EXPECT_FALSE(empty_stored->token_ids.has_value());

    const auto misleading_topic = handler->FindBatch(102, endpoint);
    ASSERT_TRUE(misleading_topic.has_value());
    EXPECT_EQ(misleading_topic->metadata.publisher_kind,
              PublisherKind::kMooncake);
    EXPECT_EQ(misleading_topic->metadata.endpoint, endpoint);
    EXPECT_EQ(misleading_topic->metadata.topic, "vllm");
    EXPECT_EQ(misleading_topic->metadata.sequence, 102);
    const auto* misleading_stored = GetMooncakeStored(*misleading_topic);
    ASSERT_NE(misleading_stored, nullptr);
    EXPECT_EQ(misleading_stored->fields.event_id, 9002u);
    EXPECT_EQ(misleading_stored->object.seq_hashes,
              (std::vector<uint64_t>{43}));
    EXPECT_EQ(publisher.ReplayRequestCount(), 0u);

    const auto start = std::chrono::steady_clock::now();
    client.Stop();
    EXPECT_LT(std::chrono::steady_clock::now() - start,
              std::chrono::seconds(2));
    EXPECT_FALSE(ZMQClientTestPeer::HasReplaySocket(client));
}

TEST(ZMQClient, SequenceTrackingWithReplayConfigured) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    const auto config = TestConfig(publisher);
    const std::string endpoint = config.endpoint;
    ZMQClient client(config, handler);
    ASSERT_EQ(client.Start(), "");
    EXPECT_EQ(client.GetLastSequence(), -1);

    const auto first_payload = PackVllmStoredBatch(1);
    ASSERT_TRUE(PublishUntilHandled(*handler, 10, endpoint, [&] {
        publisher.Publish("", first_payload, 10);
    }));
    for (uint64_t sequence = 11; sequence <= 14; ++sequence) {
        publisher.Publish("", PackVllmStoredBatch(sequence), sequence);
    }
    ASSERT_TRUE(handler->WaitForBatch(14, endpoint, std::chrono::seconds(2)));
    EXPECT_EQ(client.GetLastSequence(), 14);
    EXPECT_EQ(publisher.ReplayRequestCount(), 0u);
    client.Stop();
}

TEST(ZMQClient, EventGapIsProcessedWithoutAutomaticReplay) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    const auto config = TestConfig(publisher);
    const std::string endpoint = config.endpoint;
    ZMQClient client(config, handler);
    ASSERT_EQ(client.Start(), "");

    const auto first_payload = PackVllmStoredBatch(1);
    ASSERT_TRUE(PublishUntilHandled(*handler, 10, endpoint, [&] {
        publisher.Publish("", first_payload, 10);
    }));
    publisher.Publish("", PackVllmStoredBatch(2), 15);
    ASSERT_TRUE(handler->WaitForBatch(15, endpoint, std::chrono::seconds(2)));
    EXPECT_EQ(client.GetLastSequence(), 15);
    EXPECT_EQ(publisher.ReplayRequestCount(), 0u);
    client.Stop();
}

TEST(ZMQClient, ReconnectRequestsReplayFromNextSequence) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    ASSERT_EQ(client.Connect(), "");
    ASSERT_TRUE(ZMQClientTestPeer::HasReplaySocket(client));
    ZMQClientTestPeer::SetLastSequence(client, 10);

    ZMQClientTestPeer::MarkDisconnected(client);
    ZMQClientTestPeer::HandleReconnect(client);

    EXPECT_TRUE(ZMQClientTestPeer::IsConnected(client));
    ASSERT_TRUE(publisher.WaitForReplayRequests(1, std::chrono::seconds(2)));
    EXPECT_EQ(publisher.ReplayRequestCount(), 1u);
    EXPECT_EQ(publisher.LastReplayFromSequence(), 11u);
    client.Stop();
}

TEST(ZMQClient, FailedReconnectDoesNotRequestReplayUntilSuccess) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    ZMQClientTestPeer::SetLastSequence(client, 10);

    ZMQClientTestPeer::SetEndpoint(client, "not-a-valid-zmq-endpoint");
    ZMQClientTestPeer::HandleReconnect(client);
    EXPECT_FALSE(ZMQClientTestPeer::IsConnected(client));
    EXPECT_FALSE(ZMQClientTestPeer::HasReplaySocket(client));
    EXPECT_EQ(publisher.ReplayRequestCount(), 0u);

    ZMQClientTestPeer::SetEndpoint(client, publisher.PubEndpoint());
    ZMQClientTestPeer::HandleReconnect(client);
    EXPECT_TRUE(ZMQClientTestPeer::IsConnected(client));
    EXPECT_TRUE(ZMQClientTestPeer::HasReplaySocket(client));
    ASSERT_TRUE(publisher.WaitForReplayRequests(1, std::chrono::seconds(2)));
    EXPECT_EQ(publisher.ReplayRequestCount(), 1u);
    EXPECT_EQ(publisher.LastReplayFromSequence(), 11u);
    client.Stop();
}

}  // namespace
