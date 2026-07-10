// Tests for ZMQClient: connect and idempotent connect, start/stop,
// per-topic message processing, sequence tracking and gap handling,
// graceful stop, and replay-after-reconnect.

#include <gtest/gtest.h>
#include <msgpack.hpp>
#include <zmq.hpp>
#include <zmq_addon.hpp>

#include <atomic>
#include <chrono>
#include <mutex>
#include <sstream>
#include <thread>
#include <vector>

#include "conductor/zmq/zmq_client.h"

namespace {

using conductor::zmq::BlockStoredEvent;
using conductor::zmq::EventHandler;
using conductor::zmq::KVEvent;
using conductor::zmq::ValidateConfig;
using conductor::zmq::ZMQClient;
using conductor::zmq::ZMQClientConfig;

// --- Mock event handler ---------------------------------------------------

class MockEventHandler : public EventHandler {
   public:
    std::string HandleEvent(const KVEvent& event, int64_t dp_rank) override {
        std::lock_guard<std::mutex> lock(mu_);
        events_.push_back(event);
        dp_ranks_.push_back(dp_rank);
        return "";
    }

    std::vector<KVEvent> GetEvents() {
        std::lock_guard<std::mutex> lock(mu_);
        return events_;
    }

    size_t EventCount() {
        std::lock_guard<std::mutex> lock(mu_);
        return events_.size();
    }

    bool WaitForEvents(size_t n, std::chrono::milliseconds timeout) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            if (EventCount() >= n) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return EventCount() >= n;
    }

   private:
    std::mutex mu_;
    std::vector<KVEvent> events_;
    std::vector<int64_t> dp_ranks_;
};

// --- Mock publisher (Go: MockPublisher, PUB + ROUTER) ----------------------

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
        // Give the sockets a beat to finish binding.
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

    // Publishes a minimal vLLM BlockStored batch: [ts, [event], dp_rank].
    void PublishVllmStored(const std::vector<uint64_t>& hashes, uint64_t parent,
                           const std::vector<int32_t>& tokens,
                           int64_t block_size, int64_t dp_rank = 0,
                           uint64_t seq_override = 0) {
        std::stringstream buf;
        msgpack::packer<std::stringstream> pk(buf);
        pk.pack_array(3);
        pk.pack_int64(1700000000);
        pk.pack_array(1);
        pk.pack_array(7);
        pk.pack(std::string("BlockStored"));
        pk.pack_array(static_cast<uint32_t>(hashes.size()));
        for (const auto h : hashes) pk.pack_uint64(h);
        pk.pack_uint64(parent);
        pk.pack_array(static_cast<uint32_t>(tokens.size()));
        for (const auto t : tokens) pk.pack_int32(t);
        pk.pack_int64(block_size);
        pk.pack_nil();
        pk.pack(std::string("GPU"));
        pk.pack_int64(dp_rank);
        Send("vllm", buf.str(), seq_override);
    }

    // Publishes a Mooncake BlockStoreEvent batch: [ts, [event]].
    void PublishMooncakeStored(const std::string& key,
                               const std::vector<uint64_t>& hashes,
                               uint64_t parent,
                               const std::vector<int32_t>& tokens,
                               int64_t block_size) {
        std::stringstream buf;
        msgpack::packer<std::stringstream> pk(buf);
        pk.pack_array(2);
        pk.pack_int64(1700000000);
        pk.pack_array(1);
        pk.pack_array(8);
        pk.pack(std::string("BlockStoreEvent"));
        pk.pack(key);
        pk.pack_array(1);  // replica_list
        pk.pack_array(1);
        pk.pack(std::string("replica1"));
        pk.pack_nil();
        pk.pack_int64(block_size);
        pk.pack_array(static_cast<uint32_t>(hashes.size()));
        for (const auto h : hashes) pk.pack_uint64(h);
        pk.pack_uint64(parent);
        pk.pack_array(static_cast<uint32_t>(tokens.size()));
        for (const auto t : tokens) pk.pack_int32(t);
        Send("mooncake", buf.str(), 0);
    }

    size_t ReplayRequestCount() { return replay_requests_.load(); }

    uint64_t LastReplayFromSeq() { return last_replay_from_seq_.load(); }

   private:
    void Send(const std::string& topic, const std::string& payload,
              uint64_t seq_override) {
        const uint64_t seq = seq_override ? seq_override : ++sequence_;
        unsigned char seq_bytes[8];
        for (int i = 7; i >= 0; --i) {
            seq_bytes[i] =
                static_cast<unsigned char>((seq >> (8 * (7 - i))) & 0xFF);
        }
        if (seq_override) sequence_ = seq_override;

        std::array<::zmq::const_buffer, 3> frames = {
            ::zmq::buffer(topic),
            ::zmq::buffer(seq_bytes, sizeof(seq_bytes)),
            ::zmq::buffer(payload),
        };
        ::zmq::send_multipart(pub_, frames);
    }

    void HandleReplay() {
        router_.set(::zmq::sockopt::rcvtimeo, 100);
        while (!closed_.load()) {
            std::vector<::zmq::message_t> frames;
            const auto n = ::zmq::recv_multipart(
                router_, std::back_inserter(frames), ::zmq::recv_flags::none);
            if (!n) continue;
            // ROUTER frames: [identity, payload]
            if (frames.size() == 2 && frames[1].size() == 8) {
                const auto* b =
                    static_cast<const unsigned char*>(frames[1].data());
                uint64_t from = 0;
                for (int i = 0; i < 8; ++i) from = (from << 8) | b[i];
                last_replay_from_seq_.store(from);
                replay_requests_.fetch_add(1);
                // Send ACK back through the same identity.
                std::array<::zmq::const_buffer, 2> reply = {
                    ::zmq::buffer(frames[0].data(), frames[0].size()),
                    ::zmq::buffer(std::string("OK")),
                };
                ::zmq::send_multipart(router_, reply);
            }
        }
    }

    ::zmq::context_t ctx_;
    ::zmq::socket_t pub_;
    ::zmq::socket_t router_;
    uint64_t sequence_ = 0;
    std::atomic<bool> closed_{false};
    std::atomic<size_t> replay_requests_{0};
    std::atomic<uint64_t> last_replay_from_seq_{0};
    std::thread replay_thread_;
};

ZMQClientConfig TestConfig(MockPublisher& pub) {
    ZMQClientConfig config;
    config.cache_pool_key = "test-pod";
    config.endpoint = pub.PubEndpoint();
    config.replay_endpoint = pub.RouterEndpoint();
    config.model_name = "test-model";
    config.poll_timeout = std::chrono::milliseconds(100);
    config.replay_timeout = std::chrono::milliseconds(1000);
    config.reconnect_delay = std::chrono::milliseconds(100);
    return config;
}

// --- Tests -----------------------------------------------------------------

TEST(ValidateConfig, RequiresEndpoint) {
    ZMQClientConfig config;
    EXPECT_FALSE(ValidateConfig(config).empty());
    config.endpoint = "tcp://127.0.0.1:5557";
    EXPECT_TRUE(ValidateConfig(config).empty());
}

TEST(ZMQClient, ConnectSuccess) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    EXPECT_EQ(client.Connect(), "");
    client.Stop();
}

TEST(ZMQClient, ConnectAlreadyConnectedIsNoop) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    EXPECT_EQ(client.Connect(), "");
    EXPECT_EQ(client.Connect(), "");  // second connect must not error
    client.Stop();
}

TEST(ZMQClient, StartStopGraceful) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    EXPECT_EQ(client.Start(), "");
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    // Spec: Stop returns within roughly one poll cycle.
    const auto t0 = std::chrono::steady_clock::now();
    client.Stop();
    const auto elapsed = std::chrono::steady_clock::now() - t0;
    EXPECT_LT(elapsed, std::chrono::seconds(2));
}

TEST(ZMQClient, StopIsIdempotent) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    EXPECT_EQ(client.Start(), "");
    client.Stop();
    client.Stop();  // second call must return immediately, no crash
}

TEST(ZMQClient, ProcessMessageVllmTopic) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    ASSERT_EQ(client.Start(), "");

    // PUB/SUB slow joiner: retry publish until the event lands.
    bool got = false;
    for (int attempt = 0; attempt < 20 && !got; ++attempt) {
        publisher.PublishVllmStored({300, 400}, 1500000000000000ULL, {4, 5, 6},
                                    2048);
        got = handler->WaitForEvents(1, std::chrono::milliseconds(200));
    }
    ASSERT_TRUE(got);

    const auto events = handler->GetEvents();
    const auto* event = std::get_if<BlockStoredEvent>(&events[0]);
    ASSERT_NE(event, nullptr);
    EXPECT_EQ(event->pod_name, "test-pod");  // source key injected
    ASSERT_FALSE(event->block_hashes.empty());
    EXPECT_EQ(event->block_hashes[0], 300u);
    EXPECT_EQ(event->block_size, 2048);
    client.Stop();
}

TEST(ZMQClient, ProcessMessageMooncakeTopic) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    ASSERT_EQ(client.Start(), "");

    bool got = false;
    for (int attempt = 0; attempt < 20 && !got; ++attempt) {
        publisher.PublishMooncakeStored("mooncake-key-123", {100, 200}, 50,
                                        {1, 2, 3}, 1024);
        got = handler->WaitForEvents(1, std::chrono::milliseconds(200));
    }
    ASSERT_TRUE(got);

    const auto events = handler->GetEvents();
    const auto* event = std::get_if<BlockStoredEvent>(&events[0]);
    ASSERT_NE(event, nullptr);
    EXPECT_EQ(event->pod_name, "test-pod");
    EXPECT_EQ(event->mooncake_key, "mooncake-key-123");
    ASSERT_FALSE(event->block_hashes.empty());
    EXPECT_EQ(event->block_hashes[0], 100u);
    client.Stop();
}

TEST(ZMQClient, SequenceTracking) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    ASSERT_EQ(client.Start(), "");
    EXPECT_EQ(client.GetLastSequence(), -1);

    bool got = false;
    for (int attempt = 0; attempt < 20 && !got; ++attempt) {
        publisher.PublishVllmStored({1}, 1500000000000000ULL, {1}, 128);
        got = handler->WaitForEvents(1, std::chrono::milliseconds(200));
    }
    ASSERT_TRUE(got);
    const size_t baseline = handler->EventCount();

    for (int i = 0; i < 4; ++i) {
        publisher.PublishVllmStored({static_cast<uint64_t>(10 + i)},
                                    1500000000000000ULL,
                                    {static_cast<int32_t>(i)}, 128);
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_TRUE(
        handler->WaitForEvents(baseline + 4, std::chrono::milliseconds(2000)));
    // lastSeq tracks the publisher's monotonically increasing sequence.
    EXPECT_GE(client.GetLastSequence(), static_cast<int64_t>(baseline + 4));
    client.Stop();
}

// Spec scenario: seq gap only logs a warning, lastSeq updates
// unconditionally, and the message is processed normally.
TEST(ZMQClient, EventGapProcessedNormally) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    ASSERT_EQ(client.Start(), "");

    bool got = false;
    for (int attempt = 0; attempt < 20 && !got; ++attempt) {
        publisher.PublishVllmStored({1}, 1500000000000000ULL, {1}, 128);
        got = handler->WaitForEvents(1, std::chrono::milliseconds(200));
    }
    ASSERT_TRUE(got);
    const size_t baseline = handler->EventCount();
    const int64_t seq_before = client.GetLastSequence();

    // Skip ahead: publish with a gap (seq jumps by +5).
    publisher.PublishVllmStored({2}, 1500000000000000ULL, {2}, 128, 0,
                                static_cast<uint64_t>(seq_before + 5));
    ASSERT_TRUE(
        handler->WaitForEvents(baseline + 1, std::chrono::milliseconds(2000)));
    EXPECT_EQ(client.GetLastSequence(), seq_before + 5);
    // No automatic replay from gap detection (bug-for-bug).
    EXPECT_EQ(publisher.ReplayRequestCount(), 0u);
    client.Stop();
}

// Spec scenario: after a disconnect-reconnect cycle with lastSeq >= 0,
// the client requests replay from lastSeq+1 via the DEALER socket.
TEST(ZMQClient, ReplayRequestedAfterReconnect) {
    MockPublisher publisher;
    auto handler = std::make_shared<MockEventHandler>();
    ZMQClient client(TestConfig(publisher), handler);
    ASSERT_EQ(client.Start(), "");

    bool got = false;
    for (int attempt = 0; attempt < 20 && !got; ++attempt) {
        publisher.PublishVllmStored({1}, 1500000000000000ULL, {1}, 128);
        got = handler->WaitForEvents(1, std::chrono::milliseconds(200));
    }
    ASSERT_TRUE(got);
    const int64_t last_seq = client.GetLastSequence();
    ASSERT_GE(last_seq, 0);

    // Publish a malformed frame set (bad payload) to trip Consume into
    // the error path -> markDisconnected -> handleReconnect.
    // Simpler and deterministic: publish garbage payload on the vllm
    // topic; decode fails, the client marks itself disconnected and
    // reconnects, then requests replay.
    {
        std::stringstream buf;
        msgpack::packer<std::stringstream> pk(buf);
        pk.pack_int64(42);  // not an array -> unmarshal-shape error
        // Manually send with next seq.
        // (Reuse PublishVllmStored's channel by crafting via the public
        // API is not possible; send through a fresh PUB socket is not
        // needed — MockPublisher::Send is private, so publish a valid
        // topic with an invalid payload via a dedicated helper below.)
        // For simplicity, publish an event whose parent hash is small —
        // the decoder rejects it, which also drives the error path.
    }
    publisher.PublishVllmStored({2}, /*parent=*/50, {2}, 128);

    // Wait for reconnect + replay request to arrive at the ROUTER.
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (publisher.ReplayRequestCount() == 0 &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    ASSERT_GE(publisher.ReplayRequestCount(), 1u);
    // lastSeq updates BEFORE decoding: the sequence advances first, then
    // the decode of this bad message fails, so the bad message's own seq
    // (last_seq+1) is already consumed and replay starts at last_seq+2.
    EXPECT_EQ(publisher.LastReplayFromSeq(),
              static_cast<uint64_t>(last_seq + 2));
    client.Stop();
}

}  // namespace
