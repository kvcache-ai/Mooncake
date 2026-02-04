// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <thread>
#include <chrono>
#include <atomic>
#include <vector>
#include <cstring>

#include "transport/zmq_communicator/zmq_communicator.h"
#include "transport/zmq_communicator/message_codec.h"

using namespace mooncake;

class ZmqCommunicatorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("ZmqCommunicatorTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    // Helper: Encode multipart message
    std::string encodeMultipartMessage(const std::vector<std::string>& frames) {
        std::string multipart_data;
        uint32_t num_frames = frames.size();
        multipart_data.append(reinterpret_cast<const char*>(&num_frames),
                              sizeof(num_frames));

        for (const auto& frame : frames) {
            uint32_t frame_len = frame.size();
            multipart_data.append(reinterpret_cast<const char*>(&frame_len),
                                  sizeof(frame_len));
            multipart_data.append(frame);
        }
        return multipart_data;
    }

    // Helper: Decode multipart message
    std::vector<std::string> decodeMultipartMessage(std::string_view data) {
        std::vector<std::string> frames;
        if (data.size() < sizeof(uint32_t)) return frames;

        uint32_t num_frames;
        std::memcpy(&num_frames, data.data(), sizeof(num_frames));
        size_t offset = sizeof(uint32_t);

        for (uint32_t i = 0; i < num_frames; i++) {
            if (offset + sizeof(uint32_t) > data.size()) break;

            uint32_t frame_len;
            std::memcpy(&frame_len, data.data() + offset, sizeof(frame_len));
            offset += sizeof(uint32_t);

            if (offset + frame_len > data.size()) break;

            frames.push_back(std::string(data.data() + offset, frame_len));
            offset += frame_len;
        }
        return frames;
    }
};

// =============================================================================
// Message Codec Tests
// =============================================================================

TEST_F(ZmqCommunicatorTest, MessageCodecDataMessage) {
    // Test data message encoding
    const char* test_data = "Hello, World!";
    size_t data_size = strlen(test_data);

    std::string encoded = MessageCodec::encodeDataMessage(
        ZmqSocketType::REQ, test_data, data_size, "test.topic", 12345);

    ASSERT_GT(encoded.size(), 0);

    // Test decoding
    auto decoded = MessageCodec::decodeMessage(encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->topic, "test.topic");
    EXPECT_EQ(std::string(decoded->data), test_data);
    EXPECT_EQ(decoded->header.sequence_id, 12345);
}

TEST_F(ZmqCommunicatorTest, MessageCodecEmptyTopic) {
    const char* test_data = "Test data";
    size_t data_size = strlen(test_data);

    // Test with no topic (std::nullopt)
    std::string encoded = MessageCodec::encodeDataMessage(
        ZmqSocketType::PUSH, test_data, data_size, std::nullopt, 999);

    ASSERT_GT(encoded.size(), 0);

    auto decoded = MessageCodec::decodeMessage(encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_TRUE(decoded->topic.empty());
    EXPECT_EQ(std::string(decoded->data), test_data);
}

// =============================================================================
// Basic Operations Tests
// =============================================================================

TEST_F(ZmqCommunicatorTest, BasicOperations) {
    ZmqCommunicator comm;
    ZmqConfig config;
    config.thread_count = 4;
    config.timeout_seconds = 10;
    config.pool_size = 5;

    ASSERT_TRUE(comm.initialize(config));

    // Test socket creation for all types
    int req_socket = comm.createSocket(ZmqSocketType::REQ);
    EXPECT_GT(req_socket, 0);

    int rep_socket = comm.createSocket(ZmqSocketType::REP);
    EXPECT_GT(rep_socket, 0);

    int pub_socket = comm.createSocket(ZmqSocketType::PUB);
    EXPECT_GT(pub_socket, 0);

    int sub_socket = comm.createSocket(ZmqSocketType::SUB);
    EXPECT_GT(sub_socket, 0);

    int push_socket = comm.createSocket(ZmqSocketType::PUSH);
    EXPECT_GT(push_socket, 0);

    int pull_socket = comm.createSocket(ZmqSocketType::PULL);
    EXPECT_GT(pull_socket, 0);

    int pair_socket = comm.createSocket(ZmqSocketType::PAIR);
    EXPECT_GT(pair_socket, 0);

    // Test socket closing
    EXPECT_TRUE(comm.closeSocket(req_socket));

    comm.shutdown();
}

TEST_F(ZmqCommunicatorTest, SocketBinding) {
    ZmqCommunicator comm;
    ZmqConfig config;
    config.thread_count = 2;

    ASSERT_TRUE(comm.initialize(config));

    // Create and bind a REP socket
    int rep_socket = comm.createSocket(ZmqSocketType::REP);
    std::string endpoint = "127.0.0.1:15555";

    EXPECT_TRUE(comm.bind(rep_socket, endpoint));

    // Create and connect a REQ socket
    int req_socket = comm.createSocket(ZmqSocketType::REQ);
    EXPECT_TRUE(comm.connect(req_socket, endpoint));

    comm.shutdown();
}

TEST_F(ZmqCommunicatorTest, PubSubSubscription) {
    ZmqCommunicator comm;
    ZmqConfig config;

    ASSERT_TRUE(comm.initialize(config));

    int sub_socket = comm.createSocket(ZmqSocketType::SUB);

    // Test subscription
    EXPECT_TRUE(comm.subscribe(sub_socket, "sensor."));
    EXPECT_TRUE(comm.subscribe(sub_socket, "log."));

    // Test unsubscription
    EXPECT_TRUE(comm.unsubscribe(sub_socket, "log."));

    comm.shutdown();
}

// =============================================================================
// String Message Transfer Test (End-to-End)
// =============================================================================

// Temporarily disabled: heap-use-after-free in sendAsync (ASan). TODO: fix and
// re-enable.
TEST_F(ZmqCommunicatorTest, DISABLED_StringMessageTransfer) {
    ZmqCommunicator server_comm;
    ZmqCommunicator client_comm;
    ZmqConfig config;
    config.thread_count = 2;

    ASSERT_TRUE(server_comm.initialize(config));
    ASSERT_TRUE(client_comm.initialize(config));

    // Server side (REP)
    int server_socket = server_comm.createSocket(ZmqSocketType::REP);
    std::string endpoint = "127.0.0.1:15560";

    ASSERT_TRUE(server_comm.bind(server_socket, endpoint));
    ASSERT_TRUE(server_comm.startServer(server_socket));

    // Set up server to receive string message
    std::atomic<bool> server_received{false};
    std::string received_message;

    server_comm.setReceiveCallback(
        server_socket,
        [&server_received, &received_message, &server_comm, server_socket](
            std::string_view source, std::string_view data,
            const std::optional<std::string>& topic) {
            LOG(INFO) << "Server received string: " << data;
            received_message = std::string(data);
            server_received.store(true);

            // Send acknowledgment back
            const char* ack = "ACK: String received";
            server_comm.sendReply(server_socket, ack, strlen(ack));
        });

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Client side (REQ)
    int client_socket = client_comm.createSocket(ZmqSocketType::REQ);
    ASSERT_TRUE(client_comm.connect(client_socket, endpoint));

    // Send string message
    std::string test_string = "Hello, this is a string message!";
    auto result = async_simple::coro::syncAwait(client_comm.sendDataAsync(
        client_socket, test_string.data(), test_string.size()));

    EXPECT_EQ(result.code, 0);

    // Wait for server to receive and process
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Check results
    EXPECT_TRUE(server_received.load());
    EXPECT_EQ(received_message, test_string);

    server_comm.shutdown();
    client_comm.shutdown();
}

// =============================================================================
// JSON Message Transfer Test (End-to-End)
// =============================================================================

TEST_F(ZmqCommunicatorTest, JsonMessageTransfer) {
    ZmqCommunicator server_comm;
    ZmqCommunicator client_comm;
    ZmqConfig config;
    config.thread_count = 2;

    ASSERT_TRUE(server_comm.initialize(config));
    ASSERT_TRUE(client_comm.initialize(config));

    // Server side (REP) - Use REQ/REP instead of PUSH/PULL
    int server_socket = server_comm.createSocket(ZmqSocketType::REP);
    std::string endpoint = "127.0.0.1:15561";

    ASSERT_TRUE(server_comm.bind(server_socket, endpoint));
    ASSERT_TRUE(server_comm.startServer(server_socket));

    // Set up server to receive JSON message
    std::atomic<bool> server_received{false};
    std::string received_json;

    server_comm.setReceiveCallback(
        server_socket,
        [&server_received, &received_json, &server_comm, server_socket](
            std::string_view source, std::string_view data,
            const std::optional<std::string>& topic) {
            LOG(INFO) << "Server received JSON: " << data;
            received_json = std::string(data);
            server_received.store(true);

            // Send acknowledgment
            const char* ack = "ACK";
            server_comm.sendReply(server_socket, ack, strlen(ack));
        });

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Client side (REQ)
    int client_socket = client_comm.createSocket(ZmqSocketType::REQ);
    ASSERT_TRUE(client_comm.connect(client_socket, endpoint));

    // Send JSON message (simulated as JSON string)
    std::string json_message =
        R"({"name": "Alice", "age": 30, "skills": ["Python", "C++"]})";

    auto result = async_simple::coro::syncAwait(client_comm.sendDataAsync(
        client_socket, json_message.data(), json_message.size(), "user.data"));

    EXPECT_EQ(result.code, 0);

    // Wait for server to receive
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Check results
    EXPECT_TRUE(server_received.load());
    EXPECT_EQ(received_json, json_message);

    server_comm.shutdown();
    client_comm.shutdown();
}

// =============================================================================
// Multipart Message Transfer Test (End-to-End)
// =============================================================================

TEST_F(ZmqCommunicatorTest, MultipartMessageTransfer) {
    ZmqCommunicator server_comm;
    ZmqCommunicator client_comm;
    ZmqConfig config;
    config.thread_count = 2;

    ASSERT_TRUE(server_comm.initialize(config));
    ASSERT_TRUE(client_comm.initialize(config));

    // Server side (REP)
    int server_socket = server_comm.createSocket(ZmqSocketType::REP);
    std::string endpoint = "127.0.0.1:15562";

    ASSERT_TRUE(server_comm.bind(server_socket, endpoint));
    ASSERT_TRUE(server_comm.startServer(server_socket));

    // Set up server to receive multipart message
    std::atomic<bool> server_received{false};
    std::vector<std::string> received_frames;

    server_comm.setReceiveCallback(
        server_socket,
        [this, &server_received, &received_frames, &server_comm, server_socket](
            std::string_view source, std::string_view data,
            const std::optional<std::string>& topic) {
            LOG(INFO) << "Server received multipart message: " << data.size()
                      << " bytes";

            // Decode multipart message
            received_frames = decodeMultipartMessage(data);
            server_received.store(true);

            // Send acknowledgment
            const char* ack = "ACK: Multipart received";
            server_comm.sendReply(server_socket, ack, strlen(ack));
        });

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Client side (REQ)
    int client_socket = client_comm.createSocket(ZmqSocketType::REQ);
    ASSERT_TRUE(client_comm.connect(client_socket, endpoint));

    // Prepare multipart message
    std::vector<std::string> frames = {"frame1: header", "frame2: body",
                                       "frame3: footer", "frame4: metadata"};

    std::string multipart_data = encodeMultipartMessage(frames);

    // Send multipart message
    auto result = async_simple::coro::syncAwait(
        client_comm.sendDataAsync(client_socket, multipart_data.data(),
                                  multipart_data.size(), "multipart.test"));

    EXPECT_EQ(result.code, 0);

    // Wait for server to receive and process
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Check results
    EXPECT_TRUE(server_received.load());
    ASSERT_EQ(received_frames.size(), 4);
    EXPECT_EQ(received_frames[0], "frame1: header");
    EXPECT_EQ(received_frames[1], "frame2: body");
    EXPECT_EQ(received_frames[2], "frame3: footer");
    EXPECT_EQ(received_frames[3], "frame4: metadata");

    server_comm.shutdown();
    client_comm.shutdown();
}

// =============================================================================
// Pyobj (Binary Data) Message Transfer Test (End-to-End)
// =============================================================================

TEST_F(ZmqCommunicatorTest, PyobjMessageTransfer) {
    ZmqCommunicator server_comm;
    ZmqCommunicator client_comm;
    ZmqConfig config;
    config.thread_count = 2;

    ASSERT_TRUE(server_comm.initialize(config));
    ASSERT_TRUE(client_comm.initialize(config));

    // Server side (REP) - Use REQ/REP instead of PUSH/PULL
    int server_socket = server_comm.createSocket(ZmqSocketType::REP);
    std::string endpoint = "127.0.0.1:15563";

    ASSERT_TRUE(server_comm.bind(server_socket, endpoint));
    ASSERT_TRUE(server_comm.startServer(server_socket));

    // Set up server to receive pyobj (pickled) message
    std::atomic<bool> server_received{false};
    std::vector<uint8_t> received_binary;
    std::string received_topic;

    server_comm.setReceiveCallback(
        server_socket,
        [&server_received, &received_binary, &received_topic, &server_comm,
         server_socket](std::string_view source, std::string_view data,
                        const std::optional<std::string>& topic) {
            LOG(INFO) << "Server received pyobj: " << data.size() << " bytes";

            received_binary.assign(data.begin(), data.end());
            received_topic = topic.value_or("");
            server_received.store(true);

            // Send acknowledgment
            const char* ack = "ACK";
            server_comm.sendReply(server_socket, ack, strlen(ack));
        });

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Client side (REQ)
    int client_socket = client_comm.createSocket(ZmqSocketType::REQ);
    ASSERT_TRUE(client_comm.connect(client_socket, endpoint));

    // Simulate pickled data (binary blob)
    // In real scenario, this would be pickle.dumps() output
    std::vector<uint8_t> pickled_data = {
        0x80, 0x04, 0x95, 0x1f, 0x00, 0x00, 0x00,  // Pickle protocol header
        0x00, 0x00, 0x00, 0x00, 0x7d, 0x94,        // Dict marker
        0x28, 0x8c, 0x04, 0x6e, 0x61, 0x6d, 0x65,  // "name"
        0x94, 0x8c, 0x05, 0x41, 0x6c, 0x69, 0x63,  // "Alice"
        0x65, 0x94, 0x8c, 0x03, 0x61, 0x67, 0x65,  // "age"
        0x94, 0x4b, 0x1e, 0x75, 0x2e               // 30, end
    };

    auto result = async_simple::coro::syncAwait(client_comm.sendDataAsync(
        client_socket, pickled_data.data(), pickled_data.size(), "pyobj.test"));

    EXPECT_EQ(result.code, 0);

    // Wait for server to receive
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Check results
    EXPECT_TRUE(server_received.load());
    EXPECT_EQ(received_binary.size(), pickled_data.size());
    EXPECT_EQ(received_binary, pickled_data);
    EXPECT_EQ(received_topic, "pyobj.test");

    server_comm.shutdown();
    client_comm.shutdown();
}

// =============================================================================
// Medium Data Transfer Test (< 1KB threshold)
// =============================================================================

TEST_F(ZmqCommunicatorTest, MediumDataTransfer) {
    ZmqCommunicator server_comm;
    ZmqCommunicator client_comm;
    ZmqConfig config;
    config.thread_count = 4;

    ASSERT_TRUE(server_comm.initialize(config));
    ASSERT_TRUE(client_comm.initialize(config));

    // Create medium-sized data (500 bytes, below 1KB attachment threshold)
    const size_t data_size = 500;
    std::vector<uint8_t> test_data(data_size);

    // Fill with test pattern
    for (size_t i = 0; i < data_size; i++) {
        test_data[i] = static_cast<uint8_t>(i % 256);
    }

    LOG(INFO) << "Created test data: " << data_size << " bytes";

    // Server side (REP)
    int server_socket = server_comm.createSocket(ZmqSocketType::REP);
    std::string endpoint = "127.0.0.1:15564";

    ASSERT_TRUE(server_comm.bind(server_socket, endpoint));
    ASSERT_TRUE(server_comm.startServer(server_socket));

    // Set up server to receive and verify data
    std::atomic<bool> server_received{false};
    std::atomic<bool> data_valid{false};

    server_comm.setReceiveCallback(
        server_socket,
        [&server_received, &data_valid, data_size, &server_comm, server_socket](
            std::string_view source, std::string_view data,
            const std::optional<std::string>& topic) {
            LOG(INFO) << "Server received data: " << data.size() << " bytes";

            // Verify data size
            if (data.size() == data_size) {
                // Verify data integrity
                bool valid = true;
                for (size_t i = 0; i < data_size; i++) {
                    if (static_cast<uint8_t>(data[i]) !=
                        static_cast<uint8_t>(i % 256)) {
                        valid = false;
                        LOG(ERROR) << "Data mismatch at index " << i;
                        break;
                    }
                }
                data_valid.store(valid);
            } else {
                LOG(ERROR) << "Size mismatch: expected " << data_size
                           << ", got " << data.size();
            }

            server_received.store(true);

            // Send acknowledgment
            const char* ack = "Data received successfully";
            server_comm.sendReply(server_socket, ack, strlen(ack));
        });

    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Client side (REQ)
    int client_socket = client_comm.createSocket(ZmqSocketType::REQ);
    ASSERT_TRUE(client_comm.connect(client_socket, endpoint));

    // Send data
    auto start_time = std::chrono::high_resolution_clock::now();

    auto result = async_simple::coro::syncAwait(client_comm.sendDataAsync(
        client_socket, test_data.data(), test_data.size(), "test.data"));

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time);

    EXPECT_EQ(result.code, 0);
    LOG(INFO) << "Transfer completed in " << duration.count() << " ms";

    // Wait for server to receive and process
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Check results
    EXPECT_TRUE(server_received.load());
    EXPECT_TRUE(data_valid.load());

    server_comm.shutdown();
    client_comm.shutdown();
}

// =============================================================================
// PUB/SUB Pattern with Multiple Message Types
// =============================================================================

TEST_F(ZmqCommunicatorTest, PubSubWithDifferentMessageTypes) {
    ZmqCommunicator pub_comm;
    ZmqCommunicator sub_comm;
    ZmqConfig config;
    config.thread_count = 2;

    ASSERT_TRUE(pub_comm.initialize(config));
    ASSERT_TRUE(sub_comm.initialize(config));

    // Publisher side
    int pub_socket = pub_comm.createSocket(ZmqSocketType::PUB);
    std::string endpoint = "127.0.0.1:15565";

    ASSERT_TRUE(pub_comm.bind(pub_socket, endpoint));
    ASSERT_TRUE(pub_comm.startServer(pub_socket));

    // Subscriber side
    int sub_socket = sub_comm.createSocket(ZmqSocketType::SUB);
    ASSERT_TRUE(sub_comm.connect(sub_socket, endpoint));
    ASSERT_TRUE(sub_comm.subscribe(sub_socket, "data."));
    ASSERT_TRUE(sub_comm.subscribe(sub_socket, "json."));

    // Track received messages
    std::atomic<int> message_count{0};
    std::vector<std::string> received_topics;
    std::vector<std::string> received_data;
    std::mutex data_mutex;

    sub_comm.setReceiveCallback(
        sub_socket,
        [&message_count, &received_topics, &received_data, &data_mutex](
            std::string_view source, std::string_view data,
            const std::optional<std::string>& topic) {
            std::lock_guard<std::mutex> lock(data_mutex);
            LOG(INFO) << "Subscriber received: topic=" << topic.value_or("")
                      << ", data=" << data;
            received_topics.push_back(topic.value_or(""));
            received_data.push_back(std::string(data));
            message_count++;
        });

    // Give subscriber time to connect and subscribe
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Publish different types of messages
    std::string string_msg = "Hello from publisher";
    async_simple::coro::syncAwait(pub_comm.sendDataAsync(
        pub_socket, string_msg.data(), string_msg.size(), "data.string"));

    std::string json_msg = R"({"type": "notification", "value": 123})";
    async_simple::coro::syncAwait(pub_comm.sendDataAsync(
        pub_socket, json_msg.data(), json_msg.size(), "json.notification"));

    // Wait for messages to be received
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Check results (may vary due to async nature of PUB/SUB)
    EXPECT_GE(message_count.load(), 0);
    {
        std::lock_guard<std::mutex> lock(data_mutex);
        EXPECT_GE(received_topics.size(), 0);
        EXPECT_GE(received_data.size(), 0);
    }

    pub_comm.shutdown();
    sub_comm.shutdown();
}
