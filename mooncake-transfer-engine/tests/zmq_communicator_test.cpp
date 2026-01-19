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

#include "transport/zmq_communicator/zmq_communicator.h"
#include "transport/zmq_communicator/message_codec.h"

using namespace mooncake;

class ZmqCommunicatorTest : public ::testing::Test {
protected:
    void SetUp() override {
        google::InitGoogleLogging("ZmqCommunicatorTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override {
        google::ShutdownGoogleLogging();
    }
};

TEST_F(ZmqCommunicatorTest, MessageCodecDataMessage) {
    // Test data message encoding
    const char* test_data = "Hello, World!";
    size_t data_size = strlen(test_data);
    
    std::string encoded = MessageCodec::encodeDataMessage(
        ZmqSocketType::REQ,
        test_data,
        data_size,
        "test.topic",
        12345
    );
    
    ASSERT_GT(encoded.size(), 0);
    
    // Test decoding
    auto decoded = MessageCodec::decodeMessage(encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->topic, "test.topic");
    EXPECT_EQ(std::string(decoded->data), test_data);
    EXPECT_EQ(decoded->header.sequence_id, 12345);
}

TEST_F(ZmqCommunicatorTest, MessageCodecTensorMessage) {
    TensorInfo tensor;
    tensor.shape = {2, 3, 4};
    tensor.dtype = "torch.float32";
    tensor.total_bytes = 2 * 3 * 4 * sizeof(float);
    
    // Allocate dummy tensor data
    std::vector<float> data(2 * 3 * 4, 1.5f);
    tensor.data_ptr = data.data();
    
    std::string encoded = MessageCodec::encodeTensorMessage(
        ZmqSocketType::REQ,
        tensor,
        "tensor.topic",
        99999
    );
    
    ASSERT_GT(encoded.size(), 0);
    
    // Test decoding
    auto decoded = MessageCodec::decodeTensorMessage(encoded);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->topic, "tensor.topic");
    EXPECT_EQ(decoded->tensor.dtype, "torch.float32");
    EXPECT_EQ(decoded->tensor.shape.size(), 3);
    EXPECT_EQ(decoded->tensor.total_bytes, tensor.total_bytes);
}

TEST_F(ZmqCommunicatorTest, TensorDimensionValidation) {
    // Test valid tensor with MAX_TENSOR_DIMS dimensions
    {
        TensorInfo tensor;
        tensor.shape = {2, 3, 4, 5};  // Exactly MAX_TENSOR_DIMS (4) dimensions
        tensor.dtype = "torch.float32";
        tensor.total_bytes = 2 * 3 * 4 * 5 * sizeof(float);
        std::vector<float> data(2 * 3 * 4 * 5, 1.5f);
        tensor.data_ptr = data.data();
        
        std::string encoded = MessageCodec::encodeTensorMessage(
            ZmqSocketType::REQ, tensor, "valid.tensor", 0);
        
        ASSERT_GT(encoded.size(), 0) << "4D tensor should be encoded successfully";
        
        auto decoded = MessageCodec::decodeTensorMessage(encoded);
        ASSERT_TRUE(decoded.has_value()) << "4D tensor should be decoded successfully";
        EXPECT_EQ(decoded->tensor.shape.size(), 4);
        EXPECT_EQ(decoded->tensor.shape[0], 2);
        EXPECT_EQ(decoded->tensor.shape[1], 3);
        EXPECT_EQ(decoded->tensor.shape[2], 4);
        EXPECT_EQ(decoded->tensor.shape[3], 5);
    }
    
    // Test invalid tensor with too many dimensions
    {
        TensorInfo tensor;
        tensor.shape = {2, 3, 4, 5, 6};  // 5 dimensions (exceeds MAX_TENSOR_DIMS)
        tensor.dtype = "torch.float32";
        tensor.total_bytes = 2 * 3 * 4 * 5 * 6 * sizeof(float);
        std::vector<float> data(2 * 3 * 4 * 5 * 6, 1.5f);
        tensor.data_ptr = data.data();
        
        std::string encoded = MessageCodec::encodeTensorMessage(
            ZmqSocketType::REQ, tensor, "invalid.tensor", 0);
        
        EXPECT_EQ(encoded.size(), 0) << "5D tensor should be rejected during encoding";
    }
    
    // Test decoding with invalid ndim in header
    {
        TensorInfo tensor;
        tensor.shape = {2, 3, 4};
        tensor.dtype = "torch.float32";
        tensor.total_bytes = 2 * 3 * 4 * sizeof(float);
        std::vector<float> data(2 * 3 * 4, 1.5f);
        tensor.data_ptr = data.data();
        
        std::string encoded = MessageCodec::encodeTensorMessage(
            ZmqSocketType::REQ, tensor, "test.tensor", 0);
        ASSERT_GT(encoded.size(), 0);
        
        // Manually corrupt the ndim field to exceed MAX_TENSOR_DIMS
        TensorMessageHeader* header = reinterpret_cast<TensorMessageHeader*>(encoded.data());
        header->ndim = 10;  // Invalid: exceeds MAX_TENSOR_DIMS
        
        auto decoded = MessageCodec::decodeTensorMessage(encoded);
        EXPECT_FALSE(decoded.has_value()) << "Tensor with invalid ndim should be rejected";
    }
}

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

TEST_F(ZmqCommunicatorTest, CallbackMechanism) {
    ZmqCommunicator comm;
    ZmqConfig config;
    
    ASSERT_TRUE(comm.initialize(config));
    
    int pull_socket = comm.createSocket(ZmqSocketType::PULL);
    
    bool callback_called = false;
    
    comm.setReceiveCallback(pull_socket,
        [&callback_called](std::string_view source, std::string_view data,
                          const std::optional<std::string>& topic) {
            callback_called = true;
        }
    );
    
    // Set tensor callback
    comm.setTensorReceiveCallback(pull_socket,
        [](std::string_view source, const TensorInfo& tensor,
           const std::optional<std::string>& topic) {
            // Callback registered successfully
        }
    );
    
    comm.shutdown();
}

TEST_F(ZmqCommunicatorTest, LargeTensorTransfer) {
    ZmqCommunicator server_comm;
    ZmqCommunicator client_comm;
    ZmqConfig config;
    config.thread_count = 4;
    
    ASSERT_TRUE(server_comm.initialize(config));
    ASSERT_TRUE(client_comm.initialize(config));
    
    // Create a large tensor (10 MB)
    const size_t tensor_size = 2560 * 1024;  // 2,621,440 elements (2.5 MiB or ~2.62M)
    std::vector<float> large_data(tensor_size);
    
    // Fill with test pattern
    for (size_t i = 0; i < tensor_size; i++) {
        large_data[i] = static_cast<float>(i % 1000) / 10.0f;
    }
    
    TensorInfo send_tensor;
    send_tensor.data_ptr = large_data.data();
    send_tensor.total_bytes = tensor_size * sizeof(float);
    send_tensor.shape = {32, 80, 1024};  // 32 x 80 x 1024 = 2,621,440
    send_tensor.dtype = "torch.float32";
    
    LOG(INFO) << "Created large tensor: " 
              << send_tensor.total_bytes / (1024.0 * 1024.0) << " MB";
    
    // Server side (REP)
    int server_socket = server_comm.createSocket(ZmqSocketType::REP);
    std::string endpoint = "127.0.0.1:15556";
    
    ASSERT_TRUE(server_comm.bind(server_socket, endpoint));
    ASSERT_TRUE(server_comm.startServer(server_socket));
    
    // Set up server to receive and verify tensor
    std::atomic<bool> server_received{false};
    std::atomic<bool> data_valid{false};
    
    server_comm.setTensorReceiveCallback(server_socket,
        [&server_received, &data_valid, tensor_size, &server_comm, server_socket]
        (std::string_view source, const TensorInfo& received_tensor,
         const std::optional<std::string>& topic) {
            LOG(INFO) << "Server received tensor: " << received_tensor.total_bytes << " bytes";
            
            // Verify tensor metadata
            if (received_tensor.total_bytes == tensor_size * sizeof(float)) {
                data_valid.store(true);
            }
            
            server_received.store(true);
            
            // Send acknowledgment back
            const char* ack = "Tensor received successfully";
            server_comm.sendReply(server_socket, ack, strlen(ack));
        }
    );
    
    // Give server time to start
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Client side (REQ)
    int client_socket = client_comm.createSocket(ZmqSocketType::REQ);
    ASSERT_TRUE(client_comm.connect(client_socket, endpoint));
    
    // Send the large tensor
    auto start_time = std::chrono::high_resolution_clock::now();
    
    auto send_task = [&client_comm, client_socket, &send_tensor]() {
        auto result = async_simple::coro::syncAwait(
            client_comm.sendTensorAsync(client_socket, send_tensor)
        );
        return result;
    };
    
    int send_result = send_task();
    
    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        end_time - start_time
    );
    
    EXPECT_EQ(send_result, 0);
    
    if (send_result == 0) {
        double throughput = (send_tensor.total_bytes / (1024.0 * 1024.0)) / 
                           (duration.count() / 1000.0);
        LOG(INFO) << "Throughput: " << throughput << " MB/s";
    }
    
    // Wait for server to receive and process
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    
    // Check results
    EXPECT_TRUE(server_received.load());
    EXPECT_TRUE(data_valid.load());
    
    server_comm.shutdown();
    client_comm.shutdown();
}
