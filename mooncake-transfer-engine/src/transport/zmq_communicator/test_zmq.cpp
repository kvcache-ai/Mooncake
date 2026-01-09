#include "zmq_communicator.h"
#include "message_codec.h"
#include <glog/logging.h>
#include <iostream>
#include <thread>
#include <chrono>

using namespace mooncake;

void test_message_codec() {
    LOG(INFO) << "=== Testing Message Codec ===";
    
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
    
    LOG(INFO) << "Encoded message size: " << encoded.size();
    
    // Test decoding
    auto decoded = MessageCodec::decodeMessage(encoded);
    if (decoded) {
        LOG(INFO) << "Decoded successfully:";
        LOG(INFO) << "  Topic: " << decoded->topic;
        LOG(INFO) << "  Data: " << std::string(decoded->data);
        LOG(INFO) << "  Sequence ID: " << decoded->header.sequence_id;
    } else {
        LOG(ERROR) << "Failed to decode message";
    }
    
    LOG(INFO) << "Message codec test completed\n";
}

void test_tensor_codec() {
    LOG(INFO) << "=== Testing Tensor Codec ===";
    
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
    
    LOG(INFO) << "Encoded tensor message size: " << encoded.size();
    
    // Test decoding
    auto decoded = MessageCodec::decodeTensorMessage(encoded);
    if (decoded) {
        LOG(INFO) << "Decoded tensor successfully:";
        LOG(INFO) << "  Topic: " << decoded->topic;
        LOG(INFO) << "  Dtype: " << decoded->tensor.dtype;
        LOG(INFO) << "  Shape: [";
        for (size_t dim : decoded->tensor.shape) {
            LOG(INFO) << "    " << dim;
        }
        LOG(INFO) << "  ]";
        LOG(INFO) << "  Total bytes: " << decoded->tensor.total_bytes;
    } else {
        LOG(ERROR) << "Failed to decode tensor message";
    }
    
    LOG(INFO) << "Tensor codec test completed\n";
}

void test_zmq_communicator_basic() {
    LOG(INFO) << "=== Testing ZmqCommunicator Basic Operations ===";
    
    ZmqCommunicator comm;
    ZmqConfig config;
    config.thread_count = 4;
    config.timeout_seconds = 10;
    config.pool_size = 5;
    
    if (!comm.initialize(config)) {
        LOG(ERROR) << "Failed to initialize communicator";
        return;
    }
    LOG(INFO) << "Communicator initialized successfully";
    
    // Test socket creation
    int req_socket = comm.createSocket(ZmqSocketType::REQ);
    LOG(INFO) << "Created REQ socket: " << req_socket;
    
    int rep_socket = comm.createSocket(ZmqSocketType::REP);
    LOG(INFO) << "Created REP socket: " << rep_socket;
    
    int pub_socket = comm.createSocket(ZmqSocketType::PUB);
    LOG(INFO) << "Created PUB socket: " << pub_socket;
    
    int sub_socket = comm.createSocket(ZmqSocketType::SUB);
    LOG(INFO) << "Created SUB socket: " << sub_socket;
    
    int push_socket = comm.createSocket(ZmqSocketType::PUSH);
    LOG(INFO) << "Created PUSH socket: " << push_socket;
    
    int pull_socket = comm.createSocket(ZmqSocketType::PULL);
    LOG(INFO) << "Created PULL socket: " << pull_socket;
    
    int pair_socket = comm.createSocket(ZmqSocketType::PAIR);
    LOG(INFO) << "Created PAIR socket: " << pair_socket;
    
    // Test socket closing
    if (comm.closeSocket(req_socket)) {
        LOG(INFO) << "Closed REQ socket successfully";
    }
    
    comm.shutdown();
    LOG(INFO) << "Communicator shut down";
    LOG(INFO) << "Basic operations test completed\n";
}

void test_socket_binding() {
    LOG(INFO) << "=== Testing Socket Binding ===";
    
    ZmqCommunicator comm;
    ZmqConfig config;
    config.thread_count = 2;
    
    if (!comm.initialize(config)) {
        LOG(ERROR) << "Failed to initialize communicator";
        return;
    }
    
    // Create and bind a REP socket
    int rep_socket = comm.createSocket(ZmqSocketType::REP);
    std::string endpoint = "127.0.0.1:15555";
    
    if (comm.bind(rep_socket, endpoint)) {
        LOG(INFO) << "Successfully bound REP socket to " << endpoint;
    } else {
        LOG(ERROR) << "Failed to bind REP socket";
    }
    
    // Create and connect a REQ socket
    int req_socket = comm.createSocket(ZmqSocketType::REQ);
    if (comm.connect(req_socket, endpoint)) {
        LOG(INFO) << "Successfully connected REQ socket to " << endpoint;
    } else {
        LOG(ERROR) << "Failed to connect REQ socket";
    }
    
    comm.shutdown();
    LOG(INFO) << "Socket binding test completed\n";
}

void test_pub_sub_subscription() {
    LOG(INFO) << "=== Testing PUB/SUB Subscription ===";
    
    ZmqCommunicator comm;
    ZmqConfig config;
    
    if (!comm.initialize(config)) {
        LOG(ERROR) << "Failed to initialize communicator";
        return;
    }
    
    int sub_socket = comm.createSocket(ZmqSocketType::SUB);
    
    // Test subscription
    if (comm.subscribe(sub_socket, "sensor.")) {
        LOG(INFO) << "Successfully subscribed to 'sensor.'";
    } else {
        LOG(ERROR) << "Failed to subscribe";
    }
    
    if (comm.subscribe(sub_socket, "log.")) {
        LOG(INFO) << "Successfully subscribed to 'log.'";
    } else {
        LOG(ERROR) << "Failed to subscribe";
    }
    
    // Test unsubscription
    if (comm.unsubscribe(sub_socket, "log.")) {
        LOG(INFO) << "Successfully unsubscribed from 'log.'";
    } else {
        LOG(ERROR) << "Failed to unsubscribe";
    }
    
    comm.shutdown();
    LOG(INFO) << "PUB/SUB subscription test completed\n";
}

void test_callback_mechanism() {
    LOG(INFO) << "=== Testing Callback Mechanism ===";
    
    ZmqCommunicator comm;
    ZmqConfig config;
    
    if (!comm.initialize(config)) {
        LOG(ERROR) << "Failed to initialize communicator";
        return;
    }
    
    int pull_socket = comm.createSocket(ZmqSocketType::PULL);
    
    bool callback_called = false;
    
    comm.setReceiveCallback(pull_socket,
        [&callback_called](std::string_view source, std::string_view data,
                          const std::optional<std::string>& topic) {
            LOG(INFO) << "Callback invoked!";
            LOG(INFO) << "  Source: " << source;
            LOG(INFO) << "  Data: " << data;
            if (topic.has_value()) {
                LOG(INFO) << "  Topic: " << *topic;
            }
            callback_called = true;
        }
    );
    
    LOG(INFO) << "Callback set successfully";
    
    // Set tensor callback
    comm.setTensorReceiveCallback(pull_socket,
        [](std::string_view source, const TensorInfo& tensor,
           const std::optional<std::string>& topic) {
            LOG(INFO) << "Tensor callback invoked!";
            LOG(INFO) << "  Tensor dtype: " << tensor.dtype;
            LOG(INFO) << "  Tensor bytes: " << tensor.total_bytes;
        }
    );
    
    LOG(INFO) << "Tensor callback set successfully";
    
    comm.shutdown();
    LOG(INFO) << "Callback mechanism test completed\n";
}

int main(int argc, char* argv[]) {
    // Initialize glog
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;  // Log to stderr
    FLAGS_colorlogtostderr = 1;  // Colorful output
    
    LOG(INFO) << "Starting ZMQ Communicator Tests";
    LOG(INFO) << "================================\n";
    
    try {
        // Run tests
        test_message_codec();
        test_tensor_codec();
        test_zmq_communicator_basic();
        test_socket_binding();
        test_pub_sub_subscription();
        test_callback_mechanism();
        
        LOG(INFO) << "================================";
        LOG(INFO) << "All tests completed successfully!";
        
    } catch (const std::exception& e) {
        LOG(ERROR) << "Test failed with exception: " << e.what();
        return 1;
    }
    
    google::ShutdownGoogleLogging();
    return 0;
}

