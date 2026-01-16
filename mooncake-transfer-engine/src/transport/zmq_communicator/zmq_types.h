#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <optional>

namespace mooncake {

// Socket types
enum class ZmqSocketType : uint8_t {
    REQ = 0,   // Request
    REP = 1,   // Reply
    PUB = 2,   // Publisher
    SUB = 3,   // Subscriber
    PUSH = 4,  // Push
    PULL = 5,  // Pull
    PAIR = 6   // Pair
};

// Socket options
enum class ZmqSocketOption : uint8_t {
    RCVTIMEO = 0,      // Receive timeout (milliseconds)
    SNDTIMEO = 1,      // Send timeout (milliseconds)
    SNDHWM = 2,        // High water mark for outbound messages
    RCVHWM = 3,        // High water mark for inbound messages
    LINGER = 4,        // Linger period for socket shutdown (milliseconds)
    RECONNECT_IVL = 5, // Reconnection interval (milliseconds)
    RCVBUF = 6,        // Receive buffer size (bytes)
    SNDBUF = 7,        // Send buffer size (bytes)
    ROUTING_ID = 8     // Socket routing identity
};

// Configuration
struct ZmqConfig {
    size_t thread_count = 8;
    size_t timeout_seconds = 30;
    size_t pool_size = 10;
    bool enable_rdma = false;
    size_t high_water_mark = 1000;
    size_t rcv_timeout_ms = 5000;
    size_t snd_timeout_ms = 5000;
    size_t linger_ms = 1000;
    size_t reconnect_interval_ms = 100;
    size_t rcv_buffer_size = 65536;
    size_t snd_buffer_size = 65536;
};

// RPC result
struct RpcResult {
    int code = 0;
    std::string message;
    std::string response_data;
};

// Tensor information
struct TensorInfo {
    void* data_ptr = nullptr;
    size_t total_bytes = 0;
    std::vector<size_t> shape;
    std::string dtype;
};

// Message header
struct ZmqMessageHeader {
    uint32_t magic = 0x5A4D5121;  // "ZMQ!"
    uint32_t version = 1;
    uint8_t socket_type;
    uint8_t flags;
    uint64_t sequence_id;
    uint32_t topic_length;
    uint64_t data_length;
    uint32_t checksum;
} __attribute__((packed));

// Tensor message header
static constexpr size_t MAX_TENSOR_DIMS = 4;

struct TensorMessageHeader {
    ZmqMessageHeader base;
    uint32_t dtype;
    uint32_t ndim;
    int64_t shape[MAX_TENSOR_DIMS];
    uint8_t layout;
    uint8_t reserved[7];
} __attribute__((packed));

// Tensor dtype enumeration
enum class TensorDtype : uint32_t {
    FLOAT16 = 1,
    FLOAT32 = 2,
    FLOAT64 = 3,
    INT8 = 4,
    INT16 = 5,
    INT32 = 6,
    INT64 = 7,
    UINT8 = 8,
    BOOL = 9
};

}  // namespace mooncake
