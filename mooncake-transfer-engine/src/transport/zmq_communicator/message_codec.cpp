#include "message_codec.h"
#include <cstring>
#include <glog/logging.h>
#include <array>

namespace mooncake {

// ============================================================================
// CRC32 Optimization with Lookup Table
// ============================================================================

const uint32_t* MessageCodec::getCRC32Table() {
    static std::array<uint32_t, 256> crc_table = []() {
        std::array<uint32_t, 256> table;
        for (uint32_t i = 0; i < 256; i++) {
            uint32_t crc = i;
            for (int j = 0; j < 8; j++) {
                crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
            }
            table[i] = crc;
        }
        return table;
    }();
    return crc_table.data();
}

uint32_t MessageCodec::calculateChecksum(const void* data, size_t size) {
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    const uint32_t* table = getCRC32Table();
    uint32_t crc = 0xFFFFFFFF;

    // Process 8 bytes at a time for better performance
    size_t i = 0;
    for (; i + 7 < size; i += 8) {
        crc = table[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
        crc = table[(crc ^ bytes[i + 1]) & 0xFF] ^ (crc >> 8);
        crc = table[(crc ^ bytes[i + 2]) & 0xFF] ^ (crc >> 8);
        crc = table[(crc ^ bytes[i + 3]) & 0xFF] ^ (crc >> 8);
        crc = table[(crc ^ bytes[i + 4]) & 0xFF] ^ (crc >> 8);
        crc = table[(crc ^ bytes[i + 5]) & 0xFF] ^ (crc >> 8);
        crc = table[(crc ^ bytes[i + 6]) & 0xFF] ^ (crc >> 8);
        crc = table[(crc ^ bytes[i + 7]) & 0xFF] ^ (crc >> 8);
    }

    // Process remaining bytes
    for (; i < size; i++) {
        crc = table[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
    }

    return ~crc;
}

bool MessageCodec::verifyChecksum(const ZmqMessageHeader& header,
                                  std::string_view data) {
    // Compute CRC32 checksum of the payload and compare with header
    const uint32_t computed = calculateChecksum(data.data(), data.size());
    if (computed != header.checksum) {
        LOG(ERROR) << "Checksum verification failed: expected="
                   << header.checksum << ", computed=" << computed
                   << ", size=" << data.size();
        return false;
    }
    return true;
}

// ============================================================================
// Dtype Conversion Optimization with Hash Maps
// ============================================================================

uint32_t MessageCodec::dtypeStringToEnum(std::string_view dtype) {
    // Static hash map for fast lookup (initialized once)
    static const std::unordered_map<std::string_view, uint32_t> dtype_map = {
        {"torch.float16", static_cast<uint32_t>(TensorDtype::FLOAT16)},
        {"torch.float32", static_cast<uint32_t>(TensorDtype::FLOAT32)},
        {"torch.float64", static_cast<uint32_t>(TensorDtype::FLOAT64)},
        {"torch.int8", static_cast<uint32_t>(TensorDtype::INT8)},
        {"torch.int16", static_cast<uint32_t>(TensorDtype::INT16)},
        {"torch.int32", static_cast<uint32_t>(TensorDtype::INT32)},
        {"torch.int64", static_cast<uint32_t>(TensorDtype::INT64)},
        {"torch.uint8", static_cast<uint32_t>(TensorDtype::UINT8)},
        {"torch.bool", static_cast<uint32_t>(TensorDtype::BOOL)},
        // Shortcuts without "torch." prefix
        {"float16", static_cast<uint32_t>(TensorDtype::FLOAT16)},
        {"float32", static_cast<uint32_t>(TensorDtype::FLOAT32)},
        {"float64", static_cast<uint32_t>(TensorDtype::FLOAT64)},
        {"int8", static_cast<uint32_t>(TensorDtype::INT8)},
        {"int16", static_cast<uint32_t>(TensorDtype::INT16)},
        {"int32", static_cast<uint32_t>(TensorDtype::INT32)},
        {"int64", static_cast<uint32_t>(TensorDtype::INT64)},
        {"uint8", static_cast<uint32_t>(TensorDtype::UINT8)},
        {"bool", static_cast<uint32_t>(TensorDtype::BOOL)},
    };

    auto it = dtype_map.find(dtype);
    if (it != dtype_map.end()) {
        return it->second;
    }

    // Default to FLOAT32
    return static_cast<uint32_t>(TensorDtype::FLOAT32);
}

const char* MessageCodec::dtypeEnumToString(uint32_t dtype) {
    // Static array for O(1) lookup
    // Note: TensorDtype enum starts at 1, not 0
    static const char* dtype_strings[] = {
        "unknown",        // 0 (invalid)
        "torch.float16",  // 1
        "torch.float32",  // 2
        "torch.float64",  // 3
        "torch.int8",     // 4
        "torch.int16",    // 5
        "torch.int32",    // 6
        "torch.int64",    // 7
        "torch.uint8",    // 8
        "torch.bool",     // 9
    };

    if (dtype < sizeof(dtype_strings) / sizeof(dtype_strings[0])) {
        return dtype_strings[dtype];
    }
    return "unknown";
}

// ============================================================================
// Message Encoding
// ============================================================================

std::string MessageCodec::encodeDataMessage(
    ZmqSocketType socket_type, const void* data, size_t data_size,
    const std::optional<std::string>& topic, uint64_t sequence_id) {
    ZmqMessageHeader header;
    header.socket_type = static_cast<uint8_t>(socket_type);
    header.flags = topic.has_value() ? 0x02 : 0x00;
    header.sequence_id = sequence_id;
    header.topic_length = topic.has_value() ? topic->size() : 0;
    header.data_length = data_size;
    header.checksum = 0;

    // Pre-allocate exact size to avoid reallocation
    const size_t total_size = sizeof(header) + header.topic_length + data_size;
    std::string message;
    message.reserve(total_size);

    // Append header
    message.append(reinterpret_cast<const char*>(&header), sizeof(header));

    // Append topic if present
    if (topic.has_value()) {
        message.append(*topic);
    }

    // Append data
    message.append(static_cast<const char*>(data), data_size);

    return message;
}

std::string MessageCodec::encodeTensorMessage(
    ZmqSocketType socket_type, const TensorInfo& tensor,
    const std::optional<std::string>& topic, uint64_t sequence_id) {
    TensorMessageHeader header;
    header.base.socket_type = static_cast<uint8_t>(socket_type);
    header.base.flags = 0x01;  // is_tensor
    if (topic.has_value()) {
        header.base.flags |= 0x02;  // has_topic
    }
    header.base.sequence_id = sequence_id;
    header.base.topic_length = topic.has_value() ? topic->size() : 0;
    header.base.data_length = tensor.total_bytes;
    header.base.checksum = 0;

    // Optimized dtype conversion
    header.dtype = dtypeStringToEnum(tensor.dtype);

    // Validate tensor dimensions
    if (tensor.shape.size() > MAX_TENSOR_DIMS) {
        LOG(ERROR) << "Tensor has " << tensor.shape.size()
                   << " dimensions, but only " << MAX_TENSOR_DIMS
                   << " dimensions are supported. Tensor transmission would be "
                      "incorrect.";
        // Return empty header to signal error
        return std::string();
    }

    header.ndim = static_cast<uint32_t>(tensor.shape.size());
    for (size_t i = 0; i < tensor.shape.size(); i++) {
        header.shape[i] = static_cast<int64_t>(tensor.shape[i]);
    }
    // Zero out unused dimensions for clarity
    for (size_t i = tensor.shape.size(); i < MAX_TENSOR_DIMS; i++) {
        header.shape[i] = 0;
    }
    header.layout = 0;  // Row-major

    // Pre-allocate exact size
    const size_t total_size = sizeof(header) + header.base.topic_length;
    std::string message;
    message.reserve(total_size);

    // Append header
    message.append(reinterpret_cast<const char*>(&header), sizeof(header));

    // Append topic if present
    if (topic.has_value()) {
        message.append(*topic);
    }

    return message;
}

// ============================================================================
// Message Decoding (Zero-copy with string_view)
// ============================================================================

std::optional<ZmqMessageHeader> MessageCodec::decodeHeader(
    std::string_view data) {
    if (data.size() < sizeof(ZmqMessageHeader)) {
        LOG(ERROR) << "Data too small for message header: " << data.size()
                   << " < " << sizeof(ZmqMessageHeader);
        return std::nullopt;
    }

    ZmqMessageHeader header;
    std::memcpy(&header, data.data(), sizeof(header));

    // Verify magic number
    if (header.magic != 0x5A4D5121) {
        LOG(ERROR) << "Invalid magic number: 0x" << std::hex << header.magic;
        return std::nullopt;
    }

    return header;
}

std::optional<MessageCodec::DecodedMessage> MessageCodec::decodeMessage(
    std::string_view data) {
    auto header_opt = decodeHeader(data);
    if (!header_opt) {
        return std::nullopt;
    }

    DecodedMessage result;
    result.header = *header_opt;

    size_t offset = sizeof(ZmqMessageHeader);

    // Extract topic if present (zero-copy with string_view)
    if (result.header.flags & 0x02) {  // has_topic
        if (data.size() < offset + result.header.topic_length) {
            LOG(ERROR) << "Data too small for topic";
            return std::nullopt;
        }
        result.topic = data.substr(offset, result.header.topic_length);
        offset += result.header.topic_length;
    }

    // Extract data (zero-copy with string_view)
    if (data.size() < offset + result.header.data_length) {
        LOG(ERROR) << "Data too small for payload: " << data.size() << " < "
                   << (offset + result.header.data_length);
        return std::nullopt;
    }
    result.data = data.substr(offset, result.header.data_length);

    return result;
}

std::optional<MessageCodec::DecodedTensor> MessageCodec::decodeTensorMessage(
    std::string_view data) {
    if (data.size() < sizeof(TensorMessageHeader)) {
        LOG(ERROR) << "Data too small for tensor header: " << data.size()
                   << " < " << sizeof(TensorMessageHeader);
        return std::nullopt;
    }

    DecodedTensor result;
    std::memcpy(&result.header, data.data(), sizeof(result.header));

    // Verify magic number
    if (result.header.base.magic != 0x5A4D5121) {
        LOG(ERROR) << "Invalid magic number in tensor: 0x" << std::hex
                   << result.header.base.magic;
        return std::nullopt;
    }

    // Validate tensor dimensions to prevent buffer overflow
    if (result.header.ndim > MAX_TENSOR_DIMS) {
        LOG(ERROR) << "Received tensor with " << result.header.ndim
                   << " dimensions, but only " << MAX_TENSOR_DIMS
                   << " dimensions are supported. Rejecting tensor.";
        return std::nullopt;
    }

    size_t offset = sizeof(TensorMessageHeader);

    // Extract topic if present (zero-copy with string_view)
    if (result.header.base.flags & 0x02) {  // has_topic
        if (data.size() < offset + result.header.base.topic_length) {
            LOG(ERROR) << "Data too small for tensor topic";
            return std::nullopt;
        }
        result.topic = data.substr(offset, result.header.base.topic_length);
        offset += result.header.base.topic_length;
    }

    // Fill tensor info
    result.tensor.total_bytes = result.header.base.data_length;

    // Pre-allocate shape vector
    result.tensor.shape.reserve(result.header.ndim);
    for (uint32_t i = 0; i < result.header.ndim; i++) {
        result.tensor.shape.push_back(result.header.shape[i]);
    }

    // Optimized dtype conversion (O(1) lookup)
    result.tensor.dtype = dtypeEnumToString(result.header.dtype);

    return result;
}

}  // namespace mooncake
