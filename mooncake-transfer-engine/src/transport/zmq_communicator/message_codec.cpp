#include "message_codec.h"
#include <cstring>
#include <glog/logging.h>

namespace mooncake {

// Simple CRC32 implementation
uint32_t MessageCodec::calculateChecksum(const void* data, size_t size) {
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;
    
    for (size_t i = 0; i < size; i++) {
        crc ^= bytes[i];
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
    }
    
    return ~crc;
}

bool MessageCodec::verifyChecksum(const ZmqMessageHeader& header, 
                                  std::string_view data) {
    // For now, skip checksum verification
    return true;
}

std::string MessageCodec::encodeDataMessage(
    ZmqSocketType socket_type,
    const void* data,
    size_t data_size,
    const std::optional<std::string>& topic,
    uint64_t sequence_id
) {
    ZmqMessageHeader header;
    header.socket_type = static_cast<uint8_t>(socket_type);
    header.flags = topic.has_value() ? 0x02 : 0x00;  // Bit 1: has_topic
    header.sequence_id = sequence_id;
    header.topic_length = topic.has_value() ? topic->size() : 0;
    header.data_length = data_size;
    header.checksum = 0;  // TODO: implement checksum
    
    // Build message
    std::string message;
    message.reserve(sizeof(header) + header.topic_length + data_size);
    
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
    ZmqSocketType socket_type,
    const TensorInfo& tensor,
    const std::optional<std::string>& topic,
    uint64_t sequence_id
) {
    TensorMessageHeader header;
    header.base.socket_type = static_cast<uint8_t>(socket_type);
    header.base.flags = 0x01;  // Bit 0: is_tensor
    if (topic.has_value()) {
        header.base.flags |= 0x02;  // Bit 1: has_topic
    }
    header.base.sequence_id = sequence_id;
    header.base.topic_length = topic.has_value() ? topic->size() : 0;
    header.base.data_length = tensor.total_bytes;
    header.base.checksum = 0;
    
    // Parse dtype from string
    header.dtype = 2;  // Default to FLOAT32
    if (tensor.dtype.find("float32") != std::string::npos) {
        header.dtype = static_cast<uint32_t>(TensorDtype::FLOAT32);
    } else if (tensor.dtype.find("float16") != std::string::npos) {
        header.dtype = static_cast<uint32_t>(TensorDtype::FLOAT16);
    } else if (tensor.dtype.find("float64") != std::string::npos) {
        header.dtype = static_cast<uint32_t>(TensorDtype::FLOAT64);
    } else if (tensor.dtype.find("int8") != std::string::npos) {
        header.dtype = static_cast<uint32_t>(TensorDtype::INT8);
    } else if (tensor.dtype.find("int16") != std::string::npos) {
        header.dtype = static_cast<uint32_t>(TensorDtype::INT16);
    } else if (tensor.dtype.find("int32") != std::string::npos) {
        header.dtype = static_cast<uint32_t>(TensorDtype::INT32);
    } else if (tensor.dtype.find("int64") != std::string::npos) {
        header.dtype = static_cast<uint32_t>(TensorDtype::INT64);
    } else if (tensor.dtype.find("uint8") != std::string::npos) {
        header.dtype = static_cast<uint32_t>(TensorDtype::UINT8);
    } else if (tensor.dtype.find("bool") != std::string::npos) {
        header.dtype = static_cast<uint32_t>(TensorDtype::BOOL);
    }
    
    header.ndim = tensor.shape.size();
    for (size_t i = 0; i < tensor.shape.size() && i < MAX_TENSOR_DIMS; i++) {
        header.shape[i] = tensor.shape[i];
    }
    header.layout = 0;  // Row-major
    
    // Build message header (data will be sent separately via attachment)
    std::string message;
    message.reserve(sizeof(header) + header.base.topic_length);
    
    // Append header
    message.append(reinterpret_cast<const char*>(&header), sizeof(header));
    
    // Append topic if present
    if (topic.has_value()) {
        message.append(*topic);
    }
    
    return message;
}

std::optional<ZmqMessageHeader> MessageCodec::decodeHeader(std::string_view data) {
    if (data.size() < sizeof(ZmqMessageHeader)) {
        LOG(ERROR) << "Data too small for message header";
        return std::nullopt;
    }
    
    ZmqMessageHeader header;
    std::memcpy(&header, data.data(), sizeof(header));
    
    // Verify magic number
    if (header.magic != 0x5A4D5121) {
        LOG(ERROR) << "Invalid magic number: " << std::hex << header.magic;
        return std::nullopt;
    }
    
    return header;
}

std::optional<MessageCodec::DecodedMessage> MessageCodec::decodeMessage(
    std::string_view data
) {
    auto header_opt = decodeHeader(data);
    if (!header_opt) {
        return std::nullopt;
    }
    
    DecodedMessage result;
    result.header = *header_opt;
    
    size_t offset = sizeof(ZmqMessageHeader);
    
    // Extract topic if present
    if (result.header.flags & 0x02) {  // has_topic
        if (data.size() < offset + result.header.topic_length) {
            LOG(ERROR) << "Data too small for topic";
            return std::nullopt;
        }
        result.topic = std::string(data.substr(offset, result.header.topic_length));
        offset += result.header.topic_length;
    }
    
    // Extract data
    if (data.size() < offset + result.header.data_length) {
        LOG(ERROR) << "Data too small for payload";
        return std::nullopt;
    }
    result.data = data.substr(offset, result.header.data_length);
    
    return result;
}

std::optional<MessageCodec::DecodedTensor> MessageCodec::decodeTensorMessage(
    std::string_view data
) {
    if (data.size() < sizeof(TensorMessageHeader)) {
        LOG(ERROR) << "Data too small for tensor header";
        return std::nullopt;
    }
    
    DecodedTensor result;
    std::memcpy(&result.header, data.data(), sizeof(result.header));
    
    // Verify magic number
    if (result.header.base.magic != 0x5A4D5121) {
        LOG(ERROR) << "Invalid magic number";
        return std::nullopt;
    }
    
    size_t offset = sizeof(TensorMessageHeader);
    
    // Extract topic if present
    if (result.header.base.flags & 0x02) {  // has_topic
        if (data.size() < offset + result.header.base.topic_length) {
            LOG(ERROR) << "Data too small for topic";
            return std::nullopt;
        }
        result.topic = std::string(data.substr(offset, result.header.base.topic_length));
        offset += result.header.base.topic_length;
    }
    
    // Fill tensor info (data pointer will be set separately)
    result.tensor.total_bytes = result.header.base.data_length;
    for (uint32_t i = 0; i < result.header.ndim; i++) {
        result.tensor.shape.push_back(result.header.shape[i]);
    }
    
    // Convert dtype to string
    switch (static_cast<TensorDtype>(result.header.dtype)) {
        case TensorDtype::FLOAT16: result.tensor.dtype = "torch.float16"; break;
        case TensorDtype::FLOAT32: result.tensor.dtype = "torch.float32"; break;
        case TensorDtype::FLOAT64: result.tensor.dtype = "torch.float64"; break;
        case TensorDtype::INT8: result.tensor.dtype = "torch.int8"; break;
        case TensorDtype::INT16: result.tensor.dtype = "torch.int16"; break;
        case TensorDtype::INT32: result.tensor.dtype = "torch.int32"; break;
        case TensorDtype::INT64: result.tensor.dtype = "torch.int64"; break;
        case TensorDtype::UINT8: result.tensor.dtype = "torch.uint8"; break;
        case TensorDtype::BOOL: result.tensor.dtype = "torch.bool"; break;
        default: result.tensor.dtype = "unknown"; break;
    }
    
    return result;
}

}  // namespace mooncake

