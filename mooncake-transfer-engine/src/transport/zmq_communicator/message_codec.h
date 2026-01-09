#pragma once

#include "zmq_types.h"
#include <string>
#include <string_view>
#include <optional>

namespace mooncake {

class MessageCodec {
public:
    // Encode data message
    static std::string encodeDataMessage(
        ZmqSocketType socket_type,
        const void* data,
        size_t data_size,
        const std::optional<std::string>& topic = std::nullopt,
        uint64_t sequence_id = 0
    );
    
    // Encode tensor message
    static std::string encodeTensorMessage(
        ZmqSocketType socket_type,
        const TensorInfo& tensor,
        const std::optional<std::string>& topic = std::nullopt,
        uint64_t sequence_id = 0
    );
    
    // Decoded message structure
    struct DecodedMessage {
        ZmqMessageHeader header;
        std::string topic;
        std::string_view data;
    };
    
    // Decoded tensor structure
    struct DecodedTensor {
        TensorMessageHeader header;
        std::string topic;
        TensorInfo tensor;
    };
    
    // Decode message header
    static std::optional<ZmqMessageHeader> decodeHeader(std::string_view data);
    
    // Decode data message
    static std::optional<DecodedMessage> decodeMessage(std::string_view data);
    
    // Decode tensor message
    static std::optional<DecodedTensor> decodeTensorMessage(std::string_view data);
    
private:
    // CRC32 checksum
    static uint32_t calculateChecksum(const void* data, size_t size);
    static bool verifyChecksum(const ZmqMessageHeader& header, std::string_view data);
};

}  // namespace mooncake

