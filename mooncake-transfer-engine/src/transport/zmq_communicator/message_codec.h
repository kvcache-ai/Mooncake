#pragma once

#include "zmq_types.h"
#include <string>
#include <string_view>
#include <optional>
#include <unordered_map>

namespace mooncake {

class MessageCodec {
   public:
    // Encode data message
    static std::string encodeDataMessage(
        ZmqSocketType socket_type, const void* data, size_t data_size,
        const std::optional<std::string>& topic = std::nullopt,
        uint64_t sequence_id = 0);

    // Encode tensor message
    static std::string encodeTensorMessage(
        ZmqSocketType socket_type, const TensorInfo& tensor,
        const std::optional<std::string>& topic = std::nullopt,
        uint64_t sequence_id = 0);

    // Decoded message structure (optimized to avoid copies)
    struct DecodedMessage {
        ZmqMessageHeader header;
        std::string_view topic;  // Changed from std::string to string_view
        std::string_view data;
    };

    // Decoded tensor structure
    struct DecodedTensor {
        TensorMessageHeader header;
        std::string_view topic;  // Changed from std::string to string_view
        TensorInfo tensor;
    };

    // Decode message header
    static std::optional<ZmqMessageHeader> decodeHeader(std::string_view data);

    // Decode data message
    static std::optional<DecodedMessage> decodeMessage(std::string_view data);

    // Decode tensor message
    static std::optional<DecodedTensor> decodeTensorMessage(
        std::string_view data);

   private:
    // Optimized CRC32 with lookup table
    static uint32_t calculateChecksum(const void* data, size_t size);
    static bool verifyChecksum(const ZmqMessageHeader& header,
                               std::string_view data);

    // Optimized dtype conversion with hash maps
    static uint32_t dtypeStringToEnum(std::string_view dtype);
    static const char* dtypeEnumToString(uint32_t dtype);

    // CRC32 lookup table (initialized once)
    static const uint32_t* getCRC32Table();
};

}  // namespace mooncake
