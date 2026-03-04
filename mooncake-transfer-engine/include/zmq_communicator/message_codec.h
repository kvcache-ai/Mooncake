#pragma once

#include "zmq_types.h"
#include <string>
#include <string_view>
#include <optional>
#include <unordered_map>

namespace mooncake {

class MessageCodec {
   public:
    // Encode data message (legacy: full message in one buffer)
    static std::string encodeDataMessage(
        ZmqSocketType socket_type, const void* data, size_t data_size,
        const std::optional<std::string>& topic = std::nullopt,
        uint64_t sequence_id = 0);

    // Encode data header only (metadata: type/topic/len/seq/checksum).
    // Payload goes in attachment. Zero-copy for large data.
    static std::string encodeDataHeader(
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

    // Decode data message (legacy: full message in one buffer)
    static std::optional<DecodedMessage> decodeMessage(std::string_view data);

    // Decode from header+topic (body) + payload (attachment). Zero-copy.
    static std::optional<DecodedMessage> decodeMessageFromParts(
        std::string_view header_and_topic, std::string_view payload);

    // Decode tensor message
    static std::optional<DecodedTensor> decodeTensorMessage(
        std::string_view data);

   private:
    // Optimized CRC32 with lookup table
    static uint32_t calculateChecksum(const void* data, size_t size);
    static uint32_t calculateChecksum2(const void* data1, size_t size1,
                                       const void* data2, size_t size2);
    static bool verifyChecksum(const ZmqMessageHeader& header,
                               std::string_view data);

    // Optimized dtype conversion with hash maps
    static uint32_t dtypeStringToEnum(std::string_view dtype);
    static const char* dtypeEnumToString(uint32_t dtype);

    // CRC32 lookup table (initialized once)
    static const uint32_t* getCRC32Table();
};

}  // namespace mooncake
