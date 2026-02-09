#include <gtest/gtest.h>

#include <string>

#include "transport/zmq_communicator/message_codec.h"
#include "transport/zmq_communicator/zmq_types.h"

namespace {

using namespace mooncake;

TEST(MessageCodecTest, EncodeDecodeDataMessageWithChecksum) {
    const std::string payload = "hello";
    const std::string topic = "topic";

    auto message = MessageCodec::encodeDataMessage(
        ZmqSocketType::REQ, payload.data(), payload.size(), topic, 42);

    auto decoded = MessageCodec::decodeMessage(message);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->header.sequence_id, 42u);
    EXPECT_EQ(decoded->topic, topic);
    EXPECT_EQ(decoded->data, payload);
}

TEST(MessageCodecTest, DetectsChecksumMismatch) {
    const std::string payload = "payload";
    auto message = MessageCodec::encodeDataMessage(
        ZmqSocketType::REQ, payload.data(), payload.size(), std::nullopt, 1);

    ASSERT_FALSE(message.empty());
    message.back() ^= 0x01;

    auto decoded = MessageCodec::decodeMessage(message);
    EXPECT_FALSE(decoded.has_value());
}

TEST(MessageCodecTest, EncodeTensorRejectsTooManyDims) {
    TensorInfo tensor;
    tensor.total_bytes = 8;
    tensor.dtype = "float32";
    tensor.shape = {1, 2, 3, 4, 5};

    auto message =
        MessageCodec::encodeTensorMessage(ZmqSocketType::REQ, tensor);
    EXPECT_TRUE(message.empty());
}

TEST(MessageCodecTest, DecodeTensorRejectsNegativeDims) {
    TensorMessageHeader header{};
    header.base.magic = 0x5A4D5121;
    header.base.flags = 0x01;
    header.base.sequence_id = 0;
    header.base.topic_length = 0;
    header.base.data_length = 0;
    header.base.checksum = 0;
    header.ndim = 1;
    header.shape[0] = -1;

    std::string buffer(reinterpret_cast<const char*>(&header), sizeof(header));
    auto decoded = MessageCodec::decodeTensorMessage(buffer);
    EXPECT_FALSE(decoded.has_value());
}

TEST(MessageCodecTest, DecodeTensorDetectsChecksumMismatch) {
    TensorInfo tensor;
    tensor.total_bytes = 0;
    tensor.dtype = "float32";
    tensor.shape = {2, 2};

    const std::string topic = "tensor";
    auto message =
        MessageCodec::encodeTensorMessage(ZmqSocketType::REQ, tensor, topic, 7);

    ASSERT_FALSE(message.empty());
    message.back() ^= 0x01;

    auto decoded = MessageCodec::decodeTensorMessage(message);
    EXPECT_FALSE(decoded.has_value());
}

}  // namespace
