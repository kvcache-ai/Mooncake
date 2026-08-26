// Copyright 2026 KVCache.AI
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <string>

#include "tent/transport/tcp/high_performance_tcp_protocol.h"

namespace mooncake::tent {
namespace {

TEST(HighPerformanceTcpProtocolTest, RequestGoldenBytesAndRoundTrip) {
    const HighPerformanceTcpRequestFrame input{
        HighPerformanceTcpOpcode::kWrite,
        0x0102030405060708ULL,
        0x1112131415161718ULL,
        0x2122232425262728ULL,
        0x3132333435363738ULL,
    };
    const auto bytes = EncodeHighPerformanceTcpRequest(input);
    const std::array<uint8_t, kHighPerformanceTcpRequestSize> expected = {
        0x4d, 0x43, 0x54, 0x50, 0x00, 0x01, 0x02, 0x00, 0x01, 0x02, 0x03, 0x04,
        0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
        0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x31, 0x32, 0x33, 0x34,
        0x35, 0x36, 0x37, 0x38, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    EXPECT_EQ(bytes, expected);

    HighPerformanceTcpRequestFrame output;
    HighPerformanceTcpStatus wire_error =
        HighPerformanceTcpStatus::kInternalError;
    ASSERT_TRUE(DecodeHighPerformanceTcpRequest(bytes.data(), bytes.size(),
                                                &output, &wire_error)
                    .ok());
    EXPECT_EQ(output.opcode, input.opcode);
    EXPECT_EQ(output.request_id, input.request_id);
    EXPECT_EQ(output.registration_id, input.registration_id);
    EXPECT_EQ(output.remote_addr, input.remote_addr);
    EXPECT_EQ(output.length, input.length);
}

TEST(HighPerformanceTcpProtocolTest, RejectsMalformedRequestFields) {
    const HighPerformanceTcpRequestFrame input{HighPerformanceTcpOpcode::kRead,
                                               1, 2, 0x1000, 64};
    auto bytes = EncodeHighPerformanceTcpRequest(input);
    HighPerformanceTcpRequestFrame output;
    HighPerformanceTcpStatus wire_error = HighPerformanceTcpStatus::kOk;

    auto malformed = bytes;
    malformed[0] ^= 1;
    EXPECT_FALSE(DecodeHighPerformanceTcpRequest(
                     malformed.data(), malformed.size(), &output, &wire_error)
                     .ok());
    EXPECT_EQ(wire_error, HighPerformanceTcpStatus::kInternalError);

    malformed = bytes;
    malformed[5] = 2;
    EXPECT_FALSE(DecodeHighPerformanceTcpRequest(
                     malformed.data(), malformed.size(), &output, &wire_error)
                     .ok());
    EXPECT_EQ(wire_error, HighPerformanceTcpStatus::kBadVersion);

    malformed = bytes;
    malformed[6] = 0xff;
    EXPECT_FALSE(DecodeHighPerformanceTcpRequest(
                     malformed.data(), malformed.size(), &output, &wire_error)
                     .ok());
    EXPECT_EQ(wire_error, HighPerformanceTcpStatus::kBadOpcode);

    malformed = bytes;
    malformed[7] = 1;
    EXPECT_FALSE(DecodeHighPerformanceTcpRequest(
                     malformed.data(), malformed.size(), &output, &wire_error)
                     .ok());
    EXPECT_EQ(wire_error, HighPerformanceTcpStatus::kInternalError);

    malformed = bytes;
    malformed[47] = 1;
    EXPECT_FALSE(DecodeHighPerformanceTcpRequest(
                     malformed.data(), malformed.size(), &output, &wire_error)
                     .ok());
    EXPECT_EQ(wire_error, HighPerformanceTcpStatus::kInternalError);

    malformed = bytes;
    std::fill(malformed.begin() + 32, malformed.begin() + 40, 0);
    EXPECT_FALSE(DecodeHighPerformanceTcpRequest(
                     malformed.data(), malformed.size(), &output, &wire_error)
                     .ok());
    EXPECT_EQ(wire_error, HighPerformanceTcpStatus::kBadLength);

    const auto overflow = EncodeHighPerformanceTcpRequest(
        {HighPerformanceTcpOpcode::kRead, 1, 2, UINT64_MAX - 7, 16});
    EXPECT_FALSE(DecodeHighPerformanceTcpRequest(
                     overflow.data(), overflow.size(), &output, &wire_error)
                     .ok());
    EXPECT_EQ(wire_error, HighPerformanceTcpStatus::kBadLength);

    EXPECT_FALSE(DecodeHighPerformanceTcpRequest(bytes.data(), bytes.size() - 1,
                                                 &output, &wire_error)
                     .ok());
}

TEST(HighPerformanceTcpProtocolTest, ResponseGoldenBytesAndValidation) {
    const HighPerformanceTcpResponseFrame input{
        HighPerformanceTcpStatus::kPermissionDenied,
        0x0102030405060708ULL,
        0x1112131415161718ULL,
    };
    const auto bytes = EncodeHighPerformanceTcpResponse(input);
    const std::array<uint8_t, kHighPerformanceTcpResponseSize> expected = {
        0x4d, 0x43, 0x54, 0x50, 0x00, 0x01, 0x00, 0x05, 0x01, 0x02, 0x03,
        0x04, 0x05, 0x06, 0x07, 0x08, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16,
        0x17, 0x18, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };
    EXPECT_EQ(bytes, expected);

    HighPerformanceTcpResponseFrame output;
    ASSERT_TRUE(
        DecodeHighPerformanceTcpResponse(bytes.data(), bytes.size(), &output)
            .ok());
    EXPECT_EQ(output.status, input.status);
    EXPECT_EQ(output.request_id, input.request_id);
    EXPECT_EQ(output.committed_bytes, input.committed_bytes);

    auto malformed = bytes;
    malformed[7] = 0xff;
    EXPECT_FALSE(DecodeHighPerformanceTcpResponse(malformed.data(),
                                                  malformed.size(), &output)
                     .ok());
    malformed = bytes;
    malformed[31] = 1;
    EXPECT_FALSE(DecodeHighPerformanceTcpResponse(malformed.data(),
                                                  malformed.size(), &output)
                     .ok());
}

TEST(HighPerformanceTcpProtocolTest,
     EndpointAttributeIsStrictV1SingleEndpoint) {
    const HighPerformanceTcpEndpointAttr input{
        "00112233445566778899aabbccddeeff",
        {{"127.0.0.1", 23456}},
        1ULL << 30,
    };
    std::string encoded;
    ASSERT_TRUE(EncodeHighPerformanceTcpEndpointAttr(input, &encoded).ok());

    HighPerformanceTcpEndpointAttr output;
    ASSERT_TRUE(DecodeHighPerformanceTcpEndpointAttr(encoded, &output).ok());
    ASSERT_EQ(output.endpoints.size(), 1u);
    EXPECT_EQ(output.incarnation, input.incarnation);
    EXPECT_EQ(output.endpoints[0].host, input.endpoints[0].host);
    EXPECT_EQ(output.endpoints[0].port, input.endpoints[0].port);
    EXPECT_EQ(output.max_transfer_bytes, input.max_transfer_bytes);

    EXPECT_FALSE(
        DecodeHighPerformanceTcpEndpointAttr(
            R"({"protocol":"tent_hp_tcp","version":1,"incarnation":"bad","endpoints":[{"host":"127.0.0.1","port":1}],"max_transfer_bytes":1})",
            &output)
            .ok());
    EXPECT_FALSE(
        DecodeHighPerformanceTcpEndpointAttr(
            R"({"protocol":"tent_hp_tcp","version":1,"incarnation":"00112233445566778899aabbccddeeff","endpoints":[],"max_transfer_bytes":1})",
            &output)
            .ok());
    EXPECT_FALSE(
        DecodeHighPerformanceTcpEndpointAttr(
            R"({"protocol":"tent_hp_tcp","version":1,"incarnation":"00112233445566778899aabbccddeeff","endpoints":[{"host":"127.0.0.1","port":70000}],"max_transfer_bytes":1})",
            &output)
            .ok());
    EXPECT_FALSE(
        DecodeHighPerformanceTcpEndpointAttr("not-json", &output).ok());
}

TEST(HighPerformanceTcpProtocolTest,
     BufferAttributeRequiresRegistrationAndGlobalPermission) {
    std::string encoded;
    ASSERT_TRUE(
        EncodeHighPerformanceTcpBufferAttr({42, "global_read_write"}, &encoded)
            .ok());
    HighPerformanceTcpBufferAttr output;
    ASSERT_TRUE(DecodeHighPerformanceTcpBufferAttr(encoded, &output).ok());
    EXPECT_EQ(output.registration_id, 42u);
    EXPECT_EQ(output.permission, "global_read_write");

    EXPECT_FALSE(
        DecodeHighPerformanceTcpBufferAttr(
            R"({"protocol":"tent_hp_tcp","version":1,"registration_id":0,"permission":"global_read_write"})",
            &output)
            .ok());
    EXPECT_FALSE(
        DecodeHighPerformanceTcpBufferAttr(
            R"({"protocol":"tent_hp_tcp","version":1,"registration_id":1,"permission":"local_read_write"})",
            &output)
            .ok());
    EXPECT_FALSE(
        DecodeHighPerformanceTcpBufferAttr(
            R"({"protocol":"wrong","version":1,"registration_id":1,"permission":"global_read_only"})",
            &output)
            .ok());
}

}  // namespace
}  // namespace mooncake::tent
