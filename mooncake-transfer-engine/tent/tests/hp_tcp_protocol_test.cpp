// Copyright 2026 KVCache.AI
#include <gtest/gtest.h>

#include <limits>

#include "tent/transport/hp_tcp/hp_tcp_protocol.h"

namespace mooncake::tent {
namespace {

TEST(HighPerformanceTcpProtocolTest, RequestAndResponseRoundTrip) {
    const HighPerformanceTcpRequestFrame request{
        HighPerformanceTcpOpcode::kWrite, 11, 22, 33, 44};
    const auto request_bytes = EncodeHighPerformanceTcpRequest(request);
    HighPerformanceTcpRequestFrame decoded_request;
    ASSERT_TRUE(DecodeHighPerformanceTcpRequest(request_bytes.data(),
                                                request_bytes.size(),
                                                &decoded_request)
                    .ok());
    EXPECT_EQ(decoded_request.opcode, request.opcode);
    EXPECT_EQ(decoded_request.request_id, request.request_id);
    EXPECT_EQ(decoded_request.registration_id, request.registration_id);
    EXPECT_EQ(decoded_request.remote_addr, request.remote_addr);
    EXPECT_EQ(decoded_request.length, request.length);

    const HighPerformanceTcpResponseFrame response{
        HighPerformanceTcpStatus::kOk, request.request_id, request.length};
    const auto response_bytes = EncodeHighPerformanceTcpResponse(response);
    HighPerformanceTcpResponseFrame decoded_response;
    ASSERT_TRUE(DecodeHighPerformanceTcpResponse(response_bytes.data(),
                                                 response_bytes.size(),
                                                 &decoded_response)
                    .ok());
    EXPECT_EQ(decoded_response.status, response.status);
    EXPECT_EQ(decoded_response.request_id, response.request_id);
    EXPECT_EQ(decoded_response.committed_bytes, response.committed_bytes);
}

TEST(HighPerformanceTcpProtocolTest, RejectsMalformedWireFrames) {
    auto request = EncodeHighPerformanceTcpRequest(
        {HighPerformanceTcpOpcode::kRead, 1, 2, 3, 4});
    HighPerformanceTcpRequestFrame decoded;
    HighPerformanceTcpStatus wire_error = HighPerformanceTcpStatus::kOk;
    request[5] = 2;
    EXPECT_FALSE(DecodeHighPerformanceTcpRequest(request.data(), request.size(),
                                                 &decoded, &wire_error)
                     .ok());
    EXPECT_EQ(wire_error, HighPerformanceTcpStatus::kBadVersion);
    request[5] = 1;
    request[6] = 0xff;
    EXPECT_FALSE(DecodeHighPerformanceTcpRequest(request.data(), request.size(),
                                                 &decoded, &wire_error)
                     .ok());
    EXPECT_EQ(wire_error, HighPerformanceTcpStatus::kBadOpcode);
}

TEST(HighPerformanceTcpProtocolTest, MetadataAttributesRoundTrip) {
    HighPerformanceTcpEndpointAttr endpoint{"00112233445566778899aabbccddeeff",
                                            "127.0.0.1", 1234, 4096};
    std::string encoded;
    ASSERT_TRUE(EncodeHighPerformanceTcpEndpointAttr(endpoint, &encoded).ok());
    HighPerformanceTcpEndpointAttr decoded_endpoint;
    ASSERT_TRUE(
        DecodeHighPerformanceTcpEndpointAttr(encoded, &decoded_endpoint).ok());
    EXPECT_EQ(decoded_endpoint.incarnation, endpoint.incarnation);
    EXPECT_EQ(decoded_endpoint.host, endpoint.host);
    EXPECT_EQ(decoded_endpoint.port, endpoint.port);

    ASSERT_TRUE(
        EncodeHighPerformanceTcpBufferAttr({42, "global_read_write"}, &encoded)
            .ok());
    HighPerformanceTcpBufferAttr decoded_buffer;
    ASSERT_TRUE(
        DecodeHighPerformanceTcpBufferAttr(encoded, &decoded_buffer).ok());
    EXPECT_EQ(decoded_buffer.registration_id, 42u);
    EXPECT_EQ(decoded_buffer.permission, "global_read_write");

    const uint64_t max_registration_id = std::numeric_limits<uint64_t>::max();
    ASSERT_TRUE(EncodeHighPerformanceTcpBufferAttr(
                    {max_registration_id, "global_read_only"}, &encoded)
                    .ok());
    ASSERT_TRUE(
        DecodeHighPerformanceTcpBufferAttr(encoded, &decoded_buffer).ok());
    EXPECT_EQ(decoded_buffer.registration_id, max_registration_id);
    EXPECT_EQ(decoded_buffer.permission, "global_read_only");

    EXPECT_FALSE(
        DecodeHighPerformanceTcpEndpointAttr("not-json", &decoded_endpoint)
            .ok());
}

}  // namespace
}  // namespace mooncake::tent
