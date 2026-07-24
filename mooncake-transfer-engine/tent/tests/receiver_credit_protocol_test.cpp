// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_protocol.h"

#include <limits>
#include <string>
#include <string_view>

#include <gtest/gtest.h>

namespace mooncake::tent {
namespace {

ReceiverCreditPullRequestV1 request() {
    ReceiverCreditPullRequestV1 request;
    request.sender_peer = 0x0102030405060708ULL;
    request.qos_class = 0;
    request.expected_receiver_session_id = {11, 22};
    request.expected_epoch = 7;
    request.request_sequence = 9;
    request.last_update_sequence = 8;
    request.resources = {
        {CreditResource::DataBytes, 100, 90, 10, 200},
        {CreditResource::RequestSlots, 4, 3, 1, 8},
    };
    return request;
}

ReceiverCreditPullResponseV1 response() {
    ReceiverCreditPullResponseV1 response;
    response.status = ReceiverCreditPullStatus::Granted;
    response.activation.receiver_session_id = {11, 22};
    response.activation.epoch = 7;
    response.activation.freshness_ttl_ms = 1000;
    response.update.qos_class = 0;
    response.update.receiver_session_id = {11, 22};
    response.update.epoch = 7;
    response.update.sequence = 8;
    response.update.freshness_ttl_ms = 1000;
    response.update.grants = {
        {CreditResource::DataBytes, 1000},
        {CreditResource::RequestSlots, 16},
        {CreditResource::StagingSlots, 0},
        {CreditResource::ConsumerSlots, 0},
    };
    return response;
}

TEST(ReceiverCreditProtocol, RequestRoundTripsInNetworkByteOrder) {
    const auto original = request();
    std::string wire;
    ASSERT_TRUE(ReceiverCreditPullRequestCodecV1::encode(original, wire).ok());
    ASSERT_EQ(wire.size(),
              ReceiverCreditPullRequestCodecV1::kHeaderBytes +
                  2 * ReceiverCreditPullRequestCodecV1::kResourceBytes);
    EXPECT_EQ(static_cast<uint8_t>(wire[0]), 0x54);
    EXPECT_EQ(static_cast<uint8_t>(wire[1]), 0x43);
    EXPECT_EQ(static_cast<uint8_t>(wire[2]), 0x52);
    EXPECT_EQ(static_cast<uint8_t>(wire[3]), 0x51);
    for (size_t i = 0; i < sizeof(uint64_t); ++i)
        EXPECT_EQ(static_cast<uint8_t>(wire[8 + i]), i + 1);

    ReceiverCreditPullRequestV1 decoded;
    ASSERT_TRUE(ReceiverCreditPullRequestCodecV1::decode(wire, decoded).ok());
    EXPECT_TRUE(decoded == original);
}

TEST(ReceiverCreditProtocol, RequestRejectsMalformedInputAtomically) {
    std::string wire;
    ASSERT_TRUE(ReceiverCreditPullRequestCodecV1::encode(request(), wire).ok());
    ReceiverCreditPullRequestV1 output;
    output.sender_peer = 99;
    for (size_t length = 0; length < wire.size(); ++length) {
        EXPECT_TRUE(ReceiverCreditPullRequestCodecV1::decode(
                        std::string_view(wire.data(), length), output)
                        .IsInvalidArgument());
        EXPECT_EQ(output.sender_peer, 99);
    }

    std::string reserved = wire;
    reserved[63] = 1;
    EXPECT_TRUE(ReceiverCreditPullRequestCodecV1::decode(reserved, output)
                    .IsInvalidArgument());
    EXPECT_EQ(output.sender_peer, 99);

    std::string resource_reserved = wire;
    resource_reserved[67] = 1;
    EXPECT_TRUE(
        ReceiverCreditPullRequestCodecV1::decode(resource_reserved, output)
            .IsInvalidArgument());
    EXPECT_EQ(output.sender_peer, 99);

    auto duplicate = request();
    duplicate.resources[1].resource = CreditResource::DataBytes;
    std::string unchanged = "sentinel";
    EXPECT_TRUE(ReceiverCreditPullRequestCodecV1::encode(duplicate, unchanged)
                    .IsInvalidArgument());
    EXPECT_EQ(unchanged, "sentinel");
}

TEST(ReceiverCreditProtocol, RequestRejectsPartialSessionAndOverflow) {
    auto invalid = request();
    invalid.expected_receiver_session_id = {};
    EXPECT_TRUE(ReceiverCreditPullRequestCodecV1::validate(invalid)
                    .IsInvalidArgument());

    invalid = request();
    invalid.resources[0].completed_total =
        invalid.resources[0].consumed_total + 1;
    EXPECT_TRUE(ReceiverCreditPullRequestCodecV1::validate(invalid)
                    .IsInvalidArgument());

    invalid = request();
    invalid.resources[0].consumed_total =
        std::numeric_limits<uint64_t>::max() - 10;
    invalid.resources[0].completed_total = 0;
    invalid.resources[0].desired_available = 11;
    EXPECT_TRUE(ReceiverCreditPullRequestCodecV1::validate(invalid)
                    .IsInvalidArgument());
}

TEST(ReceiverCreditProtocol, InitialRequestUsesAllZeroSession) {
    auto initial = request();
    initial.expected_receiver_session_id = {};
    initial.expected_epoch = 0;
    initial.last_update_sequence = 0;
    std::string wire;
    ASSERT_TRUE(ReceiverCreditPullRequestCodecV1::encode(initial, wire).ok());
    ReceiverCreditPullRequestV1 decoded;
    ASSERT_TRUE(ReceiverCreditPullRequestCodecV1::decode(wire, decoded).ok());
    EXPECT_EQ(decoded.expected_receiver_session_id, (ReceiverSessionId{}));
    EXPECT_EQ(decoded.expected_epoch, 0);
}

TEST(ReceiverCreditProtocol, FullResponseRoundTrips) {
    const auto original = response();
    std::string wire;
    ASSERT_TRUE(ReceiverCreditPullResponseCodecV1::encode(original, wire).ok());
    EXPECT_EQ(wire.size(), ReceiverCreditPullResponseCodecV1::kWireBytes);
    EXPECT_EQ(static_cast<uint8_t>(wire[0]), 0x54);
    EXPECT_EQ(static_cast<uint8_t>(wire[1]), 0x43);
    EXPECT_EQ(static_cast<uint8_t>(wire[2]), 0x52);
    EXPECT_EQ(static_cast<uint8_t>(wire[3]), 0x50);

    ReceiverCreditPullResponseV1 decoded;
    ASSERT_TRUE(ReceiverCreditPullResponseCodecV1::decode(wire, decoded).ok());
    EXPECT_EQ(decoded.status, original.status);
    EXPECT_EQ(decoded.retry_after_us, 0);
    EXPECT_EQ(decoded.activation.receiver_session_id,
              original.activation.receiver_session_id);
    EXPECT_EQ(decoded.update.sequence, original.update.sequence);
    ASSERT_EQ(decoded.update.grants.size(), kCreditResourceCount);
    for (size_t i = 0; i < kCreditResourceCount; ++i) {
        EXPECT_EQ(decoded.update.grants[i].resource,
                  original.update.grants[i].resource);
        EXPECT_EQ(decoded.update.grants[i].grant_total,
                  original.update.grants[i].grant_total);
    }
}

TEST(ReceiverCreditProtocol, ResponseRejectsInvalidRetryAndPartialUpdate) {
    auto invalid = response();
    invalid.status = ReceiverCreditPullStatus::Retry;
    std::string unchanged = "sentinel";
    EXPECT_TRUE(ReceiverCreditPullResponseCodecV1::encode(invalid, unchanged)
                    .IsInvalidArgument());
    EXPECT_EQ(unchanged, "sentinel");

    invalid.retry_after_us = 10;
    ASSERT_TRUE(
        ReceiverCreditPullResponseCodecV1::encode(invalid, unchanged).ok());
    invalid.update.grants.pop_back();
    unchanged = "sentinel";
    EXPECT_TRUE(ReceiverCreditPullResponseCodecV1::encode(invalid, unchanged)
                    .IsInvalidArgument());
    EXPECT_EQ(unchanged, "sentinel");
}

TEST(ReceiverCreditProtocol, UnsupportedCanUseFixedSizeZeroPayload) {
    ReceiverCreditPullResponseV1 unsupported;
    ASSERT_TRUE(ReceiverCreditPullResponseCodecV1::makeZeroPayload(
                    ReceiverCreditPullStatus::Unsupported, unsupported)
                    .ok());

    std::string wire;
    ASSERT_TRUE(
        ReceiverCreditPullResponseCodecV1::encode(unsupported, wire).ok());
    EXPECT_EQ(wire.size(), ReceiverCreditPullResponseCodecV1::kWireBytes);
    ReceiverCreditPullResponseV1 decoded;
    ASSERT_TRUE(ReceiverCreditPullResponseCodecV1::decode(wire, decoded).ok());
    EXPECT_EQ(decoded.status, ReceiverCreditPullStatus::Unsupported);
    EXPECT_EQ(decoded.activation.schema_version, 0);
    EXPECT_EQ(decoded.update.schema_version, 0);
    EXPECT_TRUE(decoded.update.grants.empty());

    const auto prior = decoded.status;
    EXPECT_TRUE(ReceiverCreditPullResponseCodecV1::makeZeroPayload(
                    ReceiverCreditPullStatus::Granted, decoded)
                    .IsInvalidArgument());
    EXPECT_EQ(decoded.status, prior);
}

TEST(ReceiverCreditProtocol, ResponseDecodeIsAtomicForReservedAndDuplicate) {
    std::string wire;
    ASSERT_TRUE(
        ReceiverCreditPullResponseCodecV1::encode(response(), wire).ok());
    ReceiverCreditPullResponseV1 output;
    output.update.sequence = 99;
    for (size_t length = 0; length < wire.size(); ++length) {
        EXPECT_TRUE(ReceiverCreditPullResponseCodecV1::decode(
                        std::string_view(wire.data(), length), output)
                        .IsInvalidArgument());
        EXPECT_EQ(output.update.sequence, 99);
    }

    std::string reserved = wire;
    reserved[11] = 1;
    EXPECT_TRUE(ReceiverCreditPullResponseCodecV1::decode(reserved, output)
                    .IsInvalidArgument());
    EXPECT_EQ(output.update.sequence, 99);

    std::string duplicate = wire;
    // The second grant starts at 112; overwrite its resource with DataBytes.
    duplicate[112] = 0;
    duplicate[113] = 1;
    EXPECT_TRUE(ReceiverCreditPullResponseCodecV1::decode(duplicate, output)
                    .IsInvalidArgument());
    EXPECT_EQ(output.update.sequence, 99);
}

}  // namespace
}  // namespace mooncake::tent
