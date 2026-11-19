// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_control_wire.h"
#include <gtest/gtest.h>
namespace mooncake::ssd_offload {
namespace {
SsdControlMessage Message() {
    PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
    SsdGrantReferenceV1 reference{requester, {owner, 7, 8}, {9, 10}};
    SsdReadRequestV1 request{
        1,
        requester,
        reference.attempt,
        "default",
        100,
        0,
        {{"key", 4096, 123, {{12, {{{requester, 1, 2, {11, 12}}, 16, 32}}}}}}};
    return {SsdControlCommand::kSubmit, reference, request};
}
void Put32(std::string& wire, size_t offset, uint32_t value) {
    ASSERT_LE(offset + 4, wire.size());
    for (size_t i = 0; i < 4; ++i) wire[offset + i] = char(value >> (i * 8));
}
TEST(SsdControlWire, RoundTripsExactRequestAndGrant) {
    auto request = Message();
    auto wire = EncodeSsdControlMessage(request);
    ASSERT_EQ(wire.error, DestinationError::kOk);
    SsdControlMessage decoded;
    ASSERT_EQ(DecodeSsdControlMessage(wire.bytes, decoded),
              DestinationError::kOk);
    EXPECT_EQ(decoded.reference, request.reference);
    ASSERT_TRUE(decoded.request);
    EXPECT_EQ(*decoded.request, *request.request);
    request.command = SsdControlCommand::kResolve;
    EXPECT_EQ(EncodeSsdControlMessage(request).error, DestinationError::kOk);
}
TEST(SsdControlWire, BoundsWireBeforeDecodeAndLeavesOutputUnchanged) {
    std::string oversized(kSsdControlMaxWireBytes + 1, 'x');
    SsdControlMessage decoded = Message();
    auto before = decoded;
    EXPECT_EQ(DecodeSsdControlMessage(oversized, decoded),
              DestinationError::kLimit);
    EXPECT_EQ(decoded.reference, before.reference);
    EXPECT_EQ(decoded.request, before.request);
}
TEST(SsdControlWire, RejectsForgedContainerCountBeforeConstruction) {
    auto encoded = EncodeSsdControlMessage(Message());
    ASSERT_EQ(encoded.error, DestinationError::kOk);
    // Fixed header(12), grant(80), request identities/version(68),
    // tenant length/value(11), timeout(8), priority(4), then object count.
    Put32(encoded.bytes, 183, 0xffffffff);
    SsdControlMessage decoded = Message();
    EXPECT_EQ(DecodeSsdControlMessage(encoded.bytes, decoded),
              DestinationError::kLimit);
    EXPECT_EQ(decoded.request, Message().request);
}
TEST(SsdControlWire, RejectsTruncationTrailingBytesVersionAndCrossedDirection) {
    auto encoded = EncodeSsdControlMessage(Message());
    ASSERT_EQ(encoded.error, DestinationError::kOk);
    SsdControlMessage decoded;
    for (size_t i = 0; i < encoded.bytes.size(); ++i)
        EXPECT_NE(DecodeSsdControlMessage(
                      std::string_view(encoded.bytes).substr(0, i), decoded),
                  DestinationError::kOk);
    EXPECT_NE(DecodeSsdControlMessage(encoded.bytes + "x", decoded),
              DestinationError::kOk);
    Put32(encoded.bytes, 4, 2);
    EXPECT_EQ(DecodeSsdControlMessage(encoded.bytes, decoded),
              DestinationError::kNotSupported);
    SsdControlReply reply;
    EXPECT_NE(DecodeSsdControlReply(encoded.bytes, reply),
              DestinationError::kOk);
}
TEST(SsdControlWire, RoundTripsTerminalRecoveryAndAuthoritativeViews) {
    auto message = Message();
    SsdReadResultV1 result{1,
                           message.reference.attempt,
                           true,
                           {{0, AttemptOutcome::kSucceeded, 32, 123}},
                           message.reference.requester};
    SsdControlReply reply{
        SsdControlCommand::kQuery,
        DestinationError::kOk,
        SsdOwnerRequestSnapshot{DestinationError::kOk,
                                SsdOwnerRequestState::kDrained, result},
        {}};
    auto wire = EncodeSsdControlReply(reply);
    ASSERT_EQ(wire.error, DestinationError::kOk);
    SsdControlReply decoded;
    ASSERT_EQ(DecodeSsdControlReply(wire.bytes, decoded),
              DestinationError::kOk);
    ASSERT_TRUE(decoded.snapshot);
    EXPECT_EQ(decoded.snapshot->result, result);
    reply = {SsdControlCommand::kResolve,
             DestinationError::kOk,
             {},
             {{"requester-te", 4096, 32, DestinationMemory::kHost}}};
    wire = EncodeSsdControlReply(reply);
    ASSERT_EQ(wire.error, DestinationError::kOk);
    ASSERT_EQ(DecodeSsdControlReply(wire.bytes, decoded),
              DestinationError::kOk);
    ASSERT_EQ(decoded.destinations.size(), 1);
    EXPECT_EQ(decoded.destinations[0].address, 4096);
    EXPECT_EQ(decoded.destinations[0].length, 32);
}
TEST(SsdControlWire, RejectsReplyMetadataLimitsAndUnknownState) {
    SsdControlReply reply{
        SsdControlCommand::kResolve,
        DestinationError::kOk,
        {},
        {{"requester-te", UINT64_MAX, 32, DestinationMemory::kHost}}};
    EXPECT_EQ(EncodeSsdControlReply(reply).error, DestinationError::kOverflow);
    reply.destinations[0].address = 4096;
    reply.destinations[0].transport_endpoint.assign(4097, 'x');
    EXPECT_EQ(EncodeSsdControlReply(reply).error, DestinationError::kLimit);
    reply = {SsdControlCommand::kQuery,
             DestinationError::kOk,
             SsdOwnerRequestSnapshot{
                 DestinationError::kOk, SsdOwnerRequestState(99), {}},
             {}};
    EXPECT_EQ(EncodeSsdControlReply(reply).error,
              DestinationError::kInvalidArgument);
}
TEST(SsdControlWire,
     DirectReadCarriesBoundedInlineDestinationsAndTerminalReply) {
    auto message = Message();
    message.command = SsdControlCommand::kReadInto;
    message.destinations = {
        {"requester-te", 8192, 32, DestinationMemory::kHost}};
    auto wire = EncodeSsdControlMessage(message);
    ASSERT_EQ(wire.error, DestinationError::kOk);
    SsdControlMessage decoded;
    ASSERT_EQ(DecodeSsdControlMessage(wire.bytes, decoded),
              DestinationError::kOk);
    EXPECT_EQ(decoded.request, message.request);
    ASSERT_EQ(decoded.destinations.size(), 1u);
    EXPECT_EQ(decoded.destinations[0].address, 8192u);
    message.command = SsdControlCommand::kRecoverReadInto;
    EXPECT_EQ(EncodeSsdControlMessage(message).error, DestinationError::kOk);
    SsdReadResultV1 result{1,
                           message.reference.attempt,
                           true,
                           {{0, AttemptOutcome::kSucceeded, 32, 123}},
                           message.reference.requester};
    SsdControlReply reply{
        SsdControlCommand::kReadInto,
        DestinationError::kOk,
        SsdOwnerRequestSnapshot{DestinationError::kOk,
                                SsdOwnerRequestState::kDrained, result},
        {}};
    EXPECT_EQ(EncodeSsdControlReply(reply).error, DestinationError::kOk);
}
TEST(SsdControlWire, DirectReadRejectsMissingMismatchedOrOversizedInlineViews) {
    auto message = Message();
    message.command = SsdControlCommand::kReadInto;
    EXPECT_EQ(EncodeSsdControlMessage(message).error,
              DestinationError::kInvalidArgument);
    message.destinations = {
        {"requester-te", 8192, 31, DestinationMemory::kHost}};
    EXPECT_EQ(EncodeSsdControlMessage(message).error,
              DestinationError::kInvalidArgument);
    message.destinations[0].length = 32;
    message.destinations[0].transport_endpoint.assign(4097, 'x');
    EXPECT_EQ(EncodeSsdControlMessage(message).error, DestinationError::kLimit);
    message.command = SsdControlCommand::kSubmit;
    EXPECT_EQ(EncodeSsdControlMessage(message).error,
              DestinationError::kInvalidArgument);
}
}  // namespace
}  // namespace mooncake::ssd_offload
