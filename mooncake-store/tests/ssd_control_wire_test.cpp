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
    return {SsdControlCommand::kReadInto,
            reference,
            request,
            {{"requester-te", 4096, 32, DestinationMemory::kHost}}};
}
void Put32(std::string& wire, size_t offset, uint32_t value) {
    ASSERT_LE(offset + 4, wire.size());
    for (size_t i = 0; i < 4; ++i) wire[offset + i] = char(value >> (i * 8));
}
TEST(SsdControlWire, RoundTripsExactRequestAndGrant) {
    auto request = Message();
    EXPECT_EQ(ValidateSsdControlMessageWireSize(request),
              DestinationError::kOk);
    auto wire = EncodeSsdControlMessage(request);
    ASSERT_EQ(wire.error, DestinationError::kOk);
    SsdControlMessage decoded;
    ASSERT_EQ(DecodeSsdControlMessage(wire.bytes, decoded),
              DestinationError::kOk);
    EXPECT_EQ(decoded.reference, request.reference);
    ASSERT_TRUE(decoded.request);
    EXPECT_EQ(*decoded.request, *request.request);
    EXPECT_EQ(decoded.destinations, request.destinations);
    request.command = SsdControlCommand::kRecoverReadInto;
    EXPECT_EQ(ValidateSsdControlMessageWireSize(request),
              DestinationError::kOk);
    EXPECT_EQ(EncodeSsdControlMessage(request).error, DestinationError::kOk);
}
TEST(SsdControlWire, DecodesUnalignedFramesAndRejectsEveryTruncation) {
    auto message = Message();
    message.command = SsdControlCommand::kReadInto;
    message.reference.credential = {0xfedcba9876543210ULL,
                                    0x8070605040302010ULL};
    message.request->objects[0].expected_object_checksum = UINT64_MAX;
    message.destinations = {
        {"requester-te", 0x123456789abcULL, 32, DestinationMemory::kHost}};
    const auto encoded = EncodeSsdControlMessage(message);
    ASSERT_EQ(encoded.error, DestinationError::kOk);
    SsdControlReply reply{
        SsdControlCommand::kReadInto, DestinationError::kOk,
        SsdOwnerRequestSnapshot{
            DestinationError::kOk, SsdOwnerRequestState::kDrained,
            SsdReadResultV1{1,
                            message.reference.attempt,
                            true,
                            {{0, AttemptOutcome::kSucceeded, 32, UINT64_MAX}},
                            message.reference.requester}}};
    const auto terminal = EncodeSsdControlReply(reply);
    ASSERT_EQ(terminal.error, DestinationError::kOk);
    for (size_t offset = 0; offset < 8; ++offset) {
        SCOPED_TRACE(offset);
        const auto storage = std::string(offset, 'x') + encoded.bytes;
        const auto input = std::string_view(storage).substr(offset);
        SsdControlMessage decoded;
        ASSERT_EQ(DecodeSsdControlMessage(input, decoded),
                  DestinationError::kOk);
        EXPECT_EQ(decoded.reference, message.reference);
        EXPECT_EQ(decoded.request, message.request);
        EXPECT_EQ(EncodeSsdControlMessage(decoded).bytes, encoded.bytes);
        const auto before = EncodeSsdControlMessage(decoded).bytes;
        for (size_t size = 0; size < input.size(); ++size) {
            EXPECT_NE(DecodeSsdControlMessage(input.substr(0, size), decoded),
                      DestinationError::kOk);
            EXPECT_EQ(EncodeSsdControlMessage(decoded).bytes, before);
        }
        const auto response_storage = std::string(offset, 'x') + terminal.bytes;
        const auto response = std::string_view(response_storage).substr(offset);
        SsdControlReply result;
        ASSERT_EQ(DecodeSsdControlReply(response, result),
                  DestinationError::kOk);
        ASSERT_TRUE(result.snapshot);
        EXPECT_EQ(result.snapshot->result, reply.snapshot->result);
        for (size_t size = 0; size < response.size(); ++size) {
            EXPECT_NE(DecodeSsdControlReply(response.substr(0, size), result),
                      DestinationError::kOk);
            EXPECT_EQ(EncodeSsdControlReply(result).bytes, terminal.bytes);
        }
    }
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
TEST(SsdControlWire, RoundTripsTerminalRecoveryAndErrorReplies) {
    auto message = Message();
    SsdReadResultV1 result{1,
                           message.reference.attempt,
                           true,
                           {{0, AttemptOutcome::kSucceeded, 32, 123}},
                           message.reference.requester};
    SsdControlReply reply{
        SsdControlCommand::kRecoverReadInto, DestinationError::kOk,
        SsdOwnerRequestSnapshot{DestinationError::kOk,
                                SsdOwnerRequestState::kDrained, result}};
    auto wire = EncodeSsdControlReply(reply);
    ASSERT_EQ(wire.error, DestinationError::kOk);
    SsdControlReply decoded;
    ASSERT_EQ(DecodeSsdControlReply(wire.bytes, decoded),
              DestinationError::kOk);
    ASSERT_TRUE(decoded.snapshot);
    EXPECT_EQ(decoded.snapshot->result, result);
    for (auto err : {DestinationError::kTimedOut, DestinationError::kNotClosing,
                     DestinationError::kFailed}) {
        reply = {SsdControlCommand::kReadInto, err, {}};
        wire = EncodeSsdControlReply(reply);
        ASSERT_EQ(wire.error, DestinationError::kOk);
        ASSERT_EQ(DecodeSsdControlReply(wire.bytes, decoded),
                  DestinationError::kOk);
        EXPECT_EQ(decoded.error, err);
        EXPECT_FALSE(decoded.snapshot);
    }
}
TEST(SsdControlWire, RejectsUnknownCommandAndUnknownSnapshotState) {
    auto bad_message = Message();
    bad_message.command = static_cast<SsdControlCommand>(1);
    EXPECT_EQ(ValidateSsdControlMessageWireSize(bad_message),
              DestinationError::kInvalidArgument);
    EXPECT_EQ(EncodeSsdControlMessage(bad_message).error,
              DestinationError::kInvalidArgument);
    SsdControlReply reply{
        SsdControlCommand::kRecoverReadInto, DestinationError::kOk,
        SsdOwnerRequestSnapshot{
            DestinationError::kOk, SsdOwnerRequestState(99), {}}};
    EXPECT_EQ(EncodeSsdControlReply(reply).error,
              DestinationError::kInvalidArgument);
    reply = {static_cast<SsdControlCommand>(2), DestinationError::kOk, {}};
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
        SsdControlCommand::kReadInto, DestinationError::kOk,
        SsdOwnerRequestSnapshot{DestinationError::kOk,
                                SsdOwnerRequestState::kDrained, result}};
    EXPECT_EQ(EncodeSsdControlReply(reply).error, DestinationError::kOk);
}
TEST(SsdControlWire, DirectReadRejectsMissingMismatchedOrOversizedInlineViews) {
    auto message = Message();
    message.command = SsdControlCommand::kReadInto;
    message.destinations.clear();
    EXPECT_EQ(ValidateSsdControlMessageWireSize(message),
              DestinationError::kInvalidArgument);
    EXPECT_EQ(EncodeSsdControlMessage(message).error,
              DestinationError::kInvalidArgument);
    message.destinations = {
        {"requester-te", 8192, 31, DestinationMemory::kHost}};
    EXPECT_EQ(EncodeSsdControlMessage(message).error,
              DestinationError::kInvalidArgument);
    message.destinations[0].length = 32;
    message.destinations[0].address = UINT64_MAX;
    EXPECT_EQ(ValidateSsdControlMessageWireSize(message),
              DestinationError::kOverflow);
    message.destinations[0].address = 8192;
    message.destinations[0].transport_endpoint.assign(4097, 'x');
    EXPECT_EQ(ValidateSsdControlMessageWireSize(message),
              DestinationError::kLimit);
    EXPECT_EQ(EncodeSsdControlMessage(message).error, DestinationError::kLimit);
}
}  // namespace
}  // namespace mooncake::ssd_offload
