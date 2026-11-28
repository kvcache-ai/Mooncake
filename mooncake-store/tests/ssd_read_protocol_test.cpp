// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_protocol.h"
#include <limits>
#include <gtest/gtest.h>

namespace mooncake::ssd_offload {
namespace {
constexpr PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
DestinationRegion Region(uint64_t id = 1) {
    return {requester, id, 1, {11, 12}};
}
SsdReadRequestV1 Request() {
    SsdReadRequestV1 request;
    request.requester = requester;
    request.attempt = {owner, 1, 1};
    request.tenant_id = "tenant-a";
    request.relative_timeout_ms = 100;
    request.objects = {{"key", 12000, 19, {{23, {{Region(), 7, 8}}}}}};
    return request;
}
SsdReadResultV1 Result() {
    return {1,
            {owner, 1, 1},
            true,
            {{0, AttemptOutcome::kSucceeded, 8, 19}},
            requester};
}
SsdReadCapabilityV1 Capability() {
    return {1,
            owner,
            kSsdPush | kSsdDrainAck | kSsdRangeRead | kSsdScatter,
            64,
            1ULL << 30,
            65536,
            1ULL << 26,
            1ULL << 20,
            {"tcp", "rdma"}};
}
TEST(SsdReadProtocol, RequiresCompletePushAndDrainCapability) {
    auto cap = Capability();
    EXPECT_TRUE(CanUseSsdPushV1(cap, owner, "tcp"));
    cap.features &= ~uint64_t(kSsdDrainAck);
    EXPECT_FALSE(CanUseSsdPushV1(cap, owner, "tcp"));
    cap = Capability();
    cap.owner.boot_epoch++;
    EXPECT_FALSE(CanUseSsdPushV1(cap, owner, "tcp"));
    cap = Capability();
    cap.protocol_version++;
    EXPECT_FALSE(CanUseSsdPushV1(cap, owner, "tcp"));
    cap = Capability();
    cap.max_fragments = 0;
    EXPECT_FALSE(CanUseSsdPushV1(cap, owner, "tcp"));
    cap = Capability();
    EXPECT_FALSE(CanUseSsdPushV1(cap, owner, "ascend"));
}
TEST(SsdReadProtocol, AcceptsMultipleRangesAndScatterWithNondefaultTenant) {
    auto request = Request();
    request.objects[0].ranges.push_back(
        {100, {{Region(2), 0, 5}, {Region(3), 0, 7}}});
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner), DestinationError::kOk);
}
TEST(SsdReadProtocol, RejectsWrongVersionOwnerAndRequesterIdentity) {
    auto request = Request();
    request.protocol_version++;
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner),
              DestinationError::kNotSupported);
    request = Request();
    request.attempt.owner.boot_epoch++;
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner),
              DestinationError::kWrongAttempt);
    request = Request();
    request.objects[0].ranges[0].destinations[0].region.requester.boot_epoch++;
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner),
              DestinationError::kStaleRegion);
}
TEST(SsdReadProtocol,
     RejectsSourceOverflowAndConflictingDestinationReferences) {
    auto request = Request();
    request.objects[0].ranges[0].source_offset = 11999;
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner),
              DestinationError::kInvalidArgument);
    request = Request();
    request.objects[0].ranges[0].destinations[0].offset =
        std::numeric_limits<uint64_t>::max();
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner),
              DestinationError::kOverflow);
    request = Request();
    request.objects[0].ranges[0].destinations.push_back({Region(), 8, 8});
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner),
              DestinationError::kOverlap);
    request = Request();
    auto other = Region();
    other.credential.low++;
    request.objects[0].ranges[0].destinations.push_back({other, 16, 8});
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner),
              DestinationError::kUnauthorized);
}
TEST(SsdReadProtocol, BoundsAllWireMetadataAndRelativeTimeout) {
    auto request = Request();
    SsdReadProtocolLimits limits;
    limits.max_payload_bytes = 7;
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner, limits),
              DestinationError::kLimit);
    limits = {};
    limits.max_key_bytes = 2;
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner, limits),
              DestinationError::kLimit);
    limits = {};
    limits.max_ranges = 1;
    request.objects[0].ranges.push_back({0, {}});
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner, limits),
              DestinationError::kLimit);
    request = Request();
    request.relative_timeout_ms = 0;
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner),
              DestinationError::kInvalidArgument);
    request.relative_timeout_ms = 60001;
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner),
              DestinationError::kLimit);
    request = Request();
    request.tenant_id = std::string("bad\0tenant", 10);
    EXPECT_EQ(ValidateSsdReadRequestV1(request, owner),
              DestinationError::kInvalidArgument);
}
TEST(SsdReadProtocol, AcceptsTerminalFailureOnlyWhenDrained) {
    auto result = Result();
    EXPECT_EQ(ValidateSsdReadResultV1(Request(), result),
              DestinationError::kOk);
    result.objects[0].outcome = AttemptOutcome::kFailed;
    result.objects[0].valid_bytes = 3;
    EXPECT_EQ(ValidateSsdReadResultV1(Request(), result),
              DestinationError::kOk);
    result.dma_drained = false;
    EXPECT_EQ(ValidateSsdReadResultV1(Request(), result),
              DestinationError::kNotDrained);
}
TEST(SsdReadProtocol, RejectsMalformedResultEpochCountsAndChecksumFence) {
    auto result = Result();
    result.attempt.attempt_id++;
    EXPECT_EQ(ValidateSsdReadResultV1(Request(), result),
              DestinationError::kWrongAttempt);
    result = Result();
    result.objects[0].valid_bytes = 7;
    EXPECT_EQ(ValidateSsdReadResultV1(Request(), result),
              DestinationError::kInvalidArgument);
    result = Result();
    result.objects[0].original_index = 1;
    EXPECT_EQ(ValidateSsdReadResultV1(Request(), result),
              DestinationError::kInvalidArgument);
    result = Result();
    result.objects[0].object_checksum = 20;
    EXPECT_EQ(ValidateSsdReadResultV1(Request(), result),
              DestinationError::kInvalidArgument);
    result = Result();
    result.objects.push_back(result.objects[0]);
    EXPECT_EQ(ValidateSsdReadResultV1(Request(), result),
              DestinationError::kInvalidArgument);
}

TEST(SsdReadProtocol, RejectsReservedAndControlCharacterTenantNames) {
    for (const auto& tenant :
         {std::string("_internal"), std::string("bad\ntenant"),
          std::string("bad\x7ftenant")}) {
        auto request = Request();
        request.tenant_id = tenant;
        EXPECT_EQ(ValidateSsdReadRequestV1(request, owner),
                  DestinationError::kInvalidArgument);
    }
}
}  // namespace
}  // namespace mooncake::ssd_offload
