// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_rpc_types.h"
#include <ylt/struct_pack.hpp>
#include <gtest/gtest.h>

namespace mooncake::ssd_offload {
namespace {
TEST(SsdReadProtocolCodec, HasExplicitVersionedReflection) {
    EXPECT_TRUE(ylt::reflection::is_ylt_refl_v<SsdReadCapabilityV1>);
    EXPECT_TRUE(ylt::reflection::is_ylt_refl_v<SsdReadRequestV1>);
    EXPECT_TRUE(ylt::reflection::is_ylt_refl_v<SsdReadResultV1>);
    EXPECT_TRUE(ylt::reflection::is_ylt_refl_v<DestinationDrainAck>);
}
TEST(SsdReadProtocolCodec,
     PreservesEpochCredentialRangeAndChecksumThroughWire) {
    PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
    SsdReadRequestV1 request{
        1,
        requester,
        {owner, 8, 9},
        "tenant-a",
        100,
        2,
        {{"key", 12000, 19, {{23, {{{requester, 7, 10, {11, 12}}, 7, 8}}}}}}};
    const auto wire = struct_pack::serialize(request);
    auto decoded = struct_pack::deserialize<SsdReadRequestV1>(wire);
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(ValidateSsdReadRequestV1(*decoded, owner), DestinationError::kOk);
    const auto& region = decoded->objects[0].ranges[0].destinations[0].region;
    EXPECT_EQ(region.requester, requester);
    EXPECT_EQ(region.credential, (WriteCredential{11, 12}));
    EXPECT_EQ(region.generation, 10u);
    EXPECT_EQ(decoded->objects[0].expected_object_checksum, 19u);
    EXPECT_EQ(decoded->relative_timeout_ms, 100u);
    EXPECT_EQ(decoded->tenant_id, "tenant-a");
    auto truncated = wire;
    truncated.pop_back();
    EXPECT_FALSE(
        struct_pack::deserialize<SsdReadRequestV1>(truncated).has_value());
}
TEST(SsdReadProtocolCodec, PreservesRequesterFenceAndDrainedFailure) {
    DestinationDrainAck ack{
        {{4, 5, 6}, 8, 9}, AttemptOutcome::kFailed, 3, true, {1, 2, 3}};
    auto decoded = struct_pack::deserialize<DestinationDrainAck>(
        struct_pack::serialize(ack));
    ASSERT_TRUE(decoded.has_value());
    EXPECT_EQ(decoded->attempt, ack.attempt);
    EXPECT_EQ(decoded->requester, ack.requester);
    EXPECT_TRUE(decoded->dma_drained);
    EXPECT_EQ(decoded->outcome, AttemptOutcome::kFailed);
    EXPECT_EQ(decoded->valid_bytes, 3u);
}

TEST(SsdReadProtocolCodec, PreservesCapabilityAndCompleteResultContracts) {
    PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
    SsdReadCapabilityV1 capability{
        1,       owner,   kSsdPush | kSsdRangeRead | kSsdScatter | kSsdDrainAck,
        64,      1048576, 65536,
        1048576, 4096,    {"tcp", "rdma"}};
    auto cap = struct_pack::deserialize<SsdReadCapabilityV1>(
        struct_pack::serialize(capability));
    ASSERT_TRUE(cap.has_value());
    EXPECT_TRUE(CanUseSsdPushV1(*cap, owner, "tcp"));
    EXPECT_EQ(cap->supported_transports, capability.supported_transports);
    SsdReadRequestV1 request{
        1,
        requester,
        {owner, 8, 9},
        "tenant-a",
        100,
        0,
        {{"key", 12000, 19, {{23, {{{requester, 7, 10, {11, 12}}, 7, 8}}}}}}};
    SsdReadResultV1 response{1,
                             request.attempt,
                             true,
                             {{0, AttemptOutcome::kSucceeded, 8, 19}},
                             requester};
    auto result = struct_pack::deserialize<SsdReadResultV1>(
        struct_pack::serialize(response));
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(ValidateSsdReadResultV1(request, *result), DestinationError::kOk);
    result->requester.boot_epoch++;
    EXPECT_EQ(ValidateSsdReadResultV1(request, *result),
              DestinationError::kWrongAttempt);
}
}  // namespace
}  // namespace mooncake::ssd_offload
