// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_destination_registry.h"

#include <array>
#include <future>
#include <limits>
#include <stdexcept>
#include <thread>
#include <gtest/gtest.h>

namespace mooncake::ssd_offload {
namespace {
constexpr PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
constexpr WriteCredential credential{11, 12};
DestinationBinding Binding(uint64_t base = 1000, uint64_t bytes = 32) {
    return {base, bytes, base - 16, bytes + 32, DestinationMemory::kHost};
}
std::shared_ptr<void> Lifetime() { return std::make_shared<int>(1); }
AttemptIdentity Key(uint64_t request = 1, uint64_t attempt = 1) {
    return {owner, request, attempt};
}
DestinationDrainAck Ack(uint64_t request, uint64_t bytes = 8,
                        AttemptOutcome outcome = AttemptOutcome::kSucceeded) {
    return {Key(request), outcome, bytes, true, requester};
}

TEST(SsdDestinationRegistry, RejectsInvalidIdentityAndLimits) {
    EXPECT_THROW(DestinationRegistry({}, "requester"), std::invalid_argument);
    EXPECT_THROW(DestinationRegistry(requester, ""), std::invalid_argument);
    DestinationRegistryLimits limits;
    limits.max_regions = 0;
    EXPECT_THROW(DestinationRegistry(requester, "requester", limits),
                 std::invalid_argument);
}

TEST(SsdDestinationRegistry, ResolvesLogicalRangeInsidePaddedRegistration) {
    DestinationRegistry registry(requester, "receiver:100");
    auto registered = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(registered.error, DestinationError::kOk);
    EXPECT_EQ(registered.region.requester, requester);
    EXPECT_NE(registered.region.region_id, 0u);
    EXPECT_NE(registered.region.generation, 0u);
    auto acquired = registry.Acquire(Key(), {{registered.region, 7, 8}});
    ASSERT_EQ(acquired.error, DestinationError::kOk);
    EXPECT_EQ(acquired.attempt.snapshot().state,
              DestinationAttemptState::kPrepared);
    std::vector<ResolvedDestination> destinations;
    ASSERT_EQ(acquired.attempt.Publish(destinations), DestinationError::kOk);
    ASSERT_EQ(destinations.size(), 1u);
    EXPECT_EQ(destinations[0].address, 1007u);
    EXPECT_EQ(destinations[0].length, 8u);
    EXPECT_EQ(destinations[0].transport_endpoint, "receiver:100");
    EXPECT_EQ(acquired.attempt.Complete(Ack(1)), DestinationError::kOk);
    EXPECT_TRUE(acquired.attempt.snapshot().publishable);
}

TEST(SsdDestinationRegistry, RejectsMalformedRegistrationAndLogicalAliases) {
    DestinationRegistry registry(requester, "receiver");
    EXPECT_EQ(registry.Register(Binding(), {}, Lifetime()).error,
              DestinationError::kInvalidArgument);
    EXPECT_EQ(registry.Register(Binding(), credential, {}).error,
              DestinationError::kInvalidArgument);
    auto bad = Binding();
    bad.registered_bytes = 20;
    EXPECT_EQ(registry.Register(bad, credential, Lifetime()).error,
              DestinationError::kInvalidArgument);
    bad = Binding(std::numeric_limits<uint64_t>::max() - 4, 8);
    EXPECT_EQ(registry.Register(bad, credential, Lifetime()).error,
              DestinationError::kOverflow);
    ASSERT_EQ(registry.Register(Binding(), credential, Lifetime()).error,
              DestinationError::kOk);
    EXPECT_EQ(registry.Register(Binding(1016), {21, 22}, Lifetime()).error,
              DestinationError::kOverlap);
    // MR padding may overlap, but logical regions must remain disjoint.
    EXPECT_EQ(registry.Register(Binding(1032), {21, 22}, Lifetime()).error,
              DestinationError::kOk);
}

TEST(SsdDestinationRegistry, RejectsCredentialEpochPaddingAndOffsetOverflow) {
    DestinationRegistry registry(requester, "receiver");
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto wrong = region.region;
    wrong.credential.low++;
    EXPECT_EQ(registry.Acquire(Key(), {{wrong, 0, 1}}).error,
              DestinationError::kUnauthorized);
    wrong = region.region;
    wrong.requester.boot_epoch++;
    EXPECT_EQ(registry.Acquire(Key(), {{wrong, 0, 1}}).error,
              DestinationError::kStaleRegion);
    EXPECT_EQ(registry.Acquire(Key(), {{region.region, 32, 1}}).error,
              DestinationError::kInvalidArgument);
    EXPECT_EQ(registry
                  .Acquire(Key(), {{region.region,
                                    std::numeric_limits<uint64_t>::max(), 2}})
                  .error,
              DestinationError::kInvalidArgument);
    EXPECT_EQ(registry.stats().active_attempts, 0u);
}

TEST(SsdDestinationRegistry, PreflightsEntireAttemptWithoutPartialReferences) {
    DestinationRegistry registry(requester, "receiver");
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto invalid = region.region;
    invalid.region_id = 999;
    EXPECT_EQ(
        registry.Acquire(Key(), {{region.region, 0, 8}, {invalid, 0, 8}}).error,
        DestinationError::kNotFound);
    EXPECT_EQ(registry.stats().active_bytes, 0u);
    EXPECT_EQ(registry.BeginRevoke(region.region), DestinationError::kOk);
    EXPECT_EQ(registry.Retire(region.region), DestinationError::kOk);
}

TEST(SsdDestinationRegistry,
     RejectsOverlapWithinAttemptAndAcrossConcurrentWriters) {
    DestinationRegistry registry(requester, "receiver");
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    EXPECT_EQ(
        registry.Acquire(Key(), {{region.region, 0, 8}, {region.region, 4, 8}})
            .error,
        DestinationError::kOverlap);
    auto first = registry.Acquire(Key(), {{region.region, 0, 8}});
    ASSERT_EQ(first.error, DestinationError::kOk);
    EXPECT_EQ(registry.Acquire(Key(2), {{region.region, 7, 8}}).error,
              DestinationError::kOverlap);
    auto adjacent = registry.Acquire(Key(2), {{region.region, 8, 8}});
    EXPECT_EQ(adjacent.error, DestinationError::kOk);
}

TEST(SsdDestinationRegistry, BoundsAggregateBytesFragmentsAndTrackedRequests) {
    DestinationRegistryLimits limits;
    limits.max_active_bytes = 12;
    limits.max_active_fragments = 2;
    limits.max_tracked_attempts = 2;
    DestinationRegistry registry(requester, "receiver", limits);
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto first = registry.Acquire(Key(), {{region.region, 0, 8}});
    ASSERT_EQ(first.error, DestinationError::kOk);
    EXPECT_EQ(registry.Acquire(Key(2), {{region.region, 8, 8}}).error,
              DestinationError::kLimit);
    auto second = registry.Acquire(Key(2), {{region.region, 8, 4}});
    ASSERT_EQ(second.error, DestinationError::kOk);
    EXPECT_EQ(registry.Acquire(Key(3), {{region.region, 16, 1}}).error,
              DestinationError::kLimit);
    EXPECT_EQ(registry.stats().active_bytes, 12u);
    EXPECT_EQ(registry.stats().active_fragments, 2u);
}

TEST(SsdDestinationRegistry, ClosingBlocksNewLeasesAndRetirementWaitsForDrain) {
    DestinationRegistry registry(requester, "receiver");
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto acquired = registry.Acquire(Key(), {{region.region, 0, 8}});
    ASSERT_EQ(acquired.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(acquired.attempt.Publish(views), DestinationError::kOk);
    EXPECT_EQ(registry.BeginRevoke(region.region), DestinationError::kOk);
    EXPECT_EQ(registry.Acquire(Key(2), {{region.region, 8, 8}}).error,
              DestinationError::kClosing);
    EXPECT_EQ(registry.Retire(region.region), DestinationError::kBusy);
    EXPECT_EQ(acquired.attempt.Complete(Ack(1)), DestinationError::kOk);
    EXPECT_EQ(registry.Retire(region.region), DestinationError::kOk);
}

TEST(SsdDestinationRegistry, TimeoutAndAbandonedExposedHandleKeepMemoryPinned) {
    DestinationRegistry registry(requester, "receiver");
    auto backing = Lifetime();
    std::weak_ptr<void> weak = backing;
    auto region = registry.Register(Binding(), credential, backing);
    ASSERT_EQ(region.error, DestinationError::kOk);
    backing.reset();
    {
        auto acquired = registry.Acquire(Key(), {{region.region, 0, 8}});
        ASSERT_EQ(acquired.error, DestinationError::kOk);
        std::vector<ResolvedDestination> views;
        ASSERT_EQ(acquired.attempt.Publish(views), DestinationError::kOk);
        EXPECT_EQ(acquired.attempt.Timeout(), DestinationError::kOk);
    }
    EXPECT_EQ(registry.Inspect(Key()).state,
              DestinationAttemptState::kQuarantined);
    EXPECT_EQ(registry.BeginRevoke(region.region), DestinationError::kOk);
    EXPECT_EQ(registry.Retire(region.region), DestinationError::kBusy);
    EXPECT_FALSE(weak.expired());
    auto incomplete = Ack(1);
    incomplete.dma_drained = false;
    EXPECT_EQ(registry.Complete(incomplete), DestinationError::kNotDrained);
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(registry.Complete(Ack(1, 0, AttemptOutcome::kCancelled)),
              DestinationError::kOk);
    EXPECT_FALSE(registry.Inspect(Key()).publishable);
    EXPECT_EQ(registry.Retire(region.region), DestinationError::kOk);
    EXPECT_TRUE(weak.expired());
}

TEST(SsdDestinationRegistry, RejectsWrongAttemptAndMalformedSuccessAck) {
    DestinationRegistry registry(requester, "receiver");
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto acquired = registry.Acquire(Key(), {{region.region, 0, 8}});
    ASSERT_EQ(acquired.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(acquired.attempt.Publish(views), DestinationError::kOk);
    auto wrong = Ack(1);
    wrong.attempt.owner.boot_epoch++;
    EXPECT_EQ(acquired.attempt.Complete(wrong),
              DestinationError::kWrongAttempt);
    wrong = Ack(1);
    wrong.valid_bytes = 7;
    EXPECT_EQ(acquired.attempt.Complete(wrong),
              DestinationError::kInvalidArgument);
    EXPECT_EQ(registry.stats().active_bytes, 8u);
    EXPECT_EQ(acquired.attempt.Complete(Ack(1)), DestinationError::kOk);
    EXPECT_EQ(acquired.attempt.Complete(Ack(1)), DestinationError::kDuplicate);
}

TEST(SsdDestinationRegistry, PreparedHandleDestructionIsSafeWithoutDrainAck) {
    DestinationRegistry registry(requester, "receiver");
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    {
        auto acquired = registry.Acquire(Key(), {{region.region, 0, 8}});
        ASSERT_EQ(acquired.error, DestinationError::kOk);
    }
    EXPECT_EQ(registry.stats().active_attempts, 0u);
    EXPECT_EQ(registry.Inspect(Key()).state, DestinationAttemptState::kDrained);
    EXPECT_EQ(registry.BeginRevoke(region.region), DestinationError::kOk);
    EXPECT_EQ(registry.Retire(region.region), DestinationError::kOk);
}

TEST(SsdDestinationRegistry, ReuseChangesGenerationAndRejectsOldHandle) {
    DestinationRegistryLimits limits;
    limits.max_regions = 1;
    DestinationRegistry registry(requester, "receiver", limits);
    auto old = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(old.error, DestinationError::kOk);
    EXPECT_EQ(registry.Register(Binding(2000), {21, 22}, Lifetime()).error,
              DestinationError::kLimit);
    ASSERT_EQ(registry.BeginRevoke(old.region), DestinationError::kOk);
    ASSERT_EQ(registry.Retire(old.region), DestinationError::kOk);
    auto fresh = registry.Register(Binding(), {21, 22}, Lifetime());
    ASSERT_EQ(fresh.error, DestinationError::kOk);
    EXPECT_EQ(fresh.region.region_id, old.region.region_id);
    EXPECT_GT(fresh.region.generation, old.region.generation);
    EXPECT_EQ(registry.Acquire(Key(), {{old.region, 0, 8}}).error,
              DestinationError::kStaleRegion);
}

TEST(SsdDestinationRegistry,
     ForgettingCompletedRequestFencesReplayAndKeepsOlderInflight) {
    DestinationRegistry registry(requester, "receiver");
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto first = registry.Acquire(Key(), {{region.region, 0, 8}});
    auto newer = registry.Acquire(Key(2), {{region.region, 8, 8}});
    ASSERT_EQ(first.error, DestinationError::kOk);
    ASSERT_EQ(newer.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(first.attempt.Publish(views), DestinationError::kOk);
    ASSERT_EQ(newer.attempt.Publish(views), DestinationError::kOk);
    EXPECT_EQ(registry.ForgetRequest(owner, 1), DestinationError::kBusy);
    ASSERT_EQ(newer.attempt.Complete(Ack(2)), DestinationError::kOk);
    EXPECT_EQ(registry.Acquire(Key(2), {{region.region, 8, 8}}).error,
              DestinationError::kDuplicate);
    EXPECT_EQ(registry.ForgetRequest(owner, 2), DestinationError::kOk);
    EXPECT_EQ(registry.Acquire(Key(2), {{region.region, 8, 8}}).error,
              DestinationError::kStaleRequest);
    EXPECT_EQ(first.attempt.Complete(Ack(1)), DestinationError::kOk);
    EXPECT_EQ(registry.ForgetRequest(owner, 1), DestinationError::kOk);
}

TEST(SsdDestinationRegistry,
     HandleAndBackingSurviveRegistryDestructionUntilDrain) {
    auto backing = Lifetime();
    std::weak_ptr<void> weak = backing;
    auto registry =
        std::make_unique<DestinationRegistry>(requester, "receiver");
    auto region = registry->Register(Binding(), credential, backing);
    ASSERT_EQ(region.error, DestinationError::kOk);
    backing.reset();
    auto acquired = registry->Acquire(Key(), {{region.region, 0, 8}});
    ASSERT_EQ(acquired.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(acquired.attempt.Publish(views), DestinationError::kOk);
    registry.reset();
    EXPECT_FALSE(weak.expired());
    EXPECT_EQ(acquired.attempt.Complete(Ack(1)), DestinationError::kOk);
    acquired.attempt = {};
    EXPECT_TRUE(weak.expired());
}

TEST(SsdDestinationRegistry, ConcurrentOverlappingAdmissionHasOnlyOneWinner) {
    DestinationRegistry registry(requester, "receiver");
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto first = std::async(std::launch::async, [&] {
        return registry.Acquire(Key(1), {{region.region, 0, 8}});
    });
    auto second = std::async(std::launch::async, [&] {
        return registry.Acquire(Key(2), {{region.region, 0, 8}});
    });
    auto a = first.get();
    auto b = second.get();
    EXPECT_EQ(
        (a.error == DestinationError::kOk) + (b.error == DestinationError::kOk),
        1);
    EXPECT_TRUE(a.error == DestinationError::kOverlap ||
                b.error == DestinationError::kOverlap);
}

TEST(SsdDestinationRegistry, ReportsFragmentStorageWhileAttemptIsActive) {
    DestinationRegistry registry(requester, "receiver");
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto acquired = registry.Acquire(Key(), {{region.region, 0, 8}});
    ASSERT_EQ(acquired.error, DestinationError::kOk);
    EXPECT_GT(registry.stats().fragment_array_bytes, 0u);
}

TEST(SsdDestinationRegistry, RejectsLateDrainAckFromPreviousRequesterBoot) {
    auto restarted = requester;
    restarted.boot_epoch++;
    DestinationRegistry registry(restarted, "receiver");
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto acquired = registry.Acquire(Key(), {{region.region, 0, 8}});
    ASSERT_EQ(acquired.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(acquired.attempt.Publish(views), DestinationError::kOk);
    EXPECT_EQ(registry.Complete(Ack(1)), DestinationError::kWrongAttempt);
    EXPECT_EQ(registry.stats().active_attempts, 1u);
    auto fresh = Ack(1);
    fresh.requester = restarted;
    EXPECT_EQ(acquired.attempt.Complete(fresh), DestinationError::kOk);
}

TEST(SsdDestinationRegistry, DrainedSummariesReleaseAllFragmentArrayStorage) {
    DestinationRegistry registry(requester, "receiver");
    auto region = registry.Register(Binding(1000, 128), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    for (uint64_t request = 1; request <= 8; ++request) {
        std::vector<RegisteredDestination> slices;
        for (uint64_t offset = 0; offset < 64; ++offset)
            slices.push_back({region.region, offset, 1});
        auto acquired = registry.Acquire(Key(request), slices);
        ASSERT_EQ(acquired.error, DestinationError::kOk);
        std::vector<ResolvedDestination> views;
        ASSERT_EQ(acquired.attempt.Publish(views), DestinationError::kOk);
        ASSERT_EQ(acquired.attempt.Complete(Ack(request, 64)),
                  DestinationError::kOk);
        EXPECT_EQ(registry.stats().fragment_array_bytes, 0u);
    }
    EXPECT_EQ(registry.stats().tracked_attempts, 8u);
}

TEST(SsdDestinationRegistry,
     LateSuccessDrainsButNeverPublishesTimedOutAttempt) {
    DestinationRegistry registry(requester, "receiver");
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto acquired = registry.Acquire(Key(), {{region.region, 0, 8}});
    ASSERT_EQ(acquired.error, DestinationError::kOk);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(acquired.attempt.Publish(views), DestinationError::kOk);
    ASSERT_EQ(acquired.attempt.Timeout(), DestinationError::kOk);
    ASSERT_EQ(acquired.attempt.Complete(Ack(1)), DestinationError::kOk);
    EXPECT_EQ(registry.stats().active_attempts, 0u);
    EXPECT_FALSE(acquired.attempt.snapshot().publishable);
    EXPECT_EQ(acquired.attempt.snapshot().valid_bytes, 8u);
}

TEST(SsdDestinationRegistry,
     RetirementCallbackCanReenterWhileAddressRemainsReserved) {
    DestinationRegistry registry(requester, "receiver");
    bool callback_ran = false;
    auto backing = std::shared_ptr<void>(new int(1), [&](void* pointer) {
        callback_ran = true;
        EXPECT_EQ(registry.stats().regions, 1u);
        EXPECT_EQ(registry.Register(Binding(), {21, 22}, Lifetime()).error,
                  DestinationError::kOverlap);
        delete static_cast<int*>(pointer);
    });
    auto region = registry.Register(Binding(), credential, std::move(backing));
    ASSERT_EQ(region.error, DestinationError::kOk);
    ASSERT_EQ(registry.BeginRevoke(region.region), DestinationError::kOk);
    ASSERT_EQ(registry.Retire(region.region), DestinationError::kOk);
    EXPECT_TRUE(callback_ran);
    EXPECT_EQ(registry.Register(Binding(), {21, 22}, Lifetime()).error,
              DestinationError::kOk);
}

TEST(SsdDestinationRegistry,
     BoundsOwnerSessionsAndForgetsOnlyFullyDrainedLogicalRequests) {
    DestinationRegistryLimits limits;
    limits.max_owner_sessions = 1;
    DestinationRegistry registry(requester, "receiver", limits);
    auto region = registry.Register(Binding(), credential, Lifetime());
    ASSERT_EQ(region.error, DestinationError::kOk);
    auto first = registry.Acquire(Key(), {{region.region, 0, 8}});
    auto second = registry.Acquire(Key(1, 2), {{region.region, 8, 8}});
    ASSERT_EQ(first.error, DestinationError::kOk);
    ASSERT_EQ(second.error, DestinationError::kOk);
    auto another_owner = Key(2);
    another_owner.owner.boot_epoch++;
    EXPECT_EQ(registry.Acquire(another_owner, {{region.region, 16, 8}}).error,
              DestinationError::kLimit);
    std::vector<ResolvedDestination> views;
    ASSERT_EQ(first.attempt.Publish(views), DestinationError::kOk);
    ASSERT_EQ(second.attempt.Publish(views), DestinationError::kOk);
    ASSERT_EQ(first.attempt.Complete(Ack(1)), DestinationError::kOk);
    ASSERT_EQ(second.attempt.Timeout(), DestinationError::kOk);
    EXPECT_EQ(registry.ForgetRequest(owner, 1), DestinationError::kBusy);
    auto drained = Ack(1, 0, AttemptOutcome::kCancelled);
    drained.attempt.attempt_id = 2;
    ASSERT_EQ(second.attempt.Complete(drained), DestinationError::kOk);
    EXPECT_EQ(registry.ForgetRequest(owner, 1), DestinationError::kOk);
    EXPECT_EQ(registry.stats().tracked_attempts, 0u);
    EXPECT_EQ(registry.Acquire(Key(1, 3), {{region.region, 0, 8}}).error,
              DestinationError::kStaleRequest);
}

}  // namespace
}  // namespace mooncake::ssd_offload
