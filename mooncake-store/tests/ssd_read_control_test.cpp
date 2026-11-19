// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_read_control.h"
#include <array>
#include <thread>
#include <gtest/gtest.h>
namespace mooncake::ssd_offload {
namespace {
constexpr PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
class HostTransport final : public DestinationRegistrationTransport {
   public:
    TransportRegistrationResult RegisterWritable(
        const DestinationBinding&, const std::string&) noexcept override {
        return {DestinationError::kOk, true, true};
    }
    DestinationError Unregister(uint64_t) noexcept override {
        ++removed;
        return DestinationError::kOk;
    }
    size_t removed{0};
};
struct Fixture {
    std::shared_ptr<HostTransport> transport =
        std::make_shared<HostTransport>();
    std::shared_ptr<DestinationRegistration> registration =
        std::make_shared<DestinationRegistration>(requester, "requester-te",
                                                  transport);
    std::shared_ptr<std::array<std::byte, 4096>> buffer =
        std::make_shared<std::array<std::byte, 4096>>();
    DestinationRegion region;
    Fixture() {
        const auto base = reinterpret_cast<uint64_t>(buffer->data());
        auto added = registration->Register(
            {base, buffer->size(), base, buffer->size()}, "cpu:0", buffer);
        EXPECT_EQ(added.error, DestinationError::kOk);
        region = added.region;
    }
    SsdReadRequestV1 Request(uint64_t id = 1, uint64_t offset = 0) const {
        return {1,
                requester,
                {owner, id, 1},
                "default",
                100,
                0,
                {{"key", 4096, std::nullopt, {{123, {{region, offset, 32}}}}}}};
    }
};
SsdReadResultV1 Result(const SsdReadRequestV1& request) {
    return {1,
            request.attempt,
            true,
            {{0, AttemptOutcome::kSucceeded, 32, std::nullopt}},
            requester};
}
TEST(SsdRequesterGrants,
     ResolvesOnlyAuthoritativeViewsForExactRequestAndCredential) {
    Fixture f;
    SsdRequesterGrants grants(requester, f.registration);
    auto request = f.Request();
    auto prepared = grants.Prepare(request);
    ASSERT_EQ(prepared.error, DestinationError::kOk);
    EXPECT_NE(prepared.reference.credential, WriteCredential{});
    auto wrong = prepared.reference;
    ++wrong.credential.high;
    EXPECT_EQ(grants.Resolve(wrong, request).error,
              DestinationError::kUnauthorized);
    auto changed = request;
    changed.objects[0].key = "another-key";
    EXPECT_EQ(grants.Resolve(prepared.reference, changed).error,
              DestinationError::kUnauthorized);
    auto resolved = grants.Resolve(prepared.reference, request);
    ASSERT_EQ(resolved.error, DestinationError::kOk);
    ASSERT_EQ(resolved.destinations.size(), 1);
    EXPECT_EQ(resolved.destinations[0].address,
              reinterpret_cast<uint64_t>(f.buffer->data()));
    EXPECT_EQ(resolved.destinations[0].transport_endpoint, "requester-te");
    EXPECT_EQ(grants.Resolve(prepared.reference, request).error,
              DestinationError::kOk);
    EXPECT_EQ(grants.Complete(prepared.reference, Result(request)),
              DestinationError::kOk);
    EXPECT_EQ(grants.Complete(prepared.reference, Result(request)),
              DestinationError::kDuplicate);
    EXPECT_EQ(grants.Resolve(prepared.reference, request).error,
              DestinationError::kClosing);
}
TEST(SsdRequesterGrants, LostReplyQuarantinesUntilMatchingTerminalRecovery) {
    Fixture f;
    SsdRequesterGrants grants(requester, f.registration);
    auto request = f.Request();
    auto grant = grants.Prepare(request);
    ASSERT_EQ(grant.error, DestinationError::kOk);
    ASSERT_EQ(grants.Resolve(grant.reference, request).error,
              DestinationError::kOk);
    ASSERT_EQ(grants.Timeout(grant.reference), DestinationError::kOk);
    EXPECT_EQ(f.registration->Revoke(f.region), DestinationError::kBusy);
    EXPECT_EQ(grants.Forget(owner, 1), DestinationError::kBusy);
    auto result = Result(request);
    result.dma_drained = false;
    EXPECT_EQ(grants.Complete(grant.reference, result),
              DestinationError::kNotDrained);
    result.dma_drained = true;
    ++result.requester.boot_epoch;
    EXPECT_EQ(grants.Complete(grant.reference, result),
              DestinationError::kWrongAttempt);
    result.requester = requester;
    EXPECT_EQ(grants.Complete(grant.reference, result), DestinationError::kOk);
    EXPECT_FALSE(grants.Inspect(grant.reference).publishable);
    EXPECT_EQ(f.registration->Revoke(f.region), DestinationError::kOk);
    EXPECT_EQ(grants.Forget(owner, 1), DestinationError::kOk);
    EXPECT_EQ(grants.stats().records, 0);
    EXPECT_EQ(grants.stats().metadata_bytes, 0);
}
TEST(SsdRequesterGrants, UnresolvedTimeoutRevokesGrantWithoutRemoteExposure) {
    Fixture f;
    SsdRequesterGrants grants(requester, f.registration);
    auto request = f.Request();
    auto grant = grants.Prepare(request);
    ASSERT_EQ(grant.error, DestinationError::kOk);
    EXPECT_EQ(grants.Timeout(grant.reference), DestinationError::kOk);
    EXPECT_EQ(grants.Resolve(grant.reference, request).error,
              DestinationError::kClosing);
    EXPECT_EQ(f.registration->Revoke(f.region), DestinationError::kOk);
    EXPECT_EQ(grants.Forget(owner, 1), DestinationError::kOk);
}
TEST(SsdRequesterGrants, RecoveryAfterUnresolvedTimeoutCachesTheFirstTerminal) {
    Fixture f;
    SsdRequesterGrants grants(requester, f.registration);
    auto request = f.Request();
    auto grant = grants.Prepare(request);
    ASSERT_EQ(grant.error, DestinationError::kOk);
    ASSERT_EQ(grants.Timeout(grant.reference), DestinationError::kOk);
    SsdReadResultV1 result{1,
                           request.attempt,
                           true,
                           {{0, AttemptOutcome::kRejected, 0, {}}},
                           requester};
    EXPECT_EQ(grants.Complete(grant.reference, result), DestinationError::kOk);
    EXPECT_EQ(grants.Complete(grant.reference, result),
              DestinationError::kDuplicate);
    result.objects[0].outcome = AttemptOutcome::kCancelled;
    EXPECT_EQ(grants.Complete(grant.reference, result),
              DestinationError::kWrongAttempt);
    EXPECT_FALSE(grants.Inspect(grant.reference).publishable);
}
TEST(SsdRequesterGrants, BoundsRetainedMetadataAndDeniesWorkAfterClose) {
    Fixture f;
    SsdControlLimits limits;
    limits.max_records = 1;
    SsdRequesterGrants grants(requester, f.registration, limits);
    auto grant = grants.Prepare(f.Request());
    ASSERT_EQ(grant.error, DestinationError::kOk);
    EXPECT_EQ(grants.Prepare(f.Request(2, 32)).error, DestinationError::kLimit);
    EXPECT_GT(grants.stats().metadata_bytes, 0);
    grants.Close();
    EXPECT_EQ(grants.Prepare(f.Request(2, 32)).error,
              DestinationError::kClosing);
    EXPECT_EQ(grants.Resolve(grant.reference, f.Request()).error,
              DestinationError::kClosing);
    EXPECT_EQ(f.registration->Revoke(f.region), DestinationError::kOk);
}
SsdGrantReferenceV1 Reference(const SsdReadRequestV1& request) {
    return {request.requester, request.attempt, {17, 18}};
}
TEST(SsdOwnerLedger,
     DuplicateSubmissionCannotStartAnotherWriterAndQueryRecoversResult) {
    Fixture f;
    auto request = f.Request();
    auto grant = Reference(request);
    SsdOwnerRequestLedger ledger(owner);
    auto admitted = ledger.Admit(request, grant);
    ASSERT_EQ(admitted.error, DestinationError::kOk);
    EXPECT_EQ(ledger.Admit(request, grant).error, DestinationError::kDuplicate);
    auto changed = request;
    changed.tenant_id = "another-tenant";
    EXPECT_EQ(ledger.Admit(changed, grant).error,
              DestinationError::kUnauthorized);
    auto wrong = grant;
    ++wrong.credential.low;
    EXPECT_EQ(ledger.Query(wrong).error, DestinationError::kUnauthorized);
    EXPECT_EQ(admitted.operation.Start(), DestinationError::kOk);
    EXPECT_EQ(ledger.Query(grant).state, SsdOwnerRequestState::kRunning);
    EXPECT_FALSE(ledger.Query(grant).result);
    EXPECT_EQ(ledger.Forget(grant), DestinationError::kBusy);
    auto result = Result(request);
    result.dma_drained = false;
    EXPECT_EQ(admitted.operation.Complete(result),
              DestinationError::kNotDrained);
    EXPECT_EQ(ledger.stats().active_payload_bytes, 32);
    result.dma_drained = true;
    EXPECT_EQ(admitted.operation.Complete(result), DestinationError::kOk);
    auto recovered = ledger.Query(grant);
    ASSERT_TRUE(recovered.result);
    EXPECT_EQ(*recovered.result, result);
    EXPECT_EQ(recovered.state, SsdOwnerRequestState::kDrained);
    EXPECT_EQ(ledger.stats().active_payload_bytes, 0);
    EXPECT_EQ(ledger.Admit(request, grant).error, DestinationError::kDuplicate);
    EXPECT_EQ(ledger.Forget(grant), DestinationError::kOk);
    EXPECT_EQ(ledger.stats().metadata_bytes, 0);
    EXPECT_EQ(ledger.Admit(request, grant).error,
              DestinationError::kStaleRequest);
}
TEST(SsdOwnerLedger, AbandonedWorkerHasNoDrainProofAndKeepsAdmissionCredits) {
    Fixture f;
    auto request = f.Request();
    auto grant = Reference(request);
    SsdOwnerRequestLedger ledger(owner);
    {
        auto admitted = ledger.Admit(request, grant);
        ASSERT_EQ(admitted.error, DestinationError::kOk);
        EXPECT_EQ(admitted.operation.Start(), DestinationError::kOk);
    }
    auto state = ledger.Query(grant);
    EXPECT_EQ(state.state, SsdOwnerRequestState::kQuarantined);
    EXPECT_FALSE(state.result);
    EXPECT_EQ(ledger.Forget(grant), DestinationError::kBusy);
    EXPECT_EQ(ledger.stats().active_payload_bytes, 32);
}
TEST(SsdOwnerLedger,
     BoundsAdmissionAndRetainsTerminalUntilExplicitAcknowledgment) {
    Fixture f;
    SsdControlLimits limits;
    limits.max_active_payload_bytes = 32;
    SsdOwnerRequestLedger ledger(owner, limits);
    auto request = f.Request();
    auto admitted = ledger.Admit(request, Reference(request));
    ASSERT_EQ(admitted.error, DestinationError::kOk);
    auto next = f.Request(2, 32);
    EXPECT_EQ(ledger.Admit(next, Reference(next)).error,
              DestinationError::kLimit);
    ASSERT_EQ(admitted.operation.Start(), DestinationError::kOk);
    ASSERT_EQ(admitted.operation.Complete(Result(request)),
              DestinationError::kOk);
    EXPECT_EQ(ledger.stats().records, 1);
    EXPECT_EQ(ledger.Admit(next, Reference(next)).error, DestinationError::kOk);
    ledger.Close();
    auto third = f.Request(3, 64);
    EXPECT_EQ(ledger.Admit(third, Reference(third)).error,
              DestinationError::kClosing);
    EXPECT_TRUE(ledger.Query(Reference(request)).result);
}
TEST(SsdOwnerLedger, FencesOwnerAndRequesterBootIdentities) {
    Fixture f;
    auto request = f.Request();
    auto grant = Reference(request);
    SsdOwnerRequestLedger ledger(owner);
    ++request.attempt.owner.boot_epoch;
    grant.attempt = request.attempt;
    EXPECT_EQ(ledger.Admit(request, grant).error,
              DestinationError::kWrongAttempt);
    request = f.Request();
    grant = Reference(request);
    ++grant.requester.boot_epoch;
    EXPECT_EQ(ledger.Admit(request, grant).error,
              DestinationError::kWrongAttempt);
}
TEST(SsdOwnerLedger, QueueTimeCountsButExpiryCannotCreateDrainProof) {
    Fixture f;
    SsdOwnerRequestLedger ledger(owner);
    auto request = f.Request();
    auto first = ledger.Admit(request, Reference(request));
    ASSERT_EQ(first.error, DestinationError::kOk);
    EXPECT_GT(first.operation.RemainingTimeout().count(), 0);
    request.attempt.request_id = 2;
    request.relative_timeout_ms = 1;
    auto admitted = ledger.Admit(request, Reference(request));
    ASSERT_EQ(admitted.error, DestinationError::kOk);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    EXPECT_EQ(admitted.operation.Start(), DestinationError::kClosing);
    EXPECT_EQ(admitted.operation.RemainingTimeout().count(), 0);
    EXPECT_FALSE(ledger.Query(Reference(request)).result);
    EXPECT_EQ(ledger.stats().active_payload_bytes, 64);
    SsdReadResultV1 cancelled{1,
                              request.attempt,
                              true,
                              {{0, AttemptOutcome::kCancelled, 0, {}}},
                              requester};
    EXPECT_EQ(admitted.operation.Complete(cancelled), DestinationError::kOk);
    EXPECT_EQ(ledger.stats().active_payload_bytes, 32);
}
TEST(SsdOwnerLedger, LocalArrivalTimestampIncludesDecodeAndAdmissionTime) {
    Fixture f;
    SsdOwnerRequestLedger ledger(owner);
    auto request = f.Request();
    auto admitted = ledger.Admit(
        request, Reference(request),
        std::chrono::steady_clock::now() - std::chrono::seconds(1));
    ASSERT_EQ(admitted.error, DestinationError::kOk);
    EXPECT_EQ(admitted.operation.Start(), DestinationError::kClosing);
    EXPECT_EQ(admitted.operation.RemainingTimeout().count(), 0);
    EXPECT_FALSE(ledger.Query(Reference(request)).result);
}
TEST(SsdOwnerLedger, RejectsNegativeArrivalAnchorsBeforeAdmission) {
    Fixture f;
    SsdOwnerRequestLedger ledger(owner);
    auto request = f.Request();
    using Clock = std::chrono::steady_clock;
    EXPECT_EQ(ledger
                  .Admit(request, Reference(request),
                         Clock::time_point(Clock::duration(-1)))
                  .error,
              DestinationError::kInvalidArgument);
    request.attempt.request_id = 2;
    EXPECT_EQ(
        ledger.Admit(request, Reference(request), Clock::time_point::min())
            .error,
        DestinationError::kInvalidArgument);
    EXPECT_EQ(ledger.stats().records, 0);
}
}  // namespace
}  // namespace mooncake::ssd_offload

namespace mooncake::ssd_offload {
namespace {
TEST(SsdDirectLedger,
     ReclaimsOnlyTerminalCacheWithoutForgetAndNeverReexecutesOldRequest) {
    const PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
    SsdControlLimits limits;
    limits.max_records = 1;
    SsdOwnerRequestLedger ledger(owner, limits, {}, true);
    auto request = SsdReadRequestV1{
        1,
        requester,
        {owner, 1, 1},
        "default",
        1000,
        0,
        {{"key", 32, {}, {{0, {{{requester, 1, 1, {9, 10}}, 0, 32}}}}}}};
    SsdGrantReferenceV1 reference{requester, request.attempt, {11, 12}};
    auto first = ledger.Admit(request, reference,
                              std::chrono::steady_clock::now(), "view-a");
    ASSERT_EQ(first.error, DestinationError::kOk);
    ASSERT_EQ(first.operation.Start(), DestinationError::kOk);
    auto conflicting = ledger.Admit(request, reference,
                                    std::chrono::steady_clock::now(), "view-b");
    EXPECT_EQ(conflicting.error, DestinationError::kUnauthorized);
    auto next = request;
    next.attempt.request_id = 2;
    auto next_ref = reference;
    next_ref.attempt = next.attempt;
    EXPECT_EQ(ledger.Admit(next, next_ref).error, DestinationError::kLimit);
    ASSERT_EQ(
        first.operation.Complete({1,
                                  request.attempt,
                                  true,
                                  {{0, AttemptOutcome::kSucceeded, 32, {}}},
                                  requester}),
        DestinationError::kOk);
    auto second = ledger.Admit(next, next_ref);
    ASSERT_EQ(second.error, DestinationError::kOk);
    EXPECT_EQ(ledger.stats().records, 1u);
    EXPECT_EQ(ledger.Admit(request, reference).error,
              DestinationError::kStaleRequest);
    auto old = ledger.Recover(request, reference, "view-a");
    ASSERT_TRUE(old.result);
    EXPECT_TRUE(old.result->dma_drained);
    EXPECT_EQ(old.result->objects[0].outcome, AttemptOutcome::kCancelled);
    EXPECT_EQ(
        second.operation.Complete({1,
                                   next.attempt,
                                   true,
                                   {{0, AttemptOutcome::kRejected, 0, {}}},
                                   requester}),
        DestinationError::kOk);
}
TEST(SsdDirectLedger, RecoveryOfUnknownRequestFencesDelayedSubmit) {
    const PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
    SsdOwnerRequestLedger ledger(owner, {}, {}, true);
    SsdReadRequestV1 request{
        1,
        requester,
        {owner, 5, 1},
        "default",
        1000,
        0,
        {{"key", 32, {}, {{0, {{{requester, 1, 1, {9, 10}}, 0, 32}}}}}}};
    SsdGrantReferenceV1 reference{requester, request.attempt, {11, 12}};
    auto recovered = ledger.Recover(request, reference, "view");
    ASSERT_EQ(recovered.error, DestinationError::kOk);
    ASSERT_TRUE(recovered.result);
    EXPECT_TRUE(recovered.result->dma_drained);
    EXPECT_EQ(recovered.result->objects[0].valid_bytes, 0u);
    EXPECT_EQ(ledger.Admit(request, reference).error,
              DestinationError::kStaleRequest);
}
}  // namespace
}  // namespace mooncake::ssd_offload
