// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_owner_read_worker.h"
#include <gtest/gtest.h>
#include <array>
#include <cstring>
#include <stdexcept>

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
        return DestinationError::kOk;
    }
};
class HostTarget final : public SourceDrainedWriteTarget {
   public:
    explicit HostTarget(std::vector<ResolvedDestination> views, bool drained)
        : views(std::move(views)), drained(drained) {}
    Error WriteAndDrain(const ReadChunk& chunk,
                        SlotBuffer slot) noexcept override {
        for (const auto& fragment : chunk.fragments) {
            const auto& d = fragment.destination;
            if (!d.region_id || d.region_id > views.size())
                return Error::kTransferError;
            const auto& view = views[d.region_id - 1];
            if (d.offset > view.length || d.length > view.length - d.offset)
                return Error::kTransferError;
            std::memcpy(reinterpret_cast<void*>(view.address + d.offset),
                        slot.data + fragment.buffer_offset, d.length);
        }
        return Error::kOk;
    }
    bool DestinationsDrained() const noexcept override { return drained; }
    std::vector<ResolvedDestination> views;
    bool drained;
};
struct Fixture {
    std::shared_ptr<DestinationRegistration> registration =
        std::make_shared<DestinationRegistration>(
            requester, "requester-te", std::make_shared<HostTransport>());
    std::shared_ptr<std::array<std::byte, 4096>> buffer =
        std::make_shared<std::array<std::byte, 4096>>();
    SsdRequesterGrants grants{requester, registration};
    SsdOwnerRequestLedger ledger{owner};
    DestinationRegion region;
    SsdReadRequestV1 request;
    SsdGrantReferenceV1 reference;
    size_t resolves{0}, reads{0}, factories{0};
    bool drained{true}, preflight_reject{false}, throw_reader{false};
    std::function<void(SsdGrantResolution&)> change_resolution;
    Fixture() {
        const auto base = reinterpret_cast<uint64_t>(buffer->data());
        auto added = registration->Register(
            {base, buffer->size(), base, buffer->size()}, "cpu:0", buffer);
        EXPECT_EQ(added.error, DestinationError::kOk);
        region = added.region;
        request = {1,
                   requester,
                   {owner, 1, 1},
                   "default",
                   5000,
                   0,
                   {{"key", 4096, {}, {{123, {{region, 0, 32}}}}}}};
        auto prepared = grants.Prepare(request);
        EXPECT_EQ(prepared.error, DestinationError::kOk);
        reference = prepared.reference;
    }
    SsdOwnerRequesterPeer Peer() {
        return {requester, "default", "requester-te",
                [this](const auto& ref, const auto& r, auto budget) {
                    EXPECT_GT(budget.count(), 0);
                    ++resolves;
                    auto result = grants.Resolve(ref, r);
                    if (change_resolution) change_resolution(result);
                    return result;
                }};
    }
    SsdOwnerReadWorker::TargetFactory Factory() {
        return [this](auto views) {
            ++factories;
            return std::make_shared<HostTarget>(std::move(views), drained);
        };
    }
    SsdOwnerReadWorker::Reader Reader() {
        return [this](const auto& r, auto target,
                      auto deadline) -> std::optional<SsdReadResultV1> {
            EXPECT_GT(deadline, std::chrono::steady_clock::now());
            ++reads;
            if (throw_reader) throw std::runtime_error("reader failure");
            if (preflight_reject) return {};
            std::array<std::byte, 32> source;
            source.fill(std::byte{0x42});
            auto plan =
                BuildReadPlan({32, 0, {{0, {{1, 1, 0, 32}}}}}, {4096, 1, 1});
            EXPECT_EQ(target->WriteAndDrain(plan.plan.chunks[0],
                                            {source.data(), source.size()}),
                      Error::kOk);
            // Intentionally true even for an uncertain target. Worker must
            // independently require the target's physical destination proof.
            return SsdReadResultV1{1,
                                   r.attempt,
                                   true,
                                   {{0, AttemptOutcome::kSucceeded, 32, {}}},
                                   requester};
        };
    }
    DestinationError Run(std::vector<SsdOwnerRequesterPeer> peers) {
        SsdOwnerReadWorker worker(owner, std::move(peers), Factory(), Reader());
        auto admitted = ledger.Admit(request, reference);
        EXPECT_EQ(admitted.error, DestinationError::kOk);
        return worker.Run(request, reference, std::move(admitted.operation));
    }
    void DrainSynchronousTestWriter() {
        // HostTarget only performs synchronous memcpy, and Reader has returned
        // or thrown before this test-only recovery. No writer can remain. Do
        // not leave an intentionally quarantined MR to the adapter destructor:
        // its fail-safe retention would correctly fail Linux LeakSanitizer.
        SsdReadResultV1 result{1,
                               request.attempt,
                               true,
                               {{0, AttemptOutcome::kCancelled, 0, {}}},
                               requester};
        EXPECT_EQ(grants.Complete(reference, result), DestinationError::kOk);
        EXPECT_FALSE(grants.Inspect(reference).publishable);
        EXPECT_EQ(grants.Forget(owner, request.attempt.request_id),
                  DestinationError::kOk);
        EXPECT_EQ(registration->Revoke(region), DestinationError::kOk);
        EXPECT_EQ(registration->pinned_regions(), 0u);
    }
};
TEST(SsdOwnerWorker,
     ConnectsAuthoritativeGrantAndDrainedReaderToTerminalLedger) {
    Fixture f;
    EXPECT_EQ(f.Run({f.Peer()}), DestinationError::kOk);
    EXPECT_EQ(f.resolves, 1u);
    EXPECT_EQ(f.reads, 1u);
    auto snapshot = f.ledger.Query(f.reference);
    EXPECT_EQ(snapshot.state, SsdOwnerRequestState::kDrained);
    ASSERT_TRUE(snapshot.result);
    EXPECT_EQ(snapshot.result->objects[0].outcome, AttemptOutcome::kSucceeded);
    EXPECT_EQ((*f.buffer)[0], std::byte{0x42});
    EXPECT_EQ(f.grants.Complete(f.reference, *snapshot.result),
              DestinationError::kOk);
    EXPECT_TRUE(f.grants.Inspect(f.reference).publishable);
    EXPECT_EQ(f.registration->Revoke(f.region), DestinationError::kOk);
}
TEST(SsdOwnerWorker, RejectsUntrustedEpochOrTenantBeforeResolve) {
    for (bool wrong_epoch : {false, true}) {
        Fixture f;
        auto peer = f.Peer();
        if (wrong_epoch)
            ++peer.requester.boot_epoch;
        else
            peer.tenant_id = "other-tenant";
        EXPECT_EQ(f.Run({std::move(peer)}), DestinationError::kOk);
        EXPECT_EQ(f.resolves, 0u);
        EXPECT_EQ(f.factories, 0u);
        EXPECT_EQ(f.reads, 0u);
        auto snapshot = f.ledger.Query(f.reference);
        ASSERT_TRUE(snapshot.result);
        EXPECT_EQ(snapshot.result->objects[0].outcome,
                  AttemptOutcome::kRejected);
        EXPECT_EQ(f.grants.Complete(f.reference, *snapshot.result),
                  DestinationError::kOk);
    }
}
TEST(SsdOwnerWorker, RejectsMalformedResolvedViewsBeforeTargetCreation) {
    for (size_t variant = 0; variant < 5; ++variant) {
        Fixture f;
        f.change_resolution = [variant](auto& resolved) {
            if (variant == 0)
                resolved.destinations[0].transport_endpoint = "arbitrary-route";
            if (variant == 1) ++resolved.destinations[0].length;
            if (variant == 2)
                resolved.destinations[0].memory = DestinationMemory::kDevice;
            if (variant == 3) resolved.destinations[0].address = UINT64_MAX;
            if (variant == 4) resolved.destinations.clear();
        };
        EXPECT_EQ(f.Run({f.Peer()}), DestinationError::kOk) << variant;
        EXPECT_EQ(f.resolves, 1u);
        EXPECT_EQ(f.factories, 0u);
        EXPECT_EQ(f.reads, 0u);
        auto snapshot = f.ledger.Query(f.reference);
        ASSERT_TRUE(snapshot.result);
        EXPECT_TRUE(snapshot.result->dma_drained);
        EXPECT_EQ(snapshot.result->objects[0].outcome,
                  AttemptOutcome::kRejected);
        EXPECT_EQ(f.grants.Complete(f.reference, *snapshot.result),
                  DestinationError::kOk);
        EXPECT_EQ(f.registration->Revoke(f.region), DestinationError::kOk);
    }
}
TEST(SsdOwnerWorker, PreflightFailureReturnsZeroByteRejection) {
    Fixture f;
    f.preflight_reject = true;
    EXPECT_EQ(f.Run({f.Peer()}), DestinationError::kOk);
    EXPECT_EQ(f.reads, 1u);
    auto snapshot = f.ledger.Query(f.reference);
    ASSERT_TRUE(snapshot.result);
    EXPECT_EQ(snapshot.result->objects[0].outcome, AttemptOutcome::kRejected);
    EXPECT_EQ(snapshot.result->objects[0].valid_bytes, 0u);
    EXPECT_EQ(f.grants.Complete(f.reference, *snapshot.result),
              DestinationError::kOk);
}
TEST(SsdOwnerWorker, UncertainDestinationCannotBePublishedOrRetired) {
    Fixture f;
    f.drained = false;
    EXPECT_EQ(f.Run({f.Peer()}), DestinationError::kNotDrained);
    EXPECT_EQ(f.reads, 1u);
    auto snapshot = f.ledger.Query(f.reference);
    EXPECT_EQ(snapshot.state, SsdOwnerRequestState::kQuarantined);
    EXPECT_FALSE(snapshot.result);
    EXPECT_EQ(f.grants.Timeout(f.reference), DestinationError::kOk);
    EXPECT_EQ(f.registration->Revoke(f.region), DestinationError::kBusy);
    EXPECT_FALSE(f.grants.Inspect(f.reference).publishable);
    f.DrainSynchronousTestWriter();
}
TEST(SsdOwnerWorker, ReaderExceptionLeavesExposedGrantQuarantined) {
    Fixture f;
    f.throw_reader = true;
    EXPECT_THROW(f.Run({f.Peer()}), std::runtime_error);
    EXPECT_EQ(f.reads, 1u);
    EXPECT_EQ(f.ledger.Query(f.reference).state,
              SsdOwnerRequestState::kQuarantined);
    EXPECT_FALSE(f.ledger.Query(f.reference).result);
    EXPECT_EQ(f.grants.Timeout(f.reference), DestinationError::kOk);
    EXPECT_EQ(f.registration->Revoke(f.region), DestinationError::kBusy);
    f.DrainSynchronousTestWriter();
}
TEST(SsdOwnerWorker, ExpiredAdmissionStartsNoResolveOrReader) {
    Fixture f;
    auto admitted = f.ledger.Admit(
        f.request, f.reference,
        std::chrono::steady_clock::now() - std::chrono::seconds(6));
    ASSERT_EQ(admitted.error, DestinationError::kOk);
    SsdOwnerReadWorker worker(owner, {f.Peer()}, f.Factory(), f.Reader());
    EXPECT_EQ(worker.Run(f.request, f.reference, std::move(admitted.operation)),
              DestinationError::kOk);
    EXPECT_EQ(f.resolves, 0u);
    EXPECT_EQ(f.reads, 0u);
    auto snapshot = f.ledger.Query(f.reference);
    ASSERT_TRUE(snapshot.result);
    EXPECT_EQ(snapshot.result->objects[0].outcome, AttemptOutcome::kCancelled);
    EXPECT_EQ(f.grants.Complete(f.reference, *snapshot.result),
              DestinationError::kOk);
}
TEST(SsdOwnerWorker, RequiresBoundedDistinctTrustedPeersAndCallbacks) {
    Fixture f;
    EXPECT_THROW(SsdOwnerReadWorker(owner, {f.Peer(), f.Peer()}, f.Factory(),
                                    f.Reader()),
                 std::invalid_argument);
    EXPECT_THROW(SsdOwnerReadWorker(
                     owner, std::vector<SsdOwnerRequesterPeer>(65, f.Peer()),
                     f.Factory(), f.Reader()),
                 std::invalid_argument);
    EXPECT_THROW(SsdOwnerReadWorker(owner, {f.Peer()}, {}, f.Reader()),
                 std::invalid_argument);
    EXPECT_THROW(SsdOwnerReadWorker(owner, {f.Peer()}, f.Factory(), {}),
                 std::invalid_argument);
}
}  // namespace
}  // namespace mooncake::ssd_offload
