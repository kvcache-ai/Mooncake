// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_direct_read.h"
#include <gtest/gtest.h>
#include <array>
#include <cstring>
#include <future>
#include <atomic>
#include <thread>
#include <mutex>
#include <set>

namespace mooncake::ssd_offload {
namespace {
constexpr PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
class Target final : public SourceDrainedWriteTarget {
   public:
    explicit Target(std::vector<ResolvedDestination> v) : views(std::move(v)) {}
    Error WriteAndDrain(const ReadChunk& chunk,
                        SlotBuffer slot) noexcept override {
        for (const auto& f : chunk.fragments) {
            const auto& view = views[f.destination.region_id - 1];
            std::memcpy(
                reinterpret_cast<void*>(view.address + f.destination.offset),
                slot.data + f.buffer_offset, size_t(f.destination.length));
        }
        return Error::kOk;
    }
    bool DestinationsDrained() const noexcept override { return drained; }
    bool drained{true};
    std::vector<ResolvedDestination> views;
};
class SsdDirectReadTest : public ::testing::Test {
   protected:
    std::array<std::byte, 64> destination{};
    std::unique_ptr<SsdDirectReadOwner> service;
    std::shared_ptr<Target> target;
    size_t reads{0};
    std::promise<void> entered, resume;
    std::shared_future<void> resumed = resume.get_future().share();
    bool block{false}, uncertain{false};
    void Create(size_t records = 2, bool known = true,
                SsdDirectReadOwner::TrustedPeerResolver resolver = {}) {
        SsdControlLimits limits;
        limits.max_records = records;
        service = std::make_unique<SsdDirectReadOwner>(
            owner,
            known ? std::vector<SsdFixedL2Peer>{{requester,
                                                 "default",
                                                 "requester-te",
                                                 {7, 8}}}
                  : std::vector<SsdFixedL2Peer>{},
            [this](auto views) {
                target = std::make_shared<Target>(std::move(views));
                target->drained = !uncertain;
                return target;
            },
            [this](const auto& request, auto sink,
                   auto) -> std::optional<SsdReadResultV1> {
                ++reads;
                if (block) {
                    entered.set_value();
                    resumed.wait();
                }
                std::array<std::byte, 32> source;
                source.fill(std::byte{42});
                auto chunk =
                    BuildReadPlan({32, 0, {{0, {{1, 1, 0, 32}}}}}, {32, 1, 2})
                        .plan.chunks[0];
                EXPECT_EQ(
                    sink->WriteAndDrain(chunk, {source.data(), source.size()}),
                    Error::kOk);
                return SsdReadResultV1{
                    1,
                    request.attempt,
                    sink->DestinationsDrained(),
                    {{0, AttemptOutcome::kSucceeded, 32, {}}},
                    requester};
            },
            limits, SsdReadProtocolLimits{}, std::move(resolver));
    }
    SsdControlMessage Message(uint64_t id = 1) {
        SsdReadRequestV1 request{
            1,
            requester,
            {owner, id, 1},
            "default",
            1000,
            0,
            {{"key", 32, {}, {{0, {{{requester, 1, 1, {9, 10}}, 0, 32}}}}}}};
        return {
            SsdControlCommand::kReadInto,
            {requester, request.attempt, {7, 8}},
            request,
            {{"requester-te", reinterpret_cast<uint64_t>(destination.data()),
              32, DestinationMemory::kHost}}};
    }
};
TEST_F(SsdDirectReadTest,
       InlineReadReturnsTerminalAndDuplicateNeverWritesAgain) {
    Create();
    auto message = Message();
    auto reply = service->Execute(message);
    ASSERT_EQ(reply.error, DestinationError::kOk);
    ASSERT_TRUE(reply.snapshot);
    ASSERT_TRUE(reply.snapshot->result);
    EXPECT_EQ(reply.snapshot->state, SsdOwnerRequestState::kDrained);
    EXPECT_TRUE(reply.snapshot->result->dma_drained);
    EXPECT_EQ(destination[0], std::byte{42});
    auto duplicate = service->Execute(message);
    ASSERT_TRUE(duplicate.snapshot);
    EXPECT_EQ(duplicate.snapshot->result, reply.snapshot->result);
    EXPECT_EQ(reads, 1u);
    message.destinations[0].address++;
    EXPECT_EQ(service->Execute(message).error, DestinationError::kUnauthorized);
}
TEST_F(SsdDirectReadTest, DoesNotReturnAdmissionAckWhileWriteIsBlocked) {
    Create();
    block = true;
    auto waiting = entered.get_future();
    auto result = std::async(std::launch::async,
                             [&] { return service->Execute(Message()); });
    const auto started = waiting.wait_for(std::chrono::seconds(1));
    EXPECT_EQ(started, std::future_status::ready);
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(20)),
              std::future_status::timeout);
    resume.set_value();
    auto reply = result.get();
    ASSERT_TRUE(reply.snapshot);
    ASSERT_TRUE(reply.snapshot->result);
    EXPECT_TRUE(reply.snapshot->result->dma_drained);
}
TEST_F(SsdDirectReadTest,
       ContinuousReadsDoNotRequireForgetOrGrowTerminalCache) {
    Create();
    for (uint64_t id = 1; id <= 10; ++id) {
        auto reply = service->Execute(Message(id));
        ASSERT_EQ(reply.error, DestinationError::kOk);
        ASSERT_TRUE(reply.snapshot->result);
        EXPECT_LE(service->stats().records, 2u);
    }
    EXPECT_EQ(reads, 10u);
    auto old = service->Execute(Message(1));
    EXPECT_EQ(reads, 10u);
    ASSERT_TRUE(old.snapshot->result);
    EXPECT_EQ(old.snapshot->result->objects[0].outcome,
              AttemptOutcome::kCancelled);
}
TEST_F(SsdDirectReadTest, AuthTenantAndEndpointRejectBeforeWriterAdmission) {
    Create();
    auto message = Message();
    message.reference.credential.high++;
    EXPECT_EQ(service->Execute(message).error, DestinationError::kUnauthorized);
    message = Message();
    message.request->tenant_id = "foreign";
    EXPECT_EQ(service->Execute(message).error, DestinationError::kUnauthorized);
    message = Message();
    message.destinations[0].transport_endpoint = "arbitrary-peer";
    EXPECT_EQ(service->Execute(message).error, DestinationError::kUnauthorized);
    EXPECT_EQ(reads, 0u);
}
TEST_F(SsdDirectReadTest, TrustedResolverBootstrapsPreviouslyUnknownRequester) {
    size_t lookups = 0;
    Create(2, false, [&](PeerIdentity identity) {
        ++lookups;
        EXPECT_EQ(identity, requester);
        return std::optional<SsdFixedL2Peer>{
            {requester, "default", "requester-te", {7, 8}}};
    });
    auto reply = service->Execute(Message());
    ASSERT_EQ(reply.error, DestinationError::kOk);
    ASSERT_TRUE(reply.snapshot->result);
    EXPECT_TRUE(reply.snapshot->result->dma_drained);
    EXPECT_EQ(destination[0], std::byte{42});
    EXPECT_EQ(lookups, 1u);
}
TEST_F(SsdDirectReadTest, TrustedResolverCannotAuthorizeInlineChosenEndpoint) {
    Create(2, false, [](PeerIdentity) {
        return std::optional<SsdFixedL2Peer>{
            {requester, "default", "requester-te", {7, 8}}};
    });
    auto message = Message();
    message.destinations[0].transport_endpoint = "arbitrary-peer";
    EXPECT_EQ(service->Execute(message).error, DestinationError::kUnauthorized);
    message = Message();
    message.reference.credential.high++;
    EXPECT_EQ(service->Execute(message).error, DestinationError::kUnauthorized);
    EXPECT_EQ(reads, 0u);
}
TEST_F(SsdDirectReadTest, ExplicitPeerTakesPriorityOverDynamicResolver) {
    size_t lookups = 0;
    Create(2, true, [&](PeerIdentity) -> std::optional<SsdFixedL2Peer> {
        ++lookups;
        return {};
    });
    EXPECT_EQ(service->Execute(Message()).error, DestinationError::kOk);
    EXPECT_EQ(lookups, 0u);
}
TEST_F(SsdDirectReadTest, AuthenticatedAttemptRecoveryRetainsImmutablePeer) {
    bool available = true;
    size_t lookups = 0;
    Create(2, false, [&](PeerIdentity) -> std::optional<SsdFixedL2Peer> {
        ++lookups;
        if (!available) return {};
        return SsdFixedL2Peer{requester, "default", "requester-te", {7, 8}};
    });
    ASSERT_EQ(service->Execute(Message()).error, DestinationError::kOk);
    available = false;
    auto message = Message();
    message.command = SsdControlCommand::kRecoverReadInto;
    auto recovered = service->Execute(message);
    ASSERT_EQ(recovered.error, DestinationError::kOk);
    ASSERT_TRUE(recovered.snapshot->result);
    EXPECT_TRUE(recovered.snapshot->result->dma_drained);
    EXPECT_EQ(lookups, 1u);
}
TEST_F(SsdDirectReadTest, UnauthenticatedRequestCannotInstallResolverSnapshot) {
    WriteCredential credential{7, 8};
    Create(2, false, [&](PeerIdentity) -> std::optional<SsdFixedL2Peer> {
        return SsdFixedL2Peer{requester, "default", "requester-te", credential};
    });
    auto message = Message();
    message.reference.credential.high++;
    EXPECT_EQ(service->Execute(message).error, DestinationError::kUnauthorized);
    credential.high++;
    EXPECT_EQ(service->Execute(message).error, DestinationError::kOk);
}
TEST_F(SsdDirectReadTest, FullImportCacheStillUsesTrustedAttemptSnapshot) {
    Create(2, false, [](PeerIdentity) -> std::optional<SsdFixedL2Peer> {
        return SsdFixedL2Peer{requester, "default", "requester-te", {7, 8}};
    });
    for (uint64_t id = 100; id < 164; ++id)
        ASSERT_EQ(
            service->UpsertPeer({{id, 2, 3}, "default", "other-te", {7, 8}}),
            DestinationError::kOk);
    EXPECT_EQ(service->UpsertPeer({{164, 2, 3}, "default", "other-te", {7, 8}}),
              DestinationError::kLimit);
    ASSERT_EQ(service->Execute(Message()).error, DestinationError::kOk);
    auto message = Message();
    message.command = SsdControlCommand::kRecoverReadInto;
    auto recovered = service->Execute(message);
    ASSERT_EQ(recovered.error, DestinationError::kOk);
    ASSERT_TRUE(recovered.snapshot->result);
    EXPECT_TRUE(recovered.snapshot->result->dma_drained);
}
TEST_F(SsdDirectReadTest, IncrementalImportNeverOverwritesAuthenticatedPeer) {
    Create();
    SsdFixedL2Peer same{requester, "default", "requester-te", {7, 8}};
    EXPECT_EQ(service->UpsertPeer(same), DestinationError::kOk);
    same.credential.high++;
    EXPECT_EQ(service->UpsertPeer(same), DestinationError::kUnauthorized);
    EXPECT_EQ(service->Execute(Message()).error, DestinationError::kOk);
}
TEST_F(SsdDirectReadTest, IncrementalImportIsSafeDuringExistingWriter) {
    Create();
    block = true;
    auto waiting = entered.get_future();
    auto result = std::async(std::launch::async,
                             [&] { return service->Execute(Message()); });
    EXPECT_EQ(waiting.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    EXPECT_EQ(
        service->UpsertPeer({{10, 11, 12}, "default", "another-te", {13, 14}}),
        DestinationError::kOk);
    resume.set_value();
    auto reply = result.get();
    ASSERT_TRUE(reply.snapshot->result);
    EXPECT_TRUE(reply.snapshot->result->dma_drained);
}
TEST_F(SsdDirectReadTest, PeerRefreshPreservesLostTerminalReplyRecovery) {
    Create();
    ASSERT_EQ(service->Execute(Message()).error, DestinationError::kOk);
    ASSERT_EQ(service->stats().active_payload_bytes, 0u);
    ASSERT_EQ(
        service->SetPeers({{{10, 11, 12}, "default", "new-peer-te", {13, 14}}}),
        DestinationError::kOk);
    auto message = Message();
    message.command = SsdControlCommand::kRecoverReadInto;
    auto recovered = service->Execute(message);
    ASSERT_EQ(recovered.error, DestinationError::kOk);
    ASSERT_TRUE(recovered.snapshot->result);
    EXPECT_TRUE(recovered.snapshot->result->dma_drained);
    EXPECT_EQ(reads, 1u);
}
TEST_F(SsdDirectReadTest, PeerRefreshCannotRotateExistingIdentityBinding) {
    Create();
    ASSERT_EQ(service->Execute(Message()).error, DestinationError::kOk);
    SsdFixedL2Peer original{requester, "default", "requester-te", {7, 8}};
    auto changed = original;
    changed.credential.high++;
    EXPECT_EQ(service->SetPeers({changed}), DestinationError::kUnauthorized);
    changed = original;
    changed.transport_endpoint = "different-te";
    EXPECT_EQ(service->SetPeers({changed}), DestinationError::kUnauthorized);
    changed = original;
    changed.tenant_id = "foreign";
    EXPECT_EQ(service->SetPeers({changed}), DestinationError::kUnauthorized);
    auto message = Message();
    message.command = SsdControlCommand::kRecoverReadInto;
    auto recovered = service->Execute(message);
    ASSERT_EQ(recovered.error, DestinationError::kOk);
    ASSERT_TRUE(recovered.snapshot->result);
}
TEST_F(SsdDirectReadTest, PeerRefreshConflictDoesNotPartiallyImport) {
    Create();
    SsdFixedL2Peer added{{10, 11, 12}, "default", "another-te", {13, 14}};
    SsdFixedL2Peer conflict{requester, "default", "requester-te", {9, 10}};
    EXPECT_EQ(service->SetPeers({added, conflict}),
              DestinationError::kUnauthorized);
    added.credential.high++;
    EXPECT_EQ(service->UpsertPeer(added), DestinationError::kOk);
    EXPECT_EQ(service->Execute(Message()).error, DestinationError::kOk);
}
TEST_F(SsdDirectReadTest, PeerRefreshCapacityFailurePreservesPreviousBindings) {
    Create();
    std::vector<SsdFixedL2Peer> peers;
    for (uint64_t id = 100; id < 164; ++id)
        peers.push_back({{id, 2, 3}, "default", "other-te", {7, 8}});
    EXPECT_EQ(service->SetPeers(peers), DestinationError::kLimit);
    peers[0].credential.high++;
    EXPECT_EQ(service->UpsertPeer(peers[0]), DestinationError::kOk);
    EXPECT_EQ(service->Execute(Message()).error, DestinationError::kOk);
}
TEST_F(SsdDirectReadTest,
       RecoveryFencesDelayedAdmissionButCannotFabricateUncertainDrain) {
    Create();
    auto message = Message();
    message.command = SsdControlCommand::kRecoverReadInto;
    auto fenced = service->Execute(message);
    ASSERT_TRUE(fenced.snapshot);
    ASSERT_TRUE(fenced.snapshot->result);
    EXPECT_TRUE(fenced.snapshot->result->dma_drained);
    EXPECT_EQ(fenced.snapshot->result->objects[0].outcome,
              AttemptOutcome::kCancelled);
    message.command = SsdControlCommand::kReadInto;
    service->Execute(message);
    EXPECT_EQ(reads, 0u);
    uncertain = true;
    auto failed = service->Execute(Message(2));
    EXPECT_EQ(failed.error, DestinationError::kNotDrained);
    message = Message(2);
    message.command = SsdControlCommand::kRecoverReadInto;
    auto recovery = service->Execute(message);
    ASSERT_TRUE(recovery.snapshot);
    EXPECT_FALSE(recovery.snapshot->result);
}
TEST_F(SsdDirectReadTest,
       OriginalTerminalReplySurvivesConcurrentCacheEviction) {
    SsdControlLimits limits;
    limits.max_records = 1;
    std::mutex mutex;
    std::set<uint64_t> completed;
    SsdDirectReadOwner direct(
        owner, {{requester, "default", "requester-te", {7, 8}}},
        [](auto views) { return std::make_shared<Target>(std::move(views)); },
        [&](const auto& request, auto, auto) -> std::optional<SsdReadResultV1> {
            std::lock_guard lock(mutex);
            completed.insert(request.attempt.request_id);
            return SsdReadResultV1{1,
                                   request.attempt,
                                   true,
                                   {{0, AttemptOutcome::kSucceeded, 32, {}}},
                                   requester};
        },
        limits);
    std::atomic<uint64_t> next{1};
    std::atomic<size_t> lost{0};
    std::vector<std::thread> threads;
    for (size_t i = 0; i < 8; ++i)
        threads.emplace_back([&] {
            for (size_t n = 0; n < 2000; ++n) {
                auto message = Message(next++);
                auto reply = direct.Execute(message);
                std::lock_guard lock(mutex);
                if (completed.contains(message.reference.attempt.request_id) &&
                    (reply.error != DestinationError::kOk || !reply.snapshot ||
                     !reply.snapshot->result ||
                     reply.snapshot->result->objects[0].outcome !=
                         AttemptOutcome::kSucceeded))
                    ++lost;
            }
        });
    for (auto& thread : threads) thread.join();
    EXPECT_GT(completed.size(), 100u);
    EXPECT_EQ(lost, 0u);
}
}  // namespace
}  // namespace mooncake::ssd_offload
