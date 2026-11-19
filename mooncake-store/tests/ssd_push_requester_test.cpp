// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_push_requester.h"
#include "tenant_id.h"
#include <gtest/gtest.h>
#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <thread>
#include <future>

namespace mooncake::ssd_offload {
namespace {
constexpr PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
SsdPushPeerInfo Peer() {
    return {owner,
            "tenant-a",
            "legacy-owner:9000",
            "127.0.0.1",
            9001,
            "owner-te",
            {1, owner, 15, 64, 4096, 64, 4096, 4096, {"tcp"}}};
}
class Transport final : public DestinationRegistrationTransport {
   public:
    size_t registered{0}, unregistered{0};
    DestinationError unregister_error{DestinationError::kOk};
    TransportRegistrationResult register_result{DestinationError::kOk, true,
                                                true};
    TransportRegistrationResult RegisterWritable(
        const DestinationBinding&, const std::string&) noexcept override {
        ++registered;
        return register_result;
    }
    DestinationError Unregister(uint64_t) noexcept override {
        ++unregistered;
        return unregister_error;
    }
};
class SsdPushRequesterTest : public ::testing::Test {
   protected:
    std::shared_ptr<Transport> transport = std::make_shared<Transport>();
    std::unique_ptr<SsdPushRequester> client;
    std::shared_ptr<SsdOwnerRequestLedger> ledger =
        std::make_shared<SsdOwnerRequestLedger>(owner);
    std::optional<SsdOwnerRequestLedger::Operation> operation;
    SsdReadRequestV1 request;
    SsdGrantReferenceV1 reference;
    std::vector<ResolvedDestination> views;
    std::vector<SsdControlCommand> calls;
    bool lost_submit{false}, throw_submit{false}, bad_result{false};
    bool fail_forget{false};
    bool reject_before_resolve{false};
    size_t submit_delay_ms{0};
    bool lose_forget_reply_once{false};
    std::promise<void> submit_entered;
    std::shared_future<void> submit_release;
    void Create(SsdPushRequesterLimits limits = {},
                std::vector<SsdPushPeerInfo> peers = {Peer()}) {
        client = std::make_unique<SsdPushRequester>(
            requester, "tenant-a", "requester-te", transport, std::move(peers),
            [this](const auto& host, auto port, const auto& message, auto) {
                EXPECT_EQ(host, "127.0.0.1");
                EXPECT_EQ(port, 9001);
                calls.push_back(message.command);
                SsdControlReply reply{
                    message.command, DestinationError::kOk, {}, {}};
                if (message.command == SsdControlCommand::kSubmit) {
                    request = *message.request;
                    reference = message.reference;
                    if (reject_before_resolve) {
                        reply.error = DestinationError::kLimit;
                        return SsdRequesterCallResult{DestinationError::kOk,
                                                      std::move(reply)};
                    }
                    auto admitted = ledger->Admit(request, reference);
                    EXPECT_EQ(admitted.error, DestinationError::kOk);
                    operation.emplace(std::move(admitted.operation));
                    EXPECT_EQ(operation->Start(), DestinationError::kOk);
                    auto resolved =
                        client->grants()->Resolve(reference, request);
                    EXPECT_EQ(resolved.error, DestinationError::kOk);
                    views = std::move(resolved.destinations);
                    if (submit_release.valid()) {
                        submit_entered.set_value();
                        submit_release.wait();
                    }
                    if (submit_delay_ms)
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(submit_delay_ms));
                    if (throw_submit) throw std::runtime_error("lost submit");
                    if (lost_submit) return SsdRequesterCallResult{};
                    reply.snapshot = ledger->Query(reference);
                } else if (message.command == SsdControlCommand::kQuery) {
                    reply.snapshot = ledger->Query(message.reference);
                    if (bad_result && reply.snapshot->result)
                        reply.snapshot->result->requester.boot_epoch++;
                } else if (message.command == SsdControlCommand::kForget) {
                    if (fail_forget) return SsdRequesterCallResult{};
                    reply.error = ledger->Forget(message.reference);
                    if (lose_forget_reply_once) {
                        lose_forget_reply_once = false;
                        return SsdRequesterCallResult{};
                    }
                }
                return SsdRequesterCallResult{DestinationError::kOk,
                                              std::move(reply)};
            },
            limits);
    }
    SsdPushStartResult Start(
        std::chrono::milliseconds timeout = std::chrono::seconds(1)) {
        return client->Start("legacy-owner:9000",
                             {{"a", 100, 7, 17}, {"b", 200, 23, 31}}, timeout);
    }
    void Finish(AttemptOutcome outcome = AttemptOutcome::kSucceeded) {
        ASSERT_TRUE(operation);
        SsdReadResultV1 result{1, request.attempt, true, {}, requester};
        size_t index = 0;
        for (const auto& view : views) {
            std::memset(reinterpret_cast<void*>(view.address), int(40 + index),
                        size_t(view.length));
            result.objects.push_back({index++, outcome, view.length, {}});
        }
        ASSERT_EQ(operation->Complete(result), DestinationError::kOk);
        operation.reset();
    }
    void TearDown() override {
        // Drain test-owned work even if a preceding assertion failed.
        if (operation) Finish(AttemptOutcome::kCancelled);
        if (client) {
            auto active = client->active_read("legacy-owner:9000");
            if (active && !active->released()) {
                bad_result = false;
                fail_forget = false;
                transport->unregister_error = DestinationError::kOk;
                active->Poll(std::chrono::seconds(1));
                active->Release(std::chrono::seconds(1));
            }
            client->Close();
        }
    }
};
TEST_F(SsdPushRequesterTest, OwnedBatchPublishesOnlyAfterValidatedDrain) {
    Create();
    auto started = Start();
    ASSERT_EQ(started.error, DestinationError::kOk);
    ASSERT_TRUE(started.may_have_submitted);
    ASSERT_TRUE(started.read);
    EXPECT_FALSE(started.read->data(0));
    EXPECT_EQ(request.objects[0].key, "a");
    EXPECT_EQ(request.objects[0].ranges[0].source_offset, 7u);
    EXPECT_EQ(client->stats().owned_bytes, 48u);
    EXPECT_EQ(client->stats().owned_reads, 1u);
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kNotDrained);
    Finish();
    ASSERT_EQ(started.read->Poll(std::chrono::seconds(1)),
              DestinationError::kOk);
    ASSERT_TRUE(started.read->data(0));
    EXPECT_EQ(started.read->data(0)->size(), 17u);
    EXPECT_EQ((*started.read->data(0))[0], std::byte{40});
    EXPECT_EQ((*started.read->data(1))[30], std::byte{41});
    EXPECT_FALSE(started.read->data(2));
    EXPECT_TRUE(started.read->snapshot().publishable);
    ASSERT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kOk);
    EXPECT_TRUE(started.read->released());
    EXPECT_FALSE(started.read->data(0));
    EXPECT_EQ(client->stats().owned_bytes, 0u);
    EXPECT_EQ(client->stats().pinned_regions, 0u);
}
TEST_F(SsdPushRequesterTest,
       LostSubmitReplyRetainsBackingAndLateAckCannotPublish) {
    Create();
    lost_submit = true;
    auto started = Start();
    ASSERT_TRUE(started.read);
    EXPECT_EQ(started.error, DestinationError::kTransportError);
    EXPECT_TRUE(started.may_have_submitted);
    EXPECT_EQ(started.read->snapshot().state,
              DestinationAttemptState::kQuarantined);
    EXPECT_EQ(client->stats().owned_bytes, 48u);
    started.read.reset();
    auto retained = client->active_read("legacy-owner:9000");
    ASSERT_TRUE(retained);
    auto blocked = Start();
    EXPECT_EQ(blocked.error, DestinationError::kBusy);
    EXPECT_FALSE(blocked.may_have_submitted);
    EXPECT_EQ(
        std::count(calls.begin(), calls.end(), SsdControlCommand::kSubmit), 1);
    Finish();
    ASSERT_EQ(retained->Poll(std::chrono::seconds(1)), DestinationError::kOk);
    EXPECT_FALSE(retained->data(0));
    EXPECT_FALSE(retained->snapshot().publishable);
    EXPECT_EQ(retained->Release(std::chrono::seconds(1)),
              DestinationError::kOk);
}
TEST_F(SsdPushRequesterTest, ThrowingSubmitIsAlsoUncertain) {
    Create();
    throw_submit = true;
    auto started = Start();
    ASSERT_TRUE(started.read);
    EXPECT_TRUE(started.may_have_submitted);
    EXPECT_EQ(started.error, DestinationError::kTransportError);
    EXPECT_EQ(started.read->snapshot().state,
              DestinationAttemptState::kQuarantined);
    EXPECT_EQ(transport->unregistered, 0u);
}
TEST_F(SsdPushRequesterTest, DeadlineAndWrongTerminalNeverExposeBytes) {
    Create();
    auto started = Start();
    ASSERT_TRUE(started.read);
    EXPECT_EQ(started.read->Timeout(), DestinationError::kOk);
    Finish();
    bad_result = true;
    EXPECT_EQ(started.read->Poll(std::chrono::seconds(1)),
              DestinationError::kWrongAttempt);
    EXPECT_FALSE(started.read->data(0));
    EXPECT_EQ(transport->unregistered, 0u);
    bad_result = false;
    ASSERT_EQ(started.read->Poll(std::chrono::seconds(1)),
              DestinationError::kOk);
    EXPECT_FALSE(started.read->data(0));
}
TEST_F(SsdPushRequesterTest, ForgetAndUnregisterFailuresKeepCreditsUntilRetry) {
    Create();
    auto started = Start();
    ASSERT_TRUE(started.read);
    Finish();
    ASSERT_EQ(started.read->Poll(std::chrono::seconds(1)),
              DestinationError::kOk);
    fail_forget = true;
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kTransportError);
    EXPECT_EQ(transport->unregistered, 0u);
    fail_forget = false;
    transport->unregister_error = DestinationError::kTransportError;
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kTransportError);
    EXPECT_EQ(client->stats().owned_bytes, 48u);
    EXPECT_EQ(client->stats().pinned_regions, 1u);
    transport->unregister_error = DestinationError::kOk;
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kOk);
    EXPECT_EQ(client->stats().owned_reads, 0u);
    EXPECT_EQ(
        std::count(calls.begin(), calls.end(), SsdControlCommand::kForget), 2);
    auto next = Start();
    ASSERT_TRUE(next.read);
    EXPECT_GT(next.read->reference().attempt.request_id,
              started.read->reference().attempt.request_id);
}
TEST_F(SsdPushRequesterTest,
       UnknownOwnerAndInvalidRangeRejectBeforeRegistration) {
    Create();
    auto unknown = client->Start("untrusted:9000", {{"a", 100, 0, 10}},
                                 std::chrono::seconds(1));
    EXPECT_EQ(unknown.error, DestinationError::kNotSupported);
    EXPECT_FALSE(unknown.may_have_submitted);
    auto bad = client->Start("legacy-owner:9000", {{"a", 100, 99, 2}},
                             std::chrono::seconds(1));
    EXPECT_EQ(bad.error, DestinationError::kInvalidArgument);
    EXPECT_FALSE(bad.may_have_submitted);
    EXPECT_EQ(transport->registered, 0u);
    EXPECT_TRUE(calls.empty());
}
TEST_F(SsdPushRequesterTest,
       OwnedAndPeerBudgetsRejectBeforeAllocationAndSubmit) {
    SsdPushRequesterLimits limits;
    limits.max_owned_bytes = 47;
    Create(limits);
    EXPECT_EQ(Start().error, DestinationError::kLimit);
    EXPECT_EQ(client->stats().owned_bytes, 0u);
    EXPECT_EQ(transport->registered, 0u);
    auto peer = Peer();
    peer.capability.max_batch_bytes = 47;
    peer.capability.preferred_chunk_bytes = 47;
    Create({}, {peer});
    EXPECT_EQ(Start().error, DestinationError::kLimit);
    EXPECT_EQ(transport->registered, 0u);
}
TEST_F(SsdPushRequesterTest,
       BootstrapRejectsDnsDuplicateIdentityAndTenantMismatch) {
    auto peer = Peer();
    peer.control_host = "owner.example";
    EXPECT_THROW(Create({}, {peer}), std::invalid_argument);
    peer = Peer();
    peer.tenant_id = "tenant-b";
    EXPECT_THROW(Create({}, {peer}), std::invalid_argument);
    peer = Peer();
    peer.capability.owner.boot_epoch++;
    EXPECT_THROW(Create({}, {peer}), std::invalid_argument);
    peer = Peer();
    auto duplicate = peer;
    duplicate.legacy_rpc_endpoint = "another:9000";
    EXPECT_THROW(Create({}, {peer, duplicate}), std::invalid_argument);
}
TEST_F(SsdPushRequesterTest,
       AllProtocolPreflightRunsBeforeTransportRegistration) {
    SsdPushRequesterLimits limits;
    limits.protocol.max_key_bytes = 9;
    Create(limits);
    EXPECT_EQ(Start().error, DestinationError::kLimit);
    EXPECT_EQ(transport->registered, 0u);
    Create();
    auto oversized =
        client->Start("legacy-owner:9000", {{"a", UINT64_MAX, 0, 1}},
                      std::chrono::seconds(1));
    EXPECT_EQ(oversized.error, DestinationError::kInvalidArgument);
    EXPECT_EQ(transport->registered, 0u);
    limits.protocol.max_objects = 0;
    EXPECT_THROW(Create(limits), std::invalid_argument);
}
TEST_F(SsdPushRequesterTest, ExpiredDeadlineAcceptsDrainButNeverPublishes) {
    Create();
    auto started = Start(std::chrono::milliseconds(100));
    ASSERT_TRUE(started.read);
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    Finish();
    ASSERT_EQ(started.read->Poll(std::chrono::seconds(1)),
              DestinationError::kOk);
    EXPECT_FALSE(started.read->snapshot().publishable);
    EXPECT_FALSE(started.read->data(0));
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kOk);
}
TEST_F(SsdPushRequesterTest,
       FailedRegistrationKeepsAllocationCreditsUntilCloseCleanup) {
    SsdPushRequesterLimits limits;
    limits.max_owned_bytes = 48;
    Create(limits);
    transport->register_result = {DestinationError::kTransportError, true,
                                  true};
    auto started = Start();
    EXPECT_EQ(started.error, DestinationError::kTransportError);
    EXPECT_FALSE(started.may_have_submitted);
    EXPECT_FALSE(started.read);
    EXPECT_EQ(client->stats().owned_bytes, 48u);
    EXPECT_EQ(client->stats().pinned_regions, 1u);
    EXPECT_EQ(Start().error, DestinationError::kLimit);
    EXPECT_TRUE(calls.empty());
    EXPECT_EQ(client->Close(), DestinationError::kOk);
    EXPECT_EQ(client->stats().owned_bytes, 0u);
    EXPECT_EQ(transport->unregistered, 1u);
}
TEST_F(SsdPushRequesterTest,
       CloseDeniesResolveAndKeepsAcceptedDestinationPinned) {
    Create();
    auto started = Start();
    ASSERT_TRUE(started.read);
    EXPECT_EQ(client->Close(), DestinationError::kBusy);
    EXPECT_EQ(Start().error, DestinationError::kClosing);
    EXPECT_EQ(client->grants()->Resolve(reference, request).error,
              DestinationError::kClosing);
    EXPECT_EQ(client->stats().owned_bytes, 48u);
    EXPECT_EQ(transport->unregistered, 0u);
    Finish();
    EXPECT_EQ(started.read->Poll(std::chrono::seconds(1)),
              DestinationError::kOk);
    EXPECT_FALSE(started.read->data(0));
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kOk);
    EXPECT_EQ(client->Close(), DestinationError::kOk);
}
TEST_F(SsdPushRequesterTest,
       AdmissionRejectionCanReleaseUnexposedGrantAndRetry) {
    Create();
    reject_before_resolve = true;
    auto started = Start();
    ASSERT_TRUE(started.read);
    EXPECT_TRUE(started.may_have_submitted);
    EXPECT_EQ(started.error, DestinationError::kLimit);
    EXPECT_EQ(started.read->snapshot().state,
              DestinationAttemptState::kDrained);
    EXPECT_EQ(client->grants()->Resolve(reference, request).error,
              DestinationError::kClosing);
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kOk);
    EXPECT_EQ(client->stats().owned_bytes, 0u);
    reject_before_resolve = false;
    auto retry = Start();
    EXPECT_EQ(retry.error, DestinationError::kOk);
    EXPECT_TRUE(retry.read);
}
TEST_F(SsdPushRequesterTest,
       PendingSubmitReplyAfterDeadlineFencesFurtherResolve) {
    Create();
    submit_delay_ms = 150;
    auto started = Start(std::chrono::milliseconds(100));
    ASSERT_TRUE(started.read);
    EXPECT_EQ(client->grants()->Resolve(reference, request).error,
              DestinationError::kClosing);
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kNotDrained);
    EXPECT_EQ(transport->unregistered, 0u);
}
TEST_F(SsdPushRequesterTest,
       CloseWhileSubmitIsBlockedNeverUnregistersExposedMemory) {
    Create();
    std::promise<void> resume;
    submit_release = resume.get_future().share();
    auto entered = submit_entered.get_future();
    auto pending = std::async(std::launch::async, [this] { return Start(); });
    const auto state = entered.wait_for(std::chrono::seconds(2));
    if (state != std::future_status::ready) {
        resume.set_value();
        FAIL() << "Submit did not enter the test barrier";
        return;
    }
    EXPECT_EQ(client->Close(), DestinationError::kBusy);
    EXPECT_EQ(client->stats().owned_bytes, 48u);
    EXPECT_EQ(transport->unregistered, 0u);
    resume.set_value();
    auto started = pending.get();
    ASSERT_TRUE(started.read);
    Finish();
    EXPECT_EQ(started.read->Poll(std::chrono::seconds(1)),
              DestinationError::kOk);
    EXPECT_FALSE(started.read->data(0));
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kOk);
}
TEST_F(SsdPushRequesterTest,
       LostForgetReplyRetriesNotFoundWithoutEarlyLocalRelease) {
    Create();
    auto started = Start();
    ASSERT_TRUE(started.read);
    Finish();
    ASSERT_EQ(started.read->Poll(std::chrono::seconds(1)),
              DestinationError::kOk);
    lose_forget_reply_once = true;
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kTransportError);
    EXPECT_EQ(ledger->stats().records, 0u);
    EXPECT_EQ(client->stats().pinned_regions, 1u);
    EXPECT_EQ(transport->unregistered, 0u);
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kOk);
    EXPECT_EQ(client->stats().owned_bytes, 0u);
}
TEST_F(SsdPushRequesterTest, GlobalOwnedReadCreditsAlsoBoundDifferentOwners) {
    SsdPushRequesterLimits limits;
    limits.max_owned_reads = 1;
    auto other = Peer();
    ++other.owner.boot_epoch;
    other.capability.owner = other.owner;
    other.legacy_rpc_endpoint = "other-owner:9000";
    Create(limits, {Peer(), other});
    auto started = Start();
    ASSERT_TRUE(started.read);
    auto blocked = client->Start(other.legacy_rpc_endpoint, {{"a", 10, 0, 10}},
                                 std::chrono::seconds(1));
    EXPECT_EQ(blocked.error, DestinationError::kLimit);
    EXPECT_FALSE(blocked.may_have_submitted);
    EXPECT_EQ(transport->registered, 1u);
    EXPECT_EQ(client->stats().owned_reads, 1u);
}
TEST_F(SsdPushRequesterTest, DrainedFailedReadNeverPublishesPartialContents) {
    Create();
    auto started = Start();
    ASSERT_TRUE(started.read);
    Finish(AttemptOutcome::kFailed);
    ASSERT_EQ(started.read->Poll(std::chrono::seconds(1)),
              DestinationError::kOk);
    EXPECT_FALSE(started.read->data(0));
    EXPECT_FALSE(started.read->snapshot().publishable);
    EXPECT_EQ(started.read->snapshot().state,
              DestinationAttemptState::kDrained);
    EXPECT_EQ(started.read->Release(std::chrono::seconds(1)),
              DestinationError::kOk);
}
}  // namespace
}  // namespace mooncake::ssd_offload
