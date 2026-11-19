// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
// Isolated allocation fault tests: no hooks in the production requester.
#include "ssd_push_requester.h"
#include "ssd_fixed_l2_requester.h"
#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>
#include <new>
#include <thread>

namespace {
thread_local bool delay_validation = false, validation_delayed = false;
thread_local bool fail_result_copy = false, result_copy_failed = false;
thread_local int fixed_views_before_failure = -1;
thread_local bool fixed_view_failed = false;
}  // namespace
void* operator new(size_t bytes) {
    if (fixed_views_before_failure > 0 &&
        bytes == sizeof(mooncake::ssd_offload::ResolvedDestination) &&
        --fixed_views_before_failure == 0) {
        fixed_views_before_failure = -1;
        fixed_view_failed = true;
        throw std::bad_alloc();
    }
    if (delay_validation && bytes == sizeof(unsigned long)) {
        delay_validation = false;
        validation_delayed = true;
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    }
    if (fail_result_copy &&
        bytes == sizeof(mooncake::ssd_offload::SsdObjectReadResultV1)) {
        fail_result_copy = false;
        result_copy_failed = true;
        throw std::bad_alloc();
    }
    if (void* pointer = std::malloc(bytes ? bytes : 1)) return pointer;
    throw std::bad_alloc();
}
void operator delete(void* pointer) noexcept { std::free(pointer); }
void operator delete(void* pointer, size_t) noexcept { std::free(pointer); }
void* operator new[](size_t bytes) { return ::operator new(bytes); }
void operator delete[](void* pointer) noexcept { std::free(pointer); }
void operator delete[](void* pointer, size_t) noexcept { std::free(pointer); }
void* operator new(size_t bytes, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(bytes);
    } catch (...) {
        return nullptr;
    }
}
void* operator new[](size_t bytes, const std::nothrow_t&) noexcept {
    try {
        return ::operator new(bytes);
    } catch (...) {
        return nullptr;
    }
}
void operator delete(void* pointer, const std::nothrow_t&) noexcept {
    std::free(pointer);
}
void operator delete[](void* pointer, const std::nothrow_t&) noexcept {
    std::free(pointer);
}

namespace mooncake::ssd_offload {
namespace {
constexpr PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
class Transport final : public DestinationRegistrationTransport {
   public:
    TransportRegistrationResult RegisterWritable(
        const DestinationBinding&, const std::string&) noexcept override {
        return {DestinationError::kOk, true, true};
    }
    DestinationError Unregister(uint64_t) noexcept override {
        return DestinationError::kOk;
    }
};
class SsdRequesterAllocationTest : public ::testing::Test {
   protected:
    std::unique_ptr<SsdPushRequester> client;
    std::shared_ptr<SsdOwnedRead> read;
    std::shared_ptr<SsdOwnerRequestLedger> ledger =
        std::make_shared<SsdOwnerRequestLedger>(owner);
    void Prepare(bool delay, bool fail) {
        SsdPushPeerInfo peer{owner,
                             "default",
                             "legacy",
                             "127.0.0.1",
                             9001,
                             "owner-te",
                             {1, owner, 15, 1, 4096, 1, 4096, 4096, {"tcp"}}};
        client = std::make_unique<SsdPushRequester>(
            requester, "default", "requester-te", std::make_shared<Transport>(),
            std::vector<SsdPushPeerInfo>{peer},
            [this, delay, fail](const auto&, auto, const auto& message, auto) {
                SsdControlReply reply{
                    message.command, DestinationError::kOk, {}, {}};
                if (message.command == SsdControlCommand::kSubmit) {
                    auto admitted =
                        ledger->Admit(*message.request, message.reference);
                    EXPECT_EQ(admitted.error, DestinationError::kOk);
                    EXPECT_EQ(admitted.operation.Start(),
                              DestinationError::kOk);
                    auto resolved = client->grants()->Resolve(message.reference,
                                                              *message.request);
                    EXPECT_EQ(resolved.error, DestinationError::kOk);
                    if (resolved.error == DestinationError::kOk) {
                        std::memset(reinterpret_cast<void*>(
                                        resolved.destinations[0].address),
                                    42, 32);
                        EXPECT_EQ(
                            admitted.operation.Complete(
                                {1,
                                 message.reference.attempt,
                                 true,
                                 {{0, AttemptOutcome::kSucceeded, 32, {}}},
                                 requester}),
                            DestinationError::kOk);
                    }
                    // Return pending: the next Query carries the already
                    // drained result.
                    reply.snapshot =
                        SsdOwnerRequestSnapshot{DestinationError::kOk,
                                                SsdOwnerRequestState::kRunning,
                                                {}};
                } else if (message.command == SsdControlCommand::kQuery) {
                    reply.snapshot = ledger->Query(message.reference);
                    delay_validation = delay && !validation_delayed;
                    fail_result_copy = fail && !result_copy_failed;
                } else if (message.command == SsdControlCommand::kForget) {
                    reply.error = ledger->Forget(message.reference);
                }
                return SsdRequesterCallResult{DestinationError::kOk,
                                              std::move(reply)};
            });
        auto started = client->Start("legacy", {{"key", 32, 0, 32}},
                                     std::chrono::milliseconds(250));
        ASSERT_EQ(started.error, DestinationError::kOk);
        read = started.read;
        ASSERT_TRUE(read);
    }
    void TearDown() override {
        delay_validation = false;
        fail_result_copy = false;
        if (read) {
            // Clear the captured fault by replacing behavior via the one-shot
            // fired flag.
            read->Release(std::chrono::seconds(1));
        }
        if (client) client->Close();
    }
};
TEST_F(SsdRequesterAllocationTest,
       LocalValidationCrossingDeadlineNeverPublishes) {
    Prepare(true, false);
    ASSERT_TRUE(read);
    EXPECT_EQ(read->Poll(std::chrono::seconds(1)), DestinationError::kOk);
    EXPECT_TRUE(validation_delayed);
    EXPECT_FALSE(read->data(0));
    EXPECT_FALSE(read->snapshot().publishable);
    EXPECT_EQ(read->Release(std::chrono::seconds(1)), DestinationError::kOk);
    EXPECT_EQ(client->stats().owned_bytes, 0u);
}
TEST_F(SsdRequesterAllocationTest,
       TerminalCopyAllocationFailureCannotPublishOrLoseDrainRecovery) {
    Prepare(false, true);
    ASSERT_TRUE(read);
    EXPECT_EQ(read->Poll(std::chrono::seconds(1)),
              DestinationError::kTransportError);
    EXPECT_TRUE(result_copy_failed);
    EXPECT_EQ(read->snapshot().state, DestinationAttemptState::kQuarantined);
    EXPECT_FALSE(read->data(0));
    EXPECT_EQ(read->Poll(std::chrono::seconds(1)), DestinationError::kOk);
    EXPECT_FALSE(read->data(0));
    EXPECT_EQ(read->Release(std::chrono::seconds(1)), DestinationError::kOk);
    EXPECT_EQ(client->stats().owned_bytes, 0u);
}
TEST(SsdFixedRequesterAllocationTest,
     UnpublishedViewFailureReleasesSlotAndRecoveryMetadata) {
    size_t calls = 0;
    SsdFixedL2Requester fixed(
        {requester, "default", "requester-te", {7, 8}}, "tcp",
        [&](const auto&, auto, const auto& message, auto) {
            ++calls;
            return SsdRequesterCallResult{
                DestinationError::kOk,
                SsdControlReply{
                    message.command,
                    DestinationError::kOk,
                    SsdOwnerRequestSnapshot{
                        DestinationError::kOk, SsdOwnerRequestState::kDrained,
                        SsdReadResultV1{
                            1,
                            message.reference.attempt,
                            true,
                            {{0, AttemptOutcome::kSucceeded, 32, {}}},
                            requester}},
                    {}}};
        });
    auto pool = std::make_shared<std::array<std::byte, 64>>();
    const auto base = reinterpret_cast<uint64_t>(pool->data());
    auto registered = fixed.Register({base, 64, base, 64}, pool);
    ASSERT_EQ(registered.error, DestinationError::kOk);
    SsdPushPeerInfo peer{owner,
                         "default",
                         "legacy",
                         "127.0.0.1",
                         9001,
                         "owner-te",
                         {1, owner, 15, 1, 4096, 1, 4096, 4096, {"tcp"}}};
    auto objects = [&] {
        return std::vector<SsdObjectReadV1>{
            {"key", 32, {}, {{0, {{registered.region, 0, 32}}}}}};
    };
    fixed_view_failed = false;
    fixed_views_before_failure = 2;  // Acquire views, then Publish copy.
    try {
        fixed.Read(peer, objects(), std::chrono::seconds(1));
    } catch (const std::bad_alloc&) {
    }
    fixed_views_before_failure = -1;
    ASSERT_TRUE(fixed_view_failed);
    EXPECT_EQ(calls, 0u);
    EXPECT_EQ(fixed.stats().active_attempts, 0u);
    EXPECT_EQ(fixed.stats().tracked_attempts, 0u);
    EXPECT_TRUE(fixed.PendingAttempts().empty());
    EXPECT_FALSE(fixed.HasPendingWrite(base, 32));
    auto retry = fixed.Read(peer, objects(), std::chrono::seconds(1));
    EXPECT_EQ(retry.error, DestinationError::kOk);
    EXPECT_EQ(calls, 1u);
    EXPECT_EQ(fixed.Close(), DestinationError::kOk);
}
}  // namespace
}  // namespace mooncake::ssd_offload
