// Copyright 2026 KVCache.AI
// Licensed under the Apache License, Version 2.0.
#include "ssd_fixed_l2_requester.h"
#include <gtest/gtest.h>
#include <array>
#include <cstring>
#include <atomic>
#include <future>
#include <thread>

namespace mooncake::ssd_offload {
namespace {
constexpr PeerIdentity requester{1, 2, 3}, owner{4, 5, 6};
class SsdFixedL2Test : public ::testing::Test {
   protected:
    std::shared_ptr<std::array<std::byte, 64>> pool =
        std::make_shared<std::array<std::byte, 64>>();
    std::unique_ptr<SsdFixedL2Requester> client;
    DestinationRegion region;
    std::vector<SsdControlCommand> commands;
    bool lose_reply{false}, wrong_reply{false};
    bool pending_snapshot{false}, inner_error{false};
    bool reject_read{false}, delay_reply{false};
    std::function<void()> recover_hook;
    std::optional<SsdReadResultV1> terminal;
    void Create() {
        client = std::make_unique<SsdFixedL2Requester>(
            SsdFixedL2Peer{requester, "default", "requester-te", {7, 8}}, "tcp",
            [this](const auto& host, auto port, const auto& message, auto) {
                EXPECT_EQ(host, "127.0.0.1");
                EXPECT_EQ(port, 9001);
                commands.push_back(message.command);
                if (message.command == SsdControlCommand::kReadInto) {
                    EXPECT_EQ(message.destinations.size(), 1u);
                    EXPECT_EQ(message.destinations[0].address,
                              reinterpret_cast<uint64_t>(pool->data()) + 8);
                    if (!reject_read)
                        std::memset(reinterpret_cast<void*>(
                                        message.destinations[0].address),
                                    42, 32);
                    terminal = SsdReadResultV1{
                        1,
                        message.reference.attempt,
                        true,
                        {{0,
                          reject_read ? AttemptOutcome::kRejected
                                      : AttemptOutcome::kSucceeded,
                          reject_read ? 0u : 32u,
                          {}}},
                        requester};
                    if (delay_reply)
                        std::this_thread::sleep_for(
                            std::chrono::milliseconds(30));
                    if (lose_reply) return SsdRequesterCallResult{};
                }
                if (message.command == SsdControlCommand::kRecoverReadInto &&
                    recover_hook)
                    recover_hook();
                auto result = *terminal;
                if (wrong_reply) ++result.requester.boot_epoch;
                return SsdRequesterCallResult{
                    DestinationError::kOk,
                    SsdControlReply{
                        message.command,
                        DestinationError::kOk,
                        SsdOwnerRequestSnapshot{
                            inner_error ? DestinationError::kUnauthorized
                                        : DestinationError::kOk,
                            pending_snapshot ? SsdOwnerRequestState::kRunning
                                             : SsdOwnerRequestState::kDrained,
                            result},
                        {}}};
            });
        auto base = reinterpret_cast<uint64_t>(pool->data());
        auto registered =
            client->Register({base, pool->size(), base, pool->size()}, pool);
        ASSERT_EQ(registered.error, DestinationError::kOk);
        region = registered.region;
    }
    SsdPushPeerInfo Peer() {
        return {owner,
                "default",
                "legacy-owner",
                "127.0.0.1",
                9001,
                "owner-te",
                {1, owner, 15, 64, 4096, 64, 4096, 4096, {"tcp"}}};
    }
    std::vector<SsdObjectReadV1> Objects() {
        return {{"key", 32, {}, {{0, {{region, 8, 32}}}}}};
    }
    void TearDown() override {
        if (client && terminal) {
            wrong_reply = false;
            pending_snapshot = false;
            inner_error = false;
            client->Recover(terminal->attempt, std::chrono::seconds(1));
        }
        if (client) client->Close();
    }
};
TEST_F(SsdFixedL2Test,
       ReusesRegisteredPoolAndSendsExactlyOneRpcPerSuccessfulRead) {
    Create();
    ASSERT_NE(region.region_id, 0u);
    for (size_t i = 0; i < 10; ++i) {
        auto result = client->Read(Peer(), Objects(), std::chrono::seconds(1));
        ASSERT_EQ(result.error, DestinationError::kOk);
        ASSERT_TRUE(result.result);
        EXPECT_EQ((*pool)[8], std::byte{42});
        EXPECT_EQ(client->stats().regions, 1u);
        EXPECT_EQ(client->stats().tracked_attempts, 0u);
        EXPECT_FALSE(client->HasPendingWrite(
            reinterpret_cast<uint64_t>(pool->data()), pool->size()));
    }
    EXPECT_EQ(commands.size(), 10u);
    for (auto command : commands)
        EXPECT_EQ(command, SsdControlCommand::kReadInto);
    EXPECT_EQ(client->Revoke(region), DestinationError::kOk);
}
TEST_F(SsdFixedL2Test, SafeAdapterSuccessfulReadStillUsesOneRpc) {
    Create();
    auto result =
        client->ReadAndDrain(Peer(), Objects(), std::chrono::seconds(1));
    EXPECT_EQ(result.error, DestinationError::kOk);
    EXPECT_TRUE(result.result);
    EXPECT_EQ(commands,
              (std::vector<SsdControlCommand>{SsdControlCommand::kReadInto}));
    EXPECT_TRUE(client->PendingAttempts().empty());
}
TEST_F(SsdFixedL2Test, OnlyTimelyOriginalRejectionPermitsLegacyFallback) {
    Create();
    reject_read = true;
    auto rejected =
        client->ReadAndDrain(Peer(), Objects(), std::chrono::seconds(1));
    EXPECT_TRUE(rejected.allow_fallback);
    ASSERT_TRUE(rejected.result);
    EXPECT_EQ(rejected.result->objects[0].outcome, AttemptOutcome::kRejected);
    lose_reply = true;
    auto recovered = client->ReadAndDrain(
        Peer(), Objects(), std::chrono::seconds(1),
        {std::chrono::milliseconds(30), std::chrono::milliseconds(2)});
    EXPECT_FALSE(recovered.allow_fallback);
    EXPECT_NE(recovered.error, DestinationError::kOk);
    ASSERT_TRUE(recovered.result);
    EXPECT_EQ(recovered.result->objects[0].outcome, AttemptOutcome::kRejected);
    lose_reply = false;
    delay_reply = true;
    auto late =
        client->ReadAndDrain(Peer(), Objects(), std::chrono::milliseconds(10));
    EXPECT_FALSE(late.allow_fallback);
    EXPECT_NE(late.error, DestinationError::kOk);
    EXPECT_EQ((*pool)[8], std::byte{0});
    EXPECT_TRUE(client->PendingAttempts().empty());
}
TEST_F(SsdFixedL2Test, InvalidRecoveryBudgetRejectsBeforeReadIntoSubmission) {
    Create();
    for (auto options :
         {SsdFixedL2DrainOptions{std::chrono::milliseconds(0),
                                 std::chrono::milliseconds(20)},
          SsdFixedL2DrainOptions{std::chrono::seconds(61),
                                 std::chrono::milliseconds(20)},
          SsdFixedL2DrainOptions{std::chrono::milliseconds(30),
                                 std::chrono::milliseconds(0)}}) {
        auto result = client->ReadAndDrain(Peer(), Objects(),
                                           std::chrono::seconds(1), options);
        EXPECT_EQ(result.error, DestinationError::kInvalidArgument);
        EXPECT_FALSE(result.may_have_submitted);
    }
    EXPECT_TRUE(commands.empty());
    EXPECT_TRUE(client->PendingAttempts().empty());
}
TEST_F(SsdFixedL2Test,
       LostReplyQuarantinesLogicalSlotAndRecoveryNeverResubmits) {
    Create();
    ASSERT_NE(region.region_id, 0u);
    lose_reply = true;
    auto result = client->Read(Peer(), Objects(), std::chrono::seconds(1));
    EXPECT_EQ(result.error, DestinationError::kTransportError);
    EXPECT_TRUE(result.may_have_submitted);
    EXPECT_TRUE(client->HasPendingWrite(
        reinterpret_cast<uint64_t>(pool->data()) + 8, 32));
    EXPECT_EQ(client->Read(Peer(), Objects(), std::chrono::seconds(1)).error,
              DestinationError::kOverlap);
    EXPECT_EQ(client->Revoke(region), DestinationError::kBusy);
    auto recovered = client->Recover(result.attempt, std::chrono::seconds(1));
    EXPECT_EQ(recovered.error, DestinationError::kClosing);
    ASSERT_TRUE(recovered.result);
    EXPECT_EQ(client->stats().tracked_attempts, 0u);
    EXPECT_EQ(commands, (std::vector<SsdControlCommand>{
                            SsdControlCommand::kReadInto,
                            SsdControlCommand::kRecoverReadInto}));
}
TEST_F(SsdFixedL2Test,
       InvalidResultCannotUnlockDestinationBeforeMatchingDrain) {
    Create();
    ASSERT_NE(region.region_id, 0u);
    wrong_reply = true;
    auto result = client->Read(Peer(), Objects(), std::chrono::seconds(1));
    EXPECT_EQ(result.error, DestinationError::kWrongAttempt);
    EXPECT_EQ(client->stats().quarantined_attempts, 1u);
    wrong_reply = false;
    EXPECT_EQ(client->Recover(result.attempt, std::chrono::seconds(1)).error,
              DestinationError::kClosing);
    EXPECT_EQ(client->stats().active_attempts, 0u);
}
TEST_F(SsdFixedL2Test, OnlySuccessfulDrainedSnapshotCanUnlockDestination) {
    Create();
    pending_snapshot = true;
    auto read = client->Read(Peer(), Objects(), std::chrono::seconds(1));
    EXPECT_EQ(read.error, DestinationError::kNotDrained);
    EXPECT_EQ(client->stats().active_attempts, 1u);
    pending_snapshot = false;
    inner_error = true;
    EXPECT_EQ(client->Recover(read.attempt, std::chrono::seconds(1)).error,
              DestinationError::kUnauthorized);
    EXPECT_EQ(client->stats().active_attempts, 1u);
    inner_error = false;
    EXPECT_EQ(client->Recover(read.attempt, std::chrono::seconds(1)).error,
              DestinationError::kClosing);
    EXPECT_EQ(client->stats().active_attempts, 0u);
}
TEST_F(SsdFixedL2Test, ConcurrentRecoveryReusesValidatedTerminalResult) {
    Create();
    lose_reply = true;
    auto read = client->Read(Peer(), Objects(), std::chrono::seconds(1));
    ASSERT_TRUE(read.may_have_submitted);
    std::promise<void> entered, release;
    auto started = entered.get_future();
    auto released = release.get_future().share();
    std::atomic<size_t> callbacks{0};
    recover_hook = [&] {
        if (++callbacks == 1) {
            entered.set_value();
            released.wait();
        }
    };
    auto first = std::async(std::launch::async, [&] {
        return client->Recover(read.attempt, std::chrono::seconds(1));
    });
    EXPECT_EQ(started.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    auto second = std::async(std::launch::async, [&] {
        return client->Recover(read.attempt, std::chrono::seconds(1));
    });
    EXPECT_EQ(second.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    release.set_value();
    auto a = first.get(), b = second.get();
    EXPECT_EQ(a.error, DestinationError::kClosing);
    // A caller first scheduled after map removal legitimately sees NotFound;
    // a caller that captured the pending record must reuse its cached result.
    EXPECT_TRUE(b.error == DestinationError::kClosing ||
                b.error == DestinationError::kNotFound);
    if (b.error == DestinationError::kClosing) {
        EXPECT_EQ(a.result, b.result);
    }
    EXPECT_EQ(callbacks, 1u);
    EXPECT_EQ(client->stats().active_attempts, 0u);
    recover_hook = {};
}
TEST(SsdFixedL2WireTest, OversizedInlineViewsRejectBeforeExposingDestination) {
    size_t calls = 0;
    SsdFixedL2Requester fixed(
        {requester, "default", "requester-te", {7, 8}}, "tcp",
        [&](const auto&, auto, const auto& message, auto) {
            ++calls;
            auto encoded = EncodeSsdControlMessage(message);
            if (encoded.error != DestinationError::kOk)
                return SsdRequesterCallResult{encoded.error, {}};
            return SsdRequesterCallResult{};
        });
    auto backing = std::make_shared<std::vector<std::byte>>(65536);
    const auto base = reinterpret_cast<uint64_t>(backing->data());
    auto registration =
        fixed.Register({base, backing->size(), base, backing->size()}, backing);
    ASSERT_EQ(registration.error, DestinationError::kOk);
    SsdReadRangeV1 range{0, {}};
    for (size_t i = 0; i < 32000; ++i)
        range.destinations.push_back({registration.region, i * 2, 1});
    SsdPushPeerInfo peer{
        owner,
        "default",
        "legacy",
        "127.0.0.1",
        9001,
        "owner-te",
        {1, owner, 15, 64, 65536, 65536, 65536, 65536, {"tcp"}}};
    const auto result =
        fixed.Read(peer, {{"large-scatter", 32000, {}, {std::move(range)}}},
                   std::chrono::seconds(1));
    EXPECT_EQ(result.error, DestinationError::kLimit);
    EXPECT_FALSE(result.may_have_submitted);
    EXPECT_EQ(calls, 0u);
    EXPECT_EQ(fixed.stats().active_attempts, 0u);
    EXPECT_EQ(fixed.stats().tracked_attempts, 0u);
    EXPECT_TRUE(fixed.PendingAttempts().empty());
    EXPECT_EQ(fixed.Close(), DestinationError::kOk);
}
class SafeReturnTarget final : public SourceDrainedWriteTarget {
   public:
    explicit SafeReturnTarget(std::vector<ResolvedDestination> views)
        : views(std::move(views)) {}
    Error WriteAndDrain(const ReadChunk&, SlotBuffer) noexcept override {
        return Error::kNotSupported;
    }
    bool DestinationsDrained() const noexcept override { return true; }
    std::vector<ResolvedDestination> views;
};
class SsdFixedL2SafeReturnTest : public ::testing::Test {
   protected:
    std::shared_ptr<std::array<std::byte, 64>> pool =
        std::make_shared<std::array<std::byte, 64>>();
    std::unique_ptr<SsdFixedL2Requester> client;
    std::unique_ptr<SsdDirectReadOwner> service;
    DestinationRegion region;
    std::promise<void> entered, release;
    std::shared_future<void> released = release.get_future().share();
    std::promise<SsdControlReply> owner_reply;
    std::future<SsdControlReply> reply = owner_reply.get_future();
    std::thread writer;
    std::atomic<size_t> read_calls{0}, recover_calls{0}, invalid_replies{0};
    std::atomic<bool> reject_recovery{false};
    std::atomic<bool> release_once{false};
    void Create() {
        service = std::make_unique<SsdDirectReadOwner>(
            owner,
            std::vector<SsdFixedL2Peer>{
                {requester, "default", "requester-te", {7, 8}}},
            [](auto views) {
                return std::make_shared<SafeReturnTarget>(std::move(views));
            },
            [this](const auto& request, auto target,
                   auto) -> std::optional<SsdReadResultV1> {
                entered.set_value();
                released.wait();
                auto sink = std::static_pointer_cast<SafeReturnTarget>(target);
                const auto& view = sink->views.at(0);
                std::memset(reinterpret_cast<void*>(view.address), 42,
                            size_t(view.length));
                return SsdReadResultV1{
                    1,
                    request.attempt,
                    true,
                    {{0, AttemptOutcome::kSucceeded, 32, {}}},
                    requester};
            });
        client = std::make_unique<SsdFixedL2Requester>(
            SsdFixedL2Peer{requester, "default", "requester-te", {7, 8}}, "tcp",
            [this](const auto&, auto, const auto& message, auto timeout) {
                if (message.command == SsdControlCommand::kReadInto) {
                    ++read_calls;
                    writer = std::thread([this, message] {
                        owner_reply.set_value(service->Execute(message));
                    });
                    if (reply.wait_for(timeout) == std::future_status::ready)
                        return SsdRequesterCallResult{DestinationError::kOk,
                                                      reply.get()};
                    // The original RPC connection stops here. Only the owner
                    // task and the caller's logical destination lease survive.
                    return SsdRequesterCallResult{};
                }
                EXPECT_EQ(message.command, SsdControlCommand::kRecoverReadInto);
                EXPECT_LE(timeout, std::chrono::milliseconds(30));
                ++recover_calls;
                if (reject_recovery) {
                    ++invalid_replies;
                    auto old_epoch = message;
                    ++old_epoch.request->attempt.owner.boot_epoch;
                    return SsdRequesterCallResult{DestinationError::kOk,
                                                  service->Execute(old_epoch)};
                }
                return SsdRequesterCallResult{DestinationError::kOk,
                                              service->Execute(message)};
            });
        auto base = reinterpret_cast<uint64_t>(pool->data());
        auto registered =
            client->Register({base, pool->size(), base, pool->size()}, pool);
        ASSERT_EQ(registered.error, DestinationError::kOk);
        region = registered.region;
    }
    SsdPushPeerInfo Peer() {
        return {owner,
                "default",
                "legacy-owner",
                "127.0.0.1",
                9001,
                "owner-te",
                {1, owner, 15, 64, 4096, 64, 4096, 4096, {"tcp"}}};
    }
    std::vector<SsdObjectReadV1> Objects() {
        return {{"key", 32, {}, {{0, {{region, 8, 32}}}}}};
    }
    void ReleaseWriter() {
        if (!release_once.exchange(true)) release.set_value();
    }
    void TearDown() override {
        ReleaseWriter();
        if (writer.joinable()) writer.join();
        reject_recovery = false;
        if (client) {
            for (const auto& attempt : client->PendingAttempts())
                client->Recover(attempt, std::chrono::milliseconds(30));
            EXPECT_EQ(client->Close(), DestinationError::kOk);
        }
    }
};
TEST_F(SsdFixedL2SafeReturnTest,
       RpcTimeoutDoesNotReturnCallerBufferUntilOwnerTaskDrains) {
    Create();
    auto started = entered.get_future();
    auto result = std::async(std::launch::async, [this] {
        return client->ReadAndDrain(
            Peer(), Objects(), std::chrono::milliseconds(30),
            {std::chrono::milliseconds(30), std::chrono::milliseconds(2)});
    });
    EXPECT_EQ(started.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    EXPECT_TRUE(client->HasPendingWrite(
        reinterpret_cast<uint64_t>(pool->data()) + 8, 32));
    EXPECT_EQ((*pool)[8], std::byte{0});
    ReleaseWriter();
    EXPECT_EQ(result.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    auto finished = result.get();
    EXPECT_NE(finished.error, DestinationError::kOk);
    EXPECT_TRUE(finished.result);
    EXPECT_EQ(read_calls, 1u);
    EXPECT_GT(recover_calls, 0u);
    EXPECT_FALSE(client->HasPendingWrite(
        reinterpret_cast<uint64_t>(pool->data()) + 8, 32));
    EXPECT_EQ((*pool)[8], std::byte{42});
    EXPECT_EQ((*pool)[7], std::byte{0});
    EXPECT_EQ((*pool)[40], std::byte{0});
    pool->fill(std::byte{17});
    writer.join();
    EXPECT_EQ((*pool)[8], std::byte{17});
}
TEST_F(SsdFixedL2SafeReturnTest,
       WrongOwnerEpochCannotProveDrainAndCloseCannotReleaseBuffer) {
    Create();
    reject_recovery = true;
    auto result = std::async(std::launch::async, [this] {
        return client->ReadAndDrain(
            Peer(), Objects(), std::chrono::milliseconds(30),
            {std::chrono::milliseconds(30), std::chrono::milliseconds(2)});
    });
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    EXPECT_GT(invalid_replies, 0u);
    EXPECT_EQ(client->Close(), DestinationError::kBusy);
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(30)),
              std::future_status::timeout);
    EXPECT_EQ(client->stats().active_attempts, 1u);
    reject_recovery = false;
    ReleaseWriter();
    EXPECT_EQ(result.wait_for(std::chrono::seconds(1)),
              std::future_status::ready);
    auto finished = result.get();
    EXPECT_NE(finished.error, DestinationError::kOk);
    EXPECT_TRUE(finished.result);
    EXPECT_EQ(client->stats().active_attempts, 0u);
}
}  // namespace
}  // namespace mooncake::ssd_offload
