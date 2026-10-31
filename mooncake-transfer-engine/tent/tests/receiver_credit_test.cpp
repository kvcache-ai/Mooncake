// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit.h"

#include <atomic>
#include <thread>

#include <gtest/gtest.h>

namespace mooncake::tent {
namespace {
CreditKey key() { return {{1, 2}, {3, 4}, 1}; }
ReceiverCreditUpdateV1 update(uint64_t epoch, uint64_t seq,
                              std::vector<CreditAmount> grants) {
    ReceiverCreditUpdateV1 u;
    u.receiver_session_id = key().receiver_session;
    u.qos_class = key().qos_class;
    u.epoch = epoch;
    u.sequence = seq;
    u.freshness_ttl_ms = 1000;
    u.grants = std::move(grants);
    return u;
}
CreditCharge charge(uint64_t bytes, uint64_t slots) {
    return {{{CreditResource::DataBytes, bytes},
             {CreditResource::RequestSlots, slots}}};
}
void grant(SenderCreditLedger& l, uint64_t seq, uint64_t bytes = 100,
           uint64_t slots = 2) {
    CreditUpdateDisposition d;
    ASSERT_TRUE(l.applyUpdate(key(),
                              update(7, seq,
                                     {{CreditResource::DataBytes, bytes},
                                      {CreditResource::RequestSlots, slots}}),
                              d)
                    .ok());
}

TEST(ReceiverCredit, MultiResourceReserveIsAtomic) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(60, 1)).ok());
    EXPECT_TRUE(l.tryReserve(key(), charge(41, 1)).IsTooManyRequests());
    uint64_t v;
    ASSERT_TRUE(l.available(key(), CreditResource::RequestSlots, v).ok());
    EXPECT_EQ(v, 1);  // failed byte reservation did not consume a slot
}

TEST(ReceiverCredit, DuplicateAndReorderedUpdatesCannotMintCredit) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 2);
    CreditUpdateDisposition d;
    ASSERT_TRUE(l.applyUpdate(
                     key(), update(7, 2, {{CreditResource::DataBytes, 999}}), d)
                    .ok());
    EXPECT_EQ(d, CreditUpdateDisposition::DuplicateOrOld);
    ASSERT_TRUE(l.applyUpdate(
                     key(), update(7, 1, {{CreditResource::DataBytes, 999}}), d)
                    .ok());
    uint64_t v;
    ASSERT_TRUE(l.available(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 100);
}

TEST(ReceiverCredit, SequenceGapIsVisibleAndSafe) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    CreditUpdateDisposition d;
    ASSERT_TRUE(l.applyUpdate(
                     key(), update(7, 4, {{CreditResource::DataBytes, 120}}), d)
                    .ok());
    EXPECT_EQ(d, CreditUpdateDisposition::SequenceGap);
}

TEST(ReceiverCredit, PartialGrantUpdateRetainsOmittedResources) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1, 100, 5);

    CreditUpdateDisposition d;
    ASSERT_TRUE(l.applyUpdate(
                     key(), update(7, 2, {{CreditResource::DataBytes, 160}}), d)
                    .ok());
    EXPECT_EQ(d, CreditUpdateDisposition::Applied);

    uint64_t v;
    ASSERT_TRUE(l.available(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 160);
    ASSERT_TRUE(l.available(key(), CreditResource::RequestSlots, v).ok());
    EXPECT_EQ(v, 5);
}

TEST(ReceiverCredit, StaleEpochFailsAndActivationFencesOldState) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(60, 1)).ok());
    CreditUpdateDisposition d;
    EXPECT_TRUE(l.applyUpdate(
                     key(), update(6, 2, {{CreditResource::DataBytes, 999}}), d)
                    .IsInvalidEntry());
    ASSERT_TRUE(l.activate(key(), 8).ok());
    uint64_t v;
    EXPECT_TRUE(
        l.available(key(), CreditResource::DataBytes, v).IsInvalidEntry());
}

TEST(ReceiverCredit, ActivationReplayCannotMintCredit) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(80, 1)).ok());
    ASSERT_TRUE(l.activate(key(), 7).ok());  // idempotent, not a reset
    uint64_t v;
    ASSERT_TRUE(l.consumed(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 80);
    EXPECT_TRUE(l.activate(key(), 6).IsInvalidEntry());
    ASSERT_TRUE(l.consumed(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 80);
}

TEST(ReceiverCredit, LedgerEntryCountIsBounded) {
    SenderCreditLedger l(1);
    ASSERT_TRUE(l.activate(key(), 7).ok());
    auto other = key();
    ++other.sender_instance.low;
    EXPECT_TRUE(l.activate(other, 7).IsTooManyRequests());
    // A new epoch for an existing key does not consume another entry.
    EXPECT_TRUE(l.activate(key(), 8).ok());
}

TEST(ReceiverCredit, DeactivationReleasesCapacityAfterExactEpochFence) {
    SenderCreditLedger l(1);
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(80, 1)).ok());
    auto other = key();
    ++other.sender_instance.low;
    EXPECT_TRUE(l.activate(other, 1).IsTooManyRequests());

    EXPECT_TRUE(l.deactivate(key(), 6).IsInvalidEntry());
    EXPECT_TRUE(l.activate(other, 1).IsTooManyRequests());
    ASSERT_TRUE(l.deactivate(key(), 7).ok());
    ASSERT_TRUE(l.deactivate(key(), 7).ok());  // cleanup is idempotent
    EXPECT_TRUE(l.activate(other, 1).ok());
}

TEST(ReceiverCredit, OldCleanupCannotEraseReactivatedEpoch) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    ASSERT_TRUE(l.activate(key(), 8).ok());
    EXPECT_TRUE(l.deactivate(key(), 7).IsInvalidEntry());
    CreditUpdateDisposition disposition;
    auto fresh = update(8, 1, {{CreditResource::DataBytes, 10}});
    ASSERT_TRUE(l.applyUpdate(key(), fresh, disposition).ok());
}

TEST(ReceiverCredit, InvalidUpdateDoesNotPartiallyMutate) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    CreditUpdateDisposition d;
    EXPECT_TRUE(l.applyUpdate(key(),
                              update(7, 2,
                                     {{CreditResource::DataBytes, 200},
                                      {CreditResource::RequestSlots, 1}}),
                              d)
                    .IsInvalidArgument());
    uint64_t v;
    ASSERT_TRUE(l.available(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 100);
}

TEST(ReceiverCredit, DuplicateUnknownAndZeroResourcesFailClosed) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    CreditUpdateDisposition d;
    EXPECT_TRUE(l.applyUpdate(key(),
                              update(7, 1,
                                     {{CreditResource::DataBytes, 1},
                                      {CreditResource::DataBytes, 2}}),
                              d)
                    .IsInvalidArgument());
    EXPECT_TRUE(l.tryReserve(key(), {{{static_cast<CreditResource>(99), 1}}})
                    .IsInvalidArgument());
    EXPECT_TRUE(l.tryReserve(key(), {{{CreditResource::DataBytes, 0}}})
                    .IsInvalidArgument());
}

TEST(ReceiverCredit, RollbackChecksUnderflowAtomically) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(60, 1)).ok());
    EXPECT_TRUE(
        l.rollbackReservation(key(), charge(61, 1)).IsInvalidArgument());
    uint64_t v;
    ASSERT_TRUE(l.consumed(key(), CreditResource::RequestSlots, v).ok());
    EXPECT_EQ(v, 1);
    ASSERT_TRUE(l.rollbackReservation(key(), charge(60, 1)).ok());
    ASSERT_TRUE(l.consumed(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 60);  // cumulative consumption never moves backwards
    ASSERT_TRUE(l.released(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 60);
    ASSERT_TRUE(l.available(key(), CreditResource::DataBytes, v).ok());
    EXPECT_EQ(v, 40);  // returned authorization is not locally re-minted
}

TEST(ReceiverCredit, DeactivateSessionRemovesAllQosEntries) {
    SenderCreditLedger ledger(2);
    auto high = key();
    auto low = key();
    low.qos_class = 2;
    ASSERT_TRUE(ledger.activate(high, 7).ok());
    ASSERT_TRUE(ledger.activate(low, 7).ok());
    ASSERT_TRUE(ledger.deactivateSession(key().receiver_session).ok());

    auto replacement = key();
    replacement.receiver_session = {9, 10};
    replacement.sender_instance = {11, 12};
    ASSERT_TRUE(ledger.activate(replacement, 1).ok());
}

TEST(ReceiverCredit, GrantCannotDecreaseOrFallBelowConsumption) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1);
    ASSERT_TRUE(l.tryReserve(key(), charge(80, 1)).ok());
    CreditUpdateDisposition d;
    EXPECT_TRUE(
        l.applyUpdate(key(), update(7, 2, {{CreditResource::DataBytes, 79}}), d)
            .IsInvalidArgument());
}

TEST(ReceiverCredit, ConcurrentReservationsNeverExceedGrant) {
    SenderCreditLedger l;
    ASSERT_TRUE(l.activate(key(), 7).ok());
    grant(l, 1, 100, 100);
    std::atomic<int> admitted{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 16; ++i) {
        threads.emplace_back([&] {
            for (int j = 0; j < 20; ++j)
                if (l.tryReserve(key(), charge(1, 1)).ok()) ++admitted;
        });
    }
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(admitted, 100);
    uint64_t consumed;
    ASSERT_TRUE(l.consumed(key(), CreditResource::DataBytes, consumed).ok());
    EXPECT_EQ(consumed, 100);
}

TEST(ReceiverCredit, FreshnessExpiryFailsClosedAndReplayDoesNotRenew) {
    uint64_t now_ns = 100;
    SenderCreditLedger ledger(8, 1000, [&] { return now_ns; });
    ASSERT_TRUE(ledger.activate(key(), 7).ok());
    auto first = update(
        7, 1,
        {{CreditResource::DataBytes, 100}, {CreditResource::RequestSlots, 2}});
    first.freshness_ttl_ms = 1;
    CreditUpdateDisposition disposition;
    ASSERT_TRUE(ledger.applyUpdate(key(), first, disposition).ok());
    now_ns += 999999;
    EXPECT_TRUE(ledger.tryReserve(key(), charge(1, 1)).ok());

    // A replay is accepted idempotently, but must not extend the lease.
    ASSERT_TRUE(ledger.applyUpdate(key(), first, disposition).ok());
    EXPECT_EQ(disposition, CreditUpdateDisposition::DuplicateOrOld);
    ++now_ns;
    EXPECT_TRUE(ledger.tryReserve(key(), charge(1, 1)).IsInvalidEntry());
}

TEST(ReceiverCredit, TerminalReleaseRequiresReceiverReplenishment) {
    ReceiverCreditLimits limits;
    limits.capacity[0] = 100;
    limits.capacity[1] = 2;
    limits.freshness_ttl_ms = 1000;
    ReceiverCreditAuthority authority({9, 10}, 3, limits);
    ASSERT_TRUE(authority.status().ok());

    CreditKey sender{{9, 10}, {11, 12}, 1};
    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(sender, 3).ok());

    ReceiverCreditExchangeRequestV1 request;
    ASSERT_TRUE(ledger.prepareExchange(sender, charge(60, 1), request).ok());
    ReceiverCreditExchangeReplyV1 reply;
    ASSERT_TRUE(authority.exchange(request, reply).ok());
    CreditUpdateDisposition disposition;
    ASSERT_TRUE(ledger.applyUpdate(sender, reply.update, disposition).ok());
    ASSERT_TRUE(ledger.tryReserve(sender, charge(60, 1)).ok());
    ASSERT_TRUE(ledger.releaseCommitted(sender, charge(60, 1)).ok());

    uint64_t available = 0;
    ASSERT_TRUE(
        ledger.available(sender, CreditResource::DataBytes, available).ok());
    EXPECT_EQ(available, 0);  // local release never mints credit

    ASSERT_TRUE(ledger.prepareExchange(sender, charge(60, 1), request).ok());
    ASSERT_TRUE(authority.exchange(request, reply).ok());
    ASSERT_TRUE(ledger.applyUpdate(sender, reply.update, disposition).ok());
    EXPECT_TRUE(ledger.tryReserve(sender, charge(60, 1)).ok());

    uint64_t outstanding = 0;
    ASSERT_TRUE(
        authority.outstanding(CreditResource::DataBytes, outstanding).ok());
    EXPECT_EQ(outstanding, 60);
}

TEST(ReceiverCredit, AuthorityGrantsMultiResourceDemandAtomically) {
    ReceiverCreditLimits limits;
    limits.capacity[0] = 100;
    limits.capacity[1] = 1;
    ReceiverCreditAuthority authority({1, 2}, 1, limits);

    auto make_request = [](SenderInstanceId sender, uint64_t bytes) {
        ReceiverCreditExchangeRequestV1 request;
        request.expected_receiver_session_id = {1, 2};
        request.epoch = 1;
        request.sender_instance_id = sender;
        request.qos_class = 0;
        request.report_sequence = 1;
        request.released_totals = {
            {CreditResource::DataBytes, 0},
            {CreditResource::RequestSlots, 0},
        };
        request.desired_grant_totals = {
            {CreditResource::DataBytes, bytes},
            {CreditResource::RequestSlots, 1},
        };
        return request;
    };

    ReceiverCreditExchangeReplyV1 reply;
    ASSERT_TRUE(authority.exchange(make_request({3, 4}, 60), reply).ok());
    ASSERT_EQ(reply.update.grants.size(), kCreditResourceCount);
    EXPECT_EQ(reply.update.grants[0].grant_total, 60);
    EXPECT_EQ(reply.update.grants[1].grant_total, 1);

    ASSERT_TRUE(authority.exchange(make_request({5, 6}, 40), reply).ok());
    // The second sender cannot get a request slot, so bytes are not stranded
    // in a partial grant either.
    EXPECT_EQ(reply.update.grants[0].grant_total, 0);
    EXPECT_EQ(reply.update.grants[1].grant_total, 0);
    uint64_t outstanding = 0;
    ASSERT_TRUE(
        authority.outstanding(CreditResource::DataBytes, outstanding).ok());
    EXPECT_EQ(outstanding, 60);
}

TEST(ReceiverCredit, RollbackReleaseLetsAnotherSenderAcquireCapacity) {
    ReceiverCreditLimits limits;
    limits.capacity[0] = 60;
    limits.capacity[1] = 1;
    limits.max_senders = 1;
    ReceiverCreditAuthority authority({1, 2}, 1, limits);
    SenderCreditLedger first;
    CreditKey first_key{{1, 2}, {3, 4}, 0};
    ASSERT_TRUE(first.activate(first_key, 1).ok());

    ReceiverCreditExchangeRequestV1 request;
    ASSERT_TRUE(first.prepareExchange(first_key, charge(60, 1), request).ok());
    const auto initial_demand = request;
    ReceiverCreditExchangeReplyV1 reply;
    ASSERT_TRUE(authority.exchange(request, reply).ok());
    CreditUpdateDisposition disposition;
    ASSERT_TRUE(first.applyUpdate(first_key, reply.update, disposition).ok());
    ASSERT_TRUE(first.tryReserve(first_key, charge(60, 1)).ok());
    ASSERT_TRUE(first.rollbackReservation(first_key, charge(60, 1)).ok());
    ASSERT_TRUE(first.prepareReleaseExchange(first_key, request).ok());
    const auto release_report = request;
    ASSERT_TRUE(authority.exchange(request, reply).ok());
    EXPECT_EQ(reply.flags, 0);

    uint64_t outstanding = 0;
    ASSERT_TRUE(
        authority.outstanding(CreditResource::DataBytes, outstanding).ok());
    EXPECT_EQ(outstanding, 0);

    // Both a delayed initial demand and a replayed release receive retained
    // cumulative state; neither can re-mint released capacity.
    ASSERT_TRUE(authority.exchange(initial_demand, reply).ok());
    EXPECT_EQ(reply.flags, 0);
    EXPECT_EQ(reply.update.grants[0].grant_total, 60);
    ASSERT_TRUE(authority.exchange(release_report, reply).ok());
    ASSERT_TRUE(
        authority.outstanding(CreditResource::DataBytes, outstanding).ok());
    EXPECT_EQ(outstanding, 0);

    SenderCreditLedger second;
    CreditKey second_key{{1, 2}, {5, 6}, 0};
    ASSERT_TRUE(second.activate(second_key, 1).ok());
    ASSERT_TRUE(
        second.prepareExchange(second_key, charge(60, 1), request).ok());
    ASSERT_TRUE(authority.exchange(request, reply).ok());
    ASSERT_TRUE(second.applyUpdate(second_key, reply.update, disposition).ok());
    EXPECT_TRUE(second.tryReserve(second_key, charge(60, 1)).ok());
}

TEST(ReceiverCredit, SenderLimitCountsSenderNotQosEntries) {
    ReceiverCreditLimits limits;
    limits.capacity[0] = 100;
    limits.capacity[1] = 2;
    limits.max_senders = 1;
    ReceiverCreditAuthority authority({1, 2}, 1, limits);

    auto make_request = [](SenderInstanceId sender, uint32_t qos) {
        ReceiverCreditExchangeRequestV1 request;
        request.expected_receiver_session_id = {1, 2};
        request.epoch = 1;
        request.sender_instance_id = sender;
        request.qos_class = qos;
        request.report_sequence = 1;
        request.released_totals = {{CreditResource::DataBytes, 0},
                                   {CreditResource::RequestSlots, 0}};
        request.desired_grant_totals = {{CreditResource::DataBytes, 10},
                                        {CreditResource::RequestSlots, 1}};
        return request;
    };

    ReceiverCreditExchangeReplyV1 reply;
    ASSERT_TRUE(authority.exchange(make_request({3, 4}, 0), reply).ok());
    ASSERT_TRUE(authority.exchange(make_request({3, 4}, 2), reply).ok());
    ASSERT_TRUE(authority.exchange(make_request({5, 6}, 0), reply).ok());
    EXPECT_EQ(reply.update.grants[0].grant_total, 0);
    EXPECT_EQ(reply.update.grants[1].grant_total, 0);
}

TEST(ReceiverCredit, ZeroGrantHistoryDoesNotConsumeActiveSenderLimit) {
    ReceiverCreditLimits limits;
    limits.capacity[0] = 10;
    limits.capacity[1] = 1;
    limits.max_senders = 1;
    limits.max_entries = 4;
    ReceiverCreditAuthority authority({1, 2}, 1, limits);

    auto make_request = [](SenderInstanceId sender) {
        ReceiverCreditExchangeRequestV1 request;
        request.expected_receiver_session_id = {1, 2};
        request.epoch = 1;
        request.sender_instance_id = sender;
        request.qos_class = 0;
        request.report_sequence = 1;
        request.released_totals = {{CreditResource::DataBytes, 0},
                                   {CreditResource::RequestSlots, 0}};
        request.desired_grant_totals = {{CreditResource::DataBytes, 10},
                                        {CreditResource::RequestSlots, 1}};
        return request;
    };

    auto first = make_request({3, 4});
    ReceiverCreditExchangeReplyV1 first_reply;
    ASSERT_TRUE(authority.exchange(first, first_reply).ok());
    EXPECT_EQ(first_reply.update.grants[0].grant_total, 10);

    ReceiverCreditExchangeReplyV1 reply;
    ASSERT_TRUE(authority.exchange(make_request({5, 6}), reply).ok());
    EXPECT_EQ(reply.update.grants[0].grant_total, 0);

    first.known_update_sequence = first_reply.update.sequence;
    first.report_sequence = 2;
    first.released_totals = {{CreditResource::DataBytes, 10},
                             {CreditResource::RequestSlots, 1}};
    ASSERT_TRUE(authority.exchange(first, reply).ok());

    ASSERT_TRUE(authority.exchange(make_request({7, 8}), reply).ok());
    EXPECT_EQ(reply.update.grants[0].grant_total, 10);
    EXPECT_EQ(reply.update.grants[1].grant_total, 1);
}

TEST(ReceiverCredit, ReplayHistoryLimitFailsClosed) {
    ReceiverCreditLimits limits;
    limits.capacity[0] = 10;
    limits.capacity[1] = 1;
    limits.max_senders = 1;
    limits.max_entries = 2;
    ReceiverCreditAuthority authority({1, 2}, 1, limits);

    auto make_request = [](SenderInstanceId sender) {
        ReceiverCreditExchangeRequestV1 request;
        request.expected_receiver_session_id = {1, 2};
        request.epoch = 1;
        request.sender_instance_id = sender;
        request.report_sequence = 1;
        request.released_totals = {{CreditResource::DataBytes, 0},
                                   {CreditResource::RequestSlots, 0}};
        request.desired_grant_totals = {{CreditResource::DataBytes, 10},
                                        {CreditResource::RequestSlots, 1}};
        return request;
    };

    ReceiverCreditExchangeReplyV1 reply;
    ASSERT_TRUE(authority.exchange(make_request({3, 4}), reply).ok());
    ASSERT_TRUE(authority.exchange(make_request({5, 6}), reply).ok());
    EXPECT_TRUE(
        authority.exchange(make_request({7, 8}), reply).IsTooManyRequests());

    // Existing identities remain replayable after the cap is reached.
    EXPECT_TRUE(authority.exchange(make_request({3, 4}), reply).ok());
}

TEST(ReceiverCredit, ReplayHistoryLimitMustCoverActiveSenders) {
    ReceiverCreditLimits limits;
    limits.capacity[0] = 10;
    limits.capacity[1] = 1;
    limits.max_senders = 2;
    limits.max_entries = 1;
    ReceiverCreditAuthority authority({1, 2}, 1, limits);
    EXPECT_TRUE(authority.status().IsInvalidArgument());
}

TEST(ReceiverCredit, AdvertPublishesHardCapacityBounds) {
    ReceiverCreditLimits limits;
    limits.capacity[0] = 4096;
    limits.capacity[1] = 8;
    ReceiverCreditAuthority authority({1, 2}, 1, limits);
    const auto advert = authority.advert();
    ASSERT_EQ(advert.capacities.size(), 2u);
    EXPECT_EQ(advert.capacities[0].resource, CreditResource::DataBytes);
    EXPECT_EQ(advert.capacities[0].total, 4096u);
    EXPECT_EQ(advert.capacities[1].resource, CreditResource::RequestSlots);
    EXPECT_EQ(advert.capacities[1].total, 8u);

    const auto round_trip =
        nlohmann::json(advert).get<ReceiverCreditAdvertV1>();
    EXPECT_EQ(round_trip.capacities.size(), advert.capacities.size());
    EXPECT_EQ(round_trip.capacities[0].total, 4096u);
}

TEST(ReceiverCredit, ExchangeReplayIsIdempotent) {
    ReceiverCreditLimits limits;
    limits.capacity[0] = 100;
    limits.capacity[1] = 1;
    ReceiverCreditAuthority authority({1, 2}, 1, limits);
    ReceiverCreditExchangeRequestV1 request;
    request.expected_receiver_session_id = {1, 2};
    request.epoch = 1;
    request.sender_instance_id = {3, 4};
    request.qos_class = 0;
    request.report_sequence = 1;
    request.released_totals = {{CreditResource::DataBytes, 0},
                               {CreditResource::RequestSlots, 0}};
    request.desired_grant_totals = {{CreditResource::DataBytes, 50},
                                    {CreditResource::RequestSlots, 1}};

    ReceiverCreditExchangeReplyV1 first, replay;
    ASSERT_TRUE(authority.exchange(request, first).ok());
    ASSERT_TRUE(authority.exchange(request, replay).ok());
    EXPECT_EQ(replay.update.sequence, first.update.sequence);
    EXPECT_EQ(replay.update.grants[0].grant_total, 50);
    uint64_t outstanding = 0;
    ASSERT_TRUE(
        authority.outstanding(CreditResource::DataBytes, outstanding).ok());
    EXPECT_EQ(outstanding, 50);
}

TEST(ReceiverCredit, InvalidNewSenderCannotConsumeAuthorityEntry) {
    ReceiverCreditLimits limits;
    limits.capacity[0] = 100;
    limits.capacity[1] = 1;
    limits.max_senders = 1;
    ReceiverCreditAuthority authority({1, 2}, 1, limits);

    ReceiverCreditExchangeRequestV1 request;
    request.expected_receiver_session_id = {1, 2};
    request.epoch = 1;
    request.sender_instance_id = {3, 4};
    request.qos_class = 0;
    request.known_update_sequence = 99;
    request.report_sequence = 1;
    request.released_totals = {{CreditResource::DataBytes, 0}};
    request.desired_grant_totals = {{CreditResource::DataBytes, 50},
                                    {CreditResource::RequestSlots, 1}};
    ReceiverCreditExchangeReplyV1 reply;
    EXPECT_TRUE(authority.exchange(request, reply).IsInvalidArgument());

    request.sender_instance_id = {5, 6};
    request.known_update_sequence = 0;
    EXPECT_TRUE(authority.exchange(request, reply).ok());
    EXPECT_EQ(reply.update.grants[0].grant_total, 50);
}
}  // namespace
}  // namespace mooncake::tent
