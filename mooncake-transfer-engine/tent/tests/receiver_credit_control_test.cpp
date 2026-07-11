// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0

#include "tent/runtime/receiver_credit_control.h"

#include <algorithm>
#include <atomic>
#include <random>
#include <thread>
#include <unordered_set>

#include <gtest/gtest.h>

namespace mooncake::tent {
namespace {
CreditKey key() { return {{1, 2}, 3, 4}; }
CreditControlEnvelope envelope(uint64_t sequence, uint64_t total) {
    CreditControlEnvelope e;
    e.key = key();
    e.update.receiver_session_id = e.key.receiver_session;
    e.update.qos_class = e.key.qos_class;
    e.update.epoch = 7;
    e.update.sequence = sequence;
    e.update.grants = {{CreditResource::DataBytes, total}};
    return e;
}

TEST(ReceiverCreditControl, NegotiatesOptionalAndRequiredModes) {
    CreditCapabilityState optional(CreditRolloutMode::Optional);
    ASSERT_TRUE(optional.completeNegotiation({}).ok());
    EXPECT_EQ(optional.state(), CreditPeerState::Legacy);

    CreditCapabilityState required(CreditRolloutMode::Required);
    EXPECT_TRUE(required.completeNegotiation({2}).IsNotImplemented());
    EXPECT_EQ(required.state(), CreditPeerState::Failed);

    ASSERT_TRUE(required.beginNegotiation().ok());
    ASSERT_TRUE(required.completeNegotiation({2, 1}).ok());
    EXPECT_EQ(required.state(), CreditPeerState::Active);
    EXPECT_EQ(required.version(), 1);
    ASSERT_TRUE(required.markStale().ok());
    EXPECT_TRUE(required.refresh(2).IsInvalidArgument());
    ASSERT_TRUE(required.refresh(1).ok());
}

TEST(ReceiverCreditControl, CapabilityWireDrivesMixedVersionNegotiation) {
    std::string wire;
    ASSERT_TRUE(CreditCapabilityCodecV1::encode({2, 1}, wire).ok());
    std::vector<uint16_t> versions;
    ASSERT_TRUE(CreditCapabilityCodecV1::decode(wire, versions).ok());
    CreditCapabilityState supported(CreditRolloutMode::Required);
    ASSERT_TRUE(supported.completeNegotiation(versions).ok());
    EXPECT_EQ(supported.state(), CreditPeerState::Active);
    EXPECT_EQ(supported.version(), 1);

    ASSERT_TRUE(CreditCapabilityCodecV1::encode({2}, wire).ok());
    ASSERT_TRUE(CreditCapabilityCodecV1::decode(wire, versions).ok());
    CreditCapabilityState optional(CreditRolloutMode::Optional);
    ASSERT_TRUE(optional.completeNegotiation(versions).ok());
    EXPECT_EQ(optional.state(), CreditPeerState::Legacy);
    CreditCapabilityState required(CreditRolloutMode::Required);
    EXPECT_TRUE(required.completeNegotiation(versions).IsNotImplemented());
    EXPECT_EQ(required.state(), CreditPeerState::Failed);
}

TEST(ReceiverCreditControl, CapabilityWireRejectsMalformedOffersAtomically) {
    std::string wire = "sentinel";
    EXPECT_TRUE(
        CreditCapabilityCodecV1::encode({1, 1}, wire).IsInvalidArgument());
    EXPECT_EQ(wire, "sentinel");
    EXPECT_TRUE(CreditCapabilityCodecV1::encode({0}, wire).IsInvalidArgument());
    EXPECT_EQ(wire, "sentinel");
    EXPECT_TRUE(
        CreditCapabilityCodecV1::encode(
            std::vector<uint16_t>(CreditCapabilityCodecV1::kMaxVersions + 1, 1),
            wire)
            .IsInvalidArgument());
    EXPECT_EQ(wire, "sentinel");

    ASSERT_TRUE(CreditCapabilityCodecV1::encode({1, 2}, wire).ok());
    std::vector<uint16_t> output{99};
    for (size_t length = 0; length < wire.size(); ++length) {
        EXPECT_TRUE(CreditCapabilityCodecV1::decode(
                        std::string_view(wire.data(), length), output)
                        .IsInvalidArgument());
        EXPECT_EQ(output, std::vector<uint16_t>({99}));
    }
    std::string duplicate = wire;
    duplicate[10] = 0;
    duplicate[11] = 1;
    EXPECT_TRUE(
        CreditCapabilityCodecV1::decode(duplicate, output).IsInvalidArgument());
    EXPECT_EQ(output, std::vector<uint16_t>({99}));
}

TEST(ReceiverCreditControl, ActivationWireCreatesEpochFencedLedgerContext) {
    CreditActivationV1 original;
    original.receiver_session_id = {11, 22};
    original.epoch = 7;
    original.freshness_ttl_ms = 900;
    std::string wire;
    ASSERT_TRUE(CreditActivationCodecV1::encode(original, wire).ok());
    EXPECT_EQ(wire.size(), CreditActivationCodecV1::kWireBytes);
    CreditActivationV1 decoded;
    ASSERT_TRUE(CreditActivationCodecV1::decode(wire, decoded).ok());
    EXPECT_EQ(decoded.receiver_session_id, original.receiver_session_id);
    EXPECT_EQ(decoded.epoch, 7);

    CreditKey activation_key{decoded.receiver_session_id, 33, 4};
    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(activation_key, decoded.epoch).ok());
    ASSERT_TRUE(ledger.activate(activation_key, decoded.epoch + 1).ok());
    EXPECT_TRUE(
        ledger.deactivate(activation_key, decoded.epoch).IsInvalidEntry());
}

TEST(ReceiverCreditControl, ActivationWireRejectsMalformedInputAtomically) {
    CreditActivationV1 original;
    original.receiver_session_id = {11, 22};
    original.epoch = 7;
    std::string wire;
    ASSERT_TRUE(CreditActivationCodecV1::encode(original, wire).ok());
    CreditActivationV1 output;
    output.epoch = 99;
    for (size_t length = 0; length < wire.size(); ++length) {
        EXPECT_TRUE(CreditActivationCodecV1::decode(
                        std::string_view(wire.data(), length), output)
                        .IsInvalidArgument());
        EXPECT_EQ(output.epoch, 99);
    }
    std::string reserved = wire;
    reserved.back() = 1;
    EXPECT_TRUE(
        CreditActivationCodecV1::decode(reserved, output).IsInvalidArgument());
    EXPECT_EQ(output.epoch, 99);

    original.receiver_session_id = {};
    std::string unchanged = "sentinel";
    EXPECT_TRUE(CreditActivationCodecV1::encode(original, unchanged)
                    .IsInvalidArgument());
    EXPECT_EQ(unchanged, "sentinel");
}

TEST(ReceiverCreditControl, PeerContextSnapshotsActivationByTargetAndQos) {
    CreditPeerContextTable contexts;
    CreditActivationV1 activation;
    activation.receiver_session_id = {11, 22};
    activation.epoch = 7;
    activation.freshness_ttl_ms = 900;
    ASSERT_TRUE(contexts.activate(100, 200, 3, activation).ok());
    CreditPeerContextSnapshot snapshot;
    ASSERT_TRUE(contexts.lookup(100, 3, snapshot).ok());
    EXPECT_EQ(snapshot.key.receiver_session, activation.receiver_session_id);
    EXPECT_EQ(snapshot.key.sender_peer, 200);
    EXPECT_EQ(snapshot.key.qos_class, 3);
    EXPECT_EQ(snapshot.epoch, 7);
    EXPECT_EQ(snapshot.freshness_ttl_ms, 900);
    EXPECT_TRUE(contexts.lookup(100, 4, snapshot).IsInvalidEntry());
}

TEST(ReceiverCreditControl, PeerContextRestartFencesOldCleanup) {
    CreditPeerContextTable contexts;
    CreditActivationV1 first;
    first.receiver_session_id = {11, 22};
    first.epoch = 7;
    ASSERT_TRUE(contexts.activate(100, 200, 3, first).ok());
    EXPECT_TRUE(
        contexts.activate(100, 200, 3, CreditActivationV1{1, 1, {11, 22}, 6, 0})
            .IsInvalidEntry());

    CreditActivationV1 restarted;
    restarted.receiver_session_id = {33, 44};
    restarted.epoch = 1;
    ASSERT_TRUE(contexts.activate(100, 200, 3, restarted).ok());
    EXPECT_TRUE(
        contexts.deactivate(100, 3, first.receiver_session_id, first.epoch)
            .IsInvalidEntry());
    CreditPeerContextSnapshot snapshot;
    ASSERT_TRUE(contexts.lookup(100, 3, snapshot).ok());
    EXPECT_EQ(snapshot.key.receiver_session, restarted.receiver_session_id);
    EXPECT_EQ(snapshot.epoch, 1);
}

TEST(ReceiverCreditControl, PeerContextCapacityRecoversAfterExactCleanup) {
    CreditPeerContextTable contexts(1);
    CreditActivationV1 activation;
    activation.receiver_session_id = {11, 22};
    activation.epoch = 7;
    ASSERT_TRUE(contexts.activate(100, 200, 3, activation).ok());
    EXPECT_TRUE(contexts.activate(101, 200, 3, activation).IsTooManyRequests());
    ASSERT_TRUE(contexts
                    .deactivate(100, 3, activation.receiver_session_id,
                                activation.epoch)
                    .ok());
    ASSERT_TRUE(contexts.activate(101, 200, 3, activation).ok());
    EXPECT_EQ(contexts.size(), 1);
}

TEST(ReceiverCreditControl, ConcurrentPeerRestartLookupNeverTearsSnapshot) {
    CreditPeerContextTable contexts;
    CreditActivationV1 initial;
    initial.receiver_session_id = {1, 2};
    initial.epoch = 1;
    ASSERT_TRUE(contexts.activate(100, 200, 3, initial).ok());

    std::atomic<bool> writer_done{false};
    std::atomic<uint64_t> invalid_snapshots{0};
    std::thread writer([&] {
        for (uint64_t epoch = 2; epoch <= 10000; ++epoch) {
            CreditActivationV1 activation;
            activation.receiver_session_id = {epoch, epoch + 1};
            activation.epoch = epoch;
            if (!contexts.activate(100, 200, 3, activation).ok())
                ++invalid_snapshots;
        }
        writer_done = true;
    });
    std::vector<std::thread> readers;
    for (int reader = 0; reader < 16; ++reader) {
        readers.emplace_back([&] {
            do {
                CreditPeerContextSnapshot snapshot;
                if (!contexts.lookup(100, 3, snapshot).ok() ||
                    snapshot.key.receiver_session.high != snapshot.epoch ||
                    snapshot.key.receiver_session.low != snapshot.epoch + 1 ||
                    snapshot.key.sender_peer != 200 ||
                    snapshot.key.qos_class != 3)
                    ++invalid_snapshots;
            } while (!writer_done.load());
        });
    }
    writer.join();
    for (auto& reader : readers) reader.join();
    EXPECT_EQ(invalid_snapshots, 0);
    CreditPeerContextSnapshot final;
    ASSERT_TRUE(contexts.lookup(100, 3, final).ok());
    EXPECT_EQ(final.epoch, 10000);
}

TEST(ReceiverCreditControl, InboxIsBoundedAndDrainIsLimited) {
    BoundedCreditUpdateInbox inbox(2);
    ASSERT_TRUE(inbox.tryPublish(envelope(1, 10)).ok());
    ASSERT_TRUE(inbox.tryPublish(envelope(2, 20)).ok());
    EXPECT_TRUE(inbox.tryPublish(envelope(3, 30)).IsTooManyRequests());
    std::vector<CreditControlEnvelope> drained;
    EXPECT_EQ(inbox.drain(drained, 1), 1);
    EXPECT_EQ(drained[0].update.sequence, 1);
    EXPECT_EQ(inbox.size(), 1);
}

TEST(ReceiverCreditControl, ConcurrentPublishNeverExceedsBound) {
    BoundedCreditUpdateInbox inbox(64);
    std::atomic<int> accepted{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i)
        threads.emplace_back([&, i] {
            for (int j = 0; j < 32; ++j)
                if (inbox.tryPublish(envelope(i * 32 + j + 1, 1)).ok())
                    ++accepted;
        });
    for (auto& thread : threads) thread.join();
    EXPECT_EQ(accepted, 64);
    EXPECT_EQ(inbox.size(), 64);
}

TEST(ReceiverCreditControl, LossDuplicateAndReorderCannotMintCredit) {
    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(key(), 7).ok());
    BoundedCreditUpdateInbox inbox(8);
    // Sequence 2 is lost. Sequence 4 arrives before 3, then 4 is duplicated.
    ASSERT_TRUE(inbox.tryPublish(envelope(1, 10)).ok());
    ASSERT_TRUE(inbox.tryPublish(envelope(4, 40)).ok());
    ASSERT_TRUE(inbox.tryPublish(envelope(3, 30)).ok());
    ASSERT_TRUE(inbox.tryPublish(envelope(4, 400)).ok());
    std::vector<CreditControlEnvelope> drained;
    inbox.drain(drained, 8);
    std::vector<CreditUpdateDisposition> dispositions;
    for (auto& e : drained) {
        CreditUpdateDisposition d;
        ASSERT_TRUE(ledger.applyUpdate(e.key, e.update, d).ok());
        dispositions.push_back(d);
    }
    EXPECT_EQ(dispositions[1], CreditUpdateDisposition::SequenceGap);
    EXPECT_EQ(dispositions[2], CreditUpdateDisposition::DuplicateOrOld);
    EXPECT_EQ(dispositions[3], CreditUpdateDisposition::DuplicateOrOld);
    uint64_t available;
    ASSERT_TRUE(
        ledger.available(key(), CreditResource::DataBytes, available).ok());
    EXPECT_EQ(available, 40);
}

TEST(ReceiverCreditControl, DisconnectMakesActivePeerStale) {
    CreditCapabilityState peer(CreditRolloutMode::Required);
    ASSERT_TRUE(peer.completeNegotiation({1}).ok());
    ASSERT_TRUE(peer.markStale().ok());
    EXPECT_EQ(peer.state(), CreditPeerState::Stale);
    EXPECT_TRUE(peer.completeNegotiation({1}).IsInvalidEntry());
}

TEST(ReceiverCreditControl, ZeroCapacityInboxAlwaysFailsFast) {
    BoundedCreditUpdateInbox inbox(0);
    EXPECT_TRUE(inbox.tryPublish(envelope(1, 1)).IsTooManyRequests());
    std::vector<CreditControlEnvelope> drained;
    EXPECT_EQ(inbox.drain(drained, 100), 0);
}

TEST(ReceiverCreditControl, SustainedConcurrentPublishAndDrainLosesNothing) {
    constexpr int kProducers = 16;
    constexpr int kPerProducer = 2000;
    constexpr int kTotal = kProducers * kPerProducer;
    BoundedCreditUpdateInbox inbox(127);
    std::atomic<int> producers_done{0};
    std::vector<std::thread> producers;
    for (int producer = 0; producer < kProducers; ++producer) {
        producers.emplace_back([&, producer] {
            for (int i = 0; i < kPerProducer; ++i) {
                uint64_t sequence = producer * kPerProducer + i + 1;
                while (!inbox.tryPublish(envelope(sequence, sequence)).ok())
                    std::this_thread::yield();
            }
            ++producers_done;
        });
    }

    std::vector<CreditControlEnvelope> received;
    received.reserve(kTotal);
    while (producers_done != kProducers || inbox.size() != 0) {
        inbox.drain(received, 31);
        std::this_thread::yield();
    }
    for (auto& producer : producers) producer.join();
    EXPECT_EQ(received.size(), static_cast<size_t>(kTotal));
    std::unordered_set<uint64_t> unique;
    for (const auto& item : received) unique.insert(item.update.sequence);
    EXPECT_EQ(unique.size(), static_cast<size_t>(kTotal));
}

TEST(ReceiverCreditControl, DeterministicReorderDuplicateFuzzNeverMints) {
    constexpr uint64_t kUpdates = 10000;
    std::vector<CreditControlEnvelope> traffic;
    traffic.reserve(kUpdates * 2);
    for (uint64_t sequence = 1; sequence <= kUpdates; ++sequence) {
        traffic.push_back(envelope(sequence, sequence * 8));
        if (sequence % 3 == 0)
            traffic.push_back(envelope(sequence, sequence * 8));
    }
    std::mt19937_64 random(0x2849);
    std::shuffle(traffic.begin(), traffic.end(), random);

    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(key(), 7).ok());
    for (const auto& item : traffic) {
        CreditUpdateDisposition disposition;
        ASSERT_TRUE(
            ledger.applyUpdate(item.key, item.update, disposition).ok());
    }
    uint64_t available;
    ASSERT_TRUE(
        ledger.available(key(), CreditResource::DataBytes, available).ok());
    EXPECT_EQ(available, kUpdates * 8);
}

TEST(ReceiverCreditControl, ReconnectRequiresNegotiationAndNewLedgerEpoch) {
    CreditCapabilityState peer(CreditRolloutMode::Required);
    ASSERT_TRUE(peer.completeNegotiation({1}).ok());
    ASSERT_TRUE(peer.markStale().ok());
    ASSERT_TRUE(peer.beginNegotiation().ok());
    ASSERT_TRUE(peer.completeNegotiation({1}).ok());

    SenderCreditLedger ledger;
    ASSERT_TRUE(ledger.activate(key(), 7).ok());
    CreditUpdateDisposition disposition;
    ASSERT_TRUE(
        ledger.applyUpdate(key(), envelope(1, 100).update, disposition).ok());
    ASSERT_TRUE(ledger.activate(key(), 8).ok());
    EXPECT_TRUE(ledger.applyUpdate(key(), envelope(2, 200).update, disposition)
                    .IsInvalidEntry());
    auto fresh = envelope(1, 10);
    fresh.update.epoch = 8;
    ASSERT_TRUE(ledger.applyUpdate(key(), fresh.update, disposition).ok());
}

TEST(ReceiverCreditControl, WireCodecRoundTripsAndUsesNetworkByteOrder) {
    auto original =
        envelope(0x0102030405060708ULL, 0x1122334455667788ULL).update;
    original.flags = 3;
    original.freshness_ttl_ms = 900;
    original.grants.push_back({CreditResource::RequestSlots, 17});
    std::string wire;
    ASSERT_TRUE(ReceiverCreditCodecV1::encode(original, wire).ok());
    EXPECT_EQ(wire.size(), ReceiverCreditCodecV1::kHeaderBytes +
                               2 * ReceiverCreditCodecV1::kGrantBytes);
    EXPECT_EQ(static_cast<uint8_t>(wire[0]), 0x54);
    EXPECT_EQ(static_cast<uint8_t>(wire[1]), 0x43);
    ReceiverCreditUpdateV1 decoded;
    ASSERT_TRUE(ReceiverCreditCodecV1::decode(wire, decoded).ok());
    EXPECT_EQ(decoded.sequence, original.sequence);
    EXPECT_EQ(decoded.grants.size(), 2);
    EXPECT_EQ(decoded.grants[0].grant_total, 0x1122334455667788ULL);
    EXPECT_EQ(decoded.grants[1].resource, CreditResource::RequestSlots);
}

TEST(ReceiverCreditControl, EveryTruncationFailsWithoutMutatingOutput) {
    auto original = envelope(7, 100).update;
    std::string wire;
    ASSERT_TRUE(ReceiverCreditCodecV1::encode(original, wire).ok());
    for (size_t length = 0; length < wire.size(); ++length) {
        ReceiverCreditUpdateV1 output;
        output.sequence = 999;
        EXPECT_TRUE(ReceiverCreditCodecV1::decode(
                        std::string_view(wire.data(), length), output)
                        .IsInvalidArgument());
        EXPECT_EQ(output.sequence, 999);
    }
}

TEST(ReceiverCreditControl, OversizedDuplicateAndUnknownWireFieldsFail) {
    auto original = envelope(7, 100).update;
    std::string wire;
    ASSERT_TRUE(ReceiverCreditCodecV1::encode(original, wire).ok());
    ReceiverCreditUpdateV1 output;
    std::string oversized(ReceiverCreditCodecV1::kMaxWireBytes + 1, 'x');
    EXPECT_TRUE(
        ReceiverCreditCodecV1::decode(oversized, output).IsInvalidArgument());

    original.grants.push_back({CreditResource::DataBytes, 200});
    std::string unchanged = "sentinel";
    EXPECT_TRUE(
        ReceiverCreditCodecV1::encode(original, unchanged).IsInvalidArgument());
    EXPECT_EQ(unchanged, "sentinel");
}

TEST(ReceiverCreditControl, DeterministicMalformedWireFuzzIsMemorySafe) {
    std::mt19937_64 random(0x2860);
    ReceiverCreditUpdateV1 output;
    for (int iteration = 0; iteration < 100000; ++iteration) {
        size_t length = random() % (ReceiverCreditCodecV1::kMaxWireBytes + 33);
        std::string wire(length, '\0');
        for (char& byte : wire) byte = static_cast<char>(random());
        auto status = ReceiverCreditCodecV1::decode(wire, output);
        if (status.ok()) {
            EXPECT_EQ(output.schema_version, 1);
            EXPECT_LE(output.grants.size(), kCreditResourceCount);
            EXPECT_NE(output.epoch, 0);
            EXPECT_NE(output.sequence, 0);
        } else {
            EXPECT_TRUE(status.IsInvalidArgument());
        }
    }
}

TEST(ReceiverCreditControl, IngressValidatesBeforePublishing) {
    BoundedCreditUpdateInbox inbox(2);
    ReceiverCreditIngress ingress(inbox, key(), 7);
    std::string wire;
    ASSERT_TRUE(
        ReceiverCreditCodecV1::encode(envelope(1, 10).update, wire).ok());
    ASSERT_TRUE(ingress.tryAccept(wire).ok());

    auto wrong_session = envelope(2, 20).update;
    ++wrong_session.receiver_session_id.low;
    ASSERT_TRUE(ReceiverCreditCodecV1::encode(wrong_session, wire).ok());
    EXPECT_TRUE(ingress.tryAccept(wire).IsInvalidEntry());

    auto wrong_qos = envelope(2, 20).update;
    ++wrong_qos.qos_class;
    ASSERT_TRUE(ReceiverCreditCodecV1::encode(wrong_qos, wire).ok());
    EXPECT_TRUE(ingress.tryAccept(wire).IsInvalidEntry());

    auto wrong_epoch = envelope(2, 20).update;
    ++wrong_epoch.epoch;
    ASSERT_TRUE(ReceiverCreditCodecV1::encode(wrong_epoch, wire).ok());
    EXPECT_TRUE(ingress.tryAccept(wire).IsInvalidEntry());
    EXPECT_EQ(inbox.size(), 1);
}

TEST(ReceiverCreditControl, IngressQueueFullIsExplicitAndRetryable) {
    BoundedCreditUpdateInbox inbox(1);
    ReceiverCreditIngress ingress(inbox, key(), 7);
    std::string first_wire, second_wire;
    ASSERT_TRUE(
        ReceiverCreditCodecV1::encode(envelope(1, 10).update, first_wire).ok());
    ASSERT_TRUE(
        ReceiverCreditCodecV1::encode(envelope(2, 20).update, second_wire)
            .ok());
    ASSERT_TRUE(ingress.tryAccept(first_wire).ok());
    EXPECT_TRUE(ingress.tryAccept(second_wire).IsTooManyRequests());

    std::vector<CreditControlEnvelope> drained;
    ASSERT_EQ(inbox.drain(drained, 1), 1);
    ASSERT_TRUE(ingress.tryAccept(second_wire).ok());
    drained.clear();
    ASSERT_EQ(inbox.drain(drained, 1), 1);
    EXPECT_EQ(drained.front().update.sequence, 2);
}
}  // namespace
}  // namespace mooncake::tent
