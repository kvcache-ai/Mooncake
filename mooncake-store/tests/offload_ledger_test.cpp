#include "eviction/offload_ledger.h"

#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <utility>

#include "common/heap_optional.h"
#include "object_runtime_state.h"

namespace mooncake {
namespace {

using eviction::OffloadLedger;

const UUID kOwnerA{1, 1};
const UUID kOwnerB{2, 2};

TEST(OffloadLedgerTest, ATakenCreditIsChargedToItsTenantAndOwners) {
    OffloadLedger ledger;
    auto first = ledger.Take(TenantId("t1"), {kOwnerA, kOwnerB}, 100);
    auto second = ledger.Take(TenantId("t2"), {kOwnerA}, 30);

    EXPECT_EQ(ledger.bytes(), 130u);
    EXPECT_EQ(ledger.objects(), 2u);
    EXPECT_EQ(ledger.bytes(TenantId("t1")), 100u);
    EXPECT_EQ(ledger.bytes(TenantId("t2")), 30u);
    EXPECT_EQ(ledger.bytes(TenantId("t3")), 0u);
    EXPECT_EQ(ledger.objects(kOwnerA), 2u);
    EXPECT_EQ(ledger.objects(kOwnerB), 1u);
    EXPECT_EQ(ledger.release_seq(), 0u);

    first.reset();
    EXPECT_EQ(ledger.bytes(), 30u);
    EXPECT_EQ(ledger.objects(), 1u);
    EXPECT_EQ(ledger.bytes(TenantId("t1")), 0u);
    EXPECT_EQ(ledger.objects(kOwnerA), 1u);
    EXPECT_EQ(ledger.objects(kOwnerB), 0u);
    EXPECT_EQ(ledger.release_seq(), 1u);
}

// Copies share one charge: it is given back once, by the last of them.
TEST(OffloadLedgerTest, ACopiedCreditIsGivenBackOnceByItsLastCopy) {
    OffloadLedger ledger;
    auto credit = ledger.Take(TenantId::Default(), {kOwnerA}, 64);
    auto copy = credit;
    auto moved = std::move(credit);

    copy.reset();
    EXPECT_EQ(ledger.bytes(), 64u);
    EXPECT_EQ(ledger.release_seq(), 0u);
    moved.reset();
    EXPECT_EQ(ledger.bytes(), 0u);
    EXPECT_EQ(ledger.objects(), 0u);
    EXPECT_EQ(ledger.objects(kOwnerA), 0u);
    EXPECT_EQ(ledger.release_seq(), 1u);
}

// The task that holds a credit may be copied with the object's state and
// is ended by resetting it; either way the charge goes back exactly once.
TEST(OffloadLedgerTest, AnOffloadingTaskGivesItsCreditBackWhenItGoes) {
    OffloadLedger ledger;
    HeapOptional<OffloadingTask> task;
    task = OffloadingTask{ReplicaID{1},
                          std::chrono::system_clock::now(),
                          {kOwnerA},
                          ledger.Take(TenantId::Default(), {kOwnerA}, 8)};
    HeapOptional<OffloadingTask> copy = task;
    task.reset();
    EXPECT_EQ(ledger.objects(kOwnerA), 1u);
    copy.reset();
    EXPECT_EQ(ledger.objects(kOwnerA), 0u);
    EXPECT_EQ(ledger.bytes(), 0u);
    EXPECT_EQ(ledger.release_seq(), 1u);
}

// An owner is live while it keeps ending copies, or for a window after it
// got its first one; one that took copies and went quiet stops counting.
TEST(OffloadLedgerTest, OnlyOwnersMakingProgressCountAsLive) {
    using std::chrono::seconds;
    OffloadLedger ledger;
    const auto t0 = OffloadLedger::SteadyClock::now();
    const auto window = seconds(10);
    auto a1 = ledger.Take(TenantId("t1"), {kOwnerA}, 100, t0);
    auto a2 = ledger.Take(TenantId("t2"), {kOwnerA}, 10, t0 + seconds(5));
    auto b1 = ledger.Take(TenantId("t1"), {kOwnerB}, 1000, t0 + seconds(1));

    EXPECT_EQ(ledger.LiveBytes(t0 + seconds(2), window), 1110u);
    EXPECT_EQ(ledger.LiveBytes(TenantId("t1"), t0 + seconds(2), window), 1100u);
    // A second copy does not refresh an owner that ended none: A is live
    // from its first copy at t0.
    EXPECT_EQ(ledger.LiveBytes(t0 + seconds(10) + seconds(1), window), 1000u);
    EXPECT_EQ(ledger.LiveBytes(t0 + seconds(12), window), 0u);
    EXPECT_EQ(ledger.bytes(), 1110u);

    // A copy that ends is progress: A is live again for a window.
    a1.reset();
    const auto later = OffloadLedger::SteadyClock::now();
    EXPECT_EQ(ledger.LiveBytes(later, window), 10u + 1000u);
    EXPECT_EQ(ledger.LiveBytes(TenantId("t2"), later, window), 10u);
    EXPECT_EQ(ledger.LiveBytes(TenantId("t1"), later, window), 1000u);
    EXPECT_EQ(ledger.LiveBytes(later + seconds(11), window), 0u);
}

TEST(OffloadLedgerTest, ACreditMayOutliveItsLedger) {
    std::shared_ptr<void> credit;
    {
        OffloadLedger ledger;
        credit = ledger.Take(TenantId::Default(), {kOwnerA}, 8);
    }
    credit.reset();
}

}  // namespace
}  // namespace mooncake
