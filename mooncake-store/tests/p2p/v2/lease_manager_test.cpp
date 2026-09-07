// Component tests for LeaseManager (design doc sections 5.9, 6.3, 6.4, 9.3).
//
// Time is injected: every deadline in this file moves because ManualClock was
// advanced, never because a thread waited. A sleeping lease test is a flaky
// lease test, and the whole reason LeaseManager takes a Clock is to make the
// expiry branches reachable without one.
//
// The pinned-read cases use real ImmutableBlocks assembled the way the data
// plane assembles them (pool -> MutableBlock -> CompletedBlock -> BlockIndex),
// because the lease's contract is about keeping a physical block alive; a fake
// block would test the map and not the invariant.

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <variant>
#include <vector>

#include "p2p/client/data_manager_types.h"
#include "p2p/client/v2/block.h"
#include "p2p/client/v2/block_index.h"
#include "p2p/client/v2/block_pool.h"
#include "p2p/client/v2/block_registry.h"
#include "p2p/client/v2/lease_manager.h"
#include "p2p/client/v2/v2_common.h"
#include "transfer_engine.h"
#include "types.h"

namespace mooncake::v2 {
namespace {

using namespace std::chrono_literals;

/**
 * @class ManualClock
 * @brief The only time source in this file. Its origin is unrelated to the
 *        real steady clock, so a test that accidentally depended on wall time
 *        would fail rather than pass slowly.
 */
class ManualClock final : public Clock {
   public:
    time_point Now() const override {
        return now_.load(std::memory_order_acquire);
    }

    void Advance(std::chrono::milliseconds delta) {
        now_.store(now_.load(std::memory_order_acquire) + delta,
                   std::memory_order_release);
    }

   private:
    std::atomic<time_point> now_{Clock::time_point{} + 24h};
};

constexpr std::chrono::milliseconds kLease{1000};
constexpr size_t kBlockSize = 4096;
constexpr std::string_view kKey = "lease/key/alpha";
constexpr std::string_view kOtherKey = "lease/key/beta";
constexpr std::string_view kPayload = "pinned-bytes";

class LeaseManagerTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        static std::once_flag once;
        std::call_once(once, [] {
            google::InitGoogleLogging("LeaseManagerTest");
            FLAGS_logtostderr = 1;
        });
    }

    void SetUp() override {
        clock_ = std::make_shared<ManualClock>();

        DramArenaConfig arena;
        arena.capacity_bytes = 8u * 1024 * 1024;
        arena.alignment = 64;
        DramBlockPoolConfig pool_config;
        pool_config.arenas.push_back(arena);
        // No TransferEngine: the lease layer never touches a TransferAddress,
        // so the pool stays a plain local allocator here.
        pool_ = std::make_shared<DramBlockPool>(pool_config, nullptr);
        ASSERT_TRUE(pool_->Init().has_value());

        registry_ = BlockRegistry(BlockRegistryConfig{});
        index_ =
            std::make_unique<BlockIndex>(generate_uuid(), BlockIndexConfig{});
        replica_index_ =
            std::make_unique<BlockIndex>(generate_uuid(), BlockIndexConfig{});
        leases_ = std::make_unique<LeaseManager>(Config(), clock_,
                                                 /*shard_count=*/8);
    }

    /**
     * @brief A scan interval far beyond any test's lifetime: the background
     *        scanner must never race the assertions, expiry is driven only by
     *        ScanExpiredNow().
     */
    static KeyLeaseConfig Config() {
        KeyLeaseConfig config;
        config.duration_ms = 1000;
        config.scan_interval_ms = 3600u * 1000u;
        return config;
    }

    Clock::time_point Deadline(std::chrono::milliseconds after) const {
        return clock_->Now() + after;
    }

    std::variant<MutableBlock, CompletedBlock> MakeTransaction() {
        auto allocation = pool_->Allocate(kBlockSize, 0);
        EXPECT_TRUE(allocation.has_value());
        if (!allocation) return {};
        return std::variant<MutableBlock, CompletedBlock>{
            MutableBlock::MakeForTiler(std::move(allocation.value()))};
    }

    /** Bytes one block costs the pool, measured rather than assumed. */
    size_t UsageOfOneBlock() {
        const size_t before = pool_->Usage();
        size_t measured = 0;
        {
            auto allocation = pool_->Allocate(kBlockSize, 0);
            EXPECT_TRUE(allocation.has_value());
            measured = pool_->Usage() - before;
        }
        EXPECT_EQ(pool_->Usage(), before);
        return measured;
    }

    ImmutableBlock MakeVisibleBlock(BlockIndex& index,
                                    const BlockRegistrationHandle& handle,
                                    std::string_view payload) {
        auto allocation = pool_->Allocate(kBlockSize, 0);
        EXPECT_TRUE(allocation.has_value());
        if (!allocation) return {};
        auto writable =
            MutableBlock::MakeForTiler(std::move(allocation.value()));
        EXPECT_TRUE(
            writable.Write(0, std::as_bytes(std::span<const char>(payload)))
                .has_value());
        auto completed =
            std::move(writable).Complete(std::string(handle.Key()));
        EXPECT_TRUE(completed.has_value());
        if (!completed) return {};
        auto inserted = index.Insert(std::move(completed.value()), handle);
        EXPECT_TRUE(inserted.has_value());
        if (!inserted) return {};
        return std::move(inserted.value());
    }

    ImmutableBlock MakeVisibleBlock(std::string_view key) {
        auto handle = registry_.Register(key);
        EXPECT_TRUE(handle.has_value());
        if (!handle) return {};
        return MakeVisibleBlock(*index_, handle.value(), kPayload);
    }

    std::shared_ptr<ManualClock> clock_;
    std::shared_ptr<DramBlockPool> pool_;
    BlockRegistry registry_;
    std::unique_ptr<BlockIndex> index_;
    std::unique_ptr<BlockIndex> replica_index_;
    // Declared last so it is destroyed first: leases own ImmutableBlocks that
    // must go back to the pool before the fixture tears the pool down.
    std::unique_ptr<LeaseManager> leases_;
};

// ---------------------------------------------------------------------------
// Pending write: claim precedes allocation (section 6.3)
// ---------------------------------------------------------------------------

TEST_F(LeaseManagerTest, ReserveMintsAWriteTokenForAFreeKey) {
    auto token = leases_->ReservePendingWrite(kKey, Deadline(kLease));
    ASSERT_TRUE(token.has_value());
    EXPECT_FALSE(IsZeroUUID(token.value()));
    EXPECT_EQ(leases_->PendingWriteCount(), 1u);
    EXPECT_TRUE(leases_->HasPendingWrite(kKey));
}

TEST_F(LeaseManagerTest, SecondReserveOfALiveClaimIsRejectedBeforeAllocating) {
    ASSERT_TRUE(
        leases_->ReservePendingWrite(kKey, Deadline(kLease)).has_value());

    const size_t usage_before = pool_->Usage();
    auto blocked = leases_->ReservePendingWrite(kKey, Deadline(kLease));
    ASSERT_FALSE(blocked.has_value());
    EXPECT_EQ(blocked.error(), ErrorCode::REPLICA_IS_PROCESSING);
    // The rejected writer never reached an allocation, so nothing it did can
    // trigger a reclaim.
    EXPECT_EQ(pool_->Usage(), usage_before);
    EXPECT_EQ(leases_->PendingWriteCount(), 1u);
}

// The rule the whole split exists for: N racing writers of one key must
// produce exactly one allocation. Allocating first would leave N-1 wasted
// blocks, each able to trigger a synchronous reclaim, turning write contention
// into an eviction storm.
TEST_F(LeaseManagerTest,
       ConcurrentReservesOfOneKeyLetExactlyOneWriterAllocate) {
    const size_t one_block = UsageOfOneBlock();
    ASSERT_GT(one_block, 0u);
    ASSERT_EQ(pool_->Usage(), 0u);

    constexpr size_t kThreads = 16;
    std::atomic<bool> go{false};
    std::atomic<size_t> allocate_calls{0};
    std::atomic<size_t> winners{0};
    std::vector<std::optional<UUID>> won(kThreads);
    std::vector<ErrorCode> lost(kThreads, ErrorCode::OK);
    const auto deadline = Deadline(kLease);

    std::vector<std::thread> threads;
    threads.reserve(kThreads);
    for (size_t i = 0; i < kThreads; ++i) {
        threads.emplace_back([&, i] {
            while (!go.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            auto token = leases_->ReservePendingWrite(kKey, deadline);
            if (!token) {
                lost[i] = token.error();
                return;
            }
            winners.fetch_add(1, std::memory_order_relaxed);
            won[i] = token.value();
            // Allocation happens only on this side of a successful claim.
            allocate_calls.fetch_add(1, std::memory_order_relaxed);
            auto attached = leases_->AttachPendingWriteTransaction(
                kKey, token.value(), generate_uuid(), MakeTransaction());
            EXPECT_TRUE(attached.has_value());
        });
    }
    go.store(true, std::memory_order_release);
    for (auto& thread : threads) thread.join();

    EXPECT_EQ(winners.load(), 1u);
    EXPECT_EQ(allocate_calls.load(), 1u);
    EXPECT_EQ(pool_->Usage(), one_block);
    EXPECT_EQ(leases_->PendingWriteCount(), 1u);

    std::optional<UUID> winner;
    for (size_t i = 0; i < kThreads; ++i) {
        if (won[i].has_value()) {
            winner = won[i];
        } else {
            EXPECT_EQ(lost[i], ErrorCode::REPLICA_IS_PROCESSING);
        }
    }
    ASSERT_TRUE(winner.has_value());
    {
        auto taken = leases_->TakePendingWrite(kKey, winner.value());
        ASSERT_TRUE(taken.has_value());
        EXPECT_TRUE(taken->transaction.has_value());
    }
    EXPECT_EQ(pool_->Usage(), 0u);
}

// A crashed or abandoned writer must not own the key forever, and recycling
// its claim must also give back the block it had already attached.
TEST_F(LeaseManagerTest, ExpiredClaimIsReplacedAndItsBlockReleased) {
    auto first = leases_->ReservePendingWrite(kKey, Deadline(kLease));
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(leases_
                    ->AttachPendingWriteTransaction(
                        kKey, first.value(), generate_uuid(), MakeTransaction())
                    .has_value());
    ASSERT_GT(pool_->Usage(), 0u);

    clock_->Advance(kLease + 1ms);

    auto second = leases_->ReservePendingWrite(kKey, Deadline(kLease));
    ASSERT_TRUE(second.has_value());
    EXPECT_NE(first.value(), second.value());
    EXPECT_EQ(leases_->PendingWriteCount(), 1u);
    EXPECT_EQ(pool_->Usage(), 0u);

    // The abandoned writer's token now names nothing it can commit.
    auto stale = leases_->TakePendingWrite(kKey, first.value());
    ASSERT_FALSE(stale.has_value());
    EXPECT_EQ(stale.error(), ErrorCode::INVALID_WRITE);
}

// Liveness is a deadline comparison, not table membership: the record survives
// until a scan, but the key is claimable again the moment the clock passes it.
TEST_F(LeaseManagerTest, HasPendingWriteFollowsTheClockNotTableMembership) {
    ASSERT_TRUE(
        leases_->ReservePendingWrite(kKey, Deadline(kLease)).has_value());
    EXPECT_TRUE(leases_->HasPendingWrite(kKey));

    clock_->Advance(kLease + 1ms);

    EXPECT_FALSE(leases_->HasPendingWrite(kKey));
    EXPECT_EQ(leases_->PendingWriteCount(), 1u);
}

TEST_F(LeaseManagerTest, AttachBindsTheTransactionToTheMatchingClaim) {
    auto token = leases_->ReservePendingWrite(kKey, Deadline(kLease));
    ASSERT_TRUE(token.has_value());
    const UUID tiler_id = generate_uuid();

    ASSERT_TRUE(leases_
                    ->AttachPendingWriteTransaction(kKey, token.value(),
                                                    tiler_id, MakeTransaction())
                    .has_value());

    auto taken = leases_->TakePendingWrite(kKey, token.value());
    ASSERT_TRUE(taken.has_value());
    EXPECT_EQ(taken->key, std::string(kKey));
    EXPECT_EQ(taken->write_token, token.value());
    EXPECT_EQ(taken->tiler_id, tiler_id);
    ASSERT_TRUE(taken->transaction.has_value());
    EXPECT_TRUE(
        std::holds_alternative<MutableBlock>(taken->transaction.value()));
}

// A rejected attach owns the only reference to a freshly allocated block. If
// it were dropped on the floor the block would leak for the pool's lifetime,
// since pools never reclaim on their own.
TEST_F(LeaseManagerTest, AttachOnAnUnknownKeyRejectsAndReleasesTheTransaction) {
    const size_t usage_before = pool_->Usage();

    auto attached = leases_->AttachPendingWriteTransaction(
        kOtherKey, generate_uuid(), generate_uuid(), MakeTransaction());
    ASSERT_FALSE(attached.has_value());
    EXPECT_EQ(attached.error(), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(pool_->Usage(), usage_before);
}

TEST_F(LeaseManagerTest, AttachWithAForeignTokenReleasesTheTransaction) {
    auto token = leases_->ReservePendingWrite(kKey, Deadline(kLease));
    ASSERT_TRUE(token.has_value());
    const size_t usage_before = pool_->Usage();

    auto attached = leases_->AttachPendingWriteTransaction(
        kKey, generate_uuid(), generate_uuid(), MakeTransaction());
    ASSERT_FALSE(attached.has_value());
    EXPECT_EQ(attached.error(), ErrorCode::INVALID_WRITE);
    EXPECT_EQ(pool_->Usage(), usage_before);

    // The claim itself is untouched by the intruder.
    EXPECT_TRUE(leases_
                    ->AttachPendingWriteTransaction(
                        kKey, token.value(), generate_uuid(), MakeTransaction())
                    .has_value());
}

TEST_F(LeaseManagerTest,
       TakePendingWriteHandsBackTheRecordForTheMatchingToken) {
    auto token = leases_->ReservePendingWrite(kKey, Deadline(kLease));
    ASSERT_TRUE(token.has_value());

    auto taken = leases_->TakePendingWrite(kKey, token.value());
    ASSERT_TRUE(taken.has_value());
    EXPECT_EQ(taken->write_token, token.value());
    EXPECT_EQ(leases_->PendingWriteCount(), 0u);

    // Taking is a removal, so the same token cannot commit twice.
    auto again = leases_->TakePendingWrite(kKey, token.value());
    ASSERT_FALSE(again.has_value());
    EXPECT_EQ(again.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(LeaseManagerTest, TakeReportsObjectNotFoundWhenTheKeyWasNeverClaimed) {
    auto taken = leases_->TakePendingWrite(kKey, generate_uuid());
    ASSERT_FALSE(taken.has_value());
    EXPECT_EQ(taken.error(), ErrorCode::OBJECT_NOT_FOUND);
}

TEST_F(LeaseManagerTest, TakeReportsLeaseExpiredPastTheDeadlineAndDropsIt) {
    auto token = leases_->ReservePendingWrite(kKey, Deadline(kLease));
    ASSERT_TRUE(token.has_value());
    ASSERT_TRUE(leases_
                    ->AttachPendingWriteTransaction(
                        kKey, token.value(), generate_uuid(), MakeTransaction())
                    .has_value());
    const size_t usage_with_block = pool_->Usage();
    ASSERT_GT(usage_with_block, 0u);

    clock_->Advance(kLease + 1ms);

    auto taken = leases_->TakePendingWrite(kKey, token.value());
    ASSERT_FALSE(taken.has_value());
    // LEASE_EXPIRED, not OBJECT_NOT_FOUND: the code is part of the frozen V1
    // contract and callers distinguish "too late" from "never existed".
    EXPECT_EQ(taken.error(), ErrorCode::LEASE_EXPIRED);
    EXPECT_EQ(leases_->PendingWriteCount(), 0u);
    EXPECT_EQ(pool_->Usage(), 0u);
}

TEST_F(LeaseManagerTest, TakeReportsInvalidWriteOnMismatchAndKeepsTheClaim) {
    auto token = leases_->ReservePendingWrite(kKey, Deadline(kLease));
    ASSERT_TRUE(token.has_value());

    auto taken = leases_->TakePendingWrite(kKey, generate_uuid());
    ASSERT_FALSE(taken.has_value());
    EXPECT_EQ(taken.error(), ErrorCode::INVALID_WRITE);
    // A wrong token must not revoke the rightful writer's claim.
    EXPECT_EQ(leases_->PendingWriteCount(), 1u);
    EXPECT_TRUE(leases_->TakePendingWrite(kKey, token.value()).has_value());
}

// ---------------------------------------------------------------------------
// Pinned read (section 6.4)
// ---------------------------------------------------------------------------

TEST_F(LeaseManagerTest, PinMintsAReadTokenAndIndexesItByToken) {
    auto block = MakeVisibleBlock(kKey);
    ASSERT_TRUE(static_cast<bool>(block));

    const auto deadline = Deadline(kLease);
    auto pinned = leases_->Pin(block, deadline);
    ASSERT_TRUE(pinned.has_value());
    EXPECT_FALSE(IsZeroUUID(pinned->read_token));
    EXPECT_EQ(pinned->ref_count, 1u);
    EXPECT_EQ(pinned->deadline, deadline);
    // by_token is what makes UnPin O(1); it must be populated at insert time,
    // not lazily.
    EXPECT_EQ(leases_->PinnedLeaseCount(), 1u);
    EXPECT_EQ(leases_->PinnedTokenIndexSize(), 1u);
}

TEST_F(LeaseManagerTest,
       RepinningTheSameBlockReusesTheTokenAndBumpsTheRefCount) {
    auto block = MakeVisibleBlock(kKey);
    ASSERT_TRUE(static_cast<bool>(block));

    auto first = leases_->Pin(block, Deadline(kLease));
    ASSERT_TRUE(first.has_value());

    const auto renewed = Deadline(2 * kLease);
    auto second = leases_->Pin(block, renewed);
    ASSERT_TRUE(second.has_value());
    // Same (key, registration, BlockId) is the only case where a token may be
    // reused, and reuse renews rather than duplicates.
    EXPECT_EQ(second->read_token, first->read_token);
    EXPECT_EQ(second->ref_count, 2u);
    EXPECT_EQ(second->deadline, renewed);
    EXPECT_EQ(leases_->PinnedLeaseCount(), 1u);
    EXPECT_EQ(leases_->PinnedTokenIndexSize(), 1u);
}

TEST_F(LeaseManagerTest, LeaseIsReleasedOnlyAfterMatchingUnpins) {
    auto block = MakeVisibleBlock(kKey);
    ASSERT_TRUE(static_cast<bool>(block));

    auto first = leases_->Pin(block, Deadline(kLease));
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(leases_->Pin(block, Deadline(kLease)).has_value());

    ASSERT_TRUE(leases_->Unpin(kKey, first->read_token).has_value());
    // One reader is still holding it; releasing here would hand a live address
    // back to the allocator.
    EXPECT_EQ(leases_->PinnedLeaseCount(), 1u);
    EXPECT_EQ(leases_->PinnedTokenIndexSize(), 1u);

    ASSERT_TRUE(leases_->Unpin(kKey, first->read_token).has_value());
    EXPECT_EQ(leases_->PinnedLeaseCount(), 0u);
    EXPECT_EQ(leases_->PinnedTokenIndexSize(), 0u);
}

// Same key and same registration, different block: a replica in another tiler.
// Reusing the old token here would extend a lease onto a block it was never
// taken against.
TEST_F(LeaseManagerTest, ADifferentBlockForTheSameKeyGetsItsOwnToken) {
    auto handle = registry_.Register(kKey);
    ASSERT_TRUE(handle.has_value());
    auto primary = MakeVisibleBlock(*index_, handle.value(), kPayload);
    auto replica = MakeVisibleBlock(*replica_index_, handle.value(), kPayload);
    ASSERT_TRUE(static_cast<bool>(primary));
    ASSERT_TRUE(static_cast<bool>(replica));
    ASSERT_EQ(primary.Registration(), replica.Registration());
    ASSERT_FALSE(primary.Id() == replica.Id());

    auto first = leases_->Pin(primary, Deadline(kLease));
    auto second = leases_->Pin(replica, Deadline(kLease));
    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_NE(first->read_token, second->read_token);
    EXPECT_EQ(second->ref_count, 1u);
    EXPECT_EQ(leases_->PinnedLeaseCount(), 2u);
    EXPECT_EQ(leases_->PinnedTokenIndexSize(), 2u);
}

// Unpin resolves the token through the shard's own index, so the cross-key
// check is only reachable when both keys land in one shard; a single shard
// makes that certain instead of hash-dependent.
TEST_F(LeaseManagerTest, UnpinRejectsATokenThatBelongsToAnotherKey) {
    LeaseManager single_shard(Config(), clock_, /*shard_count=*/1);

    auto mine = MakeVisibleBlock(kKey);
    auto theirs = MakeVisibleBlock(kOtherKey);
    ASSERT_TRUE(static_cast<bool>(mine));
    ASSERT_TRUE(static_cast<bool>(theirs));
    auto owner = single_shard.Pin(mine, Deadline(kLease));
    ASSERT_TRUE(owner.has_value());
    ASSERT_TRUE(single_shard.Pin(theirs, Deadline(kLease)).has_value());

    auto stolen = single_shard.Unpin(kOtherKey, owner->read_token);
    ASSERT_FALSE(stolen.has_value());
    EXPECT_EQ(stolen.error(), ErrorCode::INVALID_READ);
    // Neither lease may be disturbed by the mismatched call.
    EXPECT_EQ(single_shard.PinnedLeaseCount(), 2u);
    EXPECT_EQ(single_shard.PinnedTokenIndexSize(), 2u);
    EXPECT_TRUE(single_shard.Unpin(kKey, owner->read_token).has_value());
}

// Retries and duplicate cleanup paths call UnPin on tokens that are already
// gone; V1 treated that as success and callers still depend on it.
TEST_F(LeaseManagerTest, UnpinOfAnUnknownTokenIsIdempotentSuccess) {
    EXPECT_TRUE(leases_->Unpin(kKey, generate_uuid()).has_value());

    auto block = MakeVisibleBlock(kKey);
    ASSERT_TRUE(static_cast<bool>(block));
    auto pinned = leases_->Pin(block, Deadline(kLease));
    ASSERT_TRUE(pinned.has_value());
    ASSERT_TRUE(leases_->Unpin(kKey, pinned->read_token).has_value());

    EXPECT_TRUE(leases_->Unpin(kKey, pinned->read_token).has_value());
    EXPECT_EQ(leases_->PinnedLeaseCount(), 0u);
    EXPECT_EQ(leases_->PinnedTokenIndexSize(), 0u);
}

TEST_F(LeaseManagerTest, UnpinPastTheDeadlineExpiresAndClearsBothTables) {
    auto block = MakeVisibleBlock(kKey);
    ASSERT_TRUE(static_cast<bool>(block));
    auto pinned = leases_->Pin(std::move(block), Deadline(kLease));
    ASSERT_TRUE(pinned.has_value());

    clock_->Advance(kLease + 1ms);

    auto released = leases_->Unpin(kKey, pinned->read_token);
    ASSERT_FALSE(released.has_value());
    EXPECT_EQ(released.error(), ErrorCode::LEASE_EXPIRED);
    // The error path still has to remove the record, and from both tables.
    EXPECT_EQ(leases_->PinnedLeaseCount(), 0u);
    EXPECT_EQ(leases_->PinnedTokenIndexSize(), 0u);
}

// records and by_token share one lock and one lifetime; a token left behind by
// the scanner would make a later Unpin dereference a missing record.
TEST_F(LeaseManagerTest, ExpiryScanClearsRecordsAndTokenIndexTogether) {
    auto first = MakeVisibleBlock(kKey);
    auto second = MakeVisibleBlock(kOtherKey);
    ASSERT_TRUE(static_cast<bool>(first));
    ASSERT_TRUE(static_cast<bool>(second));
    ASSERT_TRUE(leases_->Pin(std::move(first), Deadline(kLease)).has_value());
    ASSERT_TRUE(
        leases_->Pin(std::move(second), Deadline(2 * kLease)).has_value());
    ASSERT_TRUE(
        leases_->ReservePendingWrite("lease/key/gamma", Deadline(kLease))
            .has_value());
    ASSERT_EQ(leases_->PinnedLeaseCount(), 2u);

    clock_->Advance(kLease + 1ms);
    EXPECT_EQ(leases_->ScanExpiredNow(), 2u);
    // Only the lease whose deadline passed is gone.
    EXPECT_EQ(leases_->PendingWriteCount(), 0u);
    EXPECT_EQ(leases_->PinnedLeaseCount(), 1u);
    EXPECT_EQ(leases_->PinnedTokenIndexSize(), 1u);

    clock_->Advance(kLease + 1ms);
    EXPECT_EQ(leases_->ScanExpiredNow(), 1u);
    EXPECT_EQ(leases_->PinnedLeaseCount(), 0u);
    EXPECT_EQ(leases_->PinnedTokenIndexSize(), 0u);
}

TEST_F(LeaseManagerTest, StopAndDrainClearsRecordsAndTokenIndexTogether) {
    auto block = MakeVisibleBlock(kKey);
    ASSERT_TRUE(static_cast<bool>(block));
    ASSERT_TRUE(leases_->Pin(std::move(block), Deadline(kLease)).has_value());
    // The pinned block is still indexed, so the only block the drain owns
    // outright is the attached write transaction.
    const size_t usage_without_transaction = pool_->Usage();
    auto token = leases_->ReservePendingWrite(kOtherKey, Deadline(kLease));
    ASSERT_TRUE(token.has_value());
    ASSERT_TRUE(leases_
                    ->AttachPendingWriteTransaction(kOtherKey, token.value(),
                                                    generate_uuid(),
                                                    MakeTransaction())
                    .has_value());
    ASSERT_GT(pool_->Usage(), usage_without_transaction);

    leases_->StopAndDrain();

    EXPECT_EQ(leases_->PendingWriteCount(), 0u);
    EXPECT_EQ(leases_->PinnedLeaseCount(), 0u);
    EXPECT_EQ(leases_->PinnedTokenIndexSize(), 0u);
    // Draining is also the last chance to give an uncommitted block back.
    EXPECT_EQ(pool_->Usage(), usage_without_transaction);
}

// The lease is the block's only keepalive once the index has let go: a reader
// that was handed an address must keep reading valid bytes even though the key
// has been deleted or evicted underneath it.
TEST_F(LeaseManagerTest, PinnedLeaseKeepsItsBlockReadableAfterIndexErase) {
    const size_t baseline = pool_->Usage();
    auto handle = registry_.Register(kKey);
    ASSERT_TRUE(handle.has_value());
    auto block = MakeVisibleBlock(*index_, handle.value(), kPayload);
    ASSERT_TRUE(static_cast<bool>(block));
    const size_t usage_with_block = pool_->Usage();
    ASSERT_GT(usage_with_block, baseline);

    // Non-owning observer: it can prove the entry is alive without keeping it
    // alive, which is exactly what the assertion needs.
    std::weak_ptr<const BlockEntry> observer = block.Entry();
    auto pinned = leases_->Pin(std::move(block), Deadline(kLease));
    ASSERT_TRUE(pinned.has_value());

    auto detached = index_->Erase(handle.value());
    ASSERT_NE(detached, nullptr);
    detached.reset();

    auto alive = observer.lock();
    ASSERT_NE(alive, nullptr);
    {
        ImmutableBlock through_lease(alive);
        std::vector<std::byte> read_back(kPayload.size());
        ASSERT_TRUE(
            through_lease.Read(0, std::span<std::byte>(read_back)).has_value());
        EXPECT_EQ(std::string(reinterpret_cast<const char*>(read_back.data()),
                              read_back.size()),
                  std::string(kPayload));
    }
    alive.reset();
    EXPECT_EQ(pool_->Usage(), usage_with_block);

    ASSERT_TRUE(leases_->Unpin(kKey, pinned->read_token).has_value());
    EXPECT_TRUE(observer.expired());
    EXPECT_EQ(pool_->Usage(), baseline);
}

}  // namespace
}  // namespace mooncake::v2
