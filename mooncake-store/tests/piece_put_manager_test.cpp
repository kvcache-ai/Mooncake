#include "piece_put_manager.h"

#include <atomic>
#include <chrono>
#include <limits>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake::test {
namespace {

using namespace std::chrono_literals;
const TenantId kTenant("tenant-a");
const UUID kClientA{1, 1};
const UUID kClientB{1, 2};

TEST(PiecePutManagerTest, ReservesCapacityBeforeAllocationAndRollsBack) {
    PiecePutManager manager(1, 4);
    auto reserved = manager.InsertLocked(kTenant, "a", {});
    ASSERT_TRUE(reserved.has_value());
    EXPECT_EQ(manager.size(), 1);
    EXPECT_EQ((*reserved)->generation, 0);
    EXPECT_TRUE(
        manager.SnapshotExpired(PiecePutManager::TimePoint::max()).empty());
    auto full = manager.InsertLocked(kTenant, "b", {});
    ASSERT_FALSE(full.has_value());
    EXPECT_EQ(full.error(), ErrorCode::NO_AVAILABLE_HANDLE);
    ASSERT_TRUE(manager.EraseLocked(kTenant, "a", 0));
    EXPECT_TRUE(manager.empty());
    EXPECT_TRUE(manager.InsertLocked(kTenant, "b", {}).has_value());
}

TEST(PiecePutManagerTest, TenantKeyPairsCannotAlias) {
    PiecePutManager manager;
    auto a = manager.InsertLocked(TenantId("ab"), "c", {});
    auto b = manager.InsertLocked(TenantId("a"), "bc", {});
    auto c = manager.InsertLocked(TenantId("other"), "c", {});
    ASSERT_TRUE(a.has_value());
    ASSERT_TRUE(b.has_value());
    ASSERT_TRUE(c.has_value());
    EXPECT_NE(*a, *b);
    EXPECT_NE(*a, *c);
    auto duplicate = manager.InsertLocked(TenantId("ab"), "c", {});
    ASSERT_FALSE(duplicate.has_value());
    EXPECT_EQ(duplicate.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    EXPECT_EQ(manager.size(), 3);
}

TEST(PiecePutManagerTest, DeadlineSnapshotSkipsProvisionalAndFutureRounds) {
    PiecePutManager manager;
    const auto now = std::chrono::system_clock::now();
    auto due = manager.InsertLocked(kTenant, "due", {});
    auto future = manager.InsertLocked(kTenant, "future", {});
    ASSERT_TRUE(due.has_value());
    ASSERT_TRUE(future.has_value());
    ASSERT_TRUE(manager.InsertLocked(kTenant, "provisional", {}).has_value());
    manager.InitializeLocked(**due, 7, now);
    manager.InitializeLocked(**future, 8, now + 1s);
    auto snapshot = manager.SnapshotExpired(now);
    ASSERT_EQ(snapshot.size(), 1);
    EXPECT_EQ(snapshot.front().tenant_id, kTenant);
    EXPECT_EQ(snapshot.front().key, "due");
    EXPECT_EQ(snapshot.front().generation, 7);
    EXPECT_EQ(manager.SnapshotExpired(now + 1s).size(), 2);
    EXPECT_EQ(manager.size(), 3);
}

TEST(PiecePutManagerTest, StaleCleanupCannotEraseReplacement) {
    PiecePutManager manager;
    const auto deadline = std::chrono::system_clock::now();
    auto first = manager.InsertLocked(kTenant, "key", {});
    ASSERT_TRUE(first.has_value());
    manager.InitializeLocked(**first, 7, deadline);
    auto snapshot = manager.SnapshotExpired(deadline);
    ASSERT_EQ(snapshot.size(), 1);
    ASSERT_TRUE(manager.EraseLocked(kTenant, "key", 7));
    auto next = manager.InsertLocked(kTenant, "key", {});
    ASSERT_TRUE(next.has_value());
    manager.InitializeLocked(**next, 8, deadline + 1s);
    EXPECT_FALSE(manager.EraseLocked(snapshot.front().tenant_id,
                                     snapshot.front().key,
                                     snapshot.front().generation));
    EXPECT_EQ(manager.FindLocked(kTenant, "key"), *next);
}

TEST(PiecePutManagerTest, OutOfOrderAdjacentClaimsAllowHolesUntilFilled) {
    PiecePutManager manager;
    PiecePutRound round;
    ASSERT_TRUE(manager.ClaimLocked(round, kClientB, 1, 4, 4, 12).has_value());
    ASSERT_TRUE(manager.ClaimLocked(round, kClientA, 1, 0, 4, 12).has_value());
    EXPECT_EQ(round.spans.size(), 2);
    EXPECT_EQ(round.completed_bytes, 0);
    ASSERT_TRUE(manager.ClaimLocked(round, kClientA, 2, 8, 4, 12).has_value());
    EXPECT_EQ(round.spans.size(), 3);
    EXPECT_FALSE(round.spans.back().completed);
}

TEST(PiecePutManagerTest, OriginalOperationReplaysWithoutAddingSpan) {
    PiecePutManager manager(1, 1);
    PiecePutRound round;
    auto claim = manager.ClaimLocked(round, kClientA, 3, 2, 3, 8);
    ASSERT_TRUE(claim.has_value());
    (*claim)->completed = true;
    round.completed_bytes = 3;
    auto replay = manager.ClaimLocked(round, kClientA, 3, 2, 3, 8);
    ASSERT_TRUE(replay.has_value());
    EXPECT_EQ(*claim, *replay);
    EXPECT_TRUE((*replay)->completed);
    EXPECT_EQ(round.completed_bytes, 3);
    EXPECT_EQ(round.spans.size(), 1);
    auto changed = manager.ClaimLocked(round, kClientA, 3, 2, 2, 8);
    ASSERT_FALSE(changed.has_value());
    EXPECT_EQ(changed.error(), ErrorCode::INVALID_PARAMS);
    auto limit = manager.ClaimLocked(round, kClientB, 3, 5, 3, 8);
    ASSERT_FALSE(limit.has_value());
    EXPECT_EQ(limit.error(), ErrorCode::NO_AVAILABLE_HANDLE);
}

TEST(PiecePutManagerTest, DifferentOperationsCannotOverlapOrStealClaim) {
    PiecePutManager manager;
    PiecePutRound round;
    ASSERT_TRUE(manager.ClaimLocked(round, kClientA, 1, 3, 4, 12).has_value());
    for (const auto& range : std::vector<std::pair<uint64_t, uint64_t>>{
             {0, 4}, {3, 4}, {4, 1}, {6, 3}, {0, 12}}) {
        auto conflict = manager.ClaimLocked(round, kClientB, 1, range.first,
                                            range.second, 12);
        ASSERT_FALSE(conflict.has_value());
        EXPECT_EQ(conflict.error(), ErrorCode::OBJECT_ALREADY_EXISTS);
    }
    EXPECT_EQ(round.spans.size(), 1);
    EXPECT_EQ(PiecePutManager::FindSpanLocked(round, kClientB, 1), nullptr);
    EXPECT_NE(PiecePutManager::FindSpanLocked(round, kClientA, 1), nullptr);
}

TEST(PiecePutManagerTest, InvalidRangesNeverChangeProgress) {
    PiecePutManager manager;
    PiecePutRound round;
    const auto max = std::numeric_limits<uint64_t>::max();
    for (const auto& range : std::vector<std::pair<uint64_t, uint64_t>>{
             {0, 0}, {8, 1}, {7, 2}, {max, 2}, {2, max}}) {
        auto result = manager.ClaimLocked(round, kClientA, 1, range.first,
                                          range.second, 8);
        ASSERT_FALSE(result.has_value());
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }
    EXPECT_FALSE(manager.ClaimLocked(round, kClientA, 0, 0, 8, 8).has_value());
    EXPECT_TRUE(round.spans.empty());
    EXPECT_EQ(round.completed_bytes, 0);
    EXPECT_TRUE(
        manager.ClaimLocked(round, kClientA, 1, max - 1, 1, max).has_value());
    EXPECT_TRUE(
        manager.ClaimLocked(round, kClientB, 1, 0, max - 1, max).has_value());
}

TEST(PiecePutManagerTest, BorrowedRoundSurvivesOtherKeyRehash) {
    PiecePutManager manager(2048, 4);
    auto original = manager.InsertLocked(kTenant, "original", {});
    ASSERT_TRUE(original.has_value());
    manager.InitializeLocked(**original, 7, PiecePutManager::TimePoint::max());
    (*original)->write_id = "computation";
    for (size_t i = 0; i < 1024; ++i) {
        ASSERT_TRUE(
            manager.InsertLocked(kTenant, std::to_string(i), {}).has_value());
    }
    EXPECT_EQ(manager.FindLocked(kTenant, "original"), *original);
    EXPECT_EQ((*original)->write_id, "computation");
    EXPECT_EQ((*original)->generation, 7);
}

TEST(PiecePutManagerTest, ConcurrentDifferentKeysAndTimeoutSnapshots) {
    PiecePutManager manager(512, 4);
    std::atomic<bool> failed{false};
    std::atomic<bool> done{false};
    const auto now = std::chrono::system_clock::now();
    // Every writer has exclusive ownership of its keys, equivalent to holding
    // distinct existing key guards. The sweeper only snapshots identity.
    std::thread sweeper([&] {
        while (!done.load()) {
            for (const auto& entry : manager.SnapshotExpired(now)) {
                if (entry.generation == 0) failed.store(true);
            }
        }
    });
    std::vector<std::thread> workers;
    for (size_t thread = 0; thread < 4; ++thread) {
        workers.emplace_back([&, thread] {
            for (size_t i = 0; i < 100; ++i) {
                const auto key =
                    std::to_string(thread) + ":" + std::to_string(i);
                auto inserted = manager.InsertLocked(kTenant, key, {});
                if (!inserted) {
                    failed.store(true);
                    continue;
                }
                manager.InitializeLocked(**inserted, i + 1, now);
                auto claim =
                    manager.ClaimLocked(**inserted, kClientA, 1, 0, 8, 8);
                if (!claim || !manager.EraseLocked(kTenant, key, i + 1)) {
                    failed.store(true);
                }
            }
        });
    }
    for (auto& worker : workers) worker.join();
    done.store(true);
    sweeper.join();
    EXPECT_FALSE(failed.load());
    EXPECT_TRUE(manager.empty());
}

TEST(PiecePutManagerTest, CommitHandleWireRoundTripPreservesIdentity) {
    PiecePutFinishRequest request;
    auto& handle = request.handle;
    handle.key = "kv:block";
    handle.tenant_id = "tenant-a";
    handle.master_epoch = kClientA;
    handle.generation = 7;
    handle.write_start_ticks = 12345;
    handle.write_id = "request:attempt1";
    handle.client_id = kClientB;
    handle.op_id = 9;
    handle.offset = 4;
    handle.length = 8;
    request.applied = true;
    const auto wire = struct_pack::serialize(request);
    auto restored = struct_pack::deserialize<PiecePutFinishRequest>(wire);
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(restored->handle.key, handle.key);
    EXPECT_EQ(restored->handle.master_epoch, handle.master_epoch);
    EXPECT_EQ(restored->handle.generation, handle.generation);
    EXPECT_EQ(restored->handle.write_start_ticks, handle.write_start_ticks);
    EXPECT_EQ(restored->handle.write_id, handle.write_id);
    EXPECT_EQ(restored->handle.client_id, handle.client_id);
    EXPECT_EQ(restored->handle.op_id, handle.op_id);
    EXPECT_EQ(restored->handle.offset, handle.offset);
    EXPECT_EQ(restored->handle.length, handle.length);
    EXPECT_EQ(restored->handle.tenant_id, handle.tenant_id);
    EXPECT_TRUE(restored->applied);
}

}  // namespace
}  // namespace mooncake::test
