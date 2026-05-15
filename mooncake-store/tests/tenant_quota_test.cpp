#include "tenant_quota.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

namespace mooncake::test {

class TenantQuotaTableTest : public ::testing::Test {
   protected:
    static constexpr uint64_t kOneMiB = 1024 * 1024;
    static constexpr uint64_t kOneGiB = 1024 * 1024 * 1024;
};

// --- Policy CRUD ---

TEST_F(TenantQuotaTableTest, DefaultPolicyIsUnlimited) {
    TenantQuotaTable table;
    auto policy = table.GetDefaultPolicy();
    EXPECT_EQ(policy.max_bytes, 0);
}

TEST_F(TenantQuotaTableTest, ConstructWithDefaultPolicy) {
    TenantQuotaPolicy default_policy{kOneGiB};
    TenantQuotaTable table(default_policy);
    EXPECT_EQ(table.GetDefaultPolicy().max_bytes, kOneGiB);
}

TEST_F(TenantQuotaTableTest, UpsertAndGetPolicy) {
    TenantQuotaTable table;
    TenantQuotaPolicy policy{kOneGiB};
    table.UpsertPolicy("tenant_a", policy);

    auto result = table.GetPolicy("tenant_a");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->max_bytes, kOneGiB);
}

TEST_F(TenantQuotaTableTest, GetPolicyReturnsNulloptForUnknownTenant) {
    TenantQuotaTable table;
    auto result = table.GetPolicy("nonexistent");
    EXPECT_FALSE(result.has_value());
}

TEST_F(TenantQuotaTableTest, ErasePolicyReturnsTrueIfExists) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneGiB});
    EXPECT_TRUE(table.ErasePolicy("tenant_a"));
    EXPECT_FALSE(table.GetPolicy("tenant_a").has_value());
}

TEST_F(TenantQuotaTableTest, ErasePolicyReturnsFalseIfNotExists) {
    TenantQuotaTable table;
    EXPECT_FALSE(table.ErasePolicy("nonexistent"));
}

TEST_F(TenantQuotaTableTest, UpsertPolicyIsIdempotent) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneGiB});
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{2 * kOneGiB});
    auto result = table.GetPolicy("tenant_a");
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result->max_bytes, 2 * kOneGiB);
}

TEST_F(TenantQuotaTableTest, ListAllReturnsAllEntries) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneGiB});
    table.UpsertPolicy("tenant_b", TenantQuotaPolicy{2 * kOneGiB});

    auto snapshots = table.ListAll();
    EXPECT_EQ(snapshots.size(), 2);
}

TEST_F(TenantQuotaTableTest, SetDefaultPolicyUpdatesNonExplicitEntries) {
    TenantQuotaTable table;
    // Create an entry without explicit policy by reserving
    table.Reserve("tenant_implicit", 100);
    table.Commit("tenant_implicit", 100);

    // Create an entry with explicit policy
    table.UpsertPolicy("tenant_explicit", TenantQuotaPolicy{kOneGiB});

    // Update default policy
    table.SetDefaultPolicy(TenantQuotaPolicy{kOneMiB});

    // Verify explicit policy unchanged
    auto explicit_policy = table.GetPolicy("tenant_explicit");
    ASSERT_TRUE(explicit_policy.has_value());
    EXPECT_EQ(explicit_policy->max_bytes, kOneGiB);

    // Default policy should be updated
    EXPECT_EQ(table.GetDefaultPolicy().max_bytes, kOneMiB);
}

// --- Reserve / Commit / Abort ---

TEST_F(TenantQuotaTableTest, ReserveSucceedsWhenUnlimited) {
    TenantQuotaTable table;
    auto result = table.Reserve("tenant_a", kOneGiB);
    EXPECT_EQ(result, TenantQuotaTable::ReserveResult::kOk);
}

TEST_F(TenantQuotaTableTest, ReserveSucceedsWithinQuota) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});
    auto result = table.Reserve("tenant_a", kOneMiB / 2);
    EXPECT_EQ(result, TenantQuotaTable::ReserveResult::kOk);
}

TEST_F(TenantQuotaTableTest, ReserveFailsWhenExceedsQuota) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});
    auto result = table.Reserve("tenant_a", kOneMiB + 1);
    EXPECT_EQ(result, TenantQuotaTable::ReserveResult::kQuotaExceeded);
}

TEST_F(TenantQuotaTableTest, ReserveAccountsForExistingUsage) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});

    // First reserve + commit
    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB / 2),
              TenantQuotaTable::ReserveResult::kOk);
    table.Commit("tenant_a", kOneMiB / 2);

    // Second reserve should fail (used + new > max)
    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB / 2 + 1),
              TenantQuotaTable::ReserveResult::kQuotaExceeded);

    // But exactly remaining should succeed
    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB / 2),
              TenantQuotaTable::ReserveResult::kOk);
}

TEST_F(TenantQuotaTableTest, ReserveAccountsForPendingReservations) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});

    // First reserve (not yet committed)
    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB / 2),
              TenantQuotaTable::ReserveResult::kOk);

    // Second reserve should account for first (reserved + new > max)
    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB / 2 + 1),
              TenantQuotaTable::ReserveResult::kQuotaExceeded);
}

TEST_F(TenantQuotaTableTest, AbortFreesReservedBytes) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});

    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB),
              TenantQuotaTable::ReserveResult::kOk);

    // Should fail now
    EXPECT_EQ(table.Reserve("tenant_a", 1),
              TenantQuotaTable::ReserveResult::kQuotaExceeded);

    // Abort frees up space
    table.Abort("tenant_a", kOneMiB);

    // Now should succeed again
    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB),
              TenantQuotaTable::ReserveResult::kOk);
}

TEST_F(TenantQuotaTableTest, CommitMovesFromReservedToUsed) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});

    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB),
              TenantQuotaTable::ReserveResult::kOk);
    table.Commit("tenant_a", kOneMiB);

    auto snapshot = table.GetSnapshot("tenant_a");
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->state.used_bytes, kOneMiB);
    EXPECT_EQ(snapshot->state.reserved_bytes, 0);
    EXPECT_EQ(snapshot->state.committed_count, 1);
}

TEST_F(TenantQuotaTableTest, ReserveZeroBytesAlwaysSucceeds) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});
    // Fill up quota
    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB),
              TenantQuotaTable::ReserveResult::kOk);
    // Zero bytes should still succeed
    EXPECT_EQ(table.Reserve("tenant_a", 0),
              TenantQuotaTable::ReserveResult::kOk);
}

// --- Release ---

TEST_F(TenantQuotaTableTest, ReleaseFreesUsedBytes) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});

    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB),
              TenantQuotaTable::ReserveResult::kOk);
    table.Commit("tenant_a", kOneMiB);

    // Quota is now full
    EXPECT_EQ(table.Reserve("tenant_a", 1),
              TenantQuotaTable::ReserveResult::kQuotaExceeded);

    // Release some
    table.Release("tenant_a", kOneMiB / 2);

    // Should be able to reserve released amount
    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB / 2),
              TenantQuotaTable::ReserveResult::kOk);
}

TEST_F(TenantQuotaTableTest, ReleaseDecrementsCommittedCount) {
    TenantQuotaTable table;
    table.Reserve("tenant_a", 100);
    table.Commit("tenant_a", 100);
    table.Reserve("tenant_a", 200);
    table.Commit("tenant_a", 200);

    auto snapshot = table.GetSnapshot("tenant_a");
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->state.committed_count, 2);

    table.Release("tenant_a", 100);
    snapshot = table.GetSnapshot("tenant_a");
    EXPECT_EQ(snapshot->state.committed_count, 1);
    EXPECT_EQ(snapshot->state.used_bytes, 200);
}

TEST_F(TenantQuotaTableTest, ReleaseOnUnknownTenantIsNoop) {
    TenantQuotaTable table;
    // Should not crash
    table.Release("nonexistent", 100);
}

TEST_F(TenantQuotaTableTest, ReleaseZeroBytesIsNoop) {
    TenantQuotaTable table;
    table.Reserve("tenant_a", 100);
    table.Commit("tenant_a", 100);
    table.Release("tenant_a", 0);

    auto snapshot = table.GetSnapshot("tenant_a");
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->state.used_bytes, 100);
}

// --- ComputeEvictTarget ---

TEST_F(TenantQuotaTableTest, ComputeEvictTargetReturnsZeroWhenUnlimited) {
    TenantQuotaTable table;
    table.Reserve("tenant_a", kOneMiB);
    table.Commit("tenant_a", kOneMiB);
    EXPECT_EQ(table.ComputeEvictTarget("tenant_a", kOneGiB), 0);
}

TEST_F(TenantQuotaTableTest, ComputeEvictTargetReturnsCorrectAmount) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});
    table.Reserve("tenant_a", kOneMiB);
    table.Commit("tenant_a", kOneMiB);

    // Need to evict (used + incoming - max) bytes
    uint64_t target = table.ComputeEvictTarget("tenant_a", 100);
    EXPECT_EQ(target, 100);
}

TEST_F(TenantQuotaTableTest, ComputeEvictTargetReturnsZeroWhenFits) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});
    table.Reserve("tenant_a", kOneMiB / 2);
    table.Commit("tenant_a", kOneMiB / 2);

    EXPECT_EQ(table.ComputeEvictTarget("tenant_a", kOneMiB / 2), 0);
}

TEST_F(TenantQuotaTableTest, ComputeEvictTargetAccountsForReserved) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});
    table.Reserve("tenant_a", kOneMiB / 2);
    // reserved=512K, used=0, incoming=600K → need evict 100K+
    uint64_t target = table.ComputeEvictTarget("tenant_a", kOneMiB / 2 + 100);
    EXPECT_EQ(target, 100);
}

// --- ResetUsage / AccumulateUsage ---

TEST_F(TenantQuotaTableTest, ResetUsageClearsAllState) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneGiB});
    table.Reserve("tenant_a", kOneMiB);
    table.Commit("tenant_a", kOneMiB);

    table.ResetUsage();

    auto snapshot = table.GetSnapshot("tenant_a");
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->state.used_bytes, 0);
    EXPECT_EQ(snapshot->state.reserved_bytes, 0);
    EXPECT_EQ(snapshot->state.committed_count, 0);
    // Policy should be preserved
    EXPECT_EQ(snapshot->policy.max_bytes, kOneGiB);
}

TEST_F(TenantQuotaTableTest, AccumulateUsageAddsToExisting) {
    TenantQuotaTable table;
    table.AccumulateUsage("tenant_a", 100);
    table.AccumulateUsage("tenant_a", 200);

    auto snapshot = table.GetSnapshot("tenant_a");
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->state.used_bytes, 300);
    EXPECT_EQ(snapshot->state.committed_count, 2);
}

// --- Cross-tenant isolation ---

TEST_F(TenantQuotaTableTest, DifferentTenantsAreIsolated) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});
    table.UpsertPolicy("tenant_b", TenantQuotaPolicy{kOneMiB});

    // Fill tenant_a
    EXPECT_EQ(table.Reserve("tenant_a", kOneMiB),
              TenantQuotaTable::ReserveResult::kOk);
    table.Commit("tenant_a", kOneMiB);

    // tenant_b should still have full quota
    EXPECT_EQ(table.Reserve("tenant_b", kOneMiB),
              TenantQuotaTable::ReserveResult::kOk);
}

TEST_F(TenantQuotaTableTest, DefaultTenantUsesDefaultPolicy) {
    TenantQuotaPolicy default_policy{kOneMiB};
    TenantQuotaTable table(default_policy);

    // No explicit policy set, but default applies
    EXPECT_EQ(table.Reserve("default", kOneMiB),
              TenantQuotaTable::ReserveResult::kOk);
    table.Commit("default", kOneMiB);

    // Should exceed now
    EXPECT_EQ(table.Reserve("default", 1),
              TenantQuotaTable::ReserveResult::kQuotaExceeded);
}

// --- ErasePolicy with alive objects ---

TEST_F(TenantQuotaTableTest, ErasePolicyKeepsUsageButFallsBackToDefault) {
    TenantQuotaPolicy default_policy{0};  // unlimited
    TenantQuotaTable table(default_policy);
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});

    table.Reserve("tenant_a", kOneMiB / 2);
    table.Commit("tenant_a", kOneMiB / 2);

    // Erase policy -> falls back to default (unlimited)
    EXPECT_TRUE(table.ErasePolicy("tenant_a"));

    // With unlimited default, should succeed
    EXPECT_EQ(table.Reserve("tenant_a", kOneGiB),
              TenantQuotaTable::ReserveResult::kOk);
}

// --- Concurrency ---

TEST_F(TenantQuotaTableTest, ConcurrentReserveCommitAbort) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{0});  // unlimited

    constexpr int kNumThreads = 8;
    constexpr int kOpsPerThread = 1000;
    std::atomic<int> successful_reserves{0};

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);

    for (int i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([&table, &successful_reserves]() {
            for (int j = 0; j < kOpsPerThread; ++j) {
                auto result = table.Reserve("tenant_a", 100);
                if (result == TenantQuotaTable::ReserveResult::kOk) {
                    successful_reserves.fetch_add(1);
                    if (j % 2 == 0) {
                        table.Commit("tenant_a", 100);
                    } else {
                        table.Abort("tenant_a", 100);
                    }
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // All reserves should succeed (unlimited quota)
    EXPECT_EQ(successful_reserves.load(), kNumThreads * kOpsPerThread);

    // Final state: half committed, half aborted
    auto snapshot = table.GetSnapshot("tenant_a");
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->state.reserved_bytes, 0);
    // Each thread commits kOpsPerThread/2 times, each 100 bytes
    uint64_t expected_used =
        static_cast<uint64_t>(kNumThreads) * (kOpsPerThread / 2) * 100;
    EXPECT_EQ(snapshot->state.used_bytes, expected_used);
}

TEST_F(TenantQuotaTableTest, ConcurrentReserveWithLimitedQuota) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});

    constexpr int kNumThreads = 8;
    constexpr int kOpsPerThread = 500;
    constexpr uint64_t kReserveSize = 1024;  // 1 KiB per op
    std::atomic<int> successful_reserves{0};
    std::atomic<int> failed_reserves{0};

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);

    for (int i = 0; i < kNumThreads; ++i) {
        threads.emplace_back(
            [&table, &successful_reserves, &failed_reserves]() {
                for (int j = 0; j < kOpsPerThread; ++j) {
                    auto result = table.Reserve("tenant_a", kReserveSize);
                    if (result == TenantQuotaTable::ReserveResult::kOk) {
                        successful_reserves.fetch_add(1);
                        table.Commit("tenant_a", kReserveSize);
                    } else {
                        failed_reserves.fetch_add(1);
                    }
                }
            });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    auto snapshot = table.GetSnapshot("tenant_a");
    ASSERT_TRUE(snapshot.has_value());
    // Used bytes should not exceed quota
    EXPECT_LE(snapshot->state.used_bytes, kOneMiB);
    // Total should sum up
    EXPECT_EQ(successful_reserves.load() + failed_reserves.load(),
              kNumThreads * kOpsPerThread);
    // Used bytes should equal successful_reserves * kReserveSize
    EXPECT_EQ(snapshot->state.used_bytes,
              static_cast<uint64_t>(successful_reserves.load()) * kReserveSize);
}

TEST_F(TenantQuotaTableTest, ConcurrentMultipleTenants) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneMiB});
    table.UpsertPolicy("tenant_b", TenantQuotaPolicy{kOneMiB});

    constexpr int kOpsPerThread = 500;
    constexpr uint64_t kReserveSize = 512;

    auto worker = [&table](const std::string& tenant_id) {
        for (int j = 0; j < kOpsPerThread; ++j) {
            auto result = table.Reserve(tenant_id, kReserveSize);
            if (result == TenantQuotaTable::ReserveResult::kOk) {
                table.Commit(tenant_id, kReserveSize);
            }
        }
    };

    std::thread thread_a(worker, "tenant_a");
    std::thread thread_b(worker, "tenant_b");

    thread_a.join();
    thread_b.join();

    auto snapshot_a = table.GetSnapshot("tenant_a");
    auto snapshot_b = table.GetSnapshot("tenant_b");
    ASSERT_TRUE(snapshot_a.has_value());
    ASSERT_TRUE(snapshot_b.has_value());

    // Each tenant's usage should be independent and ≤ quota
    EXPECT_LE(snapshot_a->state.used_bytes, kOneMiB);
    EXPECT_LE(snapshot_b->state.used_bytes, kOneMiB);
    EXPECT_EQ(snapshot_a->state.reserved_bytes, 0);
    EXPECT_EQ(snapshot_b->state.reserved_bytes, 0);
}

TEST_F(TenantQuotaTableTest, ConcurrentPolicyCRUD) {
    TenantQuotaTable table;

    constexpr int kNumThreads = 4;
    constexpr int kOpsPerThread = 200;

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);

    for (int i = 0; i < kNumThreads; ++i) {
        threads.emplace_back([&table, i]() {
            std::string tenant_id = "tenant_" + std::to_string(i);
            for (int j = 0; j < kOpsPerThread; ++j) {
                table.UpsertPolicy(tenant_id,
                                   TenantQuotaPolicy{static_cast<uint64_t>(j)});
                table.GetPolicy(tenant_id);
                table.ListAll();
                if (j % 3 == 0) {
                    table.ErasePolicy(tenant_id);
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    // Should not crash — correctness is guaranteed by the mutex
}

// --- GetSnapshot ---

TEST_F(TenantQuotaTableTest, GetSnapshotReturnsNulloptForUnknown) {
    TenantQuotaTable table;
    EXPECT_FALSE(table.GetSnapshot("nonexistent").has_value());
}

TEST_F(TenantQuotaTableTest, GetSnapshotReturnsCorrectState) {
    TenantQuotaTable table;
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneGiB});
    table.Reserve("tenant_a", 300);
    table.Commit("tenant_a", 300);
    table.Reserve("tenant_a", 200);

    auto snapshot = table.GetSnapshot("tenant_a");
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->tenant_id, "tenant_a");
    EXPECT_EQ(snapshot->policy.max_bytes, kOneGiB);
    EXPECT_EQ(snapshot->state.used_bytes, 300);
    EXPECT_EQ(snapshot->state.reserved_bytes, 200);
    EXPECT_EQ(snapshot->state.committed_count, 1);
}

// --- Frontier API ---

namespace {

std::chrono::system_clock::time_point MakeLeaseAt(int64_t epoch_ms) {
    return std::chrono::system_clock::time_point{
        std::chrono::milliseconds{epoch_ms}};
}

}  // namespace

TEST_F(TenantQuotaTableTest, IndexFrontierIgnoresKNoneAndZeroSize) {
    TenantQuotaTable table;
    auto lease = MakeLeaseAt(1000);
    table.IndexFrontier("tenant_a", "k1", lease, 0, FrontierBucket::kNoPin);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 0);
    table.IndexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNone);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 0);
}

TEST_F(TenantQuotaTableTest, IndexFrontierInsertsIntoCorrectBucket) {
    TenantQuotaTable table;
    auto lease = MakeLeaseAt(1000);
    table.IndexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin);
    table.IndexFrontier("tenant_a", "k2", lease, 200, FrontierBucket::kSoftPin);

    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 1);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kSoftPin), 1);
}

TEST_F(TenantQuotaTableTest, UnindexFrontierRemovesExactSnapshot) {
    TenantQuotaTable table;
    auto lease = MakeLeaseAt(1000);
    table.IndexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin);
    table.UnindexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 0);
}

TEST_F(TenantQuotaTableTest, UnindexFrontierMismatchedSnapshotIsNoop) {
    TenantQuotaTable table;
    auto lease = MakeLeaseAt(1000);
    table.IndexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin);
    // Different lease snapshot must not delete; this models a stale call
    // arriving after the entry has been replaced with a fresher snapshot.
    auto different_lease = MakeLeaseAt(2000);
    table.UnindexFrontier("tenant_a", "k1", different_lease, 100,
                          FrontierBucket::kNoPin);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 1);
}

TEST_F(TenantQuotaTableTest, UnindexFrontierKNoneIsNoop) {
    TenantQuotaTable table;
    auto lease = MakeLeaseAt(1000);
    table.IndexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin);
    table.UnindexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNone);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 1);
}

TEST_F(TenantQuotaTableTest, MoveFrontierBetweenBuckets) {
    TenantQuotaTable table;
    auto lease = MakeLeaseAt(1000);
    table.IndexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin);

    table.MoveFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin,
                       FrontierBucket::kSoftPin);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 0);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kSoftPin), 1);

    table.MoveFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kSoftPin,
                       FrontierBucket::kNoPin);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 1);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kSoftPin), 0);
}

TEST_F(TenantQuotaTableTest, MoveFrontierIdenticalBucketsIsNoop) {
    TenantQuotaTable table;
    auto lease = MakeLeaseAt(1000);
    table.IndexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin);
    table.MoveFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin,
                       FrontierBucket::kNoPin);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 1);
}

TEST_F(TenantQuotaTableTest, MoveFrontierFromKNoneInsertsOnly) {
    TenantQuotaTable table;
    auto lease = MakeLeaseAt(1000);
    table.MoveFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNone,
                       FrontierBucket::kNoPin);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 1);
}

TEST_F(TenantQuotaTableTest, MoveFrontierToKNoneRemovesOnly) {
    TenantQuotaTable table;
    auto lease = MakeLeaseAt(1000);
    table.IndexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin);
    table.MoveFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin,
                       FrontierBucket::kNone);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 0);
}

TEST_F(TenantQuotaTableTest, DropStaleFrontierDelegatesToUnindex) {
    TenantQuotaTable table;
    auto lease = MakeLeaseAt(1000);
    table.IndexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin);
    table.DropStaleFrontier("tenant_a", "k1", lease, 100,
                            FrontierBucket::kNoPin);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 0);
}

TEST_F(TenantQuotaTableTest, SnapshotTopKReturnsSortedTopCandidates) {
    TenantQuotaTable table;
    // Insert 3 entries with distinct lease_timeouts; expected order is
    // ascending lease_timeout (oldest first).
    table.IndexFrontier("tenant_a", "k_newest", MakeLeaseAt(3000), 100,
                        FrontierBucket::kNoPin);
    table.IndexFrontier("tenant_a", "k_oldest", MakeLeaseAt(1000), 100,
                        FrontierBucket::kNoPin);
    table.IndexFrontier("tenant_a", "k_mid", MakeLeaseAt(2000), 100,
                        FrontierBucket::kNoPin);

    auto top2 = table.SnapshotTopK("tenant_a", FrontierBucket::kNoPin, 2);
    ASSERT_EQ(top2.size(), 2u);
    EXPECT_EQ(top2[0].key, "k_oldest");
    EXPECT_EQ(top2[1].key, "k_mid");
}

TEST_F(TenantQuotaTableTest, SnapshotTopKSecondarySortBySizeDesc) {
    TenantQuotaTable table;
    // Same lease_timeout, different sizes. Larger size must sort first
    // (neg_size ASC === size DESC).
    auto lease = MakeLeaseAt(1000);
    table.IndexFrontier("tenant_a", "k_small", lease, 100,
                        FrontierBucket::kNoPin);
    table.IndexFrontier("tenant_a", "k_large", lease, 1000,
                        FrontierBucket::kNoPin);
    table.IndexFrontier("tenant_a", "k_mid", lease, 500,
                        FrontierBucket::kNoPin);

    auto top3 = table.SnapshotTopK("tenant_a", FrontierBucket::kNoPin, 3);
    ASSERT_EQ(top3.size(), 3u);
    EXPECT_EQ(top3[0].key, "k_large");
    EXPECT_EQ(top3[1].key, "k_mid");
    EXPECT_EQ(top3[2].key, "k_small");
}

TEST_F(TenantQuotaTableTest, SnapshotTopKEmptyForUnknownTenant) {
    TenantQuotaTable table;
    auto top = table.SnapshotTopK("nonexistent", FrontierBucket::kNoPin, 10);
    EXPECT_TRUE(top.empty());
}

TEST_F(TenantQuotaTableTest, SnapshotTopKZeroBatchReturnsEmpty) {
    TenantQuotaTable table;
    table.IndexFrontier("tenant_a", "k1", MakeLeaseAt(1000), 100,
                        FrontierBucket::kNoPin);
    auto top = table.SnapshotTopK("tenant_a", FrontierBucket::kNoPin, 0);
    EXPECT_TRUE(top.empty());
}

TEST_F(TenantQuotaTableTest, SnapshotTopKHonoursBatchSizeCap) {
    TenantQuotaTable table;
    for (int i = 0; i < 10; ++i) {
        table.IndexFrontier("tenant_a", "k_" + std::to_string(i),
                            MakeLeaseAt(1000 + i), 100, FrontierBucket::kNoPin);
    }
    auto top = table.SnapshotTopK("tenant_a", FrontierBucket::kNoPin, 4);
    EXPECT_EQ(top.size(), 4u);
}

TEST_F(TenantQuotaTableTest, FrontiersPerTenantAreIsolated) {
    TenantQuotaTable table;
    auto lease = MakeLeaseAt(1000);
    table.IndexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin);
    table.IndexFrontier("tenant_b", "k1", lease, 100, FrontierBucket::kNoPin);

    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 1);
    EXPECT_EQ(table.FrontierSize("tenant_b", FrontierBucket::kNoPin), 1);

    // Removing on tenant_a must not affect tenant_b.
    table.UnindexFrontier("tenant_a", "k1", lease, 100, FrontierBucket::kNoPin);
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 0);
    EXPECT_EQ(table.FrontierSize("tenant_b", FrontierBucket::kNoPin), 1);
}

TEST_F(TenantQuotaTableTest, ConcurrentIndexUnindexFrontier) {
    TenantQuotaTable table;
    constexpr int kNumThreads = 8;
    constexpr int kInsertsPerThread = 1000;

    std::vector<std::thread> threads;
    threads.reserve(kNumThreads);
    for (int t = 0; t < kNumThreads; ++t) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < kInsertsPerThread; ++i) {
                std::string key =
                    "k_" + std::to_string(t) + "_" + std::to_string(i);
                auto lease = MakeLeaseAt(1000 + i);
                table.IndexFrontier("tenant_a", key, lease, 100,
                                    FrontierBucket::kNoPin);
                table.UnindexFrontier("tenant_a", key, lease, 100,
                                      FrontierBucket::kNoPin);
            }
        });
    }
    for (auto& th : threads) th.join();

    // Every insert paired with an unindex of the same snapshot triple, so
    // the frontier must drain back to empty regardless of interleaving.
    EXPECT_EQ(table.FrontierSize("tenant_a", FrontierBucket::kNoPin), 0);
}

// --- Snapshot / restore helpers ---

TEST_F(TenantQuotaTableTest, ListExplicitPoliciesEmpty) {
    TenantQuotaTable table(TenantQuotaPolicy{kOneGiB});
    auto policies = table.ListExplicitPolicies();
    EXPECT_TRUE(policies.empty());
}

TEST_F(TenantQuotaTableTest, ListExplicitPoliciesIgnoresImplicitTenants) {
    TenantQuotaTable table(TenantQuotaPolicy{kOneGiB});
    // Implicit entry created by Reserve/Commit traffic only.
    table.Reserve("tenant_implicit", 100);
    table.Commit("tenant_implicit", 100);
    table.UpsertPolicy("tenant_explicit", TenantQuotaPolicy{kOneMiB});

    auto policies = table.ListExplicitPolicies();
    ASSERT_EQ(policies.size(), 1u);
    EXPECT_EQ(policies[0].first, "tenant_explicit");
    EXPECT_EQ(policies[0].second.max_bytes, kOneMiB);
}

TEST_F(TenantQuotaTableTest, RestorePoliciesReplacesDefaultAndExplicit) {
    TenantQuotaTable table(TenantQuotaPolicy{kOneMiB});
    table.UpsertPolicy("tenant_keep", TenantQuotaPolicy{kOneGiB});
    table.UpsertPolicy("tenant_drop", TenantQuotaPolicy{2 * kOneGiB});

    std::vector<std::pair<std::string, TenantQuotaPolicy>> snapshot_policies = {
        {"tenant_keep", TenantQuotaPolicy{4 * kOneGiB}},
        {"tenant_new", TenantQuotaPolicy{8 * kOneGiB}},
    };
    table.RestorePolicies(TenantQuotaPolicy{16 * kOneGiB}, snapshot_policies);

    EXPECT_EQ(table.GetDefaultPolicy().max_bytes, 16 * kOneGiB);

    auto keep = table.GetPolicy("tenant_keep");
    ASSERT_TRUE(keep.has_value());
    EXPECT_EQ(keep->max_bytes, 4 * kOneGiB);

    auto fresh = table.GetPolicy("tenant_new");
    ASSERT_TRUE(fresh.has_value());
    EXPECT_EQ(fresh->max_bytes, 8 * kOneGiB);

    // tenant_drop was explicit before but is missing from the snapshot, so
    // it must lose its explicit status. With zero state it disappears
    // entirely from the policy listing.
    EXPECT_FALSE(table.GetPolicy("tenant_drop").has_value());
}

TEST_F(TenantQuotaTableTest, RestorePoliciesPreservesUsageAndFrontier) {
    TenantQuotaTable table;
    table.Reserve("tenant_busy", kOneMiB);
    table.Commit("tenant_busy", kOneMiB);
    auto lease = std::chrono::system_clock::now() + std::chrono::seconds(1);
    table.IndexFrontier("tenant_busy", "key1", lease, 1024,
                        FrontierBucket::kNoPin);

    std::vector<std::pair<std::string, TenantQuotaPolicy>> snapshot_policies = {
        {"tenant_busy", TenantQuotaPolicy{kOneGiB}},
    };
    table.RestorePolicies(TenantQuotaPolicy{0}, snapshot_policies);

    // RestorePolicies must not touch state or frontier; the live workload
    // continues to consume bytes that were already reserved/committed.
    auto snapshot = table.GetSnapshot("tenant_busy");
    ASSERT_TRUE(snapshot.has_value());
    EXPECT_EQ(snapshot->state.used_bytes, kOneMiB);
    EXPECT_EQ(snapshot->policy.max_bytes, kOneGiB);
    EXPECT_EQ(table.FrontierSize("tenant_busy", FrontierBucket::kNoPin), 1u);
}

TEST_F(TenantQuotaTableTest, RestorePoliciesEmptyClearsAllExplicit) {
    TenantQuotaTable table(TenantQuotaPolicy{kOneMiB});
    table.UpsertPolicy("tenant_a", TenantQuotaPolicy{kOneGiB});
    table.UpsertPolicy("tenant_b", TenantQuotaPolicy{2 * kOneGiB});

    table.RestorePolicies(TenantQuotaPolicy{kOneGiB}, {});

    EXPECT_EQ(table.GetDefaultPolicy().max_bytes, kOneGiB);
    EXPECT_FALSE(table.GetPolicy("tenant_a").has_value());
    EXPECT_FALSE(table.GetPolicy("tenant_b").has_value());
    EXPECT_TRUE(table.ListExplicitPolicies().empty());
}

}  // namespace mooncake::test
