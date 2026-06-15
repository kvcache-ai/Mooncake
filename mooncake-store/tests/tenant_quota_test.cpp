#include "tenant_quota.h"
#include "types.h"

#include <limits>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace mooncake {
namespace {

TenantQuotaSnapshot Snapshot(const TenantQuotaTable& table,
                             const std::string& tenant_id) {
    auto snapshot = table.GetTenantSnapshot(tenant_id);
    EXPECT_TRUE(snapshot.has_value());
    return *snapshot;
}

uint64_t SumEffectiveQuotas(const TenantQuotaTable& table) {
    uint64_t sum = 0;
    for (const auto& snapshot : table.ListTenantSnapshots()) {
        sum += snapshot.effective_quota_bytes;
    }
    return sum;
}

void MakeInheritedTenantActive(TenantQuotaTable* table,
                               const std::string& tenant_id,
                               uint64_t capacity) {
    ASSERT_TRUE(table->UpsertTenantPolicy(tenant_id, capacity).has_value());
    table->RecomputeEffectiveQuotas(capacity);
    ASSERT_TRUE(table->Reserve(tenant_id, 1).has_value());
    ASSERT_TRUE(table->Commit(tenant_id, 1).has_value());
    table->EraseTenantPolicy(tenant_id);
}

TEST(TenantQuotaTableTest, NormalizesEmptyTenantIdToDefault) {
    TenantQuotaTable table;

    EXPECT_EQ(NormalizeTenantId(""), "default");
    MakeInheritedTenantActive(&table, "", 1024);
    table.RecomputeEffectiveQuotas(1024);

    auto snapshot = Snapshot(table, "");
    EXPECT_EQ(snapshot.tenant_id, "default");
    EXPECT_EQ(snapshot.effective_quota_bytes, 1024);
}

TEST(TenantQuotaTableTest, RejectsZeroExplicitQuotaWithoutChangingState) {
    TenantQuotaTable table;

    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    auto result = table.UpsertTenantPolicy("tenant-a", 0);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kInvalidArgument);
    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_TRUE(snapshot.has_explicit_policy);
    EXPECT_EQ(snapshot.requested_quota_bytes, 100);
}

TEST(TenantQuotaTableTest, ExplicitPolicyOverridesDefaultAndEraseFallsBack) {
    TenantQuotaTable table;
    table.SetDefaultRequestedQuota(50);
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(1000);

    EXPECT_TRUE(Snapshot(table, "tenant-a").has_explicit_policy);
    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);

    ASSERT_TRUE(table.Reserve("tenant-a", 40).has_value());
    ASSERT_TRUE(table.Commit("tenant-a", 40).has_value());
    table.EraseTenantPolicy("tenant-a");
    table.RecomputeEffectiveQuotas(1000);

    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_FALSE(snapshot.has_explicit_policy);
    EXPECT_EQ(snapshot.requested_quota_bytes, 50);
    EXPECT_EQ(snapshot.effective_quota_bytes, 1000);
    EXPECT_EQ(snapshot.used_bytes, 40);
    EXPECT_EQ(snapshot.committed_count, 1);
}

TEST(TenantQuotaTableTest, EraseMissingPolicyDoesNotCreateLazyState) {
    TenantQuotaTable table;

    table.EraseTenantPolicy("missing");

    EXPECT_FALSE(table.GetTenantSnapshot("missing").has_value());
    EXPECT_TRUE(table.ListTenantSnapshots().empty());
}

TEST(TenantQuotaTableTest, PolicyMutationDoesNotRecomputeEffectiveQuota) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(1000);
    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);

    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 200).has_value());
    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);

    table.RecomputeEffectiveQuotas(1000);
    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 200);
}

TEST(TenantQuotaTableTest,
     DefaultPolicyMutationDoesNotRecomputeEffectiveQuota) {
    TenantQuotaTable table;
    MakeInheritedTenantActive(&table, "default", 1000);
    EXPECT_EQ(Snapshot(table, "default").effective_quota_bytes, 1000);

    table.SetDefaultRequestedQuota(100);
    EXPECT_EQ(Snapshot(table, "default").requested_quota_bytes, 100);
    EXPECT_EQ(Snapshot(table, "default").effective_quota_bytes, 1000);

    table.RecomputeEffectiveQuotas(500);
    EXPECT_EQ(Snapshot(table, "default").effective_quota_bytes, 500);
}

TEST(TenantQuotaTableTest, ListSnapshotsSortedAndSkipsLazyEmptyTenants) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.Reserve("z-empty", 0).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy("b", 10).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy("a", 10).has_value());
    table.RecomputeEffectiveQuotas(100);

    auto snapshots = table.ListTenantSnapshots();
    ASSERT_EQ(snapshots.size(), 2);
    EXPECT_EQ(snapshots[0].tenant_id, "a");
    EXPECT_EQ(snapshots[1].tenant_id, "b");
}

TEST(TenantQuotaTableTest, SingleDefaultTenantReceivesFullCapacity) {
    TenantQuotaTable table;
    MakeInheritedTenantActive(&table, "default", 1234);

    table.RecomputeEffectiveQuotas(1234);

    EXPECT_EQ(Snapshot(table, "default").effective_quota_bytes, 1234);
}

TEST(TenantQuotaTableTest,
     ExplicitTenantsGetRequestedAndDefaultSharesRemainder) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    MakeInheritedTenantActive(&table, "default", 250);

    table.RecomputeEffectiveQuotas(250);

    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(table, "default").effective_quota_bytes, 150);
}

TEST(TenantQuotaTableTest, DefaultTenantsSplitRemainderWithTenantIdTieBreak) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("explicit", 100).has_value());
    MakeInheritedTenantActive(&table, "b", 203);
    MakeInheritedTenantActive(&table, "a", 203);

    table.RecomputeEffectiveQuotas(203);

    EXPECT_EQ(Snapshot(table, "explicit").effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(table, "a").effective_quota_bytes, 52);
    EXPECT_EQ(Snapshot(table, "b").effective_quota_bytes, 51);
    EXPECT_LE(SumEffectiveQuotas(table), 203);
}

TEST(TenantQuotaTableTest, OverCapacityScalesOnlyExplicitTenants) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("b", 200).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy("a", 100).has_value());
    MakeInheritedTenantActive(&table, "default", 150);

    table.RecomputeEffectiveQuotas(150);

    EXPECT_EQ(Snapshot(table, "a").effective_quota_bytes, 50);
    EXPECT_EQ(Snapshot(table, "b").effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(table, "default").effective_quota_bytes, 0);
}

TEST(TenantQuotaTableTest, LeavesRemainderUnallocatedWithoutDefaultTenants) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());

    table.RecomputeEffectiveQuotas(1000);

    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);
    EXPECT_EQ(SumEffectiveQuotas(table), 100);
}

TEST(TenantQuotaTableTest, DefaultUnlimitedTenantDoesNotSqueezeExplicitQuota) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("small", 1).has_value());
    MakeInheritedTenantActive(&table, "default", 10);

    table.RecomputeEffectiveQuotas(10);

    EXPECT_EQ(Snapshot(table, "small").effective_quota_bytes, 1);
    EXPECT_EQ(Snapshot(table, "default").effective_quota_bytes, 9);
}

TEST(TenantQuotaTableTest, LazyEmptyTenantsDoNotDiluteActiveDefaultTenant) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("team-a", 30).has_value());
    MakeInheritedTenantActive(&table, "default", 100);
    ASSERT_TRUE(table.UpsertTenantPolicy("ghost", 10).has_value());
    table.EraseTenantPolicy("ghost");

    table.RecomputeEffectiveQuotas(100);

    EXPECT_EQ(Snapshot(table, "team-a").effective_quota_bytes, 30);
    EXPECT_EQ(Snapshot(table, "default").effective_quota_bytes, 70);
    EXPECT_EQ(Snapshot(table, "ghost").effective_quota_bytes, 0);

    auto snapshots = table.ListTenantSnapshots();
    ASSERT_EQ(snapshots.size(), 2);
    EXPECT_EQ(snapshots[0].tenant_id, "default");
    EXPECT_EQ(snapshots[1].tenant_id, "team-a");
}

TEST(TenantQuotaTableTest, LargestRemainderTieBreakUsesTenantId) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("b", 1).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy("a", 1).has_value());

    table.RecomputeEffectiveQuotas(1);

    EXPECT_EQ(Snapshot(table, "a").effective_quota_bytes, 1);
    EXPECT_EQ(Snapshot(table, "b").effective_quota_bytes, 0);
}

TEST(TenantQuotaTableTest, CapacityShrinkAndGrowthRefreshOverQuota) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(100);
    ASSERT_TRUE(table.Reserve("tenant-a", 80).has_value());
    ASSERT_TRUE(table.Commit("tenant-a", 80).has_value());

    table.RecomputeEffectiveQuotas(50);
    EXPECT_TRUE(Snapshot(table, "tenant-a").over_quota);

    table.RecomputeEffectiveQuotas(100);
    EXPECT_FALSE(Snapshot(table, "tenant-a").over_quota);
}

TEST(TenantQuotaTableTest, LargeValuesDoNotOverflowDuringRecompute) {
    TenantQuotaTable table;
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    ASSERT_TRUE(table.UpsertTenantPolicy("a", max).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy("b", max).has_value());

    table.RecomputeEffectiveQuotas(max);

    EXPECT_EQ(Snapshot(table, "a").effective_quota_bytes, max / 2 + max % 2);
    EXPECT_EQ(Snapshot(table, "b").effective_quota_bytes, max / 2);
    EXPECT_LE(SumEffectiveQuotas(table), max);
}

TEST(TenantQuotaTableTest, ReserveCommitUpdatesAccounting) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(100);

    ASSERT_TRUE(table.Reserve("tenant-a", 40).has_value());
    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_EQ(snapshot.reserved_bytes, 40);
    EXPECT_EQ(snapshot.used_bytes, 0);

    ASSERT_TRUE(table.Commit("tenant-a", 40).has_value());
    snapshot = Snapshot(table, "tenant-a");
    EXPECT_EQ(snapshot.reserved_bytes, 0);
    EXPECT_EQ(snapshot.used_bytes, 40);
    EXPECT_EQ(snapshot.committed_count, 1);
}

TEST(TenantQuotaTableTest, ReserveOverQuotaDoesNotModifyState) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(100);
    ASSERT_TRUE(table.Reserve("tenant-a", 80).has_value());

    auto before = Snapshot(table, "tenant-a");
    auto result = table.Reserve("tenant-a", 21);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kQuotaExceeded);
    auto after = Snapshot(table, "tenant-a");
    EXPECT_EQ(after.reserved_bytes, before.reserved_bytes);
    EXPECT_EQ(after.used_bytes, before.used_bytes);
    EXPECT_EQ(after.committed_count, before.committed_count);
}

TEST(TenantQuotaTableTest, ReserveUsesOverflowSafeHeadroomCheck) {
    TenantQuotaTable table;
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", max).has_value());
    table.RecomputeEffectiveQuotas(max);
    ASSERT_TRUE(table.Reserve("tenant-a", max).has_value());
    ASSERT_TRUE(table.Commit("tenant-a", max).has_value());

    auto before = Snapshot(table, "tenant-a");
    auto result = table.Reserve("tenant-a", 1);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kQuotaExceeded);
    auto after = Snapshot(table, "tenant-a");
    EXPECT_EQ(after.used_bytes, before.used_bytes);
    EXPECT_EQ(after.reserved_bytes, before.reserved_bytes);
    EXPECT_EQ(after.committed_count, before.committed_count);
}

TEST(TenantQuotaTableTest, ReserveMissingTenantDoesNotCreateStateOnFailure) {
    TenantQuotaTable table;

    auto result = table.Reserve("missing", 1);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kQuotaExceeded);
    EXPECT_FALSE(table.GetTenantSnapshot("missing").has_value());
    EXPECT_TRUE(table.ListTenantSnapshots().empty());
}

TEST(TenantQuotaTableTest, ReserveAbortReleasesReservation) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(100);

    ASSERT_TRUE(table.Reserve("tenant-a", 40).has_value());
    ASSERT_TRUE(table.Abort("tenant-a", 40).has_value());

    EXPECT_EQ(Snapshot(table, "tenant-a").reserved_bytes, 0);
}

TEST(TenantQuotaTableTest,
     CommitWithoutEnoughReservationDoesNotModifyStateAndReportsMismatch) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(100);
    ASSERT_TRUE(table.Reserve("tenant-a", 5).has_value());

    auto before = Snapshot(table, "tenant-a");
    auto result = table.Commit("tenant-a", 10);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kAccountingMismatch);
    auto after = Snapshot(table, "tenant-a");
    EXPECT_EQ(after.reserved_bytes, before.reserved_bytes);
    EXPECT_EQ(after.used_bytes, before.used_bytes);
    EXPECT_EQ(after.committed_count, before.committed_count);
}

TEST(TenantQuotaTableTest,
     AbortWithoutEnoughReservationDoesNotModifyStateAndReportsMismatch) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(100);
    ASSERT_TRUE(table.Reserve("tenant-a", 5).has_value());

    auto before = Snapshot(table, "tenant-a");
    auto result = table.Abort("tenant-a", 10);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kAccountingMismatch);
    auto after = Snapshot(table, "tenant-a");
    EXPECT_EQ(after.reserved_bytes, before.reserved_bytes);
    EXPECT_EQ(after.used_bytes, before.used_bytes);
    EXPECT_EQ(after.committed_count, before.committed_count);
}

TEST(TenantQuotaTableTest,
     ReleaseWithoutEnoughUsedDoesNotModifyStateAndReportsMismatch) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(100);
    ASSERT_TRUE(table.Reserve("tenant-a", 5).has_value());
    ASSERT_TRUE(table.Commit("tenant-a", 5).has_value());

    auto before = Snapshot(table, "tenant-a");
    auto result = table.Release("tenant-a", 10);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kAccountingMismatch);
    auto after = Snapshot(table, "tenant-a");
    EXPECT_EQ(after.reserved_bytes, before.reserved_bytes);
    EXPECT_EQ(after.used_bytes, before.used_bytes);
    EXPECT_EQ(after.committed_count, before.committed_count);
}

TEST(TenantQuotaTableTest, ReleasePartialDoesNotChangeCommittedCount) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(100);
    ASSERT_TRUE(table.Reserve("tenant-a", 50).has_value());
    ASSERT_TRUE(table.Commit("tenant-a", 50).has_value());

    ASSERT_TRUE(table.ReleasePartial("tenant-a", 20).has_value());

    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_EQ(snapshot.used_bytes, 30);
    EXPECT_EQ(snapshot.committed_count, 1);
}

TEST(TenantQuotaTableTest,
     ReleasePartialUnderflowReportsMismatchInReleaseBuild) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(100);

#ifdef NDEBUG
    auto before = Snapshot(table, "tenant-a");
    auto result = table.ReleasePartial("tenant-a", 10);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kAccountingMismatch);
    auto after = Snapshot(table, "tenant-a");
    EXPECT_EQ(after.reserved_bytes, before.reserved_bytes);
    EXPECT_EQ(after.used_bytes, before.used_bytes);
    EXPECT_EQ(after.committed_count, before.committed_count);
#else
    EXPECT_DEATH({ (void)table.ReleasePartial("tenant-a", 10); }, "");
#endif
}

TEST(TenantQuotaTableTest, ZeroByteAccountingOperationsAreNoOpSuccess) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    table.RecomputeEffectiveQuotas(100);

    EXPECT_TRUE(table.Reserve("tenant-a", 0).has_value());
    EXPECT_TRUE(table.Commit("tenant-a", 0).has_value());
    EXPECT_TRUE(table.Abort("tenant-a", 0).has_value());
    EXPECT_TRUE(table.Release("tenant-a", 0).has_value());
    EXPECT_TRUE(table.ReleasePartial("tenant-a", 0).has_value());

    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_EQ(snapshot.used_bytes, 0);
    EXPECT_EQ(snapshot.reserved_bytes, 0);
    EXPECT_EQ(snapshot.committed_count, 0);
}

}  // namespace
}  // namespace mooncake
