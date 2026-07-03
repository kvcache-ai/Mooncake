#include "tenant_quota.h"
#include "tenant_quota_policy_store.h"
#include "types.h"

#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

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

std::filesystem::path MakeTempPolicyPath(const std::string& suffix) {
    return std::filesystem::temp_directory_path() /
           ("mooncake_tenant_quota_policy_store_test_" +
            std::to_string(::getpid()) + "_" + suffix + ".yaml");
}

void MakeOrphanTenant(TenantQuotaTable* table, const std::string& tenant_id,
                      uint64_t bytes) {
    ASSERT_TRUE(table->UpsertTenantPolicy(tenant_id, bytes).has_value());
    table->RecomputeEffectiveQuotas(bytes);
    ASSERT_TRUE(table->Reserve(tenant_id, bytes).has_value());
    ASSERT_TRUE(table->Commit(tenant_id, bytes).has_value());
    table->EraseTenantPolicy(tenant_id);
}

TEST(TenantQuotaTableTest, NormalizesEmptyExplicitTenantIdToDefault) {
    TenantQuotaTable table;

    ASSERT_TRUE(table.UpsertTenantPolicy("", 1024).has_value());
    table.RecomputeEffectiveQuotas(4096);

    auto snapshot = Snapshot(table, "");
    EXPECT_EQ(snapshot.tenant_id, "default");
    EXPECT_TRUE(snapshot.has_explicit_policy);
    EXPECT_EQ(snapshot.requested_quota_bytes, 1024);
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

TEST(TenantQuotaTableTest, EraseExplicitPolicyCreatesOrphanState) {
    TenantQuotaTable table;
    MakeOrphanTenant(&table, "tenant-a", 40);
    table.RecomputeEffectiveQuotas(1000);

    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_FALSE(snapshot.has_explicit_policy);
    EXPECT_EQ(snapshot.requested_quota_bytes, 0);
    EXPECT_EQ(snapshot.effective_quota_bytes, 0);
    EXPECT_EQ(snapshot.used_bytes, 40);
    EXPECT_EQ(snapshot.committed_count, 1);
    EXPECT_TRUE(snapshot.over_quota);
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

TEST(TenantQuotaTableTest, ExplicitTenantsReceiveRequestedWhenCapacityFits) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-a", 100).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy("tenant-b", 200).has_value());

    table.RecomputeEffectiveQuotas(1000);

    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(table, "tenant-b").effective_quota_bytes, 200);
    EXPECT_EQ(SumEffectiveQuotas(table), 300);
}

TEST(TenantQuotaTableTest, OverCapacityScalesOnlyExplicitTenants) {
    TenantQuotaTable table;
    MakeOrphanTenant(&table, "orphan", 20);
    ASSERT_TRUE(table.UpsertTenantPolicy("b", 200).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy("a", 100).has_value());

    table.RecomputeEffectiveQuotas(150);

    EXPECT_EQ(Snapshot(table, "a").effective_quota_bytes, 50);
    EXPECT_EQ(Snapshot(table, "b").effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(table, "orphan").effective_quota_bytes, 0);
    EXPECT_TRUE(Snapshot(table, "orphan").over_quota);
}

TEST(TenantQuotaTableTest, LazyEmptyOrphansDoNotAppearInList) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy("team-a", 30).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy("ghost", 10).has_value());
    table.EraseTenantPolicy("ghost");

    table.RecomputeEffectiveQuotas(100);

    EXPECT_EQ(Snapshot(table, "team-a").effective_quota_bytes, 30);
    EXPECT_EQ(Snapshot(table, "ghost").effective_quota_bytes, 0);

    auto snapshots = table.ListTenantSnapshots();
    ASSERT_EQ(snapshots.size(), 1);
    EXPECT_EQ(snapshots[0].tenant_id, "team-a");
}

TEST(TenantQuotaPolicyStoreTest, ParsesValidYamlUnits) {
    const char* yaml = R"yaml(
version: 1

tenants:
  - name: tenant-a
    quota: 200GB
  - name: tenant-b
    quota: 500MB
  - name: experiment
    quota: 12345
)yaml";

    auto snapshot = ParseTenantQuotaPolicyYaml(yaml);

    ASSERT_TRUE(snapshot.has_value()) << snapshot.error();
    EXPECT_EQ(snapshot->tenant_quotas.at("tenant-a"),
              200ULL * 1024 * 1024 * 1024);
    EXPECT_EQ(snapshot->tenant_quotas.at("tenant-b"), 500ULL * 1024 * 1024);
    EXPECT_EQ(snapshot->tenant_quotas.at("experiment"), 12345);
}

TEST(TenantQuotaPolicyStoreTest, RejectsInvalidYamlPolicies) {
    std::vector<std::string> invalid_policies = {
        "version: 2\n\ntenants: []\n",
        "version: 1\n\ntenants:\n  - name: tenant-a\n    quota: 1XB\n",
        "version: 1\n\ntenants:\n  - name: tenant-a\n    quota: 0\n",
        "version: 1\n\ntenants:\n  - name: \"\"\n    quota: 1KB\n",
        "version: 1\n\ntenants:\n  - name: _system\n    quota: 1KB\n",
        "version: 1\n\ntenants:\n  - name: \"tenant\\0bad\"\n    quota: "
        "1KB\n",
        "version: 1\n\ntenants:\n  - name: \"tenant\\nline\"\n    quota: "
        "1KB\n",
        "version: 1\n\ntenants:\n  - name: \"tenant\\x7f\"\n    quota: 1KB\n",
        "version: 1\n\ntenants:\n  - name: tenant-a\n    quota: 1KB\n  - name: "
        "tenant-a\n    quota: 2KB\n",
        "version: 1\n\ntenants:\n  - name: tenant-a\n    quota: "
        "18446744073709551616\n",
        "version: 1\n\ntenants:\n  - name: tenant-a\n    quota: "
        "18446744073709551615TB\n",
    };

    for (const auto& policy : invalid_policies) {
        auto snapshot = ParseTenantQuotaPolicyYaml(policy);
        EXPECT_FALSE(snapshot.has_value()) << policy;
    }
}

TEST(TenantQuotaPolicyStoreTest, RoundTripsYamlFile) {
    const auto path = MakeTempPolicyPath("roundtrip");
    std::filesystem::remove(path);

    YamlTenantQuotaPolicyStore store(path.string());
    TenantQuotaPolicySnapshot snapshot;
    snapshot.tenant_quotas = {{"tenant-a", 1024}, {"tenant-b", 2048}};

    auto save = store.Save(snapshot);
    ASSERT_TRUE(save.has_value()) << save.error();

    auto loaded = store.Load();
    ASSERT_TRUE(loaded.has_value()) << loaded.error();
    EXPECT_EQ(loaded->tenant_quotas, snapshot.tenant_quotas);

    std::filesystem::remove(path);
}

TEST(TenantQuotaPolicyStoreTest, RoundTripsYamlSpecialScalarNames) {
    TenantQuotaPolicySnapshot snapshot;
    snapshot.tenant_quotas = {{"foo#bar", 1},
                              {"true", 2},
                              {"[a, b]", 3},
                              {"key: val", 4},
                              {"quote\"slash\\", 5}};

    auto parsed =
        ParseTenantQuotaPolicyYaml(FormatTenantQuotaPolicyYaml(snapshot));

    ASSERT_TRUE(parsed.has_value()) << parsed.error();
    EXPECT_EQ(parsed->tenant_quotas, snapshot.tenant_quotas);
}

TEST(TenantQuotaPolicyStoreTest, SaveFailureReturnsError) {
    const auto path = MakeTempPolicyPath("missing-dir").parent_path() /
                      ("missing_dir_" + std::to_string(::getpid())) /
                      "policy.yaml";
    YamlTenantQuotaPolicyStore store(path.string());

    TenantQuotaPolicySnapshot snapshot;
    snapshot.tenant_quotas = {{"tenant-a", 1024}};

    auto save = store.Save(snapshot);
    EXPECT_FALSE(save.has_value());
}

}  // namespace
}  // namespace mooncake
