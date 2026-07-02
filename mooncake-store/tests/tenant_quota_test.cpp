#include "tenant_quota.h"
#include "tenant_quota_policy_store.h"
#include "types.h"

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#endif

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>
#include <unistd.h>

namespace mooncake {
namespace {

#ifdef STORE_USE_ETCD
constexpr const char* kTenantQuotaEtcdEndpoints = "127.0.0.1:2379";
constexpr std::string_view kTenantQuotaEtcdProbeKey = "tenant_quota_probe";

std::string GetTenantQuotaEtcdEndpoints() {
    const char* endpoints = std::getenv("MOONCAKE_TENANT_QUOTA_ETCD_ENDPOINTS");
    if (endpoints != nullptr && endpoints[0] != '\0') {
        return endpoints;
    }
    return kTenantQuotaEtcdEndpoints;
}
#endif

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

#ifdef STORE_USE_ETCD
std::string PrefixEnd(std::string prefix) {
    for (int i = static_cast<int>(prefix.size()) - 1; i >= 0; --i) {
        unsigned char c = static_cast<unsigned char>(prefix[i]);
        if (c < 0xFF) {
            prefix[i] = static_cast<char>(c + 1);
            prefix.resize(i + 1);
            return prefix;
        }
    }
    return std::string(1, '\0');
}

std::optional<std::string> GetTenantQuotaEtcdSkipReason() {
    const std::string endpoints = GetTenantQuotaEtcdEndpoints();
    ErrorCode error = EtcdHelper::ConnectToEtcdStoreClient(endpoints);
    if (error != ErrorCode::OK) {
        return "Etcd server not reachable at " + endpoints + ": " +
               toString(error);
    }
    std::string value;
    EtcdRevisionId revision_id = 0;
    error =
        EtcdHelper::Get(kTenantQuotaEtcdProbeKey.data(),
                        kTenantQuotaEtcdProbeKey.size(), value, revision_id);
    if (error == ErrorCode::ETCD_OPERATION_ERROR) {
        return "Etcd server not reachable at " + endpoints + ": " +
               toString(error);
    }
    return std::nullopt;
}

void CleanupTenantQuotaEtcdCluster(const std::string& cluster_id) {
    std::string prefix = "mooncake-store/" + cluster_id + "/";
    std::string end = PrefixEnd(prefix);
    (void)EtcdHelper::DeleteRange(prefix.c_str(), prefix.size(), end.c_str(),
                                  end.size());
}
#endif

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

TEST(TenantQuotaPolicyStoreTest, FileFactoryCreatesYamlStore) {
    const auto path = MakeTempPolicyPath("factory-file");
    auto store =
        CreateTenantQuotaPolicyStore("file", path.string(), "test_cluster");
    ASSERT_TRUE(store.has_value()) << store.error();
}

TEST(TenantQuotaPolicyStoreTest, FileFactoryRequiresUri) {
    auto store = CreateTenantQuotaPolicyStore("file", "", "test_cluster");
    ASSERT_FALSE(store.has_value());
    EXPECT_NE(store.error().find("non-empty uri"), std::string::npos);
}

#ifndef STORE_USE_ETCD
TEST(TenantQuotaPolicyStoreTest, EtcdFactoryRequiresStoreUseEtcd) {
    auto store =
        CreateTenantQuotaPolicyStore("etcd", "127.0.0.1:2379", "test_cluster");
    ASSERT_FALSE(store.has_value());
    EXPECT_NE(store.error().find("STORE_USE_ETCD"), std::string::npos);
}
#endif

#ifdef STORE_USE_ETCD
TEST(TenantQuotaPolicyStoreTest, EtcdMissingKeyLoadsEmptySnapshot) {
    if (auto skip_reason = GetTenantQuotaEtcdSkipReason();
        skip_reason.has_value()) {
        GTEST_SKIP() << skip_reason.value();
    }

    const std::string cluster_id =
        "tenant_quota_missing_" + std::to_string(::getpid());
    CleanupTenantQuotaEtcdCluster(cluster_id);

    auto store = CreateTenantQuotaPolicyStore(
        "etcd", GetTenantQuotaEtcdEndpoints(), cluster_id);
    ASSERT_TRUE(store.has_value()) << store.error();

    auto loaded = store.value()->Load();
    ASSERT_TRUE(loaded.has_value()) << loaded.error();
    EXPECT_TRUE(loaded->tenant_quotas.empty());

    CleanupTenantQuotaEtcdCluster(cluster_id);
}

TEST(TenantQuotaPolicyStoreTest, EtcdRoundTripsSnapshot) {
    if (auto skip_reason = GetTenantQuotaEtcdSkipReason();
        skip_reason.has_value()) {
        GTEST_SKIP() << skip_reason.value();
    }

    const std::string cluster_id =
        "tenant_quota_roundtrip_" + std::to_string(::getpid());
    CleanupTenantQuotaEtcdCluster(cluster_id);

    auto store = CreateTenantQuotaPolicyStore(
        "etcd", GetTenantQuotaEtcdEndpoints(), cluster_id);
    ASSERT_TRUE(store.has_value()) << store.error();

    TenantQuotaPolicySnapshot snapshot;
    snapshot.tenant_quotas = {{"tenant-a", 1024}, {"tenant-b", 2048}};
    auto save = store.value()->Save(snapshot);
    ASSERT_TRUE(save.has_value()) << save.error();

    auto loaded = store.value()->Load();
    ASSERT_TRUE(loaded.has_value()) << loaded.error();
    EXPECT_EQ(loaded->tenant_quotas, snapshot.tenant_quotas);

    CleanupTenantQuotaEtcdCluster(cluster_id);
}
#endif

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
