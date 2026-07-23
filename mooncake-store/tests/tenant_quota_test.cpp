#include "tenant_quota.h"
#include "tenant_quota_policy_store.h"
#include "types.h"

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#endif

#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <functional>
#include <limits>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
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

template <typename Table>
TenantQuotaSnapshot Snapshot(const Table& table, const std::string& tenant_id) {
    auto snapshot = table.GetTenantSnapshot(TenantId(tenant_id));
    EXPECT_TRUE(snapshot.has_value());
    return *snapshot;
}

template <typename Table>
uint64_t SumEffectiveQuotas(const Table& table) {
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
    const TenantId canonical_tenant(tenant_id);
    ASSERT_TRUE(table->UpsertTenantPolicy(canonical_tenant, bytes).has_value());
    table->RecomputeEffectiveQuotas(bytes);
    ASSERT_TRUE(table->Reserve(canonical_tenant, bytes).has_value());
    ASSERT_TRUE(table->Commit(canonical_tenant, bytes).has_value());
    table->ApplyTenantPolicies({});
}

TEST(TenantQuotaTableTest, NormalizesEmptyExplicitTenantIdToDefault) {
    TenantQuotaTable table;

    ASSERT_TRUE(table.UpsertTenantPolicy(TenantId(""), 1024).has_value());
    table.RecomputeEffectiveQuotas(4096);

    auto snapshot = Snapshot(table, "");
    EXPECT_EQ(snapshot.tenant_id, TenantId::Default());
    EXPECT_TRUE(snapshot.has_explicit_policy);
    EXPECT_EQ(snapshot.requested_quota_bytes, 1024);
    EXPECT_EQ(snapshot.effective_quota_bytes, 1024);
}

TEST(TenantQuotaTableTest, RejectsZeroExplicitQuotaWithoutChangingState) {
    TenantQuotaTable table;

    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 100).has_value());
    auto result = table.UpsertTenantPolicy(tenant_id, 0);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kInvalidArgument);
    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_TRUE(snapshot.has_explicit_policy);
    EXPECT_EQ(snapshot.requested_quota_bytes, 100);
}

TEST(TenantQuotaTableTest, ApplyPoliciesCreatesOrphanState) {
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

TEST(TenantQuotaTableTest, ApplyPoliciesReplacesCanonicalPolicySet) {
    TenantQuotaTable table;
    const TenantId tenant_a("tenant-a");
    const TenantId tenant_b("tenant-b");
    const TenantId tenant_c("tenant-c");
    table.ApplyTenantPolicies({{tenant_a, 100}, {tenant_b, 200}});
    table.IncrementMetadataObjectCount(tenant_a);

    table.ApplyTenantPolicies({{tenant_b, 300}, {tenant_c, 400}});

    EXPECT_FALSE(table.IsTenantRegistered(tenant_a));
    EXPECT_TRUE(table.IsTenantRegistered(tenant_b));
    EXPECT_TRUE(table.IsTenantRegistered(tenant_c));
    EXPECT_TRUE(Snapshot(table, tenant_a.value()).over_quota);
    EXPECT_EQ(table.GetTenantPolicies(),
              (TenantQuotaPolicyMap{{tenant_b, 300}, {tenant_c, 400}}));
}

TEST(TenantQuotaTableTest, DisableMissingPolicyDoesNotCreateLazyState) {
    TenantQuotaTable table;

    auto result = table.DisableTenantPolicyIfEmpty(TenantId("missing"));

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kTenantNotFound);
    EXPECT_FALSE(table.GetTenantSnapshot(TenantId("missing")).has_value());
    EXPECT_TRUE(table.ListTenantSnapshots().empty());
}

TEST(TenantQuotaTableTest, PolicyMutationDoesNotRecomputeEffectiveQuota) {
    TenantQuotaTable table;
    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 100).has_value());
    table.RecomputeEffectiveQuotas(1000);
    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);

    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 200).has_value());
    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);

    table.RecomputeEffectiveQuotas(1000);
    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 200);
}

TEST(TenantQuotaTableTest, ListSnapshotsSortedAndCleansLazyEmptyTenants) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy(TenantId("z-empty"), 10).has_value());
    ASSERT_TRUE(
        table.DisableTenantPolicyIfEmpty(TenantId("z-empty")).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy(TenantId("b"), 10).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy(TenantId("a"), 10).has_value());
    table.RecomputeEffectiveQuotas(100);

    auto snapshots = table.ListTenantSnapshots();
    ASSERT_EQ(snapshots.size(), 2);
    EXPECT_EQ(snapshots[0].tenant_id, TenantId("a"));
    EXPECT_EQ(snapshots[1].tenant_id, TenantId("b"));
}

TEST(TenantQuotaTableTest, ExplicitTenantsReceiveRequestedWhenCapacityFits) {
    TenantQuotaTable table;
    ASSERT_TRUE(
        table.UpsertTenantPolicy(TenantId("tenant-a"), 100).has_value());
    ASSERT_TRUE(
        table.UpsertTenantPolicy(TenantId("tenant-b"), 200).has_value());

    table.RecomputeEffectiveQuotas(1000);

    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(table, "tenant-b").effective_quota_bytes, 200);
    EXPECT_EQ(SumEffectiveQuotas(table), 300);
}

TEST(TenantQuotaTableTest, OverCapacityScalesOnlyExplicitTenants) {
    TenantQuotaTable table;
    MakeOrphanTenant(&table, "orphan", 20);
    ASSERT_TRUE(table.UpsertTenantPolicy(TenantId("b"), 200).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy(TenantId("a"), 100).has_value());

    table.RecomputeEffectiveQuotas(150);

    EXPECT_EQ(Snapshot(table, "a").effective_quota_bytes, 50);
    EXPECT_EQ(Snapshot(table, "b").effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(table, "orphan").effective_quota_bytes, 0);
    EXPECT_TRUE(Snapshot(table, "orphan").over_quota);
}

TEST(TenantQuotaTableTest, LazyEmptyOrphansDoNotAppearInList) {
    TenantQuotaTable table;
    ASSERT_TRUE(table.UpsertTenantPolicy(TenantId("team-a"), 30).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy(TenantId("ghost"), 10).has_value());
    ASSERT_TRUE(
        table.DisableTenantPolicyIfEmpty(TenantId("ghost")).has_value());

    table.RecomputeEffectiveQuotas(100);

    EXPECT_EQ(Snapshot(table, "team-a").effective_quota_bytes, 30);
    EXPECT_FALSE(table.GetTenantSnapshot(TenantId("ghost")).has_value());

    auto snapshots = table.ListTenantSnapshots();
    ASSERT_EQ(snapshots.size(), 1);
    EXPECT_EQ(snapshots[0].tenant_id, TenantId("team-a"));
}

TEST(TenantQuotaTableTest, ReserveRequiresRegisteredTenantIncludingZeroBytes) {
    TenantQuotaTable table;

    auto regular = table.Reserve(TenantId("missing"), 1);
    auto zero = table.Reserve(TenantId("missing"), 0);

    ASSERT_FALSE(regular.has_value());
    ASSERT_FALSE(zero.has_value());
    EXPECT_EQ(regular.error(), TenantQuotaError::kTenantNotRegistered);
    EXPECT_EQ(zero.error(), TenantQuotaError::kTenantNotRegistered);
}

TEST(TenantQuotaTableTest, TracksAdditionalCommitAndMetadataCount) {
    TenantQuotaTable table;
    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 300).has_value());
    table.RecomputeEffectiveQuotas(300);

    ASSERT_TRUE(table.Reserve(tenant_id, 200).has_value());
    ASSERT_TRUE(table.Commit(tenant_id, 100).has_value());
    ASSERT_TRUE(table.CommitAdditional(tenant_id, 100).has_value());
    table.IncrementMetadataObjectCount(tenant_id);

    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_EQ(snapshot.used_bytes, 200);
    EXPECT_EQ(snapshot.reserved_bytes, 0);
    EXPECT_EQ(snapshot.committed_count, 1);
    EXPECT_EQ(snapshot.metadata_object_count, 1);
}

TEST(TenantQuotaTableTest, AccountingMismatchDoesNotMutateState) {
    TenantQuotaTable table;
    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 100).has_value());
    table.RecomputeEffectiveQuotas(100);
    ASSERT_TRUE(table.Reserve(tenant_id, 10).has_value());

    auto commit = table.Commit(tenant_id, 11);
    auto abort = table.Abort(tenant_id, 11);
    auto before_commit = Snapshot(table, "tenant-a");
    ASSERT_FALSE(commit.has_value());
    ASSERT_FALSE(abort.has_value());
    EXPECT_EQ(commit.error(), TenantQuotaError::kAccountingMismatch);
    EXPECT_EQ(abort.error(), TenantQuotaError::kAccountingMismatch);
    EXPECT_EQ(before_commit.used_bytes, 0);
    EXPECT_EQ(before_commit.reserved_bytes, 10);

    ASSERT_TRUE(table.Commit(tenant_id, 10).has_value());
    auto release = table.Release(tenant_id, 11);
    auto partial = table.ReleasePartial(tenant_id, 11);
    ASSERT_FALSE(release.has_value());
    ASSERT_FALSE(partial.has_value());
    EXPECT_EQ(release.error(), TenantQuotaError::kAccountingMismatch);
    EXPECT_EQ(partial.error(), TenantQuotaError::kAccountingMismatch);
    auto after_commit = Snapshot(table, "tenant-a");
    EXPECT_EQ(after_commit.used_bytes, 10);
    EXPECT_EQ(after_commit.committed_count, 1);

    table.RebuildUsage({{tenant_id,
                         {.used_bytes = 10,
                          .committed_count = 0,
                          .metadata_object_count = 1}}});
    auto inconsistent_release = table.Release(tenant_id, 5);
    ASSERT_FALSE(inconsistent_release.has_value());
    EXPECT_EQ(inconsistent_release.error(),
              TenantQuotaError::kAccountingMismatch);
    auto after_inconsistent_release = Snapshot(table, "tenant-a");
    EXPECT_EQ(after_inconsistent_release.used_bytes, 10);
    EXPECT_EQ(after_inconsistent_release.committed_count, 0);
}

TEST(TenantQuotaTableTest, DisablePolicyRejectsNonEmptyTenant) {
    TenantQuotaTable table;
    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 100).has_value());
    table.RecomputeEffectiveQuotas(100);
    ASSERT_TRUE(table.Reserve(tenant_id, 1).has_value());

    auto result = table.DisableTenantPolicyIfEmpty(tenant_id);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kTenantNotEmpty);
    EXPECT_TRUE(table.IsTenantRegistered(tenant_id));
}

TEST(TenantQuotaTableTest, RebuildUsageCreatesAndRemovesOrphans) {
    TenantQuotaTable table;
    const TenantId explicit_tenant("tenant-a");
    const TenantId orphan("orphan");
    ASSERT_TRUE(table.UpsertTenantPolicy(explicit_tenant, 100).has_value());

    TenantQuotaUsageMap usage{
        {explicit_tenant,
         {.used_bytes = 40, .committed_count = 1, .metadata_object_count = 1}},
        {orphan,
         {.used_bytes = 20, .committed_count = 1, .metadata_object_count = 1}},
    };
    table.RebuildUsage(usage);
    table.RecomputeEffectiveQuotas(100);

    EXPECT_TRUE(Snapshot(table, "tenant-a").has_explicit_policy);
    EXPECT_FALSE(Snapshot(table, "orphan").has_explicit_policy);
    EXPECT_TRUE(Snapshot(table, "orphan").over_quota);

    table.RebuildUsage({});
    EXPECT_FALSE(table.GetTenantSnapshot(orphan).has_value());
    EXPECT_TRUE(table.GetTenantSnapshot(explicit_tenant).has_value());
}

TEST(TenantQuotaTableTest, OverflowChecksDoNotWrapAccounting) {
    TenantQuotaTable table;
    const TenantId tenant_id("tenant-a");
    const uint64_t max = std::numeric_limits<uint64_t>::max();
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, max).has_value());
    table.RebuildUsage({{tenant_id,
                         {.used_bytes = max - 5,
                          .committed_count = max,
                          .metadata_object_count = max}}});
    table.RecomputeEffectiveQuotas(max);

    EXPECT_EQ(table.ComputeDeficit(tenant_id, 10), 5);
    auto overflow_reserve = table.Reserve(tenant_id, 10);
    ASSERT_FALSE(overflow_reserve.has_value());
    EXPECT_EQ(overflow_reserve.error(), TenantQuotaError::kQuotaExceeded);

    ASSERT_TRUE(table.Reserve(tenant_id, 5).has_value());
    ASSERT_TRUE(table.Commit(tenant_id, 5).has_value());
    table.IncrementMetadataObjectCount(tenant_id);

    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_EQ(snapshot.used_bytes, max);
    EXPECT_EQ(snapshot.reserved_bytes, 0);
    EXPECT_EQ(snapshot.committed_count, max);
    EXPECT_EQ(snapshot.metadata_object_count, max);
}

TEST(ShardedTenantQuotaTableTest, ConcurrentReserveNeverExceedsQuota) {
    ShardedTenantQuotaTable<8> table;
    const TenantId tenant_id("tenant-a");
    table.ApplyTenantPolicies({{tenant_id, 1000}}, 1000);

    std::atomic<int> successes = 0;
    std::vector<std::thread> workers;
    for (int i = 0; i < 20; ++i) {
        workers.emplace_back([&] {
            if (table.Reserve(tenant_id, 100).has_value()) {
                ++successes;
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    EXPECT_EQ(successes.load(), 10);
    EXPECT_EQ(Snapshot(table, "tenant-a").reserved_bytes, 1000);
}

TEST(ShardedTenantQuotaTableTest, DifferentShardsUpdateIndependently) {
    using TestTable = ShardedTenantQuotaTable<2>;
    const TenantId tenant_a("tenant-a");
    TenantId tenant_b("tenant-b");
    for (int suffix = 0; TenantIdHash{}(tenant_a) % TestTable::kNumShards ==
                         TenantIdHash{}(tenant_b) % TestTable::kNumShards;
         ++suffix) {
        tenant_b = TenantId("tenant-b-" + std::to_string(suffix));
    }

    TestTable table;
    table.ApplyTenantPolicies({{tenant_a, 1000}, {tenant_b, 1000}}, 2000);

    std::atomic<int> failures = 0;
    auto update = [&](const TenantId& tenant_id) {
        for (int i = 0; i < 1000; ++i) {
            if (!table.Reserve(tenant_id, 1) || !table.Abort(tenant_id, 1)) {
                ++failures;
            }
        }
    };
    std::thread first(update, std::cref(tenant_a));
    std::thread second(update, std::cref(tenant_b));
    first.join();
    second.join();

    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(Snapshot(table, tenant_a.value()).reserved_bytes, 0);
    EXPECT_EQ(Snapshot(table, tenant_b.value()).reserved_bytes, 0);
}

TEST(ShardedTenantQuotaTableTest,
     DisabledPolicyRejectsRegularAndZeroByteReservations) {
    ShardedTenantQuotaTable<8> table;
    const TenantId tenant_id("tenant-a");
    table.ApplyTenantPolicies({{tenant_id, 100}}, 100);
    ASSERT_TRUE(table.DisableTenantPolicyIfEmpty(tenant_id).has_value());

    auto regular = table.Reserve(tenant_id, 1);
    auto zero = table.Reserve(tenant_id, 0);

    ASSERT_FALSE(regular.has_value());
    ASSERT_FALSE(zero.has_value());
    EXPECT_EQ(regular.error(), TenantQuotaError::kTenantNotRegistered);
    EXPECT_EQ(zero.error(), TenantQuotaError::kTenantNotRegistered);
}

TEST(ShardedTenantQuotaTableTest, RecomputeCanRunWithAccounting) {
    ShardedTenantQuotaTable<8> table;
    const TenantId tenant_id("tenant-a");
    table.ApplyTenantPolicies({{tenant_id, 1000}}, 1000);

    std::atomic<int> failures = 0;
    std::thread accounting([&] {
        for (int i = 0; i < 1000; ++i) {
            if (!table.Reserve(tenant_id, 1) || !table.Abort(tenant_id, 1)) {
                ++failures;
            }
        }
    });
    std::thread recompute([&] {
        for (int i = 0; i < 1000; ++i) {
            table.RecomputeEffectiveQuotas(1000);
        }
    });

    accounting.join();
    recompute.join();

    EXPECT_EQ(failures.load(), 0);
    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_EQ(snapshot.reserved_bytes, 0);
    EXPECT_EQ(snapshot.effective_quota_bytes, 1000);
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
