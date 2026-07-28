#include "tenant_quota_sharded.h"
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

void MakeOrphanTenant(TenantQuotaShard* table, const std::string& tenant_id,
                      uint64_t bytes) {
    const TenantId canonical_tenant(tenant_id);
    ASSERT_TRUE(table->UpsertTenantPolicy(canonical_tenant, bytes).has_value());
    table->RecomputeEffectiveQuotas(bytes);
    ASSERT_TRUE(table->GetOrCreateTenantHandle(canonical_tenant)
                    ->TryCharge(bytes)
                    .has_value());
    ASSERT_TRUE(table->ApplyTenantPolicies({}));
}

TEST(TenantQuotaShardTest, NormalizesEmptyExplicitTenantIdToDefault) {
    TenantQuotaShard table;

    ASSERT_TRUE(table.UpsertTenantPolicy(TenantId(""), 1024).has_value());
    table.RecomputeEffectiveQuotas(4096);

    auto snapshot = Snapshot(table, "");
    EXPECT_EQ(snapshot.tenant_id, TenantId::Default());
    EXPECT_TRUE(snapshot.has_explicit_policy);
    EXPECT_EQ(snapshot.requested_quota_bytes, 1024);
    EXPECT_EQ(snapshot.effective_quota_bytes, 1024);
}

TEST(TenantQuotaShardTest, RejectsZeroExplicitQuotaWithoutChangingState) {
    TenantQuotaShard table;

    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 100).has_value());
    auto result = table.UpsertTenantPolicy(tenant_id, 0);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kInvalidArgument);
    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_TRUE(snapshot.has_explicit_policy);
    EXPECT_EQ(snapshot.requested_quota_bytes, 100);
}

TEST(TenantQuotaShardTest, RejectsPolicyAboveAtomicAccountingRange) {
    TenantQuotaShard table;
    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 100));

    auto upsert = table.UpsertTenantPolicy(
        tenant_id, TenantQuotaAccount::kMaxChargedBytes + 1);
    auto replace = table.ApplyTenantPolicies(
        {{tenant_id, TenantQuotaAccount::kMaxChargedBytes + 1}});

    ASSERT_FALSE(upsert);
    EXPECT_EQ(upsert.error(), TenantQuotaError::kInvalidArgument);
    ASSERT_FALSE(replace);
    EXPECT_EQ(replace.error(), TenantQuotaError::kInvalidArgument);
    EXPECT_EQ(Snapshot(table, "tenant-a").requested_quota_bytes, 100);
}

TEST(TenantQuotaShardTest, ApplyPoliciesCreatesOrphanState) {
    TenantQuotaShard table;
    MakeOrphanTenant(&table, "tenant-a", 40);
    table.RecomputeEffectiveQuotas(1000);

    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_FALSE(snapshot.has_explicit_policy);
    EXPECT_EQ(snapshot.requested_quota_bytes, 0);
    EXPECT_EQ(snapshot.effective_quota_bytes, 0);
    EXPECT_EQ(snapshot.charged_bytes, 40);
    EXPECT_TRUE(snapshot.admission_closed);
    EXPECT_TRUE(snapshot.over_quota);
}

TEST(TenantQuotaShardTest, ClosedOrphanAccountCanDrain) {
    TenantQuotaShard table;
    const TenantId tenant_id("tenant-a");
    MakeOrphanTenant(&table, tenant_id.value(), 40);
    auto* handle = table.GetOrCreateTenantHandle(tenant_id);

    ASSERT_TRUE(handle->Release(40));

    EXPECT_EQ(handle->ChargedBytes(), 0);
    EXPECT_TRUE(handle->AdmissionClosed());
    EXPECT_FALSE(table.GetTenantSnapshot(tenant_id).has_value());
}

TEST(TenantQuotaShardTest, ApplyPoliciesReplacesCanonicalPolicySet) {
    TenantQuotaShard table;
    const TenantId tenant_a("tenant-a");
    const TenantId tenant_b("tenant-b");
    const TenantId tenant_c("tenant-c");
    table.ApplyTenantPolicies({{tenant_a, 100}, {tenant_b, 200}});
    table.RecomputeEffectiveQuotas(300);
    ASSERT_TRUE(table.GetOrCreateTenantHandle(tenant_a)->TryCharge(1));

    table.ApplyTenantPolicies({{tenant_b, 300}, {tenant_c, 400}});

    EXPECT_FALSE(table.IsTenantRegistered(tenant_a));
    EXPECT_TRUE(table.IsTenantRegistered(tenant_b));
    EXPECT_TRUE(table.IsTenantRegistered(tenant_c));
    EXPECT_TRUE(Snapshot(table, tenant_a.value()).over_quota);
    EXPECT_EQ(table.GetTenantPolicies(),
              (TenantQuotaPolicyMap{{tenant_b, 300}, {tenant_c, 400}}));
}

TEST(TenantQuotaShardTest, DisableMissingPolicyDoesNotCreateLazyState) {
    TenantQuotaShard table;

    auto result = table.DisableTenantPolicyIfEmpty(TenantId("missing"));

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kTenantNotFound);
    EXPECT_FALSE(table.GetTenantSnapshot(TenantId("missing")).has_value());
    EXPECT_TRUE(table.ListTenantSnapshots().empty());
}

TEST(TenantQuotaShardTest, PolicyMutationDoesNotRecomputeEffectiveQuota) {
    TenantQuotaShard table;
    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 100).has_value());
    table.RecomputeEffectiveQuotas(1000);
    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);

    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 200).has_value());
    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);
    EXPECT_TRUE(Snapshot(table, "tenant-a").admission_closed);

    table.RecomputeEffectiveQuotas(1000);
    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 200);
    EXPECT_FALSE(Snapshot(table, "tenant-a").admission_closed);
}

TEST(TenantQuotaShardTest, ListSnapshotsSortedAndCleansLazyEmptyTenants) {
    TenantQuotaShard table;
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

TEST(TenantQuotaShardTest, ExplicitTenantsReceiveRequestedWhenCapacityFits) {
    TenantQuotaShard table;
    ASSERT_TRUE(
        table.UpsertTenantPolicy(TenantId("tenant-a"), 100).has_value());
    ASSERT_TRUE(
        table.UpsertTenantPolicy(TenantId("tenant-b"), 200).has_value());

    table.RecomputeEffectiveQuotas(1000);

    EXPECT_EQ(Snapshot(table, "tenant-a").effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(table, "tenant-b").effective_quota_bytes, 200);
    EXPECT_EQ(SumEffectiveQuotas(table), 300);
}

TEST(TenantQuotaShardTest, OverCapacityScalesOnlyExplicitTenants) {
    TenantQuotaShard table;
    MakeOrphanTenant(&table, "orphan", 20);
    ASSERT_TRUE(table.UpsertTenantPolicy(TenantId("b"), 200).has_value());
    ASSERT_TRUE(table.UpsertTenantPolicy(TenantId("a"), 100).has_value());

    table.RecomputeEffectiveQuotas(150);

    EXPECT_EQ(Snapshot(table, "a").effective_quota_bytes, 50);
    EXPECT_EQ(Snapshot(table, "b").effective_quota_bytes, 100);
    EXPECT_EQ(Snapshot(table, "orphan").effective_quota_bytes, 0);
    EXPECT_TRUE(Snapshot(table, "orphan").over_quota);
}

TEST(TenantQuotaShardTest, LazyEmptyOrphansDoNotAppearInList) {
    TenantQuotaShard table;
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

TEST(TenantQuotaShardTest, ChargeRequiresRegisteredTenantIncludingZeroBytes) {
    TenantQuotaShard table;
    auto* account = table.GetOrCreateTenantHandle(TenantId("missing"));

    auto regular = account->TryCharge(1);
    auto zero = account->TryCharge(0);

    ASSERT_FALSE(regular.has_value());
    ASSERT_FALSE(zero.has_value());
    EXPECT_EQ(regular.error().error, TenantQuotaError::kTenantNotRegistered);
    EXPECT_EQ(zero.error().error, TenantQuotaError::kTenantNotRegistered);
}

TEST(TenantQuotaShardTest, ChargeAndReleaseUpdateSingleCounter) {
    TenantQuotaShard table;
    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 300).has_value());
    table.RecomputeEffectiveQuotas(300);
    auto* account = table.GetOrCreateTenantHandle(tenant_id);

    ASSERT_TRUE(account->TryCharge(200).has_value());
    ASSERT_TRUE(account->Release(50).has_value());

    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_EQ(snapshot.charged_bytes, 150);
    EXPECT_FALSE(snapshot.admission_closed);
}

TEST(TenantQuotaShardTest, AccountingMismatchDoesNotMutateState) {
    TenantQuotaShard table;
    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 100).has_value());
    table.RecomputeEffectiveQuotas(100);
    auto* account = table.GetOrCreateTenantHandle(tenant_id);
    ASSERT_TRUE(account->TryCharge(10).has_value());

    auto release = account->Release(11);
    ASSERT_FALSE(release.has_value());
    EXPECT_EQ(release.error(), TenantQuotaError::kAccountingMismatch);
    EXPECT_EQ(Snapshot(table, "tenant-a").charged_bytes, 10);
}

TEST(TenantQuotaShardTest, DisablePolicyRejectsNonEmptyTenant) {
    TenantQuotaShard table;
    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 100).has_value());
    table.RecomputeEffectiveQuotas(100);
    ASSERT_TRUE(
        table.GetOrCreateTenantHandle(tenant_id)->TryCharge(1).has_value());

    auto result = table.DisableTenantPolicyIfEmpty(tenant_id);

    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), TenantQuotaError::kTenantNotEmpty);
    EXPECT_TRUE(table.IsTenantRegistered(tenant_id));
    EXPECT_FALSE(Snapshot(table, "tenant-a").admission_closed);
    EXPECT_TRUE(table.GetOrCreateTenantHandle(tenant_id)->TryCharge(1));
}

TEST(TenantQuotaShardTest, HandleRemainsStableAcrossPolicyLifecycle) {
    TenantQuotaShard table;
    const TenantId tenant_id("tenant-a");
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 100));
    table.RecomputeEffectiveQuotas(100);
    auto* handle = table.GetOrCreateTenantHandle(tenant_id);

    ASSERT_TRUE(handle->TryCharge(10));
    ASSERT_TRUE(handle->Release(10));
    ASSERT_TRUE(table.DisableTenantPolicyIfEmpty(tenant_id));
    EXPECT_FALSE(handle->TryCharge(0));
    EXPECT_FALSE(table.GetTenantSnapshot(tenant_id).has_value());

    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, 200));
    table.RecomputeEffectiveQuotas(200);
    EXPECT_EQ(table.GetOrCreateTenantHandle(tenant_id), handle);
    EXPECT_TRUE(handle->TryCharge(200));
    EXPECT_EQ(Snapshot(table, "tenant-a").charged_bytes, 200);
}

TEST(TenantQuotaShardTest, RebuildUsageCreatesAndRemovesOrphans) {
    TenantQuotaShard table;
    const TenantId explicit_tenant("tenant-a");
    const TenantId orphan("orphan");
    ASSERT_TRUE(table.UpsertTenantPolicy(explicit_tenant, 100).has_value());

    TenantQuotaUsageMap usage{
        {explicit_tenant, {.charged_bytes = 40}},
        {orphan, {.charged_bytes = 20}},
    };
    ASSERT_TRUE(table.RebuildUsage(usage));
    table.RecomputeEffectiveQuotas(100);

    EXPECT_TRUE(Snapshot(table, "tenant-a").has_explicit_policy);
    EXPECT_FALSE(Snapshot(table, "orphan").has_explicit_policy);
    EXPECT_TRUE(Snapshot(table, "orphan").over_quota);

    ASSERT_TRUE(table.RebuildUsage({}));
    EXPECT_FALSE(table.GetTenantSnapshot(orphan).has_value());
    EXPECT_TRUE(table.GetTenantSnapshot(explicit_tenant).has_value());
}

TEST(TenantQuotaShardTest, OverflowChecksDoNotWrapAccounting) {
    TenantQuotaShard table;
    const TenantId tenant_id("tenant-a");
    const uint64_t max = TenantQuotaAccount::kMaxChargedBytes;
    ASSERT_TRUE(table.UpsertTenantPolicy(tenant_id, max).has_value());
    ASSERT_TRUE(table.RebuildUsage({{tenant_id, {.charged_bytes = max - 5}}}));
    table.RecomputeEffectiveQuotas(max);

    auto* account = table.GetOrCreateTenantHandle(tenant_id);
    auto overflow_charge = account->TryCharge(10);
    ASSERT_FALSE(overflow_charge.has_value());
    EXPECT_EQ(overflow_charge.error().error, TenantQuotaError::kQuotaExceeded);
    EXPECT_EQ(overflow_charge.error().deficit_bytes, 5);

    ASSERT_TRUE(account->TryCharge(5).has_value());

    auto snapshot = Snapshot(table, "tenant-a");
    EXPECT_EQ(snapshot.charged_bytes, max);
}

TEST(ShardedTenantQuotaTableTest, ConcurrentChargeNeverExceedsQuota) {
    ShardedTenantQuotaTable<8> table;
    const TenantId tenant_id("tenant-a");
    table.ApplyTenantPolicies({{tenant_id, 1000}}, 1000);
    auto* account = table.GetOrCreateTenantHandle(tenant_id);

    std::atomic<int> successes = 0;
    std::vector<std::thread> workers;
    for (int i = 0; i < 20; ++i) {
        workers.emplace_back([&] {
            if (account->TryCharge(100).has_value()) {
                ++successes;
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }

    EXPECT_EQ(successes.load(), 10);
    EXPECT_EQ(Snapshot(table, "tenant-a").charged_bytes, 1000);
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
    auto* account_a = table.GetOrCreateTenantHandle(tenant_a);
    auto* account_b = table.GetOrCreateTenantHandle(tenant_b);

    std::atomic<int> failures = 0;
    auto update = [&](TenantQuotaHandle account) {
        for (int i = 0; i < 1000; ++i) {
            if (!account->TryCharge(1) || !account->Release(1)) {
                ++failures;
            }
        }
    };
    std::thread first(update, account_a);
    std::thread second(update, account_b);
    first.join();
    second.join();

    EXPECT_EQ(failures.load(), 0);
    EXPECT_EQ(Snapshot(table, tenant_a.value()).charged_bytes, 0);
    EXPECT_EQ(Snapshot(table, tenant_b.value()).charged_bytes, 0);
}

TEST(ShardedTenantQuotaTableTest,
     CrossShardMutationsValidateEverythingBeforeUpdating) {
    using TestTable = ShardedTenantQuotaTable<2>;
    const TenantId tenant_a("tenant-a");
    TenantId tenant_b("tenant-b");
    for (int suffix = 0; TenantIdHash{}(tenant_a) % TestTable::kNumShards ==
                         TenantIdHash{}(tenant_b) % TestTable::kNumShards;
         ++suffix) {
        tenant_b = TenantId("tenant-b-" + std::to_string(suffix));
    }

    TestTable table;
    ASSERT_TRUE(
        table.ApplyTenantPolicies({{tenant_a, 100}, {tenant_b, 200}}, 300));
    auto invalid_policy = table.ApplyTenantPolicies(
        {{tenant_a, 300}, {tenant_b, TenantQuotaAccount::kMaxChargedBytes + 1}},
        500);
    ASSERT_FALSE(invalid_policy);
    EXPECT_EQ(Snapshot(table, tenant_a.value()).requested_quota_bytes, 100);
    EXPECT_EQ(Snapshot(table, tenant_b.value()).requested_quota_bytes, 200);

    ASSERT_TRUE(table.RebuildUsage(
        {{tenant_a, {.charged_bytes = 40}}, {tenant_b, {.charged_bytes = 50}}},
        300));
    auto invalid_usage = table.RebuildUsage(
        {{tenant_a, {.charged_bytes = 60}},
         {tenant_b,
          {.charged_bytes = TenantQuotaAccount::kMaxChargedBytes + 1}}},
        300);
    ASSERT_FALSE(invalid_usage);
    EXPECT_EQ(Snapshot(table, tenant_a.value()).charged_bytes, 40);
    EXPECT_EQ(Snapshot(table, tenant_b.value()).charged_bytes, 50);
}

TEST(ShardedTenantQuotaTableTest,
     DisabledPolicyRejectsRegularAndZeroByteCharges) {
    ShardedTenantQuotaTable<8> table;
    const TenantId tenant_id("tenant-a");
    table.ApplyTenantPolicies({{tenant_id, 100}}, 100);
    auto* account = table.GetOrCreateTenantHandle(tenant_id);
    ASSERT_TRUE(table.DisableTenantPolicyIfEmpty(tenant_id).has_value());

    auto regular = account->TryCharge(1);
    auto zero = account->TryCharge(0);

    ASSERT_FALSE(regular.has_value());
    ASSERT_FALSE(zero.has_value());
    EXPECT_EQ(regular.error().error, TenantQuotaError::kTenantNotRegistered);
    EXPECT_EQ(zero.error().error, TenantQuotaError::kTenantNotRegistered);
}

TEST(ShardedTenantQuotaTableTest, RecomputeCanRunWithAccounting) {
    ShardedTenantQuotaTable<8> table;
    const TenantId tenant_id("tenant-a");
    table.ApplyTenantPolicies({{tenant_id, 1000}}, 1000);
    auto* account = table.GetOrCreateTenantHandle(tenant_id);

    std::atomic<int> failures = 0;
    std::thread accounting([&] {
        for (int i = 0; i < 1000; ++i) {
            if (!account->TryCharge(1) || !account->Release(1)) {
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
    EXPECT_EQ(snapshot.charged_bytes, 0);
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
        "9223372036854775808\n",
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
