#include "master_service/dsl/scenario.h"

#include <gtest/gtest.h>

#include <string>

namespace mooncake::test {
namespace {

std::string DefaultTenant() { return std::string(TenantId::kDefaultValue); }

}  // namespace

TEST(MasterServiceTenantScenarioTest, TenantPutGetRemoveIsolatesSameUserKey) {
    const std::string key = "shared_user_key";
    MasterScenario("tenants isolate the same user key")
        .Given(MemoryNode("memory"))
        .Given(Tenant(DefaultTenant()))
        .Given(Tenant("tenant_a"))
        .Given(Tenant("tenant_b"))
        .When(PutStart(key, 1_KB).ForTenant("tenant_a"))
        .When(PutEnd(key).ForTenant("tenant_a"))
        .When(PutStart(key, 2_KB).ForTenant("tenant_b"))
        .When(PutEnd(key).ForTenant("tenant_b"))
        .Then(Object(key).DoesNotExist())
        .Then(KeyExists(key).ForTenant("tenant_a"))
        .Then(KeyExists(key).ForTenant("tenant_b"))
        .Then(Object(key).ForTenant("tenant_a").IsReadable())
        .Then(Object(key).ForTenant("tenant_b").IsReadable())
        .When(Remove(key).ForTenant("tenant_a").Force())
        .Then(Object(key).ForTenant("tenant_a").DoesNotExist())
        .Then(Object(key).ForTenant("tenant_b").IsReadable());
}

TEST(MasterServiceTenantScenarioTest, RegexOperationsAreTenantScoped) {
    const std::string key = "regex_shared_key";
    MasterScenario("regex operations are tenant scoped")
        .Given(MemoryNode("memory"))
        .Given(Tenant(DefaultTenant()))
        .Given(Tenant("tenant_regex_a"))
        .Given(Tenant("tenant_regex_b"))
        .When(PutStart(key, 1_KB))
        .When(PutEnd(key))
        .When(PutStart(key, 1_KB).ForTenant("tenant_regex_a"))
        .When(PutEnd(key).ForTenant("tenant_regex_a"))
        .When(PutStart(key, 1_KB).ForTenant("tenant_regex_b"))
        .When(PutEnd(key).ForTenant("tenant_regex_b"))
        .Then(MatchingKeys("^regex_shared").HasCount(1))
        .When(RemoveByRegex("^regex_shared").Force().ExpectRemoved(1))
        .Then(Object(key).DoesNotExist())
        .Then(Object(key).ForTenant("tenant_regex_a").IsReadable())
        .Then(Object(key).ForTenant("tenant_regex_b").IsReadable())
        .When(RemoveByRegex("^regex_shared")
                  .ForTenant("tenant_regex_a")
                  .Force()
                  .ExpectRemoved(1))
        .Then(Object(key).ForTenant("tenant_regex_a").DoesNotExist())
        .Then(Object(key).ForTenant("tenant_regex_b").IsReadable());
}

TEST(MasterServiceTenantScenarioTest, TenantBatchUpsertAndRevokeAreScoped) {
    const std::string key_a = "tenant_batch_upsert_key_a";
    const std::string key_b = "tenant_batch_upsert_key_b";
    MasterScenario scenario("batch upsert and revoke are tenant scoped");
    scenario.Given(MemoryNode("memory"))
        .Given(Tenant(DefaultTenant()))
        .Given(Tenant("tenant_batch_upsert_a"))
        .Given(Tenant("tenant_batch_upsert_b"))
        .When(BatchUpsertStart({{key_a, 1_KB}, {key_b, 2_KB}})
                  .ForTenant("tenant_batch_upsert_a"))
        .When(BatchUpsertEnd({key_a, key_b}).ForTenant("tenant_batch_upsert_a"))
        .When(BatchUpsertStart({{key_a, 1_KB}, {key_b, 2_KB}})
                  .ForTenant("tenant_batch_upsert_b"))
        .When(
            BatchUpsertEnd({key_a, key_b}).ForTenant("tenant_batch_upsert_b"));
    for (const auto& key : {key_a, key_b}) {
        scenario.Then(Object(key).DoesNotExist())
            .Then(Object(key).ForTenant("tenant_batch_upsert_a").IsReadable())
            .Then(Object(key).ForTenant("tenant_batch_upsert_b").IsReadable());
    }
    scenario
        .When(UpsertStart("tenant_batch_upsert_revoke_key", 1_KB)
                  .ForTenant("tenant_batch_upsert_a"))
        .When(UpsertRevoke("tenant_batch_upsert_revoke_key")
                  .ForTenant("tenant_batch_upsert_a"))
        .Then(Object("tenant_batch_upsert_revoke_key")
                  .ForTenant("tenant_batch_upsert_a")
                  .DoesNotExist());
}

TEST(MasterServiceTenantScenarioTest, TenantBatchRemoveAndRemoveAllAreScoped) {
    const std::string key = "tenant_batch_remove_shared_key";
    MasterScenario("batch remove and remove all are tenant scoped")
        .Given(MemoryNode("memory"))
        .Given(Tenant(DefaultTenant()))
        .Given(Tenant("tenant_batch_remove_a"))
        .Given(Tenant("tenant_batch_remove_b"))
        .When(PutStart(key, 1_KB))
        .When(PutEnd(key))
        .When(PutStart(key, 1_KB).ForTenant("tenant_batch_remove_a"))
        .When(PutEnd(key).ForTenant("tenant_batch_remove_a"))
        .When(PutStart(key, 1_KB).ForTenant("tenant_batch_remove_b"))
        .When(PutEnd(key).ForTenant("tenant_batch_remove_b"))
        .When(BatchRemove({key}).ForTenant("tenant_batch_remove_a").Force())
        .Then(Object(key).ForTenant("tenant_batch_remove_a").DoesNotExist())
        .Then(Object(key).IsReadable())
        .Then(Object(key).ForTenant("tenant_batch_remove_b").IsReadable())
        .When(RemoveAll()
                  .ForTenant("tenant_batch_remove_b")
                  .Force()
                  .ExpectRemoved(1))
        .Then(Object(key).ForTenant("tenant_batch_remove_b").DoesNotExist())
        .Then(Object(key).IsReadable())
        .When(RemoveAll().Force().ExpectRemoved(1))
        .Then(Object(key).DoesNotExist());
}

TEST(MasterServiceTenantScenarioTest, LegacyRemoveAllRemovesAllTenants) {
    const std::string key = "legacy_remove_all_shared_key";
    MasterScenario("legacy remove all removes objects across tenants")
        .Given(MemoryNode("memory"))
        .Given(Tenant(DefaultTenant()))
        .Given(Tenant("legacy_remove_all_a"))
        .Given(Tenant("legacy_remove_all_b"))
        .When(PutStart(key, 1_KB))
        .When(PutEnd(key))
        .When(PutStart(key, 1_KB).ForTenant("legacy_remove_all_a"))
        .When(PutEnd(key).ForTenant("legacy_remove_all_a"))
        .When(PutStart(key, 1_KB).ForTenant("legacy_remove_all_b"))
        .When(PutEnd(key).ForTenant("legacy_remove_all_b"))
        .When(RemoveAll().Force().ExpectRemoved(3))
        .Then(Object(key).DoesNotExist())
        .Then(Object(key).ForTenant("legacy_remove_all_a").DoesNotExist())
        .Then(Object(key).ForTenant("legacy_remove_all_b").DoesNotExist())
        .When(RemoveAll().Force().ExpectRemoved(0));
}

TEST(MasterServiceTenantScenarioTest, BatchExistKeyTenantAwarePreservesOrder) {
    const std::string tenant_only_key = "batch_tenant_only";
    const std::string default_only_key = "batch_default_only";
    const std::string incomplete_key = "batch_tenant_incomplete";
    const std::string missing_key = "batch_tenant_missing";
    MasterScenario("batch exist key is tenant aware and preserves order")
        .Given(MemoryNode("memory"))
        .Given(Tenant(DefaultTenant()))
        .Given(Tenant("tenant_batch_exist"))
        .When(PutStart(tenant_only_key, 1_KB).ForTenant("tenant_batch_exist"))
        .When(PutEnd(tenant_only_key).ForTenant("tenant_batch_exist"))
        .When(PutStart(default_only_key, 1_KB))
        .When(PutEnd(default_only_key))
        .When(PutStart(incomplete_key, 1_KB).ForTenant("tenant_batch_exist"))
        .Then(BatchExistence({tenant_only_key, default_only_key, missing_key,
                              incomplete_key, tenant_only_key})
                  .ForTenant("tenant_batch_exist")
                  .Returns({true, false, false, false, true}))
        .Then(BatchExistence({tenant_only_key, default_only_key})
                  .Returns({false, true}));
}

TEST(MasterServiceTenantScenarioTest, BatchGetReplicaListKeepsTenantIsolation) {
    const std::string key = "batch_get_tenant_shared_key";
    MasterScenario("batch get replica list keeps tenant isolation")
        .Given(MemoryNode("memory"))
        .Given(Tenant(DefaultTenant()))
        .Given(Tenant("batch_get_tenant_a"))
        .Given(Tenant("batch_get_tenant_b"))
        .When(PutStart(key, 1_KB)
                  .ForTenant("batch_get_tenant_a")
                  .InGroup(GroupOnDifferentShard(key)))
        .When(PutEnd(key).ForTenant("batch_get_tenant_a"))
        .When(PutStart(key, 2_KB).ForTenant("batch_get_tenant_b"))
        .When(PutEnd(key).ForTenant("batch_get_tenant_b"))
        .Then(BatchReplicaLists({key})
                  .ForTenant("batch_get_tenant_a")
                  .Returns({ErrorCode::OK}))
        .Then(BatchReplicaLists({key})
                  .ForTenant("batch_get_tenant_b")
                  .Returns({ErrorCode::OK}))
        .Then(BatchReplicaLists({key}).Returns({ErrorCode::OBJECT_NOT_FOUND}));
}

TEST(MasterServiceTenantScenarioTest,
     TenantQuotaAdmissionRejectsPutsOverQuota) {
    MasterScenario("tenant quota rejects puts over the registered quota")
        .Given(MemoryNode("memory"))
        .Given(Tenant("tenant_quota_admission").Quota(100))
        .When(PutStart("quota_key_a", 80)
                  .ForTenant("tenant_quota_admission")
                  .WithHardPin())
        .When(PutEnd("quota_key_a").ForTenant("tenant_quota_admission"))
        .When(PutStart("quota_key_b", 30)
                  .ForTenant("tenant_quota_admission")
                  .ExpectError(ErrorCode::TENANT_QUOTA_EXCEEDED))
        .Then(Object("quota_key_a")
                  .ForTenant("tenant_quota_admission")
                  .IsReadable());
}

TEST(MasterServiceTenantScenarioTest, PutRevokeRefundsQuotaCharge) {
    MasterScenario("put revoke refunds the quota charged at start")
        .Given(MemoryNode("memory"))
        .Given(Tenant("tenant_quota_refund").Quota(100))
        .When(
            PutStart("quota_refund_key", 100).ForTenant("tenant_quota_refund"))
        .When(PutStart("quota_refund_other", 1)
                  .ForTenant("tenant_quota_refund")
                  .ExpectError(ErrorCode::TENANT_QUOTA_EXCEEDED))
        .When(PutRevoke("quota_refund_key").ForTenant("tenant_quota_refund"))
        .When(
            PutStart("quota_refund_other", 1).ForTenant("tenant_quota_refund"))
        .When(PutEnd("quota_refund_other").ForTenant("tenant_quota_refund"))
        .Then(Object("quota_refund_other")
                  .ForTenant("tenant_quota_refund")
                  .IsReadable());
}

TEST(MasterServiceTenantScenarioTest, UnregisteredTenantWritesAreRejected) {
    MasterScenario("multi tenant mode rejects unregistered tenant writes")
        .Given(MemoryNode("memory"))
        .Given(Tenant("tenant_registered"))
        .When(PutStart("unregistered_key", 1_KB)
                  .ForTenant("tenant_unregistered")
                  .ExpectError(ErrorCode::TENANT_NOT_REGISTERED))
        .When(PutStart("implicit_default_key", 1_KB)
                  .ExpectError(ErrorCode::TENANT_NOT_REGISTERED))
        .When(PutStart("registered_key", 1_KB).ForTenant("tenant_registered"))
        .When(PutEnd("registered_key").ForTenant("tenant_registered"))
        .Then(Object("registered_key")
                  .ForTenant("tenant_registered")
                  .IsReadable());
}

}  // namespace mooncake::test
