#include "master_scenario.h"

namespace mooncake::test {

TEST(MasterScenarioQuotaTest, OffloadRejectsUnregisteredTenant) {
    MasterScenario("offload success rejects unregistered tenant")
        .Given(Tenant("tenant-a").Quota(1000))
        .Given(MemoryNode("memory"))
        .When(NotifyOffloadSuccess("ghost", 128)
                  .By("memory")
                  .ForTenant("tenant-b")
                  .ExpectError(ErrorCode::TENANT_NOT_REGISTERED))
        .Then(Object("ghost").ForTenant("tenant-b").DoesNotExist());
}

TEST(MasterScenarioQuotaTest, OffloadAllowsRegisteredTenant) {
    MasterScenario("offload success allows registered tenant")
        .Given(Tenant("tenant-a").Quota(1000))
        .Given(MemoryNode("memory"))
        .When(NotifyOffloadSuccess("cold", 128)
                  .By("memory")
                  .ForTenant("tenant-a"))
        .Then(Object("cold").ForTenant("tenant-a").Exists())
        .Then(Tenant("tenant-a").UsedBytes(0));
}

TEST(MasterScenarioQuotaTest, SingleTenantModeNormalizesTenantIds) {
    MasterScenario("single-tenant mode normalizes tenant IDs")
        .Given(MemoryNode("memory").Capacity(1_KB))
        .When(Put("shared-key", 800).ForTenant("tenant-a"))
        .Then(Object("shared-key").ForTenant("tenant-b").IsReadable())
        .When(PutStart("shared-key", 1)
                  .ForTenant("tenant-b")
                  .ExpectError(ErrorCode::OBJECT_ALREADY_EXISTS))
        .When(Remove("shared-key").ForTenant("tenant-b").Force())
        .Then(Tenant("tenant-a").DoesNotExist());
}

TEST(MasterScenarioQuotaTest,
     MultiTenantModeRejectsUnregisteredAndImplicitDefaultWrites) {
    MasterScenario("multi-tenant mode rejects unregistered writes")
        .Given(MemoryNode("memory"))
        .Given(Tenant("tenant-a").Quota(1000))
        .When(PutStart("missing", 10)
                  .ForTenant("tenant-b")
                  .ExpectError(ErrorCode::TENANT_NOT_REGISTERED))
        .When(PutStart("default-key", 10)
                  .ForTenant("default")
                  .ExpectError(ErrorCode::TENANT_NOT_REGISTERED))
        .When(UpsertTenantPolicy("default", 100))
        .When(Put("registered-default", 10).ForTenant("default"))
        .When(Put("registered", 10).ForTenant("tenant-a"))
        .Then(Object("registered-default").ForTenant("default").IsReadable())
        .Then(Object("registered").ForTenant("tenant-a").IsReadable());
}

TEST(MasterScenarioQuotaTest,
     CopyCommitsAdditionalBytesWithoutAdditionalObjectCount) {
    MasterScenario("copy quota accounting")
        .Given(MemoryNode("segment-a").Capacity(1_KB))
        .Given(MemoryNode("segment-b").Capacity(1_KB))
        .Given(Tenant("tenant-a").Quota(300))
        .When(
            Put("key", 100).ForTenant("tenant-a").PreferredSegment("segment-a"))
        .Then(Tenant("tenant-a")
                  .UsedBytes(100)
                  .ReservedBytes(0)
                  .CommittedCount(1))
        .When(CopyStart("key")
                  .ForTenant("tenant-a")
                  .From("segment-a")
                  .To({"segment-b"}))
        .Then(Tenant("tenant-a")
                  .UsedBytes(100)
                  .ReservedBytes(100)
                  .CommittedCount(1))
        .When(CopyEnd("key").ForTenant("tenant-a"))
        .Then(Tenant("tenant-a")
                  .UsedBytes(200)
                  .ReservedBytes(0)
                  .CommittedCount(1));
}

TEST(MasterScenarioQuotaTest, MoveRequiresQuotaForTemporaryReplica) {
    MasterScenario("move requires quota for temporary replica")
        .Given(MemoryNode("segment-a").Capacity(1_KB))
        .Given(MemoryNode("segment-b").Capacity(1_KB))
        .Given(Tenant("tenant-a").Quota(150))
        .When(
            Put("key", 100).ForTenant("tenant-a").PreferredSegment("segment-a"))
        .When(MoveStart("key")
                  .ForTenant("tenant-a")
                  .From("segment-a")
                  .To({"segment-b"})
                  .ExpectError(ErrorCode::TENANT_QUOTA_EXCEEDED))
        .Then(Tenant("tenant-a").UsedBytes(100).ReservedBytes(0))
        .Then(Object("key").ForTenant("tenant-a").IsReadable());
}

TEST(MasterScenarioQuotaTest, DeletePolicyRequiresEmptyTenant) {
    MasterScenario("delete quota policy requires empty tenant")
        .Given(MemoryNode("memory"))
        .Given(Tenant("tenant-a").Quota(1000))
        .Given(Tenant("tenant-b").Quota(100))
        .When(Put("key", 100).ForTenant("tenant-a"))
        .When(DeleteTenantPolicy("tenant-a")
                  .ExpectError(ErrorCode::TENANT_NOT_EMPTY))
        .When(DeleteTenantPolicy("tenant-b"))
        .Then(Tenant("tenant-b").DoesNotExist())
        .Then(Object("key").ForTenant("tenant-a").IsReadable());
}

TEST(MasterScenarioQuotaTest, EffectiveQuotaScalesProportionallyToCapacity) {
    MasterScenario("effective quota scales proportionally to capacity")
        .Given(MemoryNode("memory").Capacity(300))
        .Given(Tenant("tenant-a").Quota(200))
        .Given(Tenant("tenant-b").Quota(400))
        .Then(Tenant("tenant-a").EffectiveQuota(100))
        .Then(Tenant("tenant-b").EffectiveQuota(200));
}

}  // namespace mooncake::test
