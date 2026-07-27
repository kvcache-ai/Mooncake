#include "master_scenario.h"

#include <functional>

namespace mooncake::test {
namespace {

std::string DifferentShardGroup(const std::string& key,
                                std::string suffix = "") {
    constexpr size_t kShardCount = 1024;
    const size_t key_shard = std::hash<std::string>{}(key) % kShardCount;
    for (int index = 0; index < 10000; ++index) {
        std::string group = key + "_group_" + suffix + std::to_string(index);
        if (std::hash<std::string>{}(group) % kShardCount != key_shard) {
            return group;
        }
    }
    return key + "_fallback_group";
}

void GivenThreeTenants(MasterScenario& scenario) {
    scenario.Given(Tenant("default").Quota(64_MB))
        .Given(Tenant("tenant-a").Quota(64_MB))
        .Given(Tenant("tenant-b").Quota(64_MB));
}

}  // namespace

TEST(MasterScenarioGroupTenantTest, PutStartValidatesGroupIds) {
    MasterScenario("put start validates group ids")
        .Given(MemoryNode("memory"))
        .When(PutStart("empty", 1_KB)
                  .InGroups({})
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(PutStart("too-many", 1_KB)
                  .InGroups({"g0", "g1"})
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(Put("explicit-ungrouped", 1_KB).InGroup(""))
        .Then(Object("explicit-ungrouped").Exists().IsReadable());
}

TEST(MasterScenarioGroupTenantTest, GroupedObjectRoutesLookupAndRemove) {
    const std::string key = "grouped-route-key";
    MasterScenario("grouped object routes lookup and remove")
        .Given(MemoryNode("memory"))
        .When(Put(key, 1_KB).InGroup(DifferentShardGroup(key)))
        .Then(Object(key).Exists().IsReadable())
        .When(Remove(key).Force())
        .Then(Object(key).DoesNotExist());
}

TEST(MasterScenarioGroupTenantTest, GroupRoutingIsTenantScoped) {
    const std::string key = "shared-key";
    MasterScenario scenario("group routing is tenant scoped");
    scenario.Given(MemoryNode("memory"));
    GivenThreeTenants(scenario);
    scenario
        .When(Put(key, 1_KB)
                  .ForTenant("tenant-a")
                  .InGroup(DifferentShardGroup(key, "a")))
        .When(Put(key, 2_KB)
                  .ForTenant("tenant-b")
                  .InGroup(DifferentShardGroup(key, "b")))
        .Then(Object(key).ForTenant("tenant-a").IsReadable().HasSize(1_KB))
        .Then(Object(key).ForTenant("tenant-b").IsReadable().HasSize(2_KB))
        .When(Remove(key).ForTenant("tenant-a").Force())
        .Then(Object(key).ForTenant("tenant-a").DoesNotExist())
        .Then(Object(key).ForTenant("tenant-b").IsReadable());
}

TEST(MasterScenarioGroupTenantTest, BatchGetPreservesGroupedKeyOrder) {
    const std::string grouped_a = "grouped-a";
    const std::string grouped_b = "grouped-b";
    const std::vector<std::string> keys = {grouped_a, "missing", "ungrouped",
                                           grouped_b, "pending"};
    MasterScenario("batch get preserves grouped key order")
        .Given(MemoryNode("memory"))
        .When(Put(grouped_a, 1_KB).InGroup(DifferentShardGroup(grouped_a)))
        .When(Put("ungrouped", 1_KB))
        .When(Put(grouped_b, 1_KB).InGroup(DifferentShardGroup(grouped_b)))
        .When(PutStart("pending", 1_KB))
        .Then(BatchObjects(keys).Are({BatchObjectsSpec::State::READABLE,
                                      BatchObjectsSpec::State::MISSING,
                                      BatchObjectsSpec::State::READABLE,
                                      BatchObjectsSpec::State::READABLE,
                                      BatchObjectsSpec::State::NOT_READY}));
}

TEST(MasterScenarioGroupTenantTest, BatchGetKeepsTenantIsolation) {
    const std::string key = "shared-key";
    MasterScenario scenario("batch get keeps tenant isolation");
    scenario.Given(MemoryNode("memory"));
    GivenThreeTenants(scenario);
    scenario
        .When(Put(key, 1_KB)
                  .ForTenant("tenant-a")
                  .InGroup(DifferentShardGroup(key)))
        .When(Put(key, 2_KB).ForTenant("tenant-b"))
        .Then(BatchObjects({key})
                  .ForTenant("tenant-a")
                  .Are({BatchObjectsSpec::State::READABLE}))
        .Then(BatchObjects({key})
                  .ForTenant("tenant-b")
                  .Are({BatchObjectsSpec::State::READABLE}))
        .Then(BatchObjects({key}).ForTenant("default").Are(
            {BatchObjectsSpec::State::MISSING}));
}

TEST(MasterScenarioGroupTenantTest, GetAllKeysListsRequestedTenantOnly) {
    MasterScenario scenario("get all keys lists requested tenant only");
    scenario.Given(MemoryNode("memory"));
    GivenThreeTenants(scenario);
    scenario.When(Put("shared", 1_KB).ForTenant("default"))
        .When(Put("default-only", 1_KB).ForTenant("default"))
        .When(Put("shared", 1_KB).ForTenant("tenant-a"))
        .When(Put("tenant-only", 1_KB).ForTenant("tenant-a"))
        .Then(AllKeys().ForTenant("default").Are({"shared", "default-only"}))
        .Then(AllKeys().ForTenant("tenant-a").Are({"shared", "tenant-only"}));
}

TEST(MasterScenarioGroupTenantTest,
     ExpiredGroupedPutCanBeReplacedByUngroupedPut) {
    const std::string key = "expired-grouped-put";
    MasterScenario("expired grouped put can become ungrouped")
        .Configured(
            ServiceConfig().PutStartDiscardTimeout(0).PutStartReleaseTimeout(1))
        .Given(MemoryNode("memory"))
        .When(PutStart(key, 1_KB).InGroup(DifferentShardGroup(key)))
        .When(WaitFor(std::chrono::milliseconds(2)))
        .When(PutStart(key, 1_KB))
        .When(PutEnd(key))
        .Then(Object(key).Exists().IsReadable());
}

TEST(MasterScenarioGroupTenantTest, BatchRemoveUnregistersGroupedRoute) {
    const std::string key = "batch-remove-grouped-route";
    MasterScenario("batch remove unregisters grouped route")
        .Given(MemoryNode("memory"))
        .When(Put(key, 1_KB).InGroup(DifferentShardGroup(key)))
        .When(BatchRemove({key}).Force().ExpectAffected(1))
        .When(Put(key, 1_KB))
        .Then(Object(key).IsReadable());
}

TEST(MasterScenarioGroupTenantTest, RegexRemoveUnregistersGroupedRoute) {
    const std::string key = "regex-remove-grouped-route";
    MasterScenario("regex remove unregisters grouped route")
        .Given(MemoryNode("memory"))
        .When(Put(key, 1_KB).InGroup(DifferentShardGroup(key)))
        .When(RemoveByRegex("^regex-remove-grouped-route$")
                  .Force()
                  .ExpectAffected(1))
        .When(Put(key, 1_KB))
        .Then(Object(key).IsReadable());
}

TEST(MasterScenarioGroupTenantTest, RemovingGroupMemberPreservesPeers) {
    const std::string group = DifferentShardGroup("group-a");
    MasterScenario("removing grouped member preserves peers")
        .Given(MemoryNode("memory"))
        .When(Put("group-a", 1_KB).InGroup(group))
        .When(Put("group-b", 1_KB).InGroup(group))
        .When(Remove("group-a").Force())
        .Then(Object("group-a").DoesNotExist())
        .Then(Object("group-b").IsReadable())
        .When(Remove("group-b").Force())
        .Then(Object("group-b").DoesNotExist());
}

TEST(MasterScenarioGroupTenantTest, RegexOperationsAreTenantScoped) {
    const std::string key = "regex-shared-key";
    MasterScenario scenario("regex operations are tenant scoped");
    scenario.Given(MemoryNode("memory"));
    GivenThreeTenants(scenario);
    scenario.When(Put(key, 1_KB).ForTenant("default"))
        .When(Put(key, 1_KB).ForTenant("tenant-a"))
        .When(Put(key, 1_KB).ForTenant("tenant-b"))
        .Then(MatchingObjects("^regex-shared").ForTenant("default").HasCount(1))
        .When(RemoveByRegex("^regex-shared")
                  .ForTenant("default")
                  .Force()
                  .ExpectAffected(1))
        .Then(Object(key).ForTenant("default").DoesNotExist())
        .Then(Object(key).ForTenant("tenant-a").IsReadable())
        .Then(Object(key).ForTenant("tenant-b").IsReadable())
        .When(RemoveByRegex("^regex-shared")
                  .ForTenant("tenant-a")
                  .Force()
                  .ExpectAffected(1))
        .Then(Object(key).ForTenant("tenant-a").DoesNotExist())
        .Then(Object(key).ForTenant("tenant-b").IsReadable());
}

TEST(MasterScenarioGroupTenantTest, TenantBatchRemoveAndRemoveAllAreScoped) {
    const std::string key = "shared-key";
    MasterScenario scenario("tenant batch remove and remove all are scoped");
    scenario.Given(MemoryNode("memory"));
    GivenThreeTenants(scenario);
    scenario.When(Put(key, 1_KB).ForTenant("default"))
        .When(Put(key, 1_KB).ForTenant("tenant-a"))
        .When(Put(key, 1_KB).ForTenant("tenant-b"))
        .When(
            BatchRemove({key}).ForTenant("tenant-a").Force().ExpectAffected(1))
        .Then(Object(key).ForTenant("tenant-a").DoesNotExist())
        .Then(Object(key).ForTenant("default").IsReadable())
        .Then(Object(key).ForTenant("tenant-b").IsReadable())
        .When(RemoveAllForTenant("tenant-b").Force().ExpectAffected(1))
        .Then(Object(key).ForTenant("tenant-b").DoesNotExist())
        .Then(Object(key).ForTenant("default").IsReadable())
        .When(RemoveAll().Force().ExpectAffected(1))
        .Then(Object(key).ForTenant("default").DoesNotExist());
}

TEST(MasterScenarioGroupTenantTest, LegacyRemoveAllRemovesAllTenants) {
    const std::string key = "shared-key";
    MasterScenario scenario("legacy remove all removes all tenants");
    scenario.Given(MemoryNode("memory"));
    GivenThreeTenants(scenario);
    scenario.When(Put(key, 1_KB).ForTenant("default"))
        .When(Put(key, 1_KB).ForTenant("tenant-a"))
        .When(Put(key, 1_KB).ForTenant("tenant-b"))
        .When(RemoveAll().Force().ExpectAffected(3))
        .Then(Object(key).ForTenant("default").DoesNotExist())
        .Then(Object(key).ForTenant("tenant-a").DoesNotExist())
        .Then(Object(key).ForTenant("tenant-b").DoesNotExist())
        .When(RemoveAll().Force().ExpectAffected(0));
}

TEST(MasterScenarioGroupTenantTest,
     BatchExistenceHandlesGroupedIncompleteAndMissingKeys) {
    const std::string group = DifferentShardGroup("grouped-a");
    const std::vector<std::string> keys = {
        "grouped-a", "completed", "incomplete", "missing", "grouped-b"};
    MasterScenario("batch existence handles grouped incomplete and missing")
        .Given(MemoryNode("memory"))
        .When(Put("grouped-a", 1_KB).InGroup(group))
        .When(Put("grouped-b", 1_KB).InGroup(group))
        .When(Put("completed", 1_KB))
        .When(PutStart("incomplete", 1_KB))
        .Then(ObjectExistence(keys).Is({true, true, false, false, true}));
}

TEST(MasterScenarioGroupTenantTest, BatchExistenceIsTenantAware) {
    MasterScenario scenario("batch existence is tenant aware");
    scenario.Given(MemoryNode("memory"));
    GivenThreeTenants(scenario);
    scenario.When(Put("tenant-only", 1_KB).ForTenant("tenant-a"))
        .When(Put("default-only", 1_KB).ForTenant("default"))
        .When(PutStart("incomplete", 1_KB).ForTenant("tenant-a"))
        .Then(ObjectExistence({"tenant-only", "default-only", "missing",
                               "incomplete", "tenant-only"})
                  .ForTenant("tenant-a")
                  .Is({true, false, false, false, true}))
        .Then(ObjectExistence({"tenant-only", "default-only"})
                  .ForTenant("default")
                  .Is({false, true}));
}

}  // namespace mooncake::test
