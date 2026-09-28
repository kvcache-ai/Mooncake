#include "master_service/dsl/scenario.h"

#include <gtest/gtest.h>

#include <chrono>
#include <functional>
#include <string>

namespace mooncake::test {

TEST(MasterServiceGroupScenarioTest, PutStartGroupIdsValidation) {
    MasterScenario("group ids are validated at put start")
        .Given(MemoryNode("memory"))
        .When(PutStart("empty_group_ids", 1_KB)
                  .InGroups({})
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(PutStart("too_many_group_ids", 1_KB)
                  .InGroups({"g0", "g1"})
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(PutStart("explicit_ungrouped", 1_KB).InGroup(""))
        .When(PutEnd("explicit_ungrouped"))
        .Then(KeyExists("explicit_ungrouped"));
}

TEST(MasterServiceGroupScenarioTest,
     GroupedObjectRoutesKeyLevelLookupAndRemove) {
    const std::string key = "grouped_route_key";
    MasterScenario("grouped object routes key-level lookup and remove")
        .Given(MemoryNode("memory"))
        .When(PutStart(key, 1_KB).InGroup(GroupOnDifferentShard(key)))
        .When(PutEnd(key))
        .Then(KeyExists(key))
        .Then(Object(key).IsReadable())
        .When(Remove(key).Force())
        .Then(Object(key).DoesNotExist());
}

TEST(MasterServiceGroupScenarioTest, GroupRoutingIsTenantScopedForSameUserKey) {
    const std::string key = "tenant_grouped_shared_user_key";
    const std::string group_a = GroupOnDifferentShard(key);
    std::string group_b;
    for (int i = 0; i < 10000; ++i) {
        group_b = key + "_tenant_b_group_" + std::to_string(i);
        if (std::hash<std::string>{}(group_b) % 1024 !=
            std::hash<std::string>{}(group_a) % 1024) {
            break;
        }
    }

    MasterScenario("group routing is tenant scoped for the same user key")
        .Given(MemoryNode("memory"))
        .Given(Tenant("tenant_group_route_a"))
        .Given(Tenant("tenant_group_route_b"))
        .When(PutStart(key, 1_KB)
                  .ForTenant("tenant_group_route_a")
                  .InGroup(group_a))
        .When(PutEnd(key).ForTenant("tenant_group_route_a"))
        .When(PutStart(key, 2_KB)
                  .ForTenant("tenant_group_route_b")
                  .InGroup(group_b))
        .When(PutEnd(key).ForTenant("tenant_group_route_b"))
        .Then(KeyExists(key).ForTenant("tenant_group_route_a"))
        .Then(KeyExists(key).ForTenant("tenant_group_route_b"))
        .Then(Object(key).ForTenant("tenant_group_route_a").IsReadable())
        .Then(Object(key).ForTenant("tenant_group_route_b").IsReadable())
        .When(Remove(key).ForTenant("tenant_group_route_a").Force())
        .Then(Object(key).ForTenant("tenant_group_route_a").DoesNotExist())
        .Then(Object(key).ForTenant("tenant_group_route_b").IsReadable());
}

TEST(MasterServiceGroupScenarioTest,
     ExpiredGroupedPutCanBeReplacedByUngroupedPut) {
    auto config = MasterServiceConfig::builder()
                      .set_put_start_discard_timeout_sec(0)
                      .set_put_start_release_timeout_sec(1)
                      .build();
    const std::string key = "expired_grouped_put_to_ungrouped";
    MasterScenario("expired grouped put can be replaced by an ungrouped put",
                   std::move(config))
        .Given(MemoryNode("memory"))
        .When(PutStart(key, 1_KB).InGroup(GroupOnDifferentShard(key)))
        .When(WaitFor(std::chrono::milliseconds(2)))
        .When(PutStart(key, 1_KB))
        .When(PutEnd(key))
        .Then(KeyExists(key));
}

TEST(MasterServiceGroupScenarioTest, BatchRemoveUnregistersGroupedRoute) {
    const std::string key = "batch_remove_grouped_route";
    MasterScenario("batch remove unregisters the grouped route")
        .Given(MemoryNode("memory"))
        .When(PutStart(key, 1_KB).InGroup(GroupOnDifferentShard(key)))
        .When(PutEnd(key))
        .When(BatchRemove({key}).Force())
        .When(PutStart(key, 1_KB))
        .When(PutEnd(key))
        .Then(Object(key).IsReadable());
}

TEST(MasterServiceGroupScenarioTest, RemoveByRegexUnregistersGroupedRoute) {
    const std::string key = "regex_remove_grouped_route";
    MasterScenario("remove by regex unregisters the grouped route")
        .Given(MemoryNode("memory"))
        .When(PutStart(key, 1_KB).InGroup(GroupOnDifferentShard(key)))
        .When(PutEnd(key))
        .When(RemoveByRegex("^regex_remove_grouped_route$")
                  .Force()
                  .ExpectRemoved(1))
        .When(PutStart(key, 1_KB))
        .When(PutEnd(key))
        .Then(Object(key).IsReadable());
}

TEST(MasterServiceGroupScenarioTest, RemoveGroupedMemberPreservesOtherMembers) {
    const std::string key_a = "remove_group_key_a";
    const std::string key_b = "remove_group_key_b";
    const std::string group = GroupOnDifferentShard(key_a);
    MasterScenario("removing one grouped member preserves the others")
        .Given(MemoryNode("memory"))
        .When(PutStart(key_a, 1_KB).InGroup(group))
        .When(PutEnd(key_a))
        .When(PutStart(key_b, 1_KB).InGroup(group))
        .When(PutEnd(key_b))
        .When(Remove(key_a).Force())
        .Then(Object(key_a).DoesNotExist())
        .Then(Object(key_b).IsReadable())
        .When(Remove(key_b).Force())
        .Then(Object(key_b).DoesNotExist());
}

TEST(MasterServiceGroupScenarioTest, UpsertPreservesGroupMembership) {
    const std::string key = "upsert_group_key";
    MasterScenario("upsert preserves existing group membership")
        .Given(MemoryNode("memory"))
        .When(PutStart(key, 1_KB).InGroup(GroupOnDifferentShard(key)))
        .When(PutEnd(key))
        .When(UpsertStart(key, 1_KB))
        .When(UpsertEnd(key))
        .Then(Object(key).IsReadable())
        .When(UpsertStart(key, 1_KB)
                  .InGroup(GroupOnDifferentShard(key + "_other"))
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(UpsertStart(key, 1_KB).InGroup("").ExpectError(
            ErrorCode::INVALID_PARAMS));
}

TEST(MasterServiceGroupScenarioTest,
     IncompleteGroupedUpsertCanBecomeUngrouped) {
    auto config = MasterServiceConfig::builder()
                      .set_put_start_discard_timeout_sec(0)
                      .set_put_start_release_timeout_sec(1)
                      .build();
    const std::string key = "incomplete_grouped_upsert_to_ungrouped";
    MasterScenario("incomplete grouped upsert can become ungrouped",
                   std::move(config))
        .Given(MemoryNode("memory"))
        .When(PutStart(key, 1_KB).InGroup(GroupOnDifferentShard(key)))
        .When(WaitFor(std::chrono::milliseconds(2)))
        .When(UpsertStart(key, 1_KB))
        .When(UpsertEnd(key))
        .Then(KeyExists(key));
}

TEST(MasterServiceGroupScenarioTest, UpsertRejectsExistingUngroupedToGrouped) {
    const std::string key = "upsert_ungrouped_to_grouped";
    MasterScenario("upsert cannot turn an ungrouped object into a grouped one")
        .Given(MemoryNode("memory"))
        .When(PutStart(key, 1_KB))
        .When(PutEnd(key))
        .When(UpsertStart(key, 2_KB)
                  .InGroup(GroupOnDifferentShard(key))
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .Then(Object(key).IsReadable());
}

TEST(MasterServiceGroupScenarioTest,
     BatchUpsertStartMixedGroupIdsPreservesOrder) {
    const std::string group_a = GroupOnDifferentShard("batch_grouped_a");
    const std::string group_b = GroupOnDifferentShard("batch_grouped_b");
    MasterScenario("batch upsert start preserves order with mixed group ids")
        .Given(MemoryNode("memory"))
        .When(BatchUpsertStart({{"batch_grouped_a", 1_KB},
                                {"batch_ungrouped", 2_KB},
                                {"batch_grouped_b", 4_KB}})
                  .InGroups({group_a, "", group_b}))
        .When(BatchUpsertEnd(
            {"batch_grouped_a", "batch_ungrouped", "batch_grouped_b"}))
        .Then(Object("batch_grouped_a").IsReadable())
        .Then(Object("batch_ungrouped").IsReadable())
        .Then(Object("batch_grouped_b").IsReadable())
        .When(BatchUpsertStart({{"batch_grouped_a", 1_KB},
                                {"batch_ungrouped", 2_KB},
                                {"batch_grouped_b", 4_KB}})
                  .InGroups({"only_one"})
                  .ExpectError(ErrorCode::INVALID_PARAMS));
}

TEST(MasterServiceGroupScenarioTest,
     BatchExistKeyGroupedAndIncompletePreservesOrder) {
    const std::string grouped_key_a = "batch_grouped_key_a";
    const std::string grouped_key_b = "batch_grouped_key_b";
    const std::string group = GroupOnDifferentShard(grouped_key_a);
    MasterScenario("batch exist key preserves order with grouped keys")
        .Given(MemoryNode("memory"))
        .When(PutStart(grouped_key_a, 1_KB).InGroup(group))
        .When(PutEnd(grouped_key_a))
        .When(PutStart(grouped_key_b, 1_KB).InGroup(group))
        .When(PutEnd(grouped_key_b))
        .When(PutStart("batch_completed_key", 1_KB))
        .When(PutEnd("batch_completed_key"))
        .When(PutStart("batch_incomplete_key", 1_KB))
        .Then(BatchExistence({grouped_key_a, "batch_completed_key",
                              "batch_incomplete_key", "batch_missing_key",
                              grouped_key_b})
                  .Returns({true, true, false, false, true}));
}

TEST(MasterServiceGroupScenarioTest,
     BatchGetReplicaListPreservesOrderWithGroupedKeys) {
    const std::string grouped_key_a = "batch_get_grouped_a";
    const std::string grouped_key_b = "batch_get_grouped_b";
    MasterScenario("batch get replica list preserves order with grouped keys")
        .Given(MemoryNode("memory"))
        .When(PutStart(grouped_key_a, 1_KB)
                  .InGroup(GroupOnDifferentShard(grouped_key_a)))
        .When(PutEnd(grouped_key_a))
        .When(PutStart("batch_get_ungrouped", 1_KB))
        .When(PutEnd("batch_get_ungrouped"))
        .When(PutStart(grouped_key_b, 1_KB)
                  .InGroup(GroupOnDifferentShard(grouped_key_b)))
        .When(PutEnd(grouped_key_b))
        .When(PutStart("batch_get_pending", 1_KB))
        .Then(BatchReplicaLists({grouped_key_a, "batch_get_missing",
                                 "batch_get_ungrouped", grouped_key_b,
                                 "batch_get_pending"})
                  .Returns({ErrorCode::OK, ErrorCode::OBJECT_NOT_FOUND,
                            ErrorCode::OK, ErrorCode::OK,
                            ErrorCode::REPLICA_IS_NOT_READY}));
}

}  // namespace mooncake::test
