#include "master_scenario.h"

#include <functional>

namespace mooncake::test {
namespace {

std::string GroupOnDifferentShard(const std::string& key,
                                  std::string suffix = "") {
    constexpr size_t kMetadataShardCount = 1024;
    const size_t key_shard =
        std::hash<std::string>{}(key) % kMetadataShardCount;
    for (int index = 0; index < 10000; ++index) {
        std::string group = key + "_group_" + suffix + std::to_string(index);
        if (std::hash<std::string>{}(group) % kMetadataShardCount !=
            key_shard) {
            return group;
        }
    }
    return key + "_fallback_group";
}

}  // namespace

TEST(MasterScenarioUpsertTest, UpsertPreservesGroupMembership) {
    const std::string key = "upsert_group_key";
    MasterScenario("upsert preserves group membership")
        .Given(MemoryNode("memory"))
        .When(Put(key, 1_KB).InGroup(GroupOnDifferentShard(key)))
        .When(UpsertStart(key, 1_KB).By("new-writer"))
        .When(UpsertEnd(key).By("new-writer"))
        .When(UpsertStart(key, 1_KB)
                  .InGroup(GroupOnDifferentShard(key, "other"))
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(UpsertStart(key, 1_KB).InGroup("").ExpectError(
            ErrorCode::INVALID_PARAMS))
        .Then(Object(key).Exists().IsReadable().HasCompleteReplicas(1));
}

TEST(MasterScenarioUpsertTest, IncompleteGroupedUpsertCanBecomeUngrouped) {
    const std::string key = "incomplete_grouped_upsert_to_ungrouped";
    MasterScenario("incomplete grouped object may become ungrouped")
        .Given(MemoryNode("memory"))
        .When(PutStart(key, 1_KB).InGroup(GroupOnDifferentShard(key)))
        .When(UpsertStart(key, 1_KB))
        .When(UpsertEnd(key))
        .Then(Object(key).Exists().IsReadable());
}

TEST(MasterScenarioUpsertTest, UpsertRejectsExistingUngroupedToGrouped) {
    const std::string key = "upsert_ungrouped_to_grouped";
    MasterScenario("existing ungrouped object rejects grouped upsert")
        .Given(MemoryNode("memory"))
        .When(Put(key, 1_KB))
        .When(UpsertStart(key, 2_KB)
                  .InGroup(GroupOnDifferentShard(key))
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .Then(Object(key).Exists().IsReadable().HasSize(1_KB));
}

TEST(MasterScenarioUpsertTest, BatchUpsertStartMixedGroupIdsPreservesOrder) {
    const std::vector<std::string> keys = {
        "batch_grouped_a",
        "batch_ungrouped",
        "batch_grouped_b",
    };
    const std::vector<uint64_t> sizes = {1_KB, 2_KB, 4_KB};
    MasterScenario("batch upsert preserves mixed group order")
        .Given(MemoryNode("memory"))
        .When(BatchUpsertStart(keys, sizes)
                  .InGroups({GroupOnDifferentShard(keys[0]), "",
                             GroupOnDifferentShard(keys[2])}))
        .When(BatchUpsertEnd(keys))
        .Then(Object(keys[0]).IsReadable().HasSize(sizes[0]))
        .Then(Object(keys[1]).IsReadable().HasSize(sizes[1]))
        .Then(Object(keys[2]).IsReadable().HasSize(sizes[2]))
        .When(BatchUpsertStart(keys, sizes)
                  .InGroups({"only_one"})
                  .ExpectError(ErrorCode::INVALID_PARAMS));
}

TEST(MasterScenarioUpsertTest, TenantBatchUpsertAndRevokeAreScoped) {
    const std::vector<std::string> keys = {"tenant_batch_upsert_key_a",
                                           "tenant_batch_upsert_key_b"};
    const std::vector<uint64_t> sizes = {1_KB, 2_KB};
    MasterScenario("tenant batch upsert and revoke are scoped")
        .Given(MemoryNode("memory"))
        .Given(Tenant("default").Quota(16_MB))
        .Given(Tenant("tenant-a").Quota(16_MB))
        .Given(Tenant("tenant-b").Quota(16_MB))
        .When(BatchUpsertStart(keys, sizes).ForTenant("tenant-a"))
        .When(BatchUpsertEnd(keys).ForTenant("tenant-a"))
        .When(BatchUpsertStart(keys, sizes).ForTenant("tenant-b"))
        .When(BatchUpsertEnd(keys).ForTenant("tenant-b"))
        .Then(Object(keys[0]).ForTenant("default").DoesNotExist())
        .Then(Object(keys[0]).ForTenant("tenant-a").IsReadable())
        .Then(Object(keys[0]).ForTenant("tenant-b").IsReadable())
        .Then(Object(keys[1]).ForTenant("default").DoesNotExist())
        .Then(Object(keys[1]).ForTenant("tenant-a").IsReadable())
        .Then(Object(keys[1]).ForTenant("tenant-b").IsReadable())
        .When(UpsertStart("tenant-revoke", 1_KB).ForTenant("tenant-a"))
        .When(UpsertRevoke("tenant-revoke").ForTenant("tenant-a"))
        .Then(Object("tenant-revoke").ForTenant("tenant-a").DoesNotExist());
}

TEST(MasterScenarioUpsertTest, UpsertNewKey) {
    MasterScenario("upsert creates a new key")
        .Given(MemoryNode("memory"))
        .When(UpsertStart("key", 1_KB).ExpectReplicas(1))
        .Then(Object("key").IsNotReady())
        .When(UpsertEnd("key"))
        .Then(Object("key").Exists().IsReadable().HasCompleteReplicas(1));
}

TEST(MasterScenarioUpsertTest, UpsertSameSizeReusesAllocation) {
    MasterScenario("same-size upsert reuses allocation")
        .Given(MemoryNode("memory"))
        .When(PutStart("key", 1_KB).SaveAs("original"))
        .When(PutEnd("key"))
        .When(UpsertStart("key", 1_KB)
                  .By("new-writer")
                  .ExpectSameAllocationAs("original"))
        .When(UpsertEnd("key").By("new-writer"))
        .Then(Object("key").IsReadable().HasSize(1_KB));
}

TEST(MasterScenarioUpsertTest, UpsertSameSizeRefreshesWriter) {
    MasterScenario("same-size upsert refreshes writer identity")
        .Given(MemoryNode("memory"))
        .When(Put("key", 1_KB).By("old-writer"))
        .When(UpsertStart("key", 1_KB).By("new-writer"))
        .When(UpsertEnd("key")
                  .By("old-writer")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(UpsertEnd("key").By("new-writer"))
        .Then(Object("key").IsReadable());
}

TEST(MasterScenarioUpsertTest, UpsertDifferentSizeReallocates) {
    MasterScenario("different-size upsert reallocates")
        .Given(MemoryNode("memory"))
        .When(PutStart("key", 1_KB).SaveAs("original"))
        .When(PutEnd("key"))
        .When(
            UpsertStart("key", 2_KB).ExpectDifferentAllocationFrom("original"))
        .When(UpsertEnd("key"))
        .Then(Object("key").IsReadable().HasSize(2_KB));
}

TEST(MasterScenarioUpsertTest, UpsertRevokeRemovesNewKey) {
    MasterScenario("upsert revoke removes a new key")
        .Given(MemoryNode("memory"))
        .When(UpsertStart("key", 1_KB))
        .When(UpsertRevoke("key"))
        .Then(Object("key").DoesNotExist());
}

TEST(MasterScenarioUpsertTest, UpsertInPlaceThenRevokeRemovesKey) {
    MasterScenario("in-place upsert revoke removes key")
        .Given(MemoryNode("memory"))
        .When(Put("key", 1_KB).By("old-writer"))
        .When(UpsertStart("key", 1_KB).By("new-writer"))
        .When(UpsertRevoke("key").By("new-writer"))
        .Then(Object("key").DoesNotExist());
}

TEST(MasterScenarioUpsertTest, BatchUpsertHandlesExistingAndNewKeys) {
    const std::vector<std::string> keys = {"existing", "new"};
    MasterScenario("batch upsert handles existing and new keys")
        .Given(MemoryNode("memory"))
        .When(Put("existing", 1_KB))
        .When(BatchUpsertStart(keys, {1_KB, 2_KB}))
        .When(BatchUpsertEnd(keys))
        .Then(Object("existing").IsReadable().HasSize(1_KB))
        .Then(Object("new").IsReadable().HasSize(2_KB));
}

TEST(MasterScenarioUpsertTest, BatchUpsertRevokeRemovesAllKeys) {
    const std::vector<std::string> keys = {"first", "second"};
    MasterScenario("batch upsert revoke removes all processing keys")
        .Given(MemoryNode("memory"))
        .When(BatchUpsertStart(keys, {1_KB, 2_KB}))
        .When(BatchUpsertRevoke(keys))
        .Then(Object("first").DoesNotExist())
        .Then(Object("second").DoesNotExist());
}

TEST(MasterScenarioUpsertTest, DifferentSizeUpsertThenRevokeRemovesKey) {
    MasterScenario("different-size upsert revoke removes key")
        .Given(MemoryNode("memory"))
        .When(Put("key", 1_KB))
        .Then(Object("key").Exists())
        .When(UpsertStart("key", 2_KB))
        .When(UpsertRevoke("key"))
        .Then(Object("key").DoesNotExist());
}

TEST(MasterScenarioUpsertTest, UpsertRejectsActiveReplicationTask) {
    MasterScenario("upsert rejects active replication task")
        .Given(MemoryNode("segment-1"))
        .Given(MemoryNode("segment-2"))
        .When(Put("key", 1_KB).PreferredSegment("segment-1"))
        .When(CopyStart("key").From("segment-1").To({"segment-2"}))
        .When(UpsertStart("key", 1_KB)
                  .ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK));
}

}  // namespace mooncake::test
