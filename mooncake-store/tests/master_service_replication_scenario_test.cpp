#include "master_service/dsl/scenario.h"

#include <gtest/gtest.h>

#include <chrono>
#include <string>

namespace mooncake::test {
namespace {

std::chrono::system_clock::time_point ExpiredLease() {
    return std::chrono::system_clock::now() - std::chrono::seconds(1);
}

}  // namespace

TEST(MasterServiceReplicationScenarioTest, CopyStart) {
    MasterScenario("copy start validates inputs and allocates free targets")
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .Given(MemoryNode("node-3"))
        .Given(MemoryNode("node-4"))
        .When(CopyStart("missing_key")
                  .From("node-1")
                  .To({"node-2"})
                  .ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(PutStart("copy_key", 1_KB).OnNode("node-1"))
        .When(CopyStart("copy_key")
                  .From("node-1")
                  .To({"node-2", "node-3"})
                  .ExpectError(ErrorCode::REPLICA_NOT_FOUND))
        .When(PutEnd("copy_key"))
        .When(CopyStart("copy_key")
                  .From("node-1")
                  .To({"node-2", "node-3"})
                  .ExpectAllocatedTargets({"node-2", "node-3"}))
        .When(Remove("copy_key").ExpectError(ErrorCode::REPLICA_IS_NOT_READY))
        .When(CopyStart("copy_key")
                  .From("node-1")
                  .To({"node-4"})
                  .ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK))
        .When(CopyEnd("copy_key"))
        .Then(Object("copy_key").IsReadable().HasReplicas(3))
        .When(CopyStart("copy_key")
                  .From("ghost-node")
                  .To({"node-3", "node-4"})
                  .ExpectError(ErrorCode::REPLICA_NOT_FOUND))
        .When(CopyStart("copy_key")
                  .From("node-1")
                  .To({"node-4", "ghost-node"})
                  .ExpectError(ErrorCode::SEGMENT_NOT_FOUND))
        .When(CopyStart("copy_key")
                  .From("node-1")
                  .To({"node-3", "node-4"})
                  .ExpectAllocatedTargets({"node-4"}))
        .When(CopyEnd("copy_key"))
        .Then(Object("copy_key").IsReadable().HasReplicas(4))
        .When(CopyStart("copy_key")
                  .From("node-1")
                  .To({"node-4"})
                  .ExpectAllocatedTargets({}))
        .When(ExpireAt("copy_key", ExpiredLease()))
        .When(Remove("copy_key")
                  .ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK))
        .When(CopyEnd("copy_key"))
        .When(ExpireAt("copy_key", ExpiredLease()))
        .When(Remove("copy_key"))
        .Then(Object("copy_key").DoesNotExist());
}

TEST(MasterServiceReplicationScenarioTest, CopyEnd) {
    MasterScenario("copy end commits targets and reports vanished replicas")
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .Given(MemoryNode("node-3"))
        .When(CopyEnd("missing_key").ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(PutStart("copy_end_key", 1_KB).OnNode("node-1"))
        .When(PutEnd("copy_end_key"))
        .When(CopyEnd("copy_end_key")
                  .ExpectError(ErrorCode::OBJECT_NO_REPLICATION_TASK))
        .When(CopyStart("copy_end_key").From("node-1").To({"node-2"}))
        .When(CopyEnd("copy_end_key")
                  .By("stranger")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(MoveEnd("copy_end_key").ExpectError(ErrorCode::INVALID_PARAMS))
        .When(CopyEnd("copy_end_key"))
        .Then(Object("copy_end_key").IsReadable().HasReplicas(2))
        .When(CopyStart("copy_end_key").From("node-1").To({"node-3"}))
        .When(UnmountMemoryNode("node-1"))
        .When(CopyEnd("copy_end_key").ExpectError(ErrorCode::REPLICA_IS_GONE))
        .Then(Object("copy_end_key")
                  .IsReadable()
                  .HasReplicas(1)
                  .IsOnMemoryNode("node-2"))
        .When(CopyStart("copy_end_key").From("node-2").To({"node-3"}))
        .When(UnmountMemoryNode("node-3"))
        .When(CopyEnd("copy_end_key").ExpectError(ErrorCode::REPLICA_IS_GONE))
        .Then(Object("copy_end_key")
                  .IsReadable()
                  .HasReplicas(1)
                  .IsOnMemoryNode("node-2"));
}

TEST(MasterServiceReplicationScenarioTest, CopyRevoke) {
    MasterScenario("copy revoke discards the pending copy")
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .When(
            CopyRevoke("missing_key").ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(PutStart("copy_revoke_key", 1_KB).OnNode("node-1"))
        .When(PutEnd("copy_revoke_key"))
        .When(CopyRevoke("copy_revoke_key")
                  .ExpectError(ErrorCode::OBJECT_NO_REPLICATION_TASK))
        .When(CopyStart("copy_revoke_key").From("node-1").To({"node-2"}))
        .When(CopyRevoke("copy_revoke_key")
                  .By("stranger")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(MoveRevoke("copy_revoke_key")
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(CopyRevoke("copy_revoke_key"))
        .Then(Object("copy_revoke_key").IsReadable().HasReplicas(1))
        .When(CopyStart("copy_revoke_key").From("node-1").To({"node-2"}))
        .When(UnmountMemoryNode("node-1"))
        .When(CopyRevoke("copy_revoke_key"))
        .Then(Object("copy_revoke_key").DoesNotExist());
}

TEST(MasterServiceReplicationScenarioTest, MoveStart) {
    MasterScenario("move start validates inputs and allocates the target")
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .Given(MemoryNode("node-3"))
        .When(MoveStart("missing_key")
                  .From("node-1")
                  .To("node-2")
                  .ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(PutStart("move_key", 1_KB).OnNode("node-1"))
        .When(MoveStart("move_key")
                  .From("node-1")
                  .To("node-2")
                  .ExpectError(ErrorCode::REPLICA_NOT_FOUND))
        .When(PutEnd("move_key"))
        .When(CopyStart("move_key").From("node-1").To({"node-3"}))
        .When(CopyEnd("move_key"))
        .When(MoveStart("move_key")
                  .From("node-1")
                  .To("node-1")
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(MoveStart("move_key")
                  .From("node-1")
                  .To("node-2")
                  .ExpectTargetAllocation())
        .When(Remove("move_key").ExpectError(ErrorCode::REPLICA_IS_NOT_READY))
        .When(MoveStart("move_key")
                  .From("node-1")
                  .To("node-3")
                  .ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK))
        .When(MoveEnd("move_key"))
        .Then(Object("move_key")
                  .IsReadable()
                  .HasReplicas(2)
                  .HasMemoryNodes({"node-2", "node-3"}))
        .When(MoveStart("move_key")
                  .From("ghost-node")
                  .To("node-1")
                  .ExpectError(ErrorCode::REPLICA_NOT_FOUND))
        .When(MoveStart("move_key")
                  .From("node-2")
                  .To("ghost-node")
                  .ExpectError(ErrorCode::SEGMENT_NOT_FOUND))
        .When(MoveStart("move_key")
                  .From("node-2")
                  .To("node-3")
                  .ExpectNoTargetAllocation())
        .When(ExpireAt("move_key", ExpiredLease()))
        .When(Remove("move_key")
                  .ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK))
        .When(MoveEnd("move_key"))
        .Then(Object("move_key")
                  .IsReadable()
                  .HasReplicas(1)
                  .IsOnMemoryNode("node-3"))
        .When(ExpireAt("move_key", ExpiredLease()))
        .When(Remove("move_key"))
        .Then(Object("move_key").DoesNotExist());
}

TEST(MasterServiceReplicationScenarioTest, MoveEnd) {
    MasterScenario("move end commits the move and reports a vanished source")
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .When(MoveEnd("missing_key").ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(PutStart("move_end_key", 1_KB).OnNode("node-1"))
        .When(PutEnd("move_end_key"))
        .When(MoveEnd("move_end_key")
                  .ExpectError(ErrorCode::OBJECT_NO_REPLICATION_TASK))
        .When(MoveStart("move_end_key").From("node-1").To("node-2"))
        .When(MoveEnd("move_end_key")
                  .By("stranger")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(CopyEnd("move_end_key").ExpectError(ErrorCode::INVALID_PARAMS))
        .When(MoveEnd("move_end_key"))
        .Then(Object("move_end_key")
                  .IsReadable()
                  .HasReplicas(1)
                  .IsOnMemoryNode("node-2"))
        .When(MoveStart("move_end_key").From("node-2").To("node-1"))
        .When(UnmountMemoryNode("node-2"))
        .When(MoveEnd("move_end_key").ExpectError(ErrorCode::REPLICA_IS_GONE));
}

TEST(MasterServiceReplicationScenarioTest,
     MoveEndWithVanishedTargetReleasesSource) {
    MasterScenario("a vanished move target fails move end and frees the source")
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .When(PutStart("target_gone_key", 1_KB).OnNode("node-1"))
        .When(PutEnd("target_gone_key"))
        .When(MoveStart("target_gone_key").From("node-1").To("node-2"))
        .When(UnmountMemoryNode("node-2"))
        .When(
            MoveEnd("target_gone_key").ExpectError(ErrorCode::REPLICA_IS_GONE))
        .When(UpsertStart("target_gone_key", 1_KB))
        .When(MoveRevoke("target_gone_key")
                  .ExpectError(ErrorCode::OBJECT_NO_REPLICATION_TASK))
        .When(UpsertRevoke("target_gone_key"));
}

TEST(MasterServiceReplicationScenarioTest, MoveRevoke) {
    MasterScenario("move revoke restores the source replica")
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .When(
            MoveRevoke("missing_key").ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(PutStart("move_revoke_key", 1_KB).OnNode("node-1"))
        .When(PutEnd("move_revoke_key"))
        .When(MoveRevoke("move_revoke_key")
                  .ExpectError(ErrorCode::OBJECT_NO_REPLICATION_TASK))
        .When(MoveStart("move_revoke_key").From("node-1").To("node-2"))
        .When(MoveRevoke("move_revoke_key")
                  .By("stranger")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(CopyRevoke("move_revoke_key")
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(MoveRevoke("move_revoke_key"))
        .Then(Object("move_revoke_key")
                  .IsReadable()
                  .HasReplicas(1)
                  .IsOnMemoryNode("node-1"))
        .When(MoveStart("move_revoke_key").From("node-1").To("node-2"))
        .When(UnmountMemoryNode("node-1"))
        .When(MoveRevoke("move_revoke_key"))
        .Then(Object("move_revoke_key").DoesNotExist());
}

TEST(MasterServiceReplicationScenarioTest,
     ReadableAfterPartialUnmountWithReplication) {
    constexpr uint64_t kNodeCapacity = 64 * 1024 * 1024;
    constexpr uint64_t kObjectSize = 1024 * 1024;
    MasterScenario("a replicated object survives losing one node")
        .Given(MemoryNode("node-1").Capacity(kNodeCapacity))
        .Given(MemoryNode("node-2").Capacity(kNodeCapacity))
        .When(PutStart("replicated_key", kObjectSize)
                  .Replicas(2)
                  .ExpectReplicas(2))
        .When(PutEnd("replicated_key"))
        .Then(Object("replicated_key")
                  .IsReadable()
                  .HasCompleteReplicas(2)
                  .HasDistinctMemoryNodes()
                  .HasMemoryReplicaSize(kObjectSize))
        .When(UnmountMemoryNode("node-1"))
        .Then(Object("replicated_key")
                  .IsReadable()
                  .HasReplicas(1)
                  .IsOnMemoryNode("node-2"));
}

TEST(MasterServiceReplicationScenarioTest, ClearReplicasAcrossAllNodes) {
    MasterScenario scenario("expired objects are cleared across all nodes");
    scenario.Given(MemoryNode("memory"))
        .Given(Objects(0, 5)
                   .NamedBy([](size_t index) {
                       return "batch_clear_key_" + std::to_string(index);
                   })
                   .Size(1_KB)
                   .CompleteOn("memory"));
    for (size_t index = 0; index < 5; ++index) {
        scenario.Then(KeyExists("batch_clear_key_" + std::to_string(index)));
        scenario.When(ExpireAt("batch_clear_key_" + std::to_string(index),
                               ExpiredLease()));
    }
    scenario
        .When(ClearReplicas({"batch_clear_key_0", "batch_clear_key_1",
                             "batch_clear_key_2", "batch_clear_key_3",
                             "batch_clear_key_4"})
                  .ExpectCleared({"batch_clear_key_0", "batch_clear_key_1",
                                  "batch_clear_key_2", "batch_clear_key_3",
                                  "batch_clear_key_4"}))
        .Then(Objects(0, 5)
                  .NamedBy([](size_t index) {
                      return "batch_clear_key_" + std::to_string(index);
                  })
                  .DoNotExist());
}

TEST(MasterServiceReplicationScenarioTest, ClearReplicasOnSpecificNode) {
    MasterScenario("an expired object is cleared from its node")
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .When(PutStart("node_specific_key", 1_KB).OnNode("node-1"))
        .When(PutEnd("node_specific_key"))
        .When(ExpireAt("node_specific_key", ExpiredLease()))
        .When(ClearReplicas({"node_specific_key"})
                  .FromNode("node-1")
                  .ExpectCleared({"node_specific_key"}))
        .Then(Object("node_specific_key").DoesNotExist());
}

TEST(MasterServiceReplicationScenarioTest, ClearReplicasSkipsLeasedObjects) {
    MasterScenario("a leased object is not cleared")
        .Given(MemoryNode("memory"))
        .When(PutStart("lease_active_key", 1_KB))
        .When(PutEnd("lease_active_key"))
        .Then(KeyExists("lease_active_key"))
        .When(ClearReplicas({"lease_active_key"}).ExpectCleared({}))
        .Then(KeyExists("lease_active_key"));
}

TEST(MasterServiceReplicationScenarioTest, ClearReplicasSkipsForeignObjects) {
    MasterScenario("another client's object is not cleared")
        .Given(MemoryNode("memory"))
        .When(PutStart("client_specific_key", 1_KB).By("writer"))
        .When(PutEnd("client_specific_key").By("writer"))
        .When(ExpireAt("client_specific_key", ExpiredLease()))
        .When(ClearReplicas({"client_specific_key"})
                  .By("other-client")
                  .ExpectCleared({}))
        .Then(KeyExists("client_specific_key"));
}

TEST(MasterServiceReplicationScenarioTest, ClearReplicasSkipsMissingKeys) {
    MasterScenario("missing keys clear nothing")
        .Given(MemoryNode("memory"))
        .When(ClearReplicas({"missing_key_1", "missing_key_2"})
                  .ExpectCleared({}));
}

TEST(MasterServiceReplicationScenarioTest, ClearReplicasWithNoKeys) {
    MasterScenario("an empty key list clears nothing")
        .Given(MemoryNode("memory"))
        .When(ClearReplicas({}).ExpectCleared({}));
}

TEST(MasterServiceReplicationScenarioTest, ClearReplicasSkipsEmptyKeys) {
    MasterScenario("empty-string keys are skipped")
        .Given(MemoryNode("memory"))
        .When(PutStart("valid_key", 1_KB))
        .When(PutEnd("valid_key"))
        .When(ExpireAt("valid_key", ExpiredLease()))
        .When(ClearReplicas({"", "valid_key", "", "another_empty"})
                  .ExpectCleared({"valid_key"}))
        .Then(Object("valid_key").DoesNotExist());
}

TEST(MasterServiceReplicationScenarioTest, ClearReplicasMixedOwnership) {
    MasterScenario("only the caller's expired keys are cleared")
        .Given(MemoryNode("memory"))
        .When(PutStart("mixed_key1", 1_KB).By("client-1"))
        .When(PutEnd("mixed_key1").By("client-1"))
        .When(PutStart("mixed_key2", 1_KB).By("client-1"))
        .When(PutEnd("mixed_key2").By("client-1"))
        .When(PutStart("mixed_key3", 1_KB).By("client-2"))
        .When(PutEnd("mixed_key3").By("client-2"))
        .When(ExpireAt("mixed_key1", ExpiredLease()))
        .When(ExpireAt("mixed_key2", ExpiredLease()))
        .When(ExpireAt("mixed_key3", ExpiredLease()))
        .When(ClearReplicas({"mixed_key1", "mixed_key2", "mixed_key3",
                             "non_existent", ""})
                  .By("client-1")
                  .ExpectCleared({"mixed_key1", "mixed_key2"}))
        .Then(Object("mixed_key1").DoesNotExist())
        .Then(Object("mixed_key2").DoesNotExist())
        .Then(KeyExists("mixed_key3"));
}

}  // namespace mooncake::test
