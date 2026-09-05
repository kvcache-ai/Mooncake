#include <gtest/gtest.h>

#include "master_service/dsl/scenario.h"

namespace mooncake::test {
namespace {

// root_fs_dir turns every PutStart into a dual allocation: one MEMORY replica
// and one DISK replica that the client is expected to persist itself.
MasterServiceConfig RootFsConfig() {
    return MasterServiceConfig::builder().set_root_fs_dir("/mnt/ssd").build();
}

}  // namespace

TEST(MasterServiceSsdScenarioTest, PutEndCompletesBothReplica) {
    MasterScenario("PutEnd of each type completes the matching replica",
                   RootFsConfig())
        .Given(MemoryNode("memory"))
        .When(PutStart("disk_key", 1_KB).ExpectReplicas(2))
        .Then(Object("disk_key").IsNotReady())
        .When(PutEnd("disk_key").OfType(ReplicaType::MEMORY))
        .When(PutEnd("disk_key").OfType(ReplicaType::DISK))
        .Then(Object("disk_key")
                  .HasReplicas(2)
                  .HasCompleteReplicas(2)
                  .HasMemoryReplicas(1)
                  .HasDiskReplicas(1));
}

TEST(MasterServiceSsdScenarioTest, PutRevokeDropsOnlyTheDiskReplica) {
    MasterScenario("revoking the disk half leaves the memory replica",
                   RootFsConfig())
        .Given(MemoryNode("memory"))
        .When(PutStart("revoke_key", 1_KB).ExpectReplicas(2))
        .When(PutEnd("revoke_key").OfType(ReplicaType::MEMORY))
        .Then(Object("revoke_key").HasReplicas(1).HasMemoryReplicas(1))
        .When(PutRevoke("revoke_key").OfType(ReplicaType::DISK))
        .Then(Object("revoke_key").HasReplicas(1).HasMemoryReplicas(1));
}

TEST(MasterServiceSsdScenarioTest, PutRevokeDropsOnlyTheMemoryReplica) {
    MasterScenario("revoking the memory half leaves the disk replica",
                   RootFsConfig())
        .Given(MemoryNode("memory"))
        .When(PutStart("revoke_key", 1_KB).ExpectReplicas(2))
        .When(PutRevoke("revoke_key").OfType(ReplicaType::MEMORY))
        .Then(Object("revoke_key").IsNotReady())
        .When(PutEnd("revoke_key").OfType(ReplicaType::DISK))
        .Then(Object("revoke_key").HasReplicas(1).HasDiskReplicas(1));
}

TEST(MasterServiceSsdScenarioTest, PutRevokeOfBothReplicasErasesTheKey) {
    MasterScenario("revoking both halves erases the object", RootFsConfig())
        .Given(MemoryNode("memory"))
        .When(PutStart("revoke_key", 1_KB).ExpectReplicas(2))
        .When(PutRevoke("revoke_key").OfType(ReplicaType::DISK))
        .Then(Object("revoke_key").IsNotReady())
        .When(PutRevoke("revoke_key").OfType(ReplicaType::MEMORY))
        .Then(Object("revoke_key").DoesNotExist());
}

TEST(MasterServiceSsdScenarioTest, RemoveErasesBothReplicas) {
    MasterScenario("Remove drops a fully committed dual-replica object",
                   RootFsConfig())
        .Given(MemoryNode("memory"))
        .When(PutStart("remove_key", 1_KB).ExpectReplicas(2))
        .When(PutEnd("remove_key").OfType(ReplicaType::MEMORY))
        .When(PutEnd("remove_key").OfType(ReplicaType::DISK))
        .When(Remove("remove_key"))
        .Then(Object("remove_key").DoesNotExist());
}

TEST(MasterServiceSsdScenarioTest, EvictDiskReplicaRemovesOnlyTheDiskReplica) {
    MasterScenario("client-driven disk eviction keeps the memory replica",
                   RootFsConfig())
        .Given(MemoryNode("memory"))
        .When(PutStart("evict_disk_key", 1_KB).ExpectReplicas(2))
        .When(PutEnd("evict_disk_key").OfType(ReplicaType::MEMORY))
        .When(PutEnd("evict_disk_key").OfType(ReplicaType::DISK))
        .Then(Object("evict_disk_key").HasReplicas(2))
        .When(EvictDiskReplica("evict_disk_key"))
        .Then(Object("evict_disk_key").HasReplicas(1).HasMemoryReplicas(1));
}

TEST(MasterServiceSsdScenarioTest, EvictDiskReplicaOfUnknownKeyIsRefused) {
    MasterScenario("disk eviction of an unknown key reports OBJECT_NOT_FOUND",
                   RootFsConfig())
        .Given(MemoryNode("memory"))
        .When(EvictDiskReplica("nonexistent_key")
                  .ExpectError(ErrorCode::OBJECT_NOT_FOUND));
}

TEST(MasterServiceSsdScenarioTest, EvictDiskReplicaOfMemoryTypeIsRefused) {
    MasterScenario("disk eviction only accepts disk replica types",
                   RootFsConfig())
        .Given(MemoryNode("memory"))
        .When(PutStart("evict_invalid_type_key", 1_KB).ExpectReplicas(2))
        .When(PutEnd("evict_invalid_type_key").OfType(ReplicaType::MEMORY))
        .When(PutEnd("evict_invalid_type_key").OfType(ReplicaType::DISK))
        .When(EvictDiskReplica("evict_invalid_type_key")
                  .OfType(ReplicaType::MEMORY)
                  .ExpectError(ErrorCode::INVALID_PARAMS));
}

}  // namespace mooncake::test
