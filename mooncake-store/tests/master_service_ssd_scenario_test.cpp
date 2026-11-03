#include <gtest/gtest.h>

#include <chrono>
#include <string>

#include "master_service/dsl/scenario.h"

namespace mooncake::test {
namespace {

// root_fs_dir turns every PutStart into a dual allocation: one MEMORY replica
// and one DISK replica that the client is expected to persist itself.
MasterServiceConfig RootFsConfig() {
    return MasterServiceConfig::builder().set_root_fs_dir("/mnt/ssd").build();
}

// Memory-pressure variant: no leases in the way, the high watermark disabled,
// and a small eviction ratio so every armed cycle reclaims exactly one
// expired one-megabyte memory replica.
MasterServiceConfig PressureRootFsConfig() {
    return MasterServiceConfig::builder()
        .set_root_fs_dir("/mnt/ssd")
        .set_default_kv_lease_ttl(0)
        .set_eviction_ratio(0.05)
        .set_eviction_high_watermark_ratio(1.0)
        .build();
}

std::string PressureKey(size_t index) {
    return "ssd_pressure_" + std::to_string(index);
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

TEST(MasterServiceSsdScenarioTest, MemoryPressureEvictsOnlyTheMemoryHalf) {
    constexpr uint64_t kLargeObject = 1024 * 1024;
    constexpr size_t kFilled = 14;
    const auto expired_base =
        std::chrono::system_clock::now() - std::chrono::hours(1);
    MasterScenario scenario("memory pressure leaves the disk halves readable",
                            PressureRootFsConfig());
    scenario.Given(MemoryNode("memory").Capacity(16 * 1024 * 1024));
    for (size_t index = 0; index < kFilled; ++index) {
        scenario
            .When(PutStart(PressureKey(index), kLargeObject).ExpectReplicas(2))
            .When(PutEnd(PressureKey(index)).OfType(ReplicaType::MEMORY))
            .When(PutEnd(PressureKey(index)).OfType(ReplicaType::DISK))
            .When(ExpireAt(PressureKey(index),
                           expired_base + std::chrono::nanoseconds(index)));
    }
    scenario
        .When(PutStart(PressureKey(kFilled), 3 * kLargeObject)
                  .ExpectReplicas(2)
                  .Eventually(std::chrono::seconds(10)))
        .When(PutEnd(PressureKey(kFilled)).OfType(ReplicaType::MEMORY))
        .When(PutEnd(PressureKey(kFilled)).OfType(ReplicaType::DISK))
        .Then(ReadableCount(Objects(0, kFilled + 1).NamedBy(PressureKey),
                            kFilled + 1));
    for (size_t index = 0; index < 3; ++index) {
        scenario.Then(Object(PressureKey(index))
                          .HasReplicas(1)
                          .HasMemoryReplicas(0)
                          .HasDiskReplicas(1));
    }
    scenario.Then(Object(PressureKey(3))
                      .HasReplicas(2)
                      .HasMemoryReplicas(1)
                      .HasDiskReplicas(1));
}

}  // namespace mooncake::test
