#include "master_service/dsl/scenario.h"

#include <gtest/gtest.h>

#include <string>

namespace mooncake::test {

TEST(MasterServicePlacementScenarioTest, PutWithPreferredSegment) {
    MasterScenario("a single preferred node receives the replica")
        .Given(MemoryNode("node-0"))
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .When(PutStart("preferred_key", 1_KB)
                  .OnNode("node-1")
                  .ExpectReplicas(1)
                  .ExpectStatus(ReplicaStatus::PROCESSING)
                  .ExpectMemoryNodes({"node-1"}))
        .When(PutEnd("preferred_key"))
        .Then(Object("preferred_key").IsReadable().IsOnMemoryNode("node-1"));
}

TEST(MasterServicePlacementScenarioTest, PutWithPreferredSegments) {
    MasterScenario("preferred nodes receive one replica each")
        .Given(MemoryNode("node-0"))
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .When(PutStart("preferred_keys", 1_KB)
                  .Replicas(2)
                  .OnNodes({"node-0", "node-1"})
                  .ExpectReplicas(2)
                  .ExpectStatus(ReplicaStatus::PROCESSING)
                  .ExpectMemoryNodes({"node-0", "node-1"}))
        .When(PutEnd("preferred_keys"))
        .Then(Object("preferred_keys")
                  .IsReadable()
                  .HasMemoryNodes({"node-0", "node-1"}));
}

TEST(MasterServicePlacementScenarioTest, LocalFirstPutPrefersWriterHost) {
    auto config =
        MasterServiceConfig::builder()
            .set_allocation_strategy_type(AllocationStrategyType::LOCAL_FIRST)
            .build();
    MasterScenario("local-first placement prefers the writer host",
                   std::move(config))
        .Given(MemoryNode("node-host0").OnHost("host0"))
        .Given(MemoryNode("node-host1").OnHost("host1"))
        .When(PutStart("local_first_key", 1_KB)
                  .FromHost("host1")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"node-host1"}));
}

TEST(MasterServicePlacementScenarioTest,
     PreferSameNodeUsesHostAwareLocalFirstPlacement) {
    MasterScenario("prefer-same-node opts into host-aware placement")
        .Given(MemoryNode("node-host0").OnHost("host0"))
        .Given(MemoryNode("node-host1").OnHost("host1"))
        .When(PutStart("prefer_same_node_key", 1_KB)
                  .PreferSameNode()
                  .FromHost("host1")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"node-host1"}));
}

TEST(MasterServicePlacementScenarioTest, LocalFirstPutFallsBackToNextHost) {
    auto config =
        MasterServiceConfig::builder()
            .set_allocation_strategy_type(AllocationStrategyType::LOCAL_FIRST)
            .build();
    MasterScenario("local-first falls back to the next ordered host",
                   std::move(config))
        .Given(MemoryNode("node-host0").OnHost("host0"))
        .Given(MemoryNode("node-host2").OnHost("host2"))
        .When(PutStart("ordered_fallback_key", 1_KB)
                  .FromHost("host1")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"node-host2"}));
}

TEST(MasterServicePlacementScenarioTest,
     LocalFirstPutFallsBackWhenLocalSegmentIsFull) {
    auto config =
        MasterServiceConfig::builder()
            .set_allocation_strategy_type(AllocationStrategyType::LOCAL_FIRST)
            .build();
    MasterScenario("local-first falls back once the local node is full",
                   std::move(config))
        .Given(MemoryNode("node-host1").OnHost("host1").Capacity(1024))
        .Given(MemoryNode("node-host2").OnHost("host2"))
        .When(PutStart("fill_local_segment", 1_KB)
                  .FromHost("host1")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"node-host1"}))
        .When(PutEnd("fill_local_segment"))
        .When(PutStart("fallback_after_local_full", 1)
                  .FromHost("host1")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"node-host2"}));
}

TEST(MasterServicePlacementScenarioTest,
     ExplicitPreferredSegmentOverridesLocalFirst) {
    auto config =
        MasterServiceConfig::builder()
            .set_allocation_strategy_type(AllocationStrategyType::LOCAL_FIRST)
            .build();
    MasterScenario("an explicit preferred node overrides local-first",
                   std::move(config))
        .Given(MemoryNode("node-host0").OnHost("host0"))
        .Given(MemoryNode("node-host1").OnHost("host1"))
        .When(PutStart("explicit_preferred_key", 1_KB)
                  .FromHost("host1")
                  .OnNode("node-host0")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"node-host0"}));
}

TEST(MasterServicePlacementScenarioTest,
     ExplicitPreferredSegmentFallsBackToLocalFirst) {
    auto config =
        MasterServiceConfig::builder()
            .set_allocation_strategy_type(AllocationStrategyType::LOCAL_FIRST)
            .build();
    MasterScenario("a full preferred node falls back to local-first",
                   std::move(config))
        .Given(MemoryNode("node-host0").OnHost("host0").Capacity(1024))
        .Given(MemoryNode("node-host1").OnHost("host1"))
        .When(PutStart("fill_preferred", 1_KB)
                  .FromHost("host1")
                  .OnNode("node-host0")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"node-host0"}))
        .When(PutEnd("fill_preferred"))
        .When(PutStart("fallback_after_preferred_full", 1)
                  .FromHost("host1")
                  .OnNode("node-host0")
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"node-host1"}));
}

TEST(MasterServicePlacementScenarioTest, SingleSliceMultiReplicaFlow) {
    constexpr uint64_t kNodeCapacity = 64 * 1024 * 1024;
    constexpr uint64_t kObjectSize = 5 * 1024 * 1024;
    MasterScenario scenario("a multi-replica object completes on every node");
    scenario.Given(MemoryNode("node-0").Capacity(kNodeCapacity))
        .Given(MemoryNode("node-1").Capacity(kNodeCapacity))
        .Given(MemoryNode("node-2").Capacity(kNodeCapacity))
        .When(PutStart("multi_slice_object", kObjectSize)
                  .Replicas(3)
                  .ExpectReplicas(3)
                  .ExpectStatus(ReplicaStatus::PROCESSING))
        .Then(Object("multi_slice_object").IsNotReady())
        .When(PutEnd("multi_slice_object"))
        .Then(Object("multi_slice_object")
                  .IsReadable()
                  .HasReplicas(3)
                  .HasCompleteReplicas(3)
                  .HasMemoryReplicaSize(kObjectSize));
}

TEST(MasterServicePlacementScenarioTest, ReplicaSegmentsAreUnique) {
    constexpr uint64_t kObjectSize = 1024 * 1024 - 16;
    constexpr size_t kReplicaCount = 10;
    MasterScenario scenario("replicas of one object land on distinct nodes");
    for (size_t index = 0; index < 20; ++index) {
        scenario.Given(MemoryNode("node-" + std::to_string(index)));
    }
    scenario
        .When(PutStart("replica_uniqueness_key", kObjectSize)
                  .Replicas(kReplicaCount)
                  .ExpectReplicas(kReplicaCount)
                  .ExpectStatus(ReplicaStatus::PROCESSING))
        .When(PutEnd("replica_uniqueness_key"))
        .Then(Object("replica_uniqueness_key")
                  .IsReadable()
                  .HasCompleteReplicas(kReplicaCount)
                  .HasDistinctMemoryNodes()
                  .HasMemoryReplicaSize(kObjectSize));
}

TEST(MasterServicePlacementScenarioTest, ReplicationFactorTwoWithSingleNode) {
    MasterScenario("best-effort allocation degrades to the only node")
        .Given(MemoryNode("single-node"))
        .When(PutStart("replication_factor_two", 1_KB)
                  .Replicas(2)
                  .ExpectReplicas(1)
                  .ExpectMemoryNodes({"single-node"}))
        .When(PutEnd("replication_factor_two"))
        .Then(Object("replication_factor_two")
                  .IsReadable()
                  .HasCompleteReplicas(1)
                  .IsOnMemoryNode("single-node")
                  .HasMemoryReplicaSize(1_KB));
}

TEST(MasterServicePlacementScenarioTest, PutStartPartialAllocationIsVisible) {
    MasterScenario("best-effort allocation reports the degraded replica count")
        .Given(MemoryNode("node-0"))
        .Given(MemoryNode("node-1"))
        .When(PutStart("partial_alloc_key", 1_KB)
                  .Replicas(3)
                  .ExpectReplicas(2)
                  .ExpectStatus(ReplicaStatus::PROCESSING))
        .When(PutEnd("partial_alloc_key"))
        .Then(Object("partial_alloc_key")
                  .IsReadable()
                  .HasCompleteReplicas(2)
                  .HasDistinctMemoryNodes())
        .When(PutStart("full_alloc_key", 1_KB).Replicas(2).ExpectReplicas(2))
        .When(PutEnd("full_alloc_key"))
        .Then(Object("full_alloc_key").IsReadable().HasCompleteReplicas(2));
}

}  // namespace mooncake::test
