#include "master_scenario.h"

namespace mooncake::test {

TEST(MasterScenarioSegmentTest, PutUsesPreferredSegment) {
    MasterScenario("put uses preferred segment")
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .Given(MemoryNode("segment-2"))
        .When(Put("key", 1_KB).PreferredSegment("segment-1"))
        .Then(Object("key").IsReadable().HasReplicasOn({"segment-1"}));
}

TEST(MasterScenarioSegmentTest, PutUsesAllPreferredSegments) {
    MasterScenario("put uses all preferred segments")
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .Given(MemoryNode("segment-2"))
        .When(Put("key", 1_KB)
                  .Replicas(2)
                  .PreferredSegments({"segment-0", "segment-1"}))
        .Then(Object("key").IsReadable().HasCompleteReplicas(2).HasReplicasOn(
            {"segment-0", "segment-1"}));
}

TEST(MasterScenarioSegmentTest, LocalFirstPrefersWriterHost) {
    MasterScenario("local-first allocation prefers writer host")
        .Configured(ServiceConfig().AllocationStrategy(
            AllocationStrategyType::LOCAL_FIRST))
        .Given(MemoryNode("segment-host-0").Host("host-0"))
        .Given(MemoryNode("segment-host-1").Host("host-1"))
        .When(Put("key", 1_KB).FromHost("host-1"))
        .Then(Object("key").HasReplicasOn({"segment-host-1"}));
}

TEST(MasterScenarioSegmentTest, LocalFirstFallsBackToOrderedHost) {
    MasterScenario("local-first allocation falls back to ordered host")
        .Configured(ServiceConfig().AllocationStrategy(
            AllocationStrategyType::LOCAL_FIRST))
        .Given(MemoryNode("segment-host-0").Host("host-0"))
        .Given(MemoryNode("segment-host-2").Host("host-2"))
        .When(Put("key", 1_KB).FromHost("host-1"))
        .Then(Object("key").HasReplicasOn({"segment-host-2"}));
}

TEST(MasterScenarioSegmentTest, LocalFirstFallsBackWhenLocalIsFull) {
    MasterScenario("local-first allocation falls back when local is full")
        .Configured(ServiceConfig().AllocationStrategy(
            AllocationStrategyType::LOCAL_FIRST))
        .Given(MemoryNode("local").Host("host-1").Capacity(1_KB))
        .Given(MemoryNode("remote").Host("host-2"))
        .When(Put("fill", 1_KB).FromHost("host-1"))
        .Then(Object("fill").HasReplicasOn({"local"}))
        .When(Put("fallback", 1).FromHost("host-1"))
        .Then(Object("fallback").HasReplicasOn({"remote"}));
}

TEST(MasterScenarioSegmentTest, ExplicitPreferenceOverridesLocalFirst) {
    MasterScenario("explicit preference overrides local-first allocation")
        .Configured(ServiceConfig().AllocationStrategy(
            AllocationStrategyType::LOCAL_FIRST))
        .Given(MemoryNode("preferred").Host("host-0"))
        .Given(MemoryNode("local").Host("host-1"))
        .When(Put("key", 1_KB).FromHost("host-1").PreferredSegment("preferred"))
        .Then(Object("key").HasReplicasOn({"preferred"}));
}

TEST(MasterScenarioSegmentTest, FullExplicitPreferenceFallsBackToLocalFirst) {
    MasterScenario("full explicit preference falls back to local-first")
        .Configured(ServiceConfig().AllocationStrategy(
            AllocationStrategyType::LOCAL_FIRST))
        .Given(MemoryNode("preferred").Host("host-0").Capacity(1_KB))
        .Given(MemoryNode("local").Host("host-1"))
        .When(
            Put("fill", 1_KB).FromHost("host-1").PreferredSegment("preferred"))
        .When(
            Put("fallback", 1).FromHost("host-1").PreferredSegment("preferred"))
        .Then(Object("fallback").HasReplicasOn({"local"}));
}

TEST(MasterScenarioSegmentTest, UnmountImmediatelyCleansSoleReplica) {
    MasterScenario("unmount immediately cleans sole replica")
        .Given(MemoryNode("segment-1"))
        .Given(MemoryNode("segment-2"))
        .When(Put("key", 1_KB).PreferredSegment("segment-1"))
        .When(UnmountNode("segment-1"))
        .Then(Object("key").DoesNotExist());
}

TEST(MasterScenarioSegmentTest, ObjectRemainsReadableAfterPartialUnmount) {
    MasterScenario("object remains readable after partial unmount")
        .Given(MemoryNode("segment-1"))
        .Given(MemoryNode("segment-2"))
        .When(Put("key", 1_KB).Replicas(2))
        .Then(Object("key").HasReplicasOn({"segment-1", "segment-2"}))
        .When(UnmountNode("segment-1"))
        .Then(Object("key")
                  .Exists()
                  .IsReadable()
                  .HasCompleteReplicas(1)
                  .HasReplicasOn({"segment-2"}));
}

TEST(MasterScenarioSegmentTest, ReplicaAllocationUsesDistinctSegments) {
    MasterScenario scenario("replica allocation uses distinct segments");
    for (int index = 0; index < 20; ++index) {
        scenario.Given(MemoryNode("segment-" + std::to_string(index)));
    }
    scenario.When(Put("key", 1_MB - 16).Replicas(10).ExpectReplicas(10))
        .Then(Object("key")
                  .IsReadable()
                  .HasCompleteReplicas(10)
                  .HasDistinctSegments(10));
}

TEST(MasterScenarioSegmentTest, ReplicationIsBestEffortOnSingleSegment) {
    MasterScenario("replication is best effort on one segment")
        .Given(MemoryNode("single"))
        .When(PutStart("key", 1_KB).Replicas(2).ExpectReplicas(1))
        .When(PutEnd("key"))
        .Then(Object("key").IsReadable().HasCompleteReplicas(1).HasReplicasOn(
            {"single"}));
}

TEST(MasterScenarioSegmentTest, GracefulUnmountTransitionsStatus) {
    MasterScenario("graceful unmount transitions segment status")
        .Given(MemoryNode("segment"))
        .Then(SegmentState("segment").HasStatus(SegmentStatus::OK))
        .When(GracefulUnmountNode("segment", std::chrono::milliseconds(100)))
        .Then(SegmentState("segment").HasStatus(
            SegmentStatus::GRACEFULLY_UNMOUNTING))
        .When(WaitFor(std::chrono::milliseconds(200)))
        .Then(SegmentState("segment").IsUnmounted());
}

TEST(MasterScenarioSegmentTest, GracefulUnmountRejectsWrongClient) {
    MasterScenario("graceful unmount rejects wrong client")
        .Given(MemoryNode("segment"))
        .When(GracefulUnmountNode("segment", std::chrono::milliseconds(100))
                  .By("intruder")
                  .ExpectError(ErrorCode::SEGMENT_NOT_FOUND))
        .Then(SegmentState("segment").HasStatus(SegmentStatus::OK))
        .When(GracefulUnmountNode("segment", std::chrono::milliseconds(100)));
}

TEST(MasterScenarioSegmentTest, GracefulUnmountIsIdempotent) {
    MasterScenario("graceful unmount is idempotent")
        .Given(MemoryNode("segment"))
        .When(GracefulUnmountNode("segment", std::chrono::milliseconds(100)))
        .When(GracefulUnmountNode("segment", std::chrono::milliseconds(100)))
        .Then(SegmentState("segment").HasStatus(
            SegmentStatus::GRACEFULLY_UNMOUNTING));
}

TEST(MasterScenarioSegmentTest, EarlierGraceTimerPreemptsLongWait) {
    MasterScenario("earlier graceful unmount timer preempts long wait")
        .Given(MemoryNode("long"))
        .Given(MemoryNode("short"))
        .When(GracefulUnmountNode("long", std::chrono::milliseconds(500)))
        .When(WaitFor(std::chrono::milliseconds(10)))
        .When(GracefulUnmountNode("short", std::chrono::milliseconds(20)))
        .When(WaitFor(std::chrono::milliseconds(100)))
        .Then(SegmentState("short").IsUnmounted())
        .Then(SegmentState("long").HasStatus(
            SegmentStatus::GRACEFULLY_UNMOUNTING));
}

TEST(MasterScenarioSegmentTest,
     GracefulUnmountPreservesExistingAndPreventsNewAllocation) {
    MasterScenario("graceful unmount preserves reads and prevents allocation")
        .Given(MemoryNode("draining"))
        .Given(MemoryNode("available"))
        .When(Put("existing", 1_KB).PreferredSegment("draining"))
        .When(GracefulUnmountNode("draining", std::chrono::milliseconds(500)))
        .Then(Object("existing").IsReadable().HasReplicasOn({"draining"}))
        .Then(SegmentState("draining")
                  .HasStatus(SegmentStatus::GRACEFULLY_UNMOUNTING))
        .Then(SegmentState("available").HasStatus(SegmentStatus::OK))
        .When(Put("new", 1_KB))
        .Then(Object("new").HasReplicasOn({"available"}));
}

}  // namespace mooncake::test
