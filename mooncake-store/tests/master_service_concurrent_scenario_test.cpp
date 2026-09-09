#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <vector>

#include "master_service/dsl/scenario.h"

namespace mooncake::test {
namespace {

MasterServiceConfig LeaseConfig(uint64_t lease_ttl_ms) {
    return MasterServiceConfig::builder()
        .set_default_kv_lease_ttl(lease_ttl_ms)
        .build();
}

MasterServiceConfig OffloadEnabledConfig() {
    MasterServiceConfig config;
    config.enable_offload = true;
    return config;
}

std::string ReadKey(size_t index) {
    return "read_race_" + std::to_string(index);
}

std::vector<std::string> ReadKeys(size_t begin, size_t end) {
    std::vector<std::string> keys;
    keys.reserve(end - begin);
    for (size_t index = begin; index < end; ++index) {
        keys.push_back(ReadKey(index));
    }
    return keys;
}

std::string RemovedKey(size_t index) {
    return "remove_race_" + std::to_string(index);
}

}  // namespace

TEST(MasterServiceConcurrentScenarioTest, RemountsAcrossChangingCapacities) {
    MasterScenario("a segment remounts cleanly across changing capacities")
        .Given(MemoryNode("memory"))
        .When(MountUnmountMemoryCapacities(
            "variable", {1 * 16384_KB, 4 * 16384_KB, 10 * 16384_KB,
                         2 * 16384_KB, 7 * 16384_KB, 1 * 16384_KB, 3 * 16384_KB,
                         9 * 16384_KB, 5 * 16384_KB, 8 * 16384_KB}));
}

TEST(MasterServiceConcurrentScenarioTest, MountUnmountCyclesDoNotInterfere) {
    MasterScenario("concurrent mount and unmount cycles do not interfere")
        .Given(MemoryNode("memory"))
        .When(RaceMountUnmount("race-node")
                  .Nodes(4)
                  .Iterations(100)
                  .ExpectCyclesAtLeast(1));
}

TEST(MasterServiceConcurrentScenarioTest, ConcurrentPutStartsAdmitOneWriter) {
    MasterScenario("racing writers of one key admit a single writer")
        .Given(MemoryNode("memory"))
        .When(RacePutStart("contested_key", 1_KB).Threads(16))
        .Then(Object("contested_key").HasReplicas(1).HasCompleteReplicas(1))
        .Then(MatchingKeys("^contested_key$").HasCount(1));
}

TEST(MasterServiceConcurrentScenarioTest,
     GroupedAndUngroupedFirstCreatesAdmitOneWriter) {
    const std::string key = "grouped_ungrouped_first_create";
    MasterScenario scenario(
        "grouped and ungrouped first creates admit a single writer");
    scenario.Given(MemoryNode("memory"))
        .When(RacePutStart(key, 1_KB).Threads(16).AcrossGroups(
            {GroupOnDifferentShard(key), ""}))
        .Then(Object(key).HasReplicas(1).HasCompleteReplicas(1))
        .Then(MatchingKeys("^" + key + "$").HasCount(1));
}

TEST(MasterServiceConcurrentScenarioTest,
     DifferentlyGroupedFirstCreatesAdmitOneWriter) {
    const std::string key = "different_grouped_first_create";
    const std::string group_a = GroupOnDifferentShard(key);
    MasterScenario scenario(
        "first creates under different groups admit a single writer");
    scenario.Given(MemoryNode("memory"))
        .When(RacePutStart(key, 1_KB).Threads(16).AcrossGroups(
            {group_a, GroupOnDifferentShard(group_a)}))
        .Then(Object(key).HasReplicas(1).HasCompleteReplicas(1))
        .Then(MatchingKeys("^" + key + "$").HasCount(1));
}

TEST(MasterServiceConcurrentScenarioTest, RemoveAllSweepsAroundInFlightWrites) {
    MasterScenario scenario("RemoveAll sweeps around writes still in flight");
    scenario.Given(MemoryNode("memory").Capacity(256 * 1024 * 1024))
        .When(RaceWritesWithRemoveAll("write-race")
                  .Writers(4)
                  .ObjectsPerWriter(100)
                  .OfSize(1_KB))
        .Then(Object("write-race-0-0").DoesNotExist())
        .Then(Object("write-race-3-99").DoesNotExist());
}

TEST(MasterServiceConcurrentScenarioTest, RemoveAllPreservesInFlightWrite) {
    MasterScenario scenario("an in-flight write completes after RemoveAll",
                            LeaseConfig(0));
    scenario.Given(MemoryNode("memory"))
        .When(PutStart("completed", 1_KB))
        .When(PutEnd("completed"))
        .When(PutStart("in-flight", 1_KB))
        .When(RemoveAll().ExpectRemoved(1))
        .Then(Object("completed").DoesNotExist())
        .When(PutEnd("in-flight"))
        .Then(Object("in-flight").IsReadable())
        .When(RemoveAll().ExpectRemoved(1))
        .Then(Object("in-flight").DoesNotExist());
}

TEST(MasterServiceConcurrentScenarioTest, LeasedReadsHoldOffARacingRemoveAll) {
    constexpr size_t kObjectCount = 1000;
    constexpr uint64_t kLeaseTtlMs = 200;
    MasterScenario scenario("reads lease objects past a racing RemoveAll",
                            LeaseConfig(kLeaseTtlMs));
    scenario.Given(MemoryNode("memory").Capacity(256 * 1024 * 1024))
        .Given(Objects(0, kObjectCount)
                   .NamedBy(ReadKey)
                   .Size(1_KB)
                   .CompleteOn("memory"))
        .When(RaceReadsWithRemoveAll(ReadKeys(0, kObjectCount))
                  .Readers(4)
                  .FinalRemoveAfter(std::chrono::milliseconds(2 * kLeaseTtlMs))
                  .ExpectTotalRemoved(kObjectCount))
        .Then(Objects(0, kObjectCount).NamedBy(ReadKey).DoNotExist());
}

TEST(MasterServiceConcurrentScenarioTest,
     ConcurrentRemoveAllOperationsPartitionTheStore) {
    constexpr size_t kObjectCount = 1000;
    MasterScenario scenario("two RemoveAll sweeps partition the objects");
    scenario.Given(MemoryNode("memory").Capacity(256 * 1024 * 1024))
        .Given(Objects(0, kObjectCount)
                   .NamedBy(RemovedKey)
                   .Size(1_KB)
                   .CompleteOn("memory"))
        .When(RaceRemoveAll().Callers(2).ExpectTotalRemoved(kObjectCount))
        .Then(Objects(0, kObjectCount).NamedBy(RemovedKey).DoNotExist());
}

TEST(MasterServiceConcurrentScenarioTest, ConcurrentLocalDiskMountsAllSucceed) {
    MasterScenario("one hundred clients mount local disks together",
                   OffloadEnabledConfig())
        .Given(MemoryNode("memory"))
        .When(RaceMountLocalDisk("disk-client").Clients(100));
}

}  // namespace mooncake::test
