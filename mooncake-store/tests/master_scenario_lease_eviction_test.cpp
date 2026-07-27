#include "master_scenario.h"

#include <functional>

namespace mooncake::test {
namespace {

std::string LeaseGroup(const std::string& key) {
    constexpr size_t kShardCount = 1024;
    const size_t key_shard = std::hash<std::string>{}(key) % kShardCount;
    for (int index = 0; index < 10000; ++index) {
        std::string group = key + "_group_" + std::to_string(index);
        if (std::hash<std::string>{}(group) % kShardCount != key_shard) {
            return group;
        }
    }
    return key + "_fallback_group";
}

}  // namespace

TEST(MasterScenarioLeaseTest, GroupLeaseRefreshProtectsCurrentMembers) {
    const std::string group = LeaseGroup("member-a");
    MasterScenario("group lease refresh protects current members")
        .Configured(ServiceConfig().DefaultLeaseTtl(200))
        .Given(MemoryNode("memory"))
        .When(Put("member-a", 1_KB).InGroup(group))
        .When(Put("member-b", 1_KB).InGroup(group))
        .Then(Object("member-a").Exists())
        .When(WaitFor(std::chrono::milliseconds(120)))
        .Then(Object("member-a").Exists())
        .When(WaitFor(std::chrono::milliseconds(100)))
        .When(Remove("member-b").ExpectError(ErrorCode::OBJECT_HAS_LEASE))
        .When(Remove("member-a").Force())
        .When(Remove("member-b").Force());
}

TEST(MasterScenarioLeaseTest,
     GroupLeaseRefreshIncludesMembersAddedAfterInitialRead) {
    const std::string group = LeaseGroup("member-a");
    MasterScenario("group lease refresh includes newly added members")
        .Configured(ServiceConfig().DefaultLeaseTtl(500))
        .Given(MemoryNode("memory"))
        .When(Put("member-a", 1_KB).InGroup(group))
        .Then(Object("member-a").Exists())
        .When(Put("member-b", 1_KB).InGroup(group))
        .When(WaitFor(std::chrono::milliseconds(150)))
        .Then(Object("member-a").Exists())
        .When(WaitFor(std::chrono::milliseconds(390)))
        .When(Remove("member-b").ExpectError(ErrorCode::OBJECT_HAS_LEASE))
        .When(Remove("member-a").Force())
        .When(Remove("member-b").Force());
}

TEST(MasterScenarioLeaseTest, ActiveLeaseBlocksRemoveUntilExpiry) {
    MasterScenario("active lease blocks remove until expiry")
        .Configured(ServiceConfig().DefaultLeaseTtl(50))
        .Given(MemoryNode("memory"))
        .When(Put("key", 1_KB))
        .Then(Object("key").Exists())
        .When(Remove("key").ExpectError(ErrorCode::OBJECT_HAS_LEASE))
        .When(WaitFor(std::chrono::milliseconds(60)))
        .When(Remove("key"))
        .Then(Object("key").DoesNotExist());
}

TEST(MasterScenarioLeaseTest, RepeatedReadRefreshesLease) {
    MasterScenario("repeated read refreshes lease")
        .Configured(ServiceConfig().DefaultLeaseTtl(50))
        .Given(MemoryNode("memory"))
        .When(Put("key", 1_KB))
        .When(Read("key"))
        .When(WaitFor(std::chrono::milliseconds(30)))
        .When(Read("key"))
        .When(WaitFor(std::chrono::milliseconds(30)))
        .When(Remove("key").ExpectError(ErrorCode::OBJECT_HAS_LEASE))
        .When(WaitFor(std::chrono::milliseconds(60)))
        .When(Remove("key"));
}

TEST(MasterScenarioLeaseTest, RemoveAllSkipsLeasedObjects) {
    MasterScenario scenario("remove all skips leased objects");
    scenario.Configured(ServiceConfig().DefaultLeaseTtl(50))
        .Given(MemoryNode("memory"));
    for (int index = 0; index < 10; ++index) {
        const std::string key = "key-" + std::to_string(index);
        scenario.When(Put(key, 1_KB));
        if (index >= 5) {
            scenario.When(Read(key));
        }
    }
    scenario.When(RemoveAll().ExpectAffected(5))
        .When(WaitFor(std::chrono::milliseconds(60)))
        .When(RemoveAll().ExpectAffected(5))
        .Then(MatchingObjects(".*").HasCount(0));
}

TEST(MasterScenarioLeaseTest, ForceRemoveOverridesActiveLease) {
    MasterScenario("force remove overrides active lease")
        .Configured(ServiceConfig().DefaultLeaseTtl(10000))
        .Given(MemoryNode("memory"))
        .When(Put("key", 1_KB))
        .When(Read("key"))
        .When(Remove("key").ExpectError(ErrorCode::OBJECT_HAS_LEASE))
        .When(Remove("key").Force())
        .Then(Object("key").DoesNotExist());
}

TEST(MasterScenarioLeaseTest, ForceRegexRemoveOverridesActiveLeases) {
    MasterScenario scenario("force regex remove overrides active leases");
    scenario.Configured(ServiceConfig().DefaultLeaseTtl(10000))
        .Given(MemoryNode("memory"));
    for (int index = 0; index < 5; ++index) {
        const std::string key = "force-key-" + std::to_string(index);
        scenario.When(Put(key, 1_KB)).When(Read(key));
    }
    scenario.When(RemoveByRegex("^force-key-").ExpectAffected(0))
        .When(RemoveByRegex("^force-key-").Force().ExpectAffected(5))
        .Then(MatchingObjects("^force-key-").HasCount(0));
}

TEST(MasterScenarioLeaseTest, ForceRemoveAllOverridesActiveLeases) {
    MasterScenario scenario("force remove all overrides active leases");
    scenario.Configured(ServiceConfig().DefaultLeaseTtl(10000))
        .Given(MemoryNode("memory"));
    for (int index = 0; index < 10; ++index) {
        const std::string key = "force-key-" + std::to_string(index);
        scenario.When(Put(key, 1_KB)).When(Read(key));
    }
    scenario.When(RemoveAll().ExpectAffected(0))
        .When(RemoveAll().Force().ExpectAffected(10))
        .Then(MatchingObjects(".*").HasCount(0));
}

TEST(MasterScenarioLeaseTest, HardPinIsOptIn) {
    MasterScenario("hard pin is opt in")
        .Given(MemoryNode("memory"))
        .When(Put("normal", 1_KB))
        .When(Put("hard-pinned", 1_KB).HardPinned())
        .Then(Object("normal").IsReadable())
        .Then(Object("hard-pinned").IsReadable())
        .When(Remove("normal").Force())
        .When(Remove("hard-pinned").Force());
}

TEST(MasterScenarioEvictionTest, GroupEvictionExpandsToSafeMembers) {
    const std::string group = LeaseGroup("grouped-evict-a");
    MasterScenario("group eviction expands to all safe members")
        .Configured(ServiceConfig().DefaultLeaseTtl(1000))
        .Given(MemoryNode("memory").Capacity(4_MB))
        .When(Put("grouped-evict-a", 2_MB).InGroup(group))
        .When(Put("grouped-evict-b", 2_MB).InGroup(group))
        .When(PutStart("trigger", 2_MB)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .Eventually(Object("grouped-evict-a").DoesNotExist())
        .Then(Object("grouped-evict-b").DoesNotExist());
}

TEST(MasterScenarioEvictionTest, ActiveGroupLeasePreventsEviction) {
    const std::string group = LeaseGroup("grouped-leased-a");
    MasterScenario("active group lease prevents eviction")
        .Configured(ServiceConfig().DefaultLeaseTtl(1000))
        .Given(MemoryNode("memory").Capacity(4_MB))
        .When(Put("grouped-leased-a", 2_MB).InGroup(group))
        .When(Put("grouped-leased-b", 2_MB).InGroup(group))
        .When(Read("grouped-leased-a"))
        .When(PutStart("trigger", 2_MB)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .When(WaitFor(std::chrono::milliseconds(250)))
        .Then(Object("grouped-leased-a").IsReadable())
        .Then(Object("grouped-leased-b").IsReadable());
}

TEST(MasterScenarioEvictionTest, UnsafeGroupMemberDoesNotProtectSafePeer) {
    const std::string group = LeaseGroup("safe-member");
    MasterScenario("unsafe group member does not protect safe peer")
        .Given(MemoryNode("memory").Capacity(4_MB))
        .When(Put("safe-member", 2_MB).InGroup(group))
        .When(Put("hard-pinned-member", 2_MB).InGroup(group).HardPinned())
        .When(PutStart("trigger", 2_MB)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .Eventually(Object("safe-member").DoesNotExist())
        .Then(Object("hard-pinned-member").IsReadable())
        .When(Remove("hard-pinned-member").Force());
}

TEST(MasterScenarioEvictionTest, ActiveObjectLeasesPreventEviction) {
    MasterScenario("active object leases prevent eviction")
        .Configured(ServiceConfig().DefaultLeaseTtl(500))
        .Given(MemoryNode("memory").Capacity(4_MB))
        .When(Put("leased-a", 2_MB))
        .When(Put("leased-b", 2_MB))
        .When(Read("leased-a"))
        .When(Read("leased-b"))
        .When(PutStart("trigger", 2_MB)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .When(WaitFor(std::chrono::milliseconds(100)))
        .Then(Object("leased-a").IsReadable())
        .Then(Object("leased-b").IsReadable());
}

TEST(MasterScenarioEvictionTest, SoftPinDoesNotBlockExplicitRemoval) {
    MasterScenario("soft pin does not block explicit removal")
        .Configured(ServiceConfig()
                        .DefaultLeaseTtl(200)
                        .DefaultSoftPinTtl(10000)
                        .AllowEvictSoftPinned(true))
        .Given(MemoryNode("memory"))
        .When(Put("soft-pinned", 1_KB).SoftPinned())
        .When(Remove("soft-pinned"))
        .When(Put("soft-pinned", 1_KB).SoftPinned())
        .When(RemoveAll().ExpectAffected(1))
        .Then(Object("soft-pinned").DoesNotExist());
}

TEST(MasterScenarioEvictionTest, DisabledSoftPinEvictionPreservesObjects) {
    MasterScenario("disabled soft pin eviction preserves objects")
        .Configured(ServiceConfig()
                        .DefaultLeaseTtl(200)
                        .DefaultSoftPinTtl(10000)
                        .AllowEvictSoftPinned(false))
        .Given(MemoryNode("memory").Capacity(4_MB))
        .When(Put("soft-a", 2_MB).SoftPinned())
        .When(Put("soft-b", 2_MB).SoftPinned())
        .When(PutStart("trigger", 2_MB)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .When(WaitFor(std::chrono::milliseconds(250)))
        .Then(Object("soft-a").IsReadable())
        .Then(Object("soft-b").IsReadable());
}

TEST(MasterScenarioEvictionTest, HardPinSurvivesMemoryPressure) {
    MasterScenario("hard pin survives memory pressure")
        .Configured(ServiceConfig().DefaultLeaseTtl(200))
        .Given(MemoryNode("memory").Capacity(4_MB))
        .When(Put("hard-pinned", 1_MB).HardPinned())
        .When(Put("normal-a", 1_MB))
        .When(Put("normal-b", 1_MB))
        .When(Put("normal-c", 1_MB))
        .When(PutStart("trigger", 1_MB)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .When(WaitFor(std::chrono::milliseconds(700)))
        .Then(Object("hard-pinned").IsReadable())
        .When(Remove("hard-pinned").Force())
        .Then(Object("hard-pinned").DoesNotExist());
}

TEST(MasterScenarioEvictionTest, HardPinOutlivesSoftAndNormalObjects) {
    MasterScenario("hard pin outlives soft and normal objects")
        .Configured(ServiceConfig()
                        .DefaultLeaseTtl(200)
                        .DefaultSoftPinTtl(10000)
                        .AllowEvictSoftPinned(true)
                        .EvictionRatio(0.5))
        .Given(MemoryNode("memory").Capacity(4_MB))
        .When(Put("hard-pinned", 1_MB).HardPinned())
        .When(Put("soft-pinned", 1_MB).SoftPinned())
        .When(Put("normal-a", 1_MB))
        .When(Put("normal-b", 1_MB))
        .When(PutStart("trigger", 1_MB)
                  .ExpectError(ErrorCode::NO_AVAILABLE_HANDLE))
        .When(WaitFor(std::chrono::milliseconds(700)))
        .Then(Object("hard-pinned").IsReadable())
        .When(Remove("hard-pinned").Force());
}

}  // namespace mooncake::test
