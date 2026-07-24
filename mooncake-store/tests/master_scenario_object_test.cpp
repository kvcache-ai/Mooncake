#include "master_scenario.h"

namespace mooncake::test {
namespace {

const std::vector<std::string> kDiverseKeys = {
    "test_key_01",
    "test_key_02",
    "test_key_10",
    "prod_key_alpha",
    "prod_key_beta",
    "data_part_1_chunk_a",
    "data_part_2_chunk_b",
    "config/user/settings.json",
    "logs/app-2025-08-13.log",
    "short",
    "a_very_very_very_long_key_that_tests_length_limits",
    "test-key-extra",
    "another_key",
};

void PutAll(MasterScenario& scenario,
            const std::vector<std::string>& keys = kDiverseKeys) {
    for (const auto& key : keys) {
        scenario.When(Put(key, 1_KB));
    }
}

}  // namespace

TEST(MasterScenarioObjectTest, PutLifecycleForEveryReplicaCount) {
    MasterScenario scenario(
        "put lifecycle for replica counts one through five");
    for (int index = 0; index < 5; ++index) {
        scenario.Given(MemoryNode("memory-" + std::to_string(index)));
    }
    for (uint32_t replica_count = 1; replica_count <= 5; ++replica_count) {
        const std::string key = "key-" + std::to_string(replica_count);
        scenario
            .When(PutStart(key, 1_KB)
                      .Replicas(replica_count)
                      .ExpectReplicas(replica_count))
            .Then(Object(key).IsNotReady())
            .When(Remove(key).ExpectError(ErrorCode::REPLICA_IS_NOT_READY))
            .When(PutEnd(key))
            .Then(Object(key).Exists().IsReadable().HasCompleteReplicas(
                replica_count));
    }
}

TEST(MasterScenarioObjectTest, GetReplicaListDistinguishesMissingAndReadable) {
    MasterScenario("get replica list distinguishes missing and readable")
        .Given(MemoryNode("memory"))
        .Then(Object("missing").DoesNotExist())
        .When(Put("key", 1_KB))
        .Then(
            Object("key").Exists().IsReadable().HasCompleteReplicas(1).HasSize(
                1_KB));
}

TEST(MasterScenarioObjectTest, RemoveObjectAndRejectMissingObject) {
    MasterScenario("remove object")
        .Given(MemoryNode("memory"))
        .When(Put("key", 1_KB))
        .When(Remove("key"))
        .Then(Object("key").DoesNotExist())
        .When(Remove("missing").ExpectError(ErrorCode::OBJECT_NOT_FOUND));
}

TEST(MasterScenarioObjectTest, RepeatedPutAndRemoveIsDeterministic) {
    MasterScenario scenario("repeated put and remove");
    scenario.Given(MemoryNode("memory"));
    for (int index = 0; index < 10; ++index) {
        const std::string key = "key-" + std::to_string(index);
        scenario.When(Put(key, 1_KB))
            .When(Remove(key))
            .Then(Object(key).DoesNotExist());
    }
}

TEST(MasterScenarioObjectTest, RegexLookupSupportsComplexPatterns) {
    MasterScenario scenario("regex lookup supports complex patterns");
    scenario.Given(MemoryNode("memory"));
    PutAll(scenario);
    scenario
        .Then(MatchingObjects("^test_key_")
                  .HasKeys({"test_key_01", "test_key_02", "test_key_10"}))
        .Then(MatchingObjects("^test_key_\\d+$").HasCount(3))
        .Then(MatchingObjects("^data_part_\\d_chunk_.$").HasCount(2))
        .Then(MatchingObjects("key").HasCount(8))
        .Then(MatchingObjects("\\.log$").HasKeys({"logs/app-2025-08-13.log"}))
        .Then(MatchingObjects("^prod|\\.json$").HasCount(3))
        .Then(MatchingObjects("^non_existent_prefix_").HasCount(0))
        .Then(MatchingObjects("^short$").HasKeys({"short"}));
}

TEST(MasterScenarioObjectTest, RemoveByRegexReportsAndRemovesMatches) {
    MasterScenario scenario("remove by regex reports and removes matches");
    scenario.Given(MemoryNode("memory"))
        .Configured(ServiceConfig().DefaultLeaseTtl(5));
    PutAll(scenario);
    scenario.When(WaitFor(std::chrono::milliseconds(10)))
        .When(RemoveByRegex("chunk|config").ExpectAffected(3))
        .Then(Object("data_part_1_chunk_a").DoesNotExist())
        .Then(Object("data_part_2_chunk_b").DoesNotExist())
        .Then(Object("config/user/settings.json").DoesNotExist())
        .Then(Object("prod_key_alpha").Exists())
        .When(RemoveByRegex("^does-not-match").ExpectAffected(0));
}

TEST(MasterScenarioObjectTest, RemoveAllRemovesEveryObject) {
    MasterScenario scenario("remove all");
    scenario.Given(MemoryNode("memory"))
        .Configured(ServiceConfig().DefaultLeaseTtl(5));
    const std::vector<std::string> keys = {"first", "second", "third"};
    PutAll(scenario, keys);
    scenario.When(WaitFor(std::chrono::milliseconds(10)))
        .When(RemoveAll().ExpectAffected(keys.size()))
        .Then(MatchingObjects(".*").HasCount(0))
        .Then(ObjectExistence(keys).Is({false, false, false}));
}

TEST(MasterScenarioObjectTest, UnmountCleansStaleObjectHandles) {
    MasterScenario("unmount cleans stale object handles")
        .Given(MemoryNode("memory"))
        .When(Put("key", 1_MB).PreferredSegment("memory"))
        .Then(Object("key").IsReadable().HasReplicasOn({"memory"}))
        .When(UnmountNode("memory"))
        .Then(Object("key").DoesNotExist())
        .When(Remove("key").ExpectError(ErrorCode::OBJECT_NOT_FOUND));
}

TEST(MasterScenarioObjectTest, BatchExistencePreservesOrder) {
    std::vector<std::string> keys;
    MasterScenario scenario("batch existence preserves order");
    scenario.Given(MemoryNode("memory"));
    for (int index = 0; index < 10; ++index) {
        keys.push_back("key-" + std::to_string(index));
        scenario.When(Put(keys.back(), 1_KB));
    }
    keys.push_back("missing");
    scenario.Then(ObjectExistence(keys).Is(
        {true, true, true, true, true, true, true, true, true, true, false}));
}

}  // namespace mooncake::test
