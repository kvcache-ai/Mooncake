#include "master_service/dsl/scenario.h"

#include <array>
#include <chrono>
#include <string>

#include <gtest/gtest.h>

namespace mooncake::test {

TEST(MasterServiceTest, PutStartEndFlow) {
    MasterScenario("put start/end flow")
        .Given(MemoryNode("memory"))
        .When(PutStart("test_key", 1_KB)
                  .By("writer")
                  .ExpectReplicas(1)
                  .ExpectStatus(ReplicaStatus::PROCESSING))
        .Then(Object("test_key").IsNotReady())
        .When(Remove("test_key").ExpectError(ErrorCode::REPLICA_IS_NOT_READY))
        .When(PutEnd("test_key")
                  .By("other-writer")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(PutRevoke("test_key")
                  .By("other-writer")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(PutEnd("test_key").By("writer"))
        .Then(Object("test_key")
                  .IsReadable()
                  .HasReplicas(1)
                  .HasCompleteReplicas(1));
}

TEST(MasterServiceTest, PutLifecycleForEveryReplicaCount) {
    constexpr std::array<size_t, 5> kReplicaCounts{1, 2, 3, 4, 5};
    MasterScenario scenario(
        "put lifecycle for replica counts one through five");
    for (size_t index : kReplicaCounts) {
        scenario.Given(MemoryNode("memory-" + std::to_string(index)));
    }
    for (size_t replica_count : kReplicaCounts) {
        const std::string key = "key-" + std::to_string(replica_count);
        scenario
            .When(PutStart(key, 1_KB)
                      .Replicas(replica_count)
                      .ExpectReplicas(replica_count)
                      .ExpectStatus(ReplicaStatus::PROCESSING))
            .Then(Object(key).IsNotReady())
            .When(Remove(key).ExpectError(ErrorCode::REPLICA_IS_NOT_READY))
            .When(PutEnd(key))
            .Then(Object(key)
                      .IsReadable()
                      .HasReplicas(replica_count)
                      .HasCompleteReplicas(replica_count));
    }
}

TEST(MasterServiceTest, GetReplicaListDistinguishesMissingAndReadable) {
    MasterScenario("get replica list distinguishes missing and readable")
        .Given(MemoryNode("memory"))
        .Then(Object("missing").DoesNotExist())
        .When(PutStart("key", 1_KB))
        .When(PutEnd("key"))
        .Then(Object("key").IsReadable().HasReplicas(1));
}

TEST(MasterServiceTest, RemoveObjectAndRejectMissingObject) {
    MasterScenario("remove object and reject a missing object")
        .Given(MemoryNode("memory"))
        .When(PutStart("key", 1_KB))
        .When(PutEnd("key"))
        .When(Remove("key"))
        .Then(Object("key").DoesNotExist())
        .When(Remove("missing").ExpectError(ErrorCode::OBJECT_NOT_FOUND));
}

TEST(MasterServiceTest, RepeatedPutAndRemoveIsDeterministic) {
    MasterScenario scenario("repeated put and remove with fixed keys");
    scenario.Given(MemoryNode("memory"));
    for (int index = 0; index < 10; ++index) {
        const std::string key = "key-" + std::to_string(index);
        scenario.When(PutStart(key, 1_KB))
            .When(PutEnd(key))
            .When(Remove(key))
            .Then(Object(key).DoesNotExist());
    }
}

TEST(MasterServiceTest, UpsertNewKey) {
    MasterScenario("upsert creates a new object")
        .Given(MemoryNode("memory"))
        .When(UpsertStart("upsert_new_key", 1_KB)
                  .By("writer")
                  .ExpectReplicas(1)
                  .ExpectStatus(ReplicaStatus::PROCESSING))
        .Then(Object("upsert_new_key").IsNotReady())
        .When(UpsertEnd("upsert_new_key").By("writer"))
        .Then(Object("upsert_new_key")
                  .IsReadable()
                  .HasReplicas(1)
                  .HasCompleteReplicas(1));
}

TEST(MasterServiceTest, UpsertPreemptsInProgressPut) {
    MasterScenario("upsert preempts an in-progress put")
        .Given(MemoryNode("memory"))
        .When(PutStart("upsert_preempt", 1_KB).By("old-writer"))
        .When(UpsertStart("upsert_preempt", 1_KB)
                  .By("new-writer")
                  .ExpectReplicas(1)
                  .ExpectStatus(ReplicaStatus::PROCESSING))
        .When(PutEnd("upsert_preempt")
                  .By("old-writer")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(UpsertEnd("upsert_preempt").By("new-writer"))
        .Then(Object("upsert_preempt")
                  .IsReadable()
                  .HasReplicas(1)
                  .HasCompleteReplicas(1));
}

TEST(MasterServiceTest, UpsertSameSizeRefreshesMetadata) {
    MasterScenario("same-size upsert refreshes writer identity")
        .Given(MemoryNode("memory"))
        .When(PutStart("upsert_refresh_metadata", 1_KB).By("old-writer"))
        .When(PutEnd("upsert_refresh_metadata").By("old-writer"))
        .When(UpsertStart("upsert_refresh_metadata", 1_KB).By("new-writer"))
        .When(UpsertEnd("upsert_refresh_metadata")
                  .By("old-writer")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(UpsertEnd("upsert_refresh_metadata").By("new-writer"))
        .Then(Object("upsert_refresh_metadata")
                  .IsReadable()
                  .HasReplicas(1)
                  .HasCompleteReplicas(1));
}

TEST(MasterServiceTest, UpsertRevoke) {
    MasterScenario("revoke a new-key upsert")
        .Given(MemoryNode("memory"))
        .When(UpsertStart("upsert_revoke", 1_KB).By("writer"))
        .Then(Object("upsert_revoke").IsNotReady())
        .When(UpsertRevoke("upsert_revoke").By("writer"))
        .Then(Object("upsert_revoke").DoesNotExist());
}

TEST(MasterServiceTest, UpsertInPlaceThenRevoke) {
    MasterScenario("revoke an in-place upsert")
        .Given(MemoryNode("memory"))
        .When(PutStart("upsert_inplace_revoke", 1_KB).By("old-writer"))
        .When(PutEnd("upsert_inplace_revoke").By("old-writer"))
        .When(UpsertStart("upsert_inplace_revoke", 1_KB).By("new-writer"))
        .Then(Object("upsert_inplace_revoke").IsNotReady())
        .When(UpsertRevoke("upsert_inplace_revoke").By("new-writer"))
        .Then(Object("upsert_inplace_revoke").DoesNotExist());
}

TEST(MasterServiceTest, UpsertPreemptsInProgressUpsert) {
    MasterScenario("upsert preempts an in-progress upsert")
        .Given(MemoryNode("memory"))
        .When(PutStart("upsert_preempt_upsert", 1_KB).By("first-writer"))
        .When(PutEnd("upsert_preempt_upsert").By("first-writer"))
        .When(
            UpsertStart("upsert_preempt_upsert", 1_KB).By("old-upsert-writer"))
        .Then(Object("upsert_preempt_upsert").IsNotReady())
        .When(UpsertStart("upsert_preempt_upsert", 1_KB)
                  .By("new-upsert-writer")
                  .ExpectReplicas(1)
                  .ExpectStatus(ReplicaStatus::PROCESSING))
        .When(UpsertEnd("upsert_preempt_upsert")
                  .By("old-upsert-writer")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(UpsertEnd("upsert_preempt_upsert").By("new-upsert-writer"))
        .Then(Object("upsert_preempt_upsert")
                  .IsReadable()
                  .HasReplicas(1)
                  .HasCompleteReplicas(1));
}

TEST(MasterServiceTest, UpsertDifferentSizeThenRevoke) {
    MasterScenario("revoke a different-size upsert")
        .Given(MemoryNode("memory"))
        .When(PutStart("upsert_diff_revoke", 1_KB).By("writer"))
        .When(PutEnd("upsert_diff_revoke").By("writer"))
        .Then(Object("upsert_diff_revoke")
                  .IsReadable()
                  .HasReplicas(1)
                  .HasCompleteReplicas(1))
        .When(UpsertStart("upsert_diff_revoke", 2_KB)
                  .By("writer")
                  .ExpectReplicas(1)
                  .ExpectStatus(ReplicaStatus::PROCESSING))
        .Then(Object("upsert_diff_revoke").IsNotReady())
        .When(UpsertRevoke("upsert_diff_revoke").By("writer"))
        .Then(Object("upsert_diff_revoke").DoesNotExist());
}

TEST(MasterServiceTest, PutStartInvalidParams) {
    MasterScenario("put start rejects invalid parameters")
        .Given(MemoryNode("memory"))
        .When(PutStart("test_key", 1_KB)
                  .Replicas(0)
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(PutStart("test_key", 0).ExpectError(ErrorCode::INVALID_PARAMS))
        .When(PutStart("test_key", 1_KB)
                  .NofReplicas(1)
                  .PreferSameNode()
                  .ExpectError(ErrorCode::INVALID_PARAMS));
}

TEST(MasterServiceTest, ObjectChecksumIsStoredAndClearedByUpsert) {
    MasterScenario("object checksum is stored and cleared by upsert")
        .Given(MemoryNode("memory"))
        .When(PutStart("checksum_key", 1_KB))
        .When(PutEnd("checksum_key").WithChecksum(0))
        .Then(Object("checksum_key").IsReadable().HasChecksum(0))
        .When(UpsertStart("checksum_key", 1_KB))
        .When(UpsertEnd("checksum_key"))
        .Then(Object("checksum_key").IsReadable().HasNoChecksum());
}

TEST(MasterServiceTest, UpsertSameSizeReusesBuffer) {
    MasterScenario("same-size upsert reuses the existing buffer")
        .Given(MemoryNode("memory"))
        .When(PutStart("upsert_same_size", 1_KB).By("writer"))
        .When(PutEnd("upsert_same_size").By("writer"))
        .When(UpsertStart("upsert_same_size", 1_KB)
                  .By("new-writer")
                  .ExpectReplicas(1)
                  .ExpectStatus(ReplicaStatus::PROCESSING)
                  .ExpectBufferReuse())
        .When(UpsertEnd("upsert_same_size").By("new-writer"))
        .Then(Object("upsert_same_size").IsReadable().HasCompleteReplicas(1));
}

TEST(MasterServiceTest, UpsertDifferentSizeAllocatesNewBuffer) {
    MasterScenario("different-size upsert allocates a new buffer")
        .Given(MemoryNode("memory"))
        .When(PutStart("upsert_diff_size", 1_KB))
        .When(PutEnd("upsert_diff_size"))
        .When(UpsertStart("upsert_diff_size", 2_KB)
                  .ExpectReplicas(1)
                  .ExpectStatus(ReplicaStatus::PROCESSING)
                  .ExpectNewBuffer())
        .When(UpsertEnd("upsert_diff_size"))
        .Then(Object("upsert_diff_size").IsReadable().HasCompleteReplicas(1));
}

TEST(MasterServiceTest, UpsertConflictReplicationTask) {
    MasterScenario("upsert fails while a copy is in progress")
        .Given(MemoryNode("node-a"))
        .Given(MemoryNode("node-b"))
        .When(PutStart("upsert_conflict_copy", 1_KB).OnNode("node-a"))
        .When(PutEnd("upsert_conflict_copy"))
        .When(CopyStart("upsert_conflict_copy").From("node-a").To({"node-b"}))
        .When(UpsertStart("upsert_conflict_copy", 1_KB)
                  .ExpectError(ErrorCode::OBJECT_HAS_REPLICATION_TASK));
}

TEST(MasterServiceTest, BatchUpsertStartMixesNewAndExistingKeys) {
    MasterScenario("batch upsert start handles new and existing keys")
        .Given(MemoryNode("memory"))
        .When(PutStart("key_1", 1_KB))
        .When(PutEnd("key_1"))
        .When(BatchUpsertStart({{"key_1", 1_KB}, {"key_2", 2_KB}}))
        .When(BatchUpsertEnd({"key_1", "key_2"}))
        .Then(Object("key_1").IsReadable().HasCompleteReplicas(1))
        .Then(Object("key_2").IsReadable().HasCompleteReplicas(1));
}

TEST(MasterServiceTest, RemoveAllRemovesEveryObject) {
    MasterScenario scenario("remove all removes every completed object");
    scenario.Given(MemoryNode("memory"))
        .Given(Objects(0, 10)
                   .NamedBy([](size_t index) {
                       return "test_key" + std::to_string(index);
                   })
                   .Size(1_KB)
                   .CompleteOn("memory"))
        .When(RemoveAll().ExpectRemoved(10))
        .Then(Objects(0, 10)
                  .NamedBy([](size_t index) {
                      return "test_key" + std::to_string(index);
                  })
                  .DoNotExist());
}

TEST(MasterServiceTest, RemoveAllSkipsLeasedObjects) {
    const auto expired =
        std::chrono::system_clock::now() - std::chrono::seconds(1);
    const auto name = [](size_t index) {
        return "test_key" + std::to_string(index);
    };
    MasterScenario scenario("remove all skips leased objects");
    scenario.Given(MemoryNode("memory"))
        .Given(Objects(0, 10).NamedBy(name).Size(1_KB).CompleteOn("memory"));
    for (size_t index = 5; index < 10; ++index) {
        scenario.Then(KeyExists(name(index)));
    }
    scenario.When(RemoveAll().ExpectRemoved(5))
        .Then(Objects(0, 5).NamedBy(name).DoNotExist());
    for (size_t index = 5; index < 10; ++index) {
        scenario.When(ExpireAt(name(index), expired));
    }
    scenario.When(RemoveAll().ExpectRemoved(5))
        .Then(Objects(5, 10).NamedBy(name).DoNotExist());
}

TEST(MasterServiceTest, RemoveLeasedObject) {
    const auto expired =
        std::chrono::system_clock::now() - std::chrono::seconds(1);
    MasterScenario("reads grant and extend leases that block remove")
        .Given(MemoryNode("memory"))
        .When(PutStart("test_key", 1_KB))
        .When(PutEnd("test_key"))
        .Then(KeyExists("test_key"))
        .When(Remove("test_key").ExpectError(ErrorCode::OBJECT_HAS_LEASE))
        .When(ExpireAt("test_key", expired))
        .Then(KeyExists("test_key"))
        .When(Remove("test_key").ExpectError(ErrorCode::OBJECT_HAS_LEASE))
        .When(ExpireAt("test_key", expired))
        .When(Remove("test_key"))
        .When(PutStart("test_key", 1_KB))
        .When(PutEnd("test_key"))
        .Then(Object("test_key").IsReadable())
        .When(Remove("test_key").ExpectError(ErrorCode::OBJECT_HAS_LEASE))
        .When(ExpireAt("test_key", expired))
        .Then(Object("test_key").IsReadable())
        .When(Remove("test_key").ExpectError(ErrorCode::OBJECT_HAS_LEASE))
        .When(ExpireAt("test_key", expired))
        .When(Remove("test_key"))
        .Then(Object("test_key").DoesNotExist());
}

TEST(MasterServiceTest, ForceRemoveLeasedObject) {
    MasterScenario("force remove bypasses an active lease")
        .Given(MemoryNode("memory"))
        .When(PutStart("leased_key", 1_KB))
        .When(PutEnd("leased_key"))
        .Then(KeyExists("leased_key"))
        .When(Remove("leased_key").ExpectError(ErrorCode::OBJECT_HAS_LEASE))
        .When(Remove("leased_key").Force())
        .Then(Object("leased_key").DoesNotExist());
}

TEST(MasterServiceTest, ForceRemoveByRegexLeasedObjects) {
    const auto name = [](size_t index) {
        return "force_regex_key_" + std::to_string(index);
    };
    MasterScenario scenario("force remove by regex bypasses active leases");
    scenario.Given(MemoryNode("memory"))
        .Given(Objects(0, 5).NamedBy(name).Size(1_KB).CompleteOn("memory"));
    for (size_t index = 0; index < 5; ++index) {
        scenario.Then(KeyExists(name(index)));
    }
    scenario.When(RemoveByRegex("^force_regex_key_").ExpectRemoved(0));
    for (size_t index = 0; index < 5; ++index) {
        scenario.Then(KeyExists(name(index)));
    }
    scenario.When(RemoveByRegex("^force_regex_key_").Force().ExpectRemoved(5))
        .Then(Objects(0, 5).NamedBy(name).DoNotExist());
}

TEST(MasterServiceTest, ForceRemoveAllLeasedObjects) {
    const auto name = [](size_t index) {
        return "force_all_key_" + std::to_string(index);
    };
    MasterScenario scenario("force remove all bypasses active leases");
    scenario.Given(MemoryNode("memory"))
        .Given(Objects(0, 10).NamedBy(name).Size(1_KB).CompleteOn("memory"));
    for (size_t index = 0; index < 10; ++index) {
        scenario.Then(KeyExists(name(index)));
    }
    scenario.When(RemoveAll().ExpectRemoved(0));
    for (size_t index = 0; index < 10; ++index) {
        scenario.Then(KeyExists(name(index)));
    }
    scenario.When(RemoveAll().Force().ExpectRemoved(10))
        .Then(Objects(0, 10).NamedBy(name).DoNotExist());
}

TEST(MasterServiceTest, GetReplicaListByRegex) {
    MasterScenario scenario("get replica list by regex matches completed keys");
    scenario.Given(MemoryNode("memory"))
        .Given(Objects(0, 10)
                   .NamedBy([](size_t index) {
                       return "test_key" + std::to_string(index);
                   })
                   .Size(1_KB)
                   .CompleteOn("memory"))
        .Then(Object("non_existent_key").DoesNotExist())
        .Then(MatchingKeys("^test_key").HasCount(10));
}

TEST(MasterServiceTest, GetReplicaListByRegexComplex) {
    MasterScenario("get replica list by regex handles complex patterns")
        .Given(MemoryNode("memory"))
        .Given(
            Objects({"test_key_01", "test_key_02", "test_key_10",
                     "prod_key_alpha", "prod_key_beta", "data_part_1_chunk_a",
                     "data_part_2_chunk_b", "config/user/settings.json",
                     "logs/app-2025-08-13.log", "short",
                     "a_very_very_very_long_key_that_tests_length_limits",
                     "test-key-extra", "another_key"})
                .Size(1_KB)
                .CompleteOn("memory"))
        .Then(MatchingKeys("^test_key_").HasCount(3))
        .Then(MatchingKeys("^test_key_\\d+$").HasCount(3))
        .Then(MatchingKeys("^data_part_\\d_chunk_.$").HasCount(2))
        .Then(MatchingKeys("key").HasCount(8))
        .Then(MatchingKeys("\\.log$").HasCount(1).HasKeys(
            {"logs/app-2025-08-13.log"}))
        .Then(MatchingKeys("^prod|\\.json$").HasCount(3))
        .Then(MatchingKeys("^non_existent_prefix_").HasCount(0))
        .Then(MatchingKeys("^short$").HasCount(1).HasKeys({"short"}))
        .Then(MatchingKeys(".*absolutely_non_existent.*").HasCount(0));
}

TEST(MasterServiceTest, RemoveByRegex) {
    const auto name = [](size_t index) {
        return "test_key" + std::to_string(index);
    };
    MasterScenario scenario("remove by regex removes matching keys");
    scenario.Given(MemoryNode("memory"))
        .Given(Objects(0, 10).NamedBy(name).Size(1_KB).CompleteOn("memory"))
        .When(RemoveByRegex("^test_key").ExpectRemoved(10))
        .Then(Objects(0, 10).NamedBy(name).DoNotExist());
}

TEST(MasterServiceTest, RemoveByRegexComplex) {
    const auto keys = [] {
        return Objects({"test_key_01", "test_key_02", "test_key_10",
                        "prod_key_alpha", "prod_key_beta",
                        "data_part_1_chunk_a", "data_part_2_chunk_b",
                        "config/user/settings.json", "logs/app-2025-08-13.log",
                        "short",
                        "a_very_very_very_long_key_that_tests_length_limits",
                        "test-key-extra", "another_key"})
            .Size(1_KB)
            .CompleteOn("memory");
    };

    MasterScenario("remove by regex removes a prefix subset")
        .Given(MemoryNode("memory"))
        .Given(keys())
        .When(RemoveByRegex("^test_key_").ExpectRemoved(3))
        .Then(
            Objects({"test_key_01", "test_key_02", "test_key_10"}).DoNotExist())
        .Then(KeyExists("prod_key_alpha"))
        .Then(KeyExists("short"))
        .Then(KeyExists("test-key-extra"));

    MasterScenario("remove by regex removes everything with a wildcard")
        .Given(MemoryNode("memory"))
        .Given(keys())
        .When(RemoveByRegex(".*").ExpectRemoved(13))
        .Then(MatchingKeys(".*").HasCount(0));

    MasterScenario(
        "remove by regex with a non-matching pattern removes nothing")
        .Given(MemoryNode("memory"))
        .Given(keys())
        .When(RemoveByRegex("^nonexistent-pattern-").ExpectRemoved(0))
        .Then(MatchingKeys(".*").HasCount(13));

    MasterScenario("remove by regex handles alternation patterns")
        .Given(MemoryNode("memory"))
        .Given(keys())
        .When(RemoveByRegex("chunk|config").ExpectRemoved(3))
        .Then(Objects({"data_part_1_chunk_a", "data_part_2_chunk_b",
                       "config/user/settings.json"})
                  .DoNotExist())
        .Then(KeyExists("prod_key_alpha"));

    MasterScenario("remove by regex combines path and trailing-digit matches")
        .Given(MemoryNode("memory"))
        .Given(keys())
        .When(RemoveByRegex("/|\\d$").ExpectRemoved(5))
        .Then(Objects({"test_key_01", "test_key_02", "test_key_10",
                       "config/user/settings.json", "logs/app-2025-08-13.log"})
                  .DoNotExist())
        .Then(MatchingKeys(".*").HasCount(8).HasKeys(
            {"prod_key_alpha", "prod_key_beta", "data_part_1_chunk_a",
             "data_part_2_chunk_b", "short",
             "a_very_very_very_long_key_that_tests_length_limits",
             "test-key-extra", "another_key"}));
}

TEST(MasterServiceTest, BatchExistKey) {
    const auto name = [](size_t index) {
        return "test_key" + std::to_string(index);
    };
    MasterScenario scenario(
        "batch exist key reports completed and missing keys");
    scenario.Given(MemoryNode("memory"))
        .Given(Objects(0, 10).NamedBy(name).Size(1_KB).CompleteOn("memory"));
    for (size_t index = 0; index < 10; ++index) {
        scenario.Then(KeyExists(name(index)));
    }
    scenario.Then(
        BatchExistence({"test_key0", "test_key1", "test_key2", "test_key3",
                        "test_key4", "test_key5", "test_key6", "test_key7",
                        "test_key8", "test_key9", "non_existent_key"})
            .Returns({true, true, true, true, true, true, true, true, true,
                      true, false}));
}

TEST(MasterServiceTest, SoftPinRequestValidation) {
    auto config = MasterServiceConfig::builder()
                      .set_default_kv_soft_pin_ttl(50)
                      .set_max_kv_soft_pin_ttl(100)
                      .build();
    MasterScenario("soft pin requests are validated at put start",
                   std::move(config))
        .Given(MemoryNode("memory"))
        .When(PutStart("preserve_with_ttl", 1_KB)
                  .WithSoftPinTtl(10)
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(PutStart("disable_with_ttl", 1_KB)
                  .WithSoftPinAction(SoftPinAction::DISABLE)
                  .WithSoftPinTtl(10)
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(PutStart("over_limit", 1_KB)
                  .WithSoftPin()
                  .WithSoftPinTtl(101)
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(PutStart("invalid_action", 1_KB)
                  .WithSoftPinAction(static_cast<SoftPinAction>(255))
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(PutStart("zero_ttl", 1_KB).WithSoftPin().WithSoftPinTtl(0))
        .When(PutEnd("zero_ttl"))
        .Then(Object("zero_ttl").IsReadable());
}

TEST(MasterServiceTest, SoftPinDoesNotBlockRemove) {
    MasterScenario("soft pinned objects can be removed")
        .Given(MemoryNode("memory"))
        .When(PutStart("soft_pinned", 1_KB).WithSoftPin())
        .When(PutEnd("soft_pinned"))
        .When(Remove("soft_pinned"))
        .Then(Object("soft_pinned").DoesNotExist())
        .When(PutStart("soft_pinned", 1_KB).WithSoftPin())
        .When(PutEnd("soft_pinned"))
        .When(RemoveAll().ExpectRemoved(1))
        .Then(Object("soft_pinned").DoesNotExist());
}

TEST(MasterServiceTest, HardPinDefaultIsFalse) {
    MasterScenario("hard pin is opt-in per put")
        .Given(MemoryNode("memory"))
        .When(PutStart("normal_key", 1_KB))
        .When(PutEnd("normal_key"))
        .When(PutStart("hp_key", 1_KB).WithHardPin())
        .When(PutEnd("hp_key"))
        .Then(Object("normal_key").IsReadable())
        .Then(Object("hp_key").IsReadable());
}

}  // namespace mooncake::test
