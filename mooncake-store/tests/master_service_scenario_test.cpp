#include "master_service/dsl/scenario.h"

#include <array>

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

}  // namespace mooncake::test
