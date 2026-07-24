#include "master_scenario.h"

#include <atomic>
#include <chrono>
#include <thread>

namespace mooncake::test {

TEST(MasterScenarioTest, PutStartRejectsInvalidParameters) {
    MasterScenario("put start rejects invalid parameters")
        .Given(MemoryNode("node-a"))
        .When(PutStart("no-replicas", 1_KB)
                  .Replicas(0)
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(PutStart("zero-size", 0).ExpectError(ErrorCode::INVALID_PARAMS))
        .When(PutStart("invalid-placement", 1_KB)
                  .Replicas(1)
                  .NoFReplicas(1)
                  .PreferSameNode()
                  .ExpectError(ErrorCode::INVALID_PARAMS));
}

TEST(MasterScenarioTest, PutLifecycleUsesPublicContract) {
    MasterScenario("put lifecycle uses public contract")
        .Given(MemoryNode("node-a"))
        .When(PutStart("key", 1_KB).By("writer").SaveAs("start"))
        .Then(Object("key").IsNotReady())
        .When(Remove("key").ExpectError(ErrorCode::REPLICA_IS_NOT_READY))
        .When(PutEnd("key")
                  .By("other-writer")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(PutRevoke("key")
                  .By("other-writer")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT))
        .When(PutEnd("key").By("writer"))
        .Then(
            Object("key").Exists().IsReadable().HasCompleteReplicas(1).HasSize(
                1_KB));
}

TEST(MasterScenarioTest, ParallelRunsOrderIndependentOperations) {
    MasterScenario("parallel order-independent puts")
        .Given(MemoryNode("node-a"))
        .Parallel({
            Put("key-a", 1_KB).By("writer-a"),
            Put("key-b", 2_KB).By("writer-b"),
        })
        .Then(Object("key-a").Exists().IsReadable().HasSize(1_KB))
        .Then(Object("key-b").Exists().IsReadable().HasSize(2_KB));
}

TEST(MasterScenarioTest, TenantPutGetRemoveIsolatesSameUserKey) {
    MasterScenario("tenant isolation for the same user key")
        .Given(MemoryNode("node-a"))
        .Given(Tenant(std::string(TenantId::kDefaultValue)).Quota(4_MB))
        .Given(Tenant("tenant-a").Quota(4_MB))
        .Given(Tenant("tenant-b").Quota(4_MB))
        .When(Put("shared-key", 1_KB).ForTenant("tenant-a").By("writer-a"))
        .When(Put("shared-key", 2_KB).ForTenant("tenant-b").By("writer-b"))
        .Then(Object("shared-key").DoesNotExist())
        .Then(Object("shared-key")
                  .ForTenant("tenant-a")
                  .Exists()
                  .IsReadable()
                  .HasSize(1_KB))
        .Then(Object("shared-key")
                  .ForTenant("tenant-b")
                  .Exists()
                  .IsReadable()
                  .HasSize(2_KB))
        .When(Remove("shared-key").ForTenant("tenant-a").Force())
        .Then(Object("shared-key").ForTenant("tenant-a").DoesNotExist())
        .Then(Object("shared-key").ForTenant("tenant-b").Exists().IsReadable());
}

TEST(MasterScenarioTest, RegisteredTenantQuotaAdmissionIsIsolated) {
    MasterScenario("registered tenant quota admission is isolated")
        .Given(MemoryNode("node-a").Capacity(4_KB))
        .Given(Tenant("tenant-a").Quota(100))
        .When(Put("key-a", 80).ForTenant("tenant-a").By("writer").HardPinned())
        .When(PutStart("key-b", 30)
                  .ForTenant("tenant-a")
                  .By("writer")
                  .ExpectError(ErrorCode::TENANT_QUOTA_EXCEEDED))
        .Then(Tenant("tenant-a").UsedBytes(80).ReservedBytes(0))
        .Then(Tenant("tenant-b").DoesNotExist());
}

TEST(MasterScenarioTest, CopyStartRequiresQuotaForNewReplica) {
    MasterScenario("copy start reserves quota for a new replica")
        .Given(MemoryNode("node-a").Capacity(1_KB))
        .Given(MemoryNode("node-b").Capacity(1_KB))
        .Given(Tenant("tenant-a").Quota(150))
        .When(Put("key", 100)
                  .ForTenant("tenant-a")
                  .By("node-a")
                  .PreferredSegment("node-a"))
        .When(CopyStart("key")
                  .ForTenant("tenant-a")
                  .By("node-a")
                  .From("node-a")
                  .To({"node-b"})
                  .ExpectError(ErrorCode::TENANT_QUOTA_EXCEEDED))
        .Then(Tenant("tenant-a")
                  .UsedBytes(100)
                  .ReservedBytes(0)
                  .CommittedCount(1));
}

TEST(MasterScenarioTest, StalePutWriterCannotCompleteAfterUpsertPreemption) {
    MasterScenario("stale put writer cannot complete after upsert preemption")
        .Given(MemoryNode("node-a"))
        .When(PutStart("key", 1_KB).By("old-writer"))
        .Interleave({
            RunUntil("new-writer", UpsertStart("key", 1_KB).By("new-writer"),
                     MasterTestCheckpoint::UPSERT_AFTER_PREEMPT),
            Start("old-writer", PutEnd("key")
                                    .By("old-writer")
                                    .ExpectError(ErrorCode::ILLEGAL_CLIENT)),
            Resume("new-writer"),
            Join("new-writer"),
            Join("old-writer"),
        })
        .When(UpsertEnd("key").By("new-writer"))
        .Then(Object("key").IsReadable().HasCompleteReplicas(1));
}

TEST(MasterScenarioTest, StaleUpsertWriterCannotCompleteAfterPreemption) {
    MasterScenario("stale upsert writer cannot complete after preemption")
        .Given(MemoryNode("node-a"))
        .When(Put("key", 1_KB).By("seed-writer"))
        .When(UpsertStart("key", 1_KB).By("old-writer"))
        .Then(Object("key").IsNotReady())
        .Interleave({
            RunUntil("new-writer", UpsertStart("key", 1_KB).By("new-writer"),
                     MasterTestCheckpoint::UPSERT_AFTER_PREEMPT),
            Start("old-writer", UpsertEnd("key")
                                    .By("old-writer")
                                    .ExpectError(ErrorCode::ILLEGAL_CLIENT)),
            Resume("new-writer"),
            Join("new-writer"),
            Join("old-writer"),
        })
        .When(UpsertEnd("key").By("new-writer"))
        .Then(Object("key").IsReadable().HasCompleteReplicas(1));
}

TEST(MasterScenarioTest, TenantDeleteWaitsForInFlightReplicaMutation) {
    MasterScenario("tenant delete waits for in-flight replica mutation")
        .Given(MemoryNode("node-a").Capacity(1_KB))
        .Given(Tenant("tenant-a").Quota(1000))
        .Interleave({
            RunUntil("replica-writer",
                     AddReplica("cold", 128)
                         .ForTenant("tenant-a")
                         .By("replica-writer"),
                     MasterTestCheckpoint::ADD_REPLICA_AFTER_TENANT_VALIDATION),
            Start("admin", DeleteTenantPolicy("tenant-a")
                               .By("admin")
                               .ExpectError(ErrorCode::TENANT_NOT_EMPTY)),
            Resume("replica-writer"),
            Join("replica-writer"),
            Join("admin"),
        })
        .Then(Object("cold").ForTenant("tenant-a").Exists().IsReadable());
}

TEST(ScenarioCheckpointSchedulerTest, LaterArrivalWaitsForRelease) {
    ScenarioCheckpointScheduler scheduler(std::chrono::seconds(1));
    const UUID client = generate_uuid();
    scheduler.RegisterActor("writer", client);
    std::atomic<bool> continued{false};

    std::thread actor([&] {
        scheduler.Reach({MasterTestCheckpoint::UPSERT_AFTER_PREEMPT,
                         TenantId::Default(), "key", client});
        continued.store(true);
    });

    std::string error;
    ASSERT_TRUE(scheduler.WaitUntilReached(
        "writer", MasterTestCheckpoint::UPSERT_AFTER_PREEMPT, &error))
        << error;
    EXPECT_FALSE(continued.load());
    ASSERT_TRUE(scheduler.Resume("writer", &error)) << error;
    actor.join();
    EXPECT_TRUE(continued.load());
}

TEST(ScenarioCheckpointSchedulerTest, OccurrenceAndReplayAreDeterministic) {
    const UUID client = generate_uuid();
    ScenarioCheckpointScheduler capture(std::chrono::seconds(1));
    capture.RegisterActor("writer", client);
    capture.RecordOperation("OPERATION_BEGIN", "writer", "UpsertStart",
                            "default", "key", client);

    for (int i = 0; i < 2; ++i) {
        std::thread actor([&] {
            capture.Reach({MasterTestCheckpoint::UPSERT_AFTER_PREEMPT,
                           TenantId::Default(), "key", client});
        });
        std::string error;
        ASSERT_TRUE(capture.WaitUntilReached(
            "writer", MasterTestCheckpoint::UPSERT_AFTER_PREEMPT, &error))
            << error;
        ASSERT_TRUE(capture.Resume("writer", &error)) << error;
        actor.join();
    }
    capture.RecordOperation("OPERATION_END", "writer", "UpsertStart", "default",
                            "key", client, "OK");

    const auto releases = capture.Releases();
    ASSERT_EQ(releases.size(), 2u);
    EXPECT_EQ(releases[0].occurrence, 1u);
    EXPECT_EQ(releases[1].occurrence, 2u);

    ScenarioCheckpointScheduler replay(std::chrono::seconds(1));
    replay.RegisterActor("writer", client);
    replay.SetReplay(releases);
    replay.RecordOperation("OPERATION_BEGIN", "writer", "UpsertStart",
                           "default", "key", client);
    for (int i = 0; i < 2; ++i) {
        std::thread actor([&] {
            replay.Reach({MasterTestCheckpoint::UPSERT_AFTER_PREEMPT,
                          TenantId::Default(), "key", client});
        });
        std::string error;
        ASSERT_TRUE(replay.WaitUntilReached(
            "writer", MasterTestCheckpoint::UPSERT_AFTER_PREEMPT, &error))
            << error;
        ASSERT_TRUE(replay.Resume("writer", &error)) << error;
        actor.join();
    }
    replay.RecordOperation("OPERATION_END", "writer", "UpsertStart", "default",
                           "key", client, "OK");
    std::string error;
    EXPECT_TRUE(replay.ValidateReplayComplete(&error)) << error;
    const auto captured_trace = capture.Trace();
    const auto replayed_trace = replay.Trace();
    ASSERT_EQ(captured_trace.size(), replayed_trace.size());
    for (size_t index = 0; index < captured_trace.size(); ++index) {
        EXPECT_EQ(captured_trace[index].phase, replayed_trace[index].phase);
        EXPECT_EQ(captured_trace[index].actor, replayed_trace[index].actor);
        EXPECT_EQ(captured_trace[index].operation,
                  replayed_trace[index].operation);
        EXPECT_EQ(captured_trace[index].checkpoint,
                  replayed_trace[index].checkpoint);
        EXPECT_EQ(captured_trace[index].occurrence,
                  replayed_trace[index].occurrence);
        EXPECT_EQ(captured_trace[index].result, replayed_trace[index].result);
    }
}

TEST(ScenarioCheckpointSchedulerTest, TimeoutCancelsWaiters) {
    ScenarioCheckpointScheduler scheduler(std::chrono::milliseconds(200));
    const UUID first_client = generate_uuid();
    const UUID second_client = generate_uuid();
    scheduler.RegisterActor("first", first_client);
    scheduler.RegisterActor("second", second_client);
    std::atomic<int> continued{0};
    std::thread first([&] {
        scheduler.Reach({MasterTestCheckpoint::UPSERT_AFTER_PREEMPT,
                         TenantId::Default(), "key-a", first_client});
        ++continued;
    });
    std::thread second([&] {
        scheduler.Reach(
            {MasterTestCheckpoint::ADD_REPLICA_AFTER_TENANT_VALIDATION,
             TenantId::Default(), "key-b", second_client});
        ++continued;
    });

    std::string error;
    const bool first_reached = scheduler.WaitUntilReached(
        "first", MasterTestCheckpoint::UPSERT_AFTER_PREEMPT, &error);
    const bool second_reached = scheduler.WaitUntilReached(
        "second", MasterTestCheckpoint::ADD_REPLICA_AFTER_TENANT_VALIDATION,
        &error);
    first.join();
    second.join();
    EXPECT_TRUE(first_reached) << error;
    EXPECT_TRUE(second_reached) << error;
    EXPECT_EQ(continued.load(), 2);
    error = scheduler.Failure();
    EXPECT_NE(error.find("timeout"), std::string::npos);
    EXPECT_NE(error.find("paused actors"), std::string::npos);
}

TEST(ScenarioCheckpointSchedulerTest, ReplayMismatchFailsFast) {
    ScenarioCheckpointScheduler scheduler(std::chrono::seconds(1));
    const UUID client = generate_uuid();
    scheduler.RegisterActor("writer", client);
    scheduler.SetReplay({{"other", "UPSERT_AFTER_PREEMPT", 1}});

    std::thread actor([&] {
        scheduler.Reach({MasterTestCheckpoint::UPSERT_AFTER_PREEMPT,
                         TenantId::Default(), "key", client});
    });
    std::string error;
    ASSERT_TRUE(scheduler.WaitUntilReached(
        "writer", MasterTestCheckpoint::UPSERT_AFTER_PREEMPT, &error))
        << error;
    EXPECT_FALSE(scheduler.Resume("writer", &error));
    actor.join();
    EXPECT_NE(error.find("replay mismatch"), std::string::npos);
}

TEST(ScenarioCheckpointSchedulerTest, ReplayMetadataMismatchFailsFast) {
    ScenarioReplayArtifact artifact;
    artifact.version = 2;
    artifact.scenario = "scenario";
    artifact.test = "Suite.Test";
    auto error =
        ValidateScenarioReplayArtifact(artifact, "scenario", "Suite.Test");
    ASSERT_TRUE(error.has_value());
    EXPECT_NE(error->find("version"), std::string::npos);

    artifact.version = 1;
    artifact.scenario = "other";
    error = ValidateScenarioReplayArtifact(artifact, "scenario", "Suite.Test");
    ASSERT_TRUE(error.has_value());
    EXPECT_NE(error->find("scenario mismatch"), std::string::npos);
}

}  // namespace mooncake::test
