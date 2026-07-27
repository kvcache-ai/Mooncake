#include "master_scenario.h"

namespace mooncake::test {

TEST(MasterScenarioReplicaClearTest, ClearsAllEligibleReplicas) {
    const std::vector<std::string> keys = {"key-0", "key-1", "key-2", "key-3",
                                           "key-4"};
    MasterScenario scenario("batch replica clear removes eligible replicas");
    scenario.Configured(ServiceConfig().DefaultLeaseTtl(50))
        .Given(MemoryNode("memory"));
    for (const auto& key : keys) {
        scenario.When(Put(key, 1_KB).By("writer")).When(Read(key));
    }
    scenario.When(WaitFor(std::chrono::milliseconds(60)))
        .When(BatchReplicaClear(keys).By("writer").ExpectAffected(keys.size()))
        .Then(ObjectExistence(keys).Is({false, false, false, false, false}));
}

TEST(MasterScenarioReplicaClearTest, ClearsOnlyRequestedSegment) {
    MasterScenario("batch replica clear filters by segment")
        .Given(MemoryNode("segment-1"))
        .Given(MemoryNode("segment-2"))
        .When(Put("key", 1_KB).By("writer").PreferredSegment("segment-1"))
        .When(BatchReplicaClear({"key"}, "segment-1")
                  .By("writer")
                  .ExpectAffected(1))
        .Then(Object("key").DoesNotExist());
}

TEST(MasterScenarioReplicaClearTest, ActiveLeasePreventsClear) {
    MasterScenario("active lease prevents replica clear")
        .Configured(ServiceConfig().DefaultLeaseTtl(2000))
        .Given(MemoryNode("memory"))
        .When(Put("key", 1_KB).By("writer"))
        .When(Read("key"))
        .When(BatchReplicaClear({"key"}).By("writer").ExpectAffected(0))
        .Then(Object("key").Exists().IsReadable());
}

TEST(MasterScenarioReplicaClearTest, DifferentClientCannotClearReplica) {
    MasterScenario("different client cannot clear replica")
        .Given(MemoryNode("memory"))
        .When(Put("key", 1_KB).By("writer"))
        .When(BatchReplicaClear({"key"}).By("other").ExpectAffected(0))
        .Then(Object("key").Exists().IsReadable());
}

TEST(MasterScenarioReplicaClearTest, EmptyAndMissingInputsAreIgnored) {
    MasterScenario("empty and missing replica clear inputs are ignored")
        .Given(MemoryNode("memory"))
        .When(BatchReplicaClear({}).By("writer").ExpectAffected(0))
        .When(BatchReplicaClear({"missing-a", "missing-b"})
                  .By("writer")
                  .ExpectAffected(0));
}

TEST(MasterScenarioReplicaClearTest, EmptyKeysDoNotHideValidKey) {
    MasterScenario("empty replica clear keys do not hide valid key")
        .Given(MemoryNode("memory"))
        .When(Put("valid", 1_KB).By("writer"))
        .When(BatchReplicaClear({"", "valid", "", "missing"})
                  .By("writer")
                  .ExpectAffected(1))
        .Then(Object("valid").DoesNotExist());
}

TEST(MasterScenarioReplicaClearTest, MixedClientsPreserveInputSemantics) {
    MasterScenario("batch replica clear handles mixed clients")
        .Given(MemoryNode("memory"))
        .When(Put("writer-a-1", 1_KB).By("writer-a"))
        .When(Put("writer-a-2", 1_KB).By("writer-a"))
        .When(Put("writer-b", 1_KB).By("writer-b"))
        .When(BatchReplicaClear(
                  {"writer-a-1", "writer-a-2", "writer-b", "missing", ""})
                  .By("writer-a")
                  .ExpectAffected(2))
        .Then(Object("writer-a-1").DoesNotExist())
        .Then(Object("writer-a-2").DoesNotExist())
        .Then(Object("writer-b").Exists().IsReadable());
}

TEST(MasterScenarioTaskTest, CreateCopyTaskValidatesAndAssignsSourceOwner) {
    MasterScenario("create copy task validates and assigns source owner")
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .Given(MemoryNode("segment-2"))
        .When(Put("key", 1_KB).PreferredSegment("segment-0"))
        .When(CreateCopyTask("key")
                  .To({"segment-1", "segment-2"})
                  .ExpectTaskType(TaskType::REPLICA_COPY)
                  .ExpectAssignedTo("segment-0")
                  .SaveAs("copy"))
        .When(
            CreateCopyTask("key").To({}).ExpectError(ErrorCode::INVALID_PARAMS))
        .When(CreateCopyTask("missing")
                  .To({"segment-1"})
                  .ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(CreateCopyTask("key")
                  .To({"not-mounted"})
                  .ExpectError(ErrorCode::INVALID_PARAMS));
}

TEST(MasterScenarioTaskTest, CreateMoveTaskValidatesAndAssignsSourceOwner) {
    MasterScenario("create move task validates and assigns source owner")
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .Given(MemoryNode("segment-2"))
        .When(Put("key", 1_KB).PreferredSegment("segment-0"))
        .When(CreateMoveTask("key")
                  .From("segment-0")
                  .To({"segment-1"})
                  .ExpectTaskType(TaskType::REPLICA_MOVE)
                  .ExpectAssignedTo("segment-0")
                  .SaveAs("move"))
        .When(CreateMoveTask("missing")
                  .From("segment-0")
                  .To({"segment-1"})
                  .ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(CreateMoveTask("key")
                  .From("segment-1")
                  .To({"segment-1"})
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(CreateMoveTask("key")
                  .From("segment-0")
                  .To({"not-mounted"})
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(CreateMoveTask("key")
                  .From("segment-2")
                  .To({"segment-1"})
                  .ExpectError(ErrorCode::INVALID_PARAMS));
}

TEST(MasterScenarioTaskTest, FetchTasksIsAssignedAndDrainsQueue) {
    MasterScenario("fetch tasks is assigned and drains queue")
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .When(FetchTasks().By("segment-0").ExpectTasks({}))
        .When(Put("key", 1_KB).PreferredSegment("segment-0"))
        .When(CreateCopyTask("key").To({"segment-1"}).SaveAs("copy"))
        .When(CreateMoveTask("key")
                  .From("segment-0")
                  .To({"segment-1"})
                  .SaveAs("move"))
        .When(FetchTasks().By("segment-0").ExpectTasks({"copy", "move"}))
        .When(FetchTasks().By("segment-1").ExpectTasks({}))
        .When(FetchTasks().By("segment-0").ExpectTasks({}));
}

TEST(MasterScenarioTaskTest, FetchTasksRespectsBatchSize) {
    MasterScenario("fetch tasks respects batch size")
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .When(Put("key", 1_KB).PreferredSegment("segment-0"))
        .When(CreateCopyTask("key").To({"segment-1"}).SaveAs("copy"))
        .When(CreateMoveTask("key")
                  .From("segment-0")
                  .To({"segment-1"})
                  .SaveAs("move"))
        .When(FetchTasks(1).By("segment-0").ExpectAffected(1))
        .When(FetchTasks(1).By("segment-0").ExpectAffected(1))
        .When(FetchTasks(1).By("segment-0").ExpectTasks({}));
}

TEST(MasterScenarioTaskTest, TaskCompletionUpdatesQueryableState) {
    MasterScenario("task completion updates queryable state")
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .When(Put("key", 1_KB).PreferredSegment("segment-0"))
        .When(CreateCopyTask("key")
                  .To({"segment-1"})
                  .ExpectAssignedTo("segment-0")
                  .SaveAs("copy"))
        .When(FetchTasks().By("segment-0").ExpectTasks({"copy"}))
        .When(CompleteTask("copy", TaskStatus::SUCCESS)
                  .By("segment-0")
                  .WithMessage("done"))
        .When(QueryTask("copy")
                  .ExpectTaskStatus(TaskStatus::SUCCESS)
                  .ExpectAssignedTo("segment-0")
                  .WithMessage("done"))
        .When(FetchTasks().By("segment-0").ExpectTasks({}));
}

TEST(MasterScenarioTaskTest, WrongClientCannotCompleteTask) {
    MasterScenario("wrong client cannot complete task")
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .When(Put("key", 1_KB).PreferredSegment("segment-0"))
        .When(CreateMoveTask("key")
                  .From("segment-0")
                  .To({"segment-1"})
                  .SaveAs("move"))
        .When(FetchTasks().By("segment-0").ExpectTasks({"move"}))
        .When(CompleteTask("move", TaskStatus::SUCCESS)
                  .By("segment-1")
                  .WithMessage("rejected")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT));
}

TEST(MasterScenarioTaskTest, CompletingUnknownTaskFails) {
    MasterScenario("completing unknown task fails")
        .Given(MemoryNode("segment-0"))
        .When(CompleteTask("missing", TaskStatus::FAILED)
                  .By("segment-0")
                  .ExpectError(ErrorCode::TASK_NOT_FOUND));
}

TEST(MasterScenarioDrainTest, CreateDrainMarksSegmentAndSkipsAllocation) {
    MasterScenario("create drain marks segment and skips allocation")
        .Configured(ServiceConfig().DefaultLeaseTtl(0))
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .When(CreateDrainJob({"segment-0"})
                  .To({"segment-1"})
                  .MaxConcurrency(1)
                  .SaveAs("drain"))
        .Then(SegmentState("segment-0").HasStatus(SegmentStatus::DRAINING))
        .When(Put("key", 1_KB).PreferredSegment("segment-0"))
        .Then(Object("key").HasReplicasOn({"segment-1"}));
}

TEST(MasterScenarioDrainTest, CancelDrainRestoresSegmentStatus) {
    MasterScenario("cancel drain restores segment status")
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .When(CreateDrainJob({"segment-0"})
                  .To({"segment-1"})
                  .MaxConcurrency(1)
                  .SaveAs("drain"))
        .Then(SegmentState("segment-0").HasStatus(SegmentStatus::DRAINING))
        .When(CancelDrainJob("drain"))
        .Then(Job("drain").HasStatus(JobStatus::CANCELED))
        .Then(SegmentState("segment-0").HasStatus(SegmentStatus::OK));
}

TEST(MasterScenarioTaskTest, TenantTasksCarryTenantInPayload) {
    constexpr auto kTenant = "tenant_for_async_task";
    MasterScenario("tenant task payload preserves tenant identity")
        .Given(Tenant(kTenant).Quota(16_MB))
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .When(Put("tenant_task_key", 1_KB)
                  .ForTenant(kTenant)
                  .PreferredSegment("segment-0"))
        .When(CreateCopyTask("tenant_task_key")
                  .ForTenant(kTenant)
                  .To({"segment-1"})
                  .SaveAs("copy"))
        .When(CreateMoveTask("tenant_task_key")
                  .ForTenant(kTenant)
                  .From("segment-0")
                  .To({"segment-1"})
                  .SaveAs("move"))
        .When(FetchTasks()
                  .By("segment-0")
                  .ExpectTasks({"copy", "move"})
                  .ExpectPayloadTenant(kTenant)
                  .ExpectPayloadKey("tenant_task_key"));
}

TEST(MasterScenarioDrainTest, MoveTaskCompletionConvergesToDrained) {
    MasterScenario("drain worker completion converges to drained")
        .Configured(ServiceConfig().DefaultLeaseTtl(0))
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .When(Put("drained-key", 1_KB).PreferredSegment("segment-0"))
        .When(CreateDrainJob({"segment-0"})
                  .To({"segment-1"})
                  .MaxConcurrency(1)
                  .SaveAs("drain"))
        .When(ExecuteNextMoveTask().By("segment-0").ExpectAffected(1))
        .Eventually(Job("drain")
                        .HasStatus(JobStatus::SUCCEEDED)
                        .HasActiveUnits(0)
                        .HasAtLeastSucceededUnits(1))
        .Then(SegmentState("segment-0").HasStatus(SegmentStatus::DRAINED))
        .Then(Object("drained-key").IsReadable().HasReplicasOn({"segment-1"}));
}

TEST(MasterScenarioDrainTest, ActiveMoveTaskPreventsCancellation) {
    MasterScenario("active drain move task prevents cancellation")
        .Configured(ServiceConfig().DefaultLeaseTtl(0))
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .When(Put("active-key", 1_KB).PreferredSegment("segment-0"))
        .When(CreateDrainJob({"segment-0"})
                  .To({"segment-1"})
                  .MaxConcurrency(1)
                  .SaveAs("drain"))
        .When(
            FetchTasks().By("segment-0").WaitUntilAvailable().ExpectAffected(1))
        .When(CancelDrainJob("drain").ExpectError(
            ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));
}

TEST(MasterScenarioDrainTest, RetryBudgetExhaustionFailsDrain) {
    MasterScenario("drain fails after move retry budget is exhausted")
        .Configured(ServiceConfig().DefaultLeaseTtl(0))
        .Given(MemoryNode("segment-0"))
        .Given(MemoryNode("segment-1"))
        .When(Put("failed-key", 1_KB).PreferredSegment("segment-0"))
        .When(CreateDrainJob({"segment-0"})
                  .To({"segment-1"})
                  .MaxConcurrency(1)
                  .SaveAs("drain"))
        .When(ExecuteNextMoveTask(TaskStatus::FAILED)
                  .By("segment-0")
                  .ExpectAffected(1))
        .When(ExecuteNextMoveTask(TaskStatus::FAILED)
                  .By("segment-0")
                  .ExpectAffected(1))
        .When(ExecuteNextMoveTask(TaskStatus::FAILED)
                  .By("segment-0")
                  .ExpectAffected(1))
        .Eventually(Job("drain")
                        .HasStatus(JobStatus::FAILED)
                        .HasActiveUnits(0)
                        .HasAtLeastFailedUnits(3))
        .Then(SegmentState("segment-0").HasStatus(SegmentStatus::OK));
}

}  // namespace mooncake::test
