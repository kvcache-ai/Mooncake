#include "master_service/dsl/scenario.h"

#include <gtest/gtest.h>

namespace mooncake::test {

TEST(MasterServiceTaskScenarioTest, CreateCopyTask) {
    MasterScenario("copy tasks validate inputs and go to the source owner")
        .Given(MemoryNode("node-0"))
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .When(PutStart("copy_task_key", 1_KB).OnNode("node-0"))
        .When(PutEnd("copy_task_key"))
        .When(CreateCopyTask("copy-task", "copy_task_key")
                  .To({"node-1", "node-2"}))
        .Then(NamedTask("copy-task")
                  .HasType(TaskType::REPLICA_COPY)
                  .IsAssignedTo("node-0"))
        .When(CreateCopyTask("empty-targets", "copy_task_key")
                  .To({})
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(CreateCopyTask("missing-object", "missing_key")
                  .To({"node-1"})
                  .ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(CreateCopyTask("unmounted-target", "copy_task_key")
                  .To({"ghost-node"})
                  .ExpectError(ErrorCode::INVALID_PARAMS));
}

TEST(MasterServiceTaskScenarioTest, CreateMoveTask) {
    MasterScenario("move tasks validate inputs and go to the source owner")
        .Given(MemoryNode("node-0"))
        .Given(MemoryNode("node-1"))
        .Given(MemoryNode("node-2"))
        .When(PutStart("move_task_key", 1_KB).OnNode("node-0"))
        .When(PutEnd("move_task_key"))
        .When(CreateMoveTask("move-task", "move_task_key")
                  .From("node-0")
                  .To("node-1"))
        .Then(NamedTask("move-task")
                  .HasType(TaskType::REPLICA_MOVE)
                  .IsAssignedTo("node-0"))
        .When(CreateMoveTask("missing-object", "missing_key")
                  .From("node-0")
                  .To("node-1")
                  .ExpectError(ErrorCode::OBJECT_NOT_FOUND))
        .When(CreateMoveTask("same-source-target", "move_task_key")
                  .From("node-1")
                  .To("node-1")
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(CreateMoveTask("unmounted-target", "move_task_key")
                  .From("node-0")
                  .To("ghost-node")
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(CreateMoveTask("source-without-replica", "move_task_key")
                  .From("node-2")
                  .To("node-1")
                  .ExpectError(ErrorCode::INVALID_PARAMS))
        .When(CreateMoveTask("unmounted-source", "move_task_key")
                  .From("ghost-node")
                  .To("node-1")
                  .ExpectError(ErrorCode::INVALID_PARAMS));
}

TEST(MasterServiceTaskScenarioTest, QueryTask) {
    MasterScenario("query task reports known and unknown tasks")
        .Given(MemoryNode("node-0"))
        .Given(MemoryNode("node-1"))
        .When(PutStart("query_task_key", 1_KB).OnNode("node-0"))
        .When(PutEnd("query_task_key"))
        .When(CreateMoveTask("move-task", "query_task_key")
                  .From("node-0")
                  .To("node-1"))
        .Then(UnknownTask("never-created"))
        .Then(NamedTask("move-task")
                  .HasType(TaskType::REPLICA_MOVE)
                  .IsAssignedTo("node-0"));
}

TEST(MasterServiceTaskScenarioTest, FetchTasksEmptyWhenNoTasks) {
    MasterScenario("fetching without tasks returns nothing")
        .Given(MemoryNode("node-0"))
        .When(FetchTasks("node-0").ExpectCount(0));
}

TEST(MasterServiceTaskScenarioTest, FetchTasksReturnsAssignedTasksOnly) {
    MasterScenario("fetch returns the owner's tasks once")
        .Given(MemoryNode("node-0"))
        .Given(MemoryNode("node-1"))
        .When(PutStart("fetch_tasks_key", 1_KB).OnNode("node-0"))
        .When(PutEnd("fetch_tasks_key"))
        .When(CreateCopyTask("copy-task", "fetch_tasks_key").To({"node-1"}))
        .When(CreateMoveTask("move-task", "fetch_tasks_key")
                  .From("node-0")
                  .To("node-1"))
        .When(FetchTasks("node-0").ExpectCount(2))
        .Then(NamedTask("copy-task")
                  .HasStatus(TaskStatus::PROCESSING)
                  .IsAssignedTo("node-0"))
        .Then(NamedTask("move-task")
                  .HasStatus(TaskStatus::PROCESSING)
                  .IsAssignedTo("node-0"))
        .When(FetchTasks("node-1").ExpectCount(0))
        .When(FetchTasks("node-0").ExpectCount(0));
}

TEST(MasterServiceTaskScenarioTest, FetchTasksRespectsBatchSize) {
    MasterScenario("fetch drains pending tasks one batch at a time")
        .Given(MemoryNode("node-0"))
        .Given(MemoryNode("node-1"))
        .When(PutStart("batch_size_key", 1_KB).OnNode("node-0"))
        .When(PutEnd("batch_size_key"))
        .When(CreateCopyTask("copy-task", "batch_size_key").To({"node-1"}))
        .When(CreateMoveTask("move-task", "batch_size_key")
                  .From("node-0")
                  .To("node-1"))
        .When(FetchTasks("node-0").Limit(1).ExpectCount(1))
        .When(FetchTasks("node-0").Limit(1).ExpectCount(1))
        .Then(NamedTask("copy-task").HasStatus(TaskStatus::PROCESSING))
        .Then(NamedTask("move-task").HasStatus(TaskStatus::PROCESSING))
        .When(FetchTasks("node-0").Limit(1).ExpectCount(0));
}

TEST(MasterServiceTaskScenarioTest, CompleteTaskSuccessFlow) {
    MasterScenario("a fetched task completes with status and message")
        .Given(MemoryNode("node-0"))
        .Given(MemoryNode("node-1"))
        .When(PutStart("complete_task_key", 1_KB).OnNode("node-0"))
        .When(PutEnd("complete_task_key"))
        .When(CreateCopyTask("copy-task", "complete_task_key").To({"node-1"}))
        .When(FetchTasks("node-0").ExpectCount(1))
        .When(CompleteTask("copy-task").By("node-0").WithMessage("done"))
        .Then(NamedTask("copy-task")
                  .HasStatus(TaskStatus::SUCCESS)
                  .IsAssignedTo("node-0")
                  .HasMessage("done"))
        .When(FetchTasks("node-0").ExpectCount(0));
}

TEST(MasterServiceTaskScenarioTest, CompleteTaskRejectsWrongClient) {
    MasterScenario("only the assignee may complete a task")
        .Given(MemoryNode("node-0"))
        .Given(MemoryNode("node-1"))
        .When(PutStart("wrong_client_key", 1_KB).OnNode("node-0"))
        .When(PutEnd("wrong_client_key"))
        .When(CreateMoveTask("move-task", "wrong_client_key")
                  .From("node-0")
                  .To("node-1"))
        .When(FetchTasks("node-0").ExpectCount(1))
        .When(CompleteTask("move-task")
                  .By("node-1")
                  .WithMessage("should_not_work")
                  .ExpectError(ErrorCode::ILLEGAL_CLIENT));
}

TEST(MasterServiceTaskScenarioTest, CompleteTaskNotFound) {
    MasterScenario("completing an unknown task fails")
        .Given(MemoryNode("node-0"))
        .When(CompleteUnknownTask("never-created")
                  .By("node-0")
                  .WithMessage("not_found")
                  .ExpectError(ErrorCode::TASK_NOT_FOUND));
}

}  // namespace mooncake::test
