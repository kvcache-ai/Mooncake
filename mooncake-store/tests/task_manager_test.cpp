#include <gtest/gtest.h>
#include <glog/logging.h>
#include "task_manager.h"
#include <thread>
#include <tuple>

namespace {
template <typename T, typename E>
T unwrap_expected_or_fail(const tl::expected<T, E>& exp) {
    EXPECT_TRUE(exp.has_value());
    return exp.value();
}
}  // namespace

namespace mooncake {

class ClientTaskManagerTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("TaskManagerTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST_F(ClientTaskManagerTest, SubmitAndPopTask) {
    ClientTaskManager manager({10000, 10000, 10000, 0, 0, 3});
    UUID client_id = generate_uuid();
    ReplicaCopyPayload payload{
        .key = "test_key", .source = "seg1", .targets = {"seg2"}};

    auto task_id_exp =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, payload);
    ASSERT_TRUE(task_id_exp.has_value());
    UUID task_id = task_id_exp.value();

    auto tasks = manager.get_write_access().pop_tasks(client_id, 10);
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(tasks[0].id, task_id);
    EXPECT_EQ(tasks[0].status, TaskStatus::PROCESSING);
}

TEST_F(ClientTaskManagerTest, FetchReplayChargesProcessingQuotaOnce) {
    ClientTaskManager manager({10000, 10000, 2, 0, 0, 3});
    const auto owner = generate_uuid();
    const auto peer = generate_uuid();
    const auto session = std::make_shared<ClientLivenessRecord>(
        owner, ClientLivenessRecord::Clock::now());
    auto write = manager.get_write_access();
    const ReplicaCopyPayload payload{
        .key = "key", .source = "a", .targets = {"b"}};
    auto first =
        write.submit_task_typed<TaskType::REPLICA_COPY>(owner, payload);
    auto second =
        write.submit_task_typed<TaskType::REPLICA_COPY>(owner, payload);
    auto other = write.submit_task_typed<TaskType::REPLICA_COPY>(peer, payload);
    ASSERT_TRUE(first && second && other);

    const auto opened = write.fetch_tasks_replay(session, {}, 0, 0);
    ASSERT_TRUE(opened);
    EXPECT_TRUE(opened->tasks.empty());
    EXPECT_FALSE(write.task_fetch_replay_active(session));
    const auto claimed = write.fetch_tasks_replay(session, opened->token, 1, 2);
    ASSERT_TRUE(claimed);
    ASSERT_EQ(claimed->tasks.size(), 2);
    EXPECT_EQ(claimed->tasks[0].id, *first);
    EXPECT_EQ(claimed->tasks[1].id, *second);
    EXPECT_TRUE(write.task_fetch_replay_active(session));

    const auto replayed =
        write.fetch_tasks_replay(session, opened->token, 1, 2);
    ASSERT_TRUE(replayed);
    ASSERT_EQ(replayed->tasks.size(), 2);
    EXPECT_EQ(replayed->tasks[0].id, *first);
    EXPECT_EQ(replayed->tasks[0].payload, claimed->tasks[0].payload);
    EXPECT_EQ(replayed->tasks[1].id, *second);
    EXPECT_FALSE(write.fetch_tasks_replay(session, opened->token, 1, 3));
    EXPECT_FALSE(write.fetch_tasks_replay(session, opened->token, 3, 1));
    EXPECT_TRUE(write.pop_tasks(peer, 2).empty());

    ASSERT_EQ(write.complete_task(owner, *first, TaskStatus::SUCCESS, ""),
              ErrorCode::OK);
    const auto peer_claim = write.pop_tasks(peer, 2);
    ASSERT_EQ(peer_claim.size(), 1);
    EXPECT_EQ(peer_claim[0].id, *other);
    const auto after_completion =
        write.fetch_tasks_replay(session, opened->token, 1, 2);
    ASSERT_TRUE(after_completion);
    ASSERT_EQ(after_completion->tasks.size(), 1);
    EXPECT_EQ(after_completion->tasks[0].id, *second);

    const auto acknowledged =
        write.fetch_tasks_replay(session, opened->token, 2, 1);
    ASSERT_TRUE(acknowledged);
    EXPECT_TRUE(acknowledged->tasks.empty());
    EXPECT_FALSE(write.fetch_tasks_replay(session, opened->token, 1, 2));
}

TEST_F(ClientTaskManagerTest, FetchReplayRejectsStaleSessionAndReplaysEmpty) {
    ClientTaskManager manager({10000, 10000, 2, 0, 0, 3});
    const auto owner = generate_uuid();
    const auto now = ClientLivenessRecord::Clock::now();
    const auto old_session = std::make_shared<ClientLivenessRecord>(owner, now);
    const auto new_session = std::make_shared<ClientLivenessRecord>(owner, now);
    auto write = manager.get_write_access();
    const auto old_lane = write.fetch_tasks_replay(old_session, {}, 0, 0);
    ASSERT_TRUE(old_lane);
    write.retire_task_fetch_replay(old_session);
    const auto new_lane = write.fetch_tasks_replay(new_session, {}, 0, 0);
    ASSERT_TRUE(new_lane);
    ASSERT_NE(old_lane->token, new_lane->token);
    write.retire_task_fetch_replay(old_session);
    EXPECT_FALSE(write.fetch_tasks_replay(new_session, old_lane->token, 1, 1));
    const auto empty =
        write.fetch_tasks_replay(new_session, new_lane->token, 1, 1);
    ASSERT_TRUE(empty);
    EXPECT_TRUE(empty->tasks.empty());
    auto id = write.submit_task_typed<TaskType::REPLICA_COPY>(
        owner,
        ReplicaCopyPayload{.key = "key", .source = "a", .targets = {"b"}});
    ASSERT_TRUE(id);
    const auto repeated =
        write.fetch_tasks_replay(new_session, new_lane->token, 1, 1);
    ASSERT_TRUE(repeated);
    EXPECT_TRUE(repeated->tasks.empty());
    const auto next =
        write.fetch_tasks_replay(new_session, new_lane->token, 2, 1);
    ASSERT_TRUE(next);
    ASSERT_EQ(next->tasks.size(), 1);
    EXPECT_EQ(next->tasks.front().id, *id);
}

TEST_F(ClientTaskManagerTest, FetchReplayClosesWithoutCreatingANewGrant) {
    ClientTaskManager manager({10000, 10000, 1, 0, 0, 3});
    const auto owner = generate_uuid();
    const auto peer = generate_uuid();
    const auto now = ClientLivenessRecord::Clock::now();
    const auto session = std::make_shared<ClientLivenessRecord>(owner, now);
    const auto peer_session = std::make_shared<ClientLivenessRecord>(peer, now);
    auto write = manager.get_write_access();
    const ReplicaCopyPayload payload{
        .key = "key", .source = "a", .targets = {"b"}};
    auto owner_task =
        write.submit_task_typed<TaskType::REPLICA_COPY>(owner, payload);
    auto peer_task =
        write.submit_task_typed<TaskType::REPLICA_COPY>(peer, payload);
    ASSERT_TRUE(owner_task && peer_task);

    const auto owner_lane = write.fetch_tasks_replay(session, {}, 0, 0);
    const auto peer_lane = write.fetch_tasks_replay(peer_session, {}, 0, 0);
    ASSERT_TRUE(owner_lane && peer_lane);
    const auto closed =
        write.fetch_tasks_replay(session, owner_lane->token, 1, 1, true);
    ASSERT_TRUE(closed);
    EXPECT_TRUE(closed->tasks.empty());
    EXPECT_FALSE(write.fetch_tasks_replay(session, owner_lane->token, 1, 1));
    const auto repeated_close =
        write.fetch_tasks_replay(session, owner_lane->token, 1, 1, true);
    ASSERT_TRUE(repeated_close);
    EXPECT_TRUE(repeated_close->tasks.empty());

    const auto granted =
        write.fetch_tasks_replay(peer_session, peer_lane->token, 1, 1);
    ASSERT_TRUE(granted);
    ASSERT_EQ(granted->tasks.size(), 1);
    const auto recovered =
        write.fetch_tasks_replay(peer_session, peer_lane->token, 1, 1, true);
    ASSERT_TRUE(recovered);
    ASSERT_EQ(recovered->tasks.size(), 1);
    EXPECT_EQ(recovered->tasks.front().id, *peer_task);
    EXPECT_FALSE(
        write.fetch_tasks_replay(peer_session, peer_lane->token, 2, 1));
    EXPECT_FALSE(
        write.fetch_tasks_replay(peer_session, peer_lane->token, 2, 1, true));
}

TEST_F(ClientTaskManagerTest, MarkTaskComplete) {
    ClientTaskManager manager({10000, 10000, 10000, 0, 0, 3});
    UUID client_id = generate_uuid();

    auto task_id_exp =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id,
            ReplicaCopyPayload{
                .key = "key1", .source = "seg1", .targets = {"seg2"}});
    ASSERT_TRUE(task_id_exp.has_value());
    UUID task_id = task_id_exp.value();

    // Initially pending
    auto task_opt = manager.get_read_access().find_task_by_id(task_id);
    ASSERT_TRUE(task_opt.has_value());
    EXPECT_EQ(task_opt->status, TaskStatus::PENDING);

    // Pop to make it processing
    manager.get_write_access().pop_tasks(client_id, 1);
    task_opt = manager.get_read_access().find_task_by_id(task_id);
    EXPECT_EQ(task_opt->status, TaskStatus::PROCESSING);

    // Mark success
    auto ec = manager.get_write_access().complete_task(
        client_id, task_id, TaskStatus::SUCCESS, "Completed successfully");
    EXPECT_EQ(ec, ErrorCode::OK);
    task_opt = manager.get_read_access().find_task_by_id(task_id);
    EXPECT_EQ(task_opt->status, TaskStatus::SUCCESS);
}

TEST_F(ClientTaskManagerTest, PruningLogic) {
    uint32_t max_tasks = 5;
    ClientTaskManager manager({max_tasks, 10000, 10000, 0, 0, 3});
    UUID client_id = generate_uuid();

    std::vector<UUID> task_ids;
    for (size_t i = 0; i < max_tasks + 2; ++i) {
        auto id_exp = manager.get_write_access()
                          .submit_task_typed<TaskType::REPLICA_COPY>(
                              client_id, ReplicaCopyPayload{
                                             .key = "key" + std::to_string(i),
                                             .source = "seg1",
                                             .targets = {"seg2"}});
        ASSERT_TRUE(id_exp.has_value());
        UUID id = id_exp.value();
        task_ids.push_back(id);
        manager.get_write_access().pop_tasks(client_id, 1);
        auto ec = manager.get_write_access().complete_task(
            client_id, id, TaskStatus::SUCCESS, "Done");
        EXPECT_EQ(ec, ErrorCode::OK);
    }

    manager.get_write_access().prune_finished_tasks();

    // The first 2 tasks should have been pruned
    EXPECT_FALSE(
        manager.get_read_access().find_task_by_id(task_ids[0]).has_value());
    EXPECT_FALSE(
        manager.get_read_access().find_task_by_id(task_ids[1]).has_value());

    // The last 5 tasks should still exist
    for (size_t i = 2; i < task_ids.size(); ++i) {
        EXPECT_TRUE(
            manager.get_read_access().find_task_by_id(task_ids[i]).has_value());
    }
}

TEST_F(ClientTaskManagerTest, MultipleClients) {
    ClientTaskManager manager({10000, 10000, 10000, 0, 0, 3});
    UUID client1 = generate_uuid();
    UUID client2 = generate_uuid();

    auto id1_exp =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client1, ReplicaCopyPayload{
                         .key = "key1", .source = "seg1", .targets = {"seg2"}});
    auto id2_exp =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client2, ReplicaCopyPayload{
                         .key = "key2", .source = "seg1", .targets = {"seg3"}});
    ASSERT_TRUE(id1_exp.has_value());
    ASSERT_TRUE(id2_exp.has_value());
    UUID id1 = id1_exp.value();
    UUID id2 = id2_exp.value();

    auto tasks1 = manager.get_write_access().pop_tasks(client1, 10);
    ASSERT_EQ(tasks1.size(), 1);
    EXPECT_EQ(tasks1[0].id, id1);

    auto tasks2 = manager.get_write_access().pop_tasks(client2, 10);
    ASSERT_EQ(tasks2.size(), 1);
    EXPECT_EQ(tasks2[0].id, id2);

    // Cross check: client1 shouldn't get client2's tasks
    auto tasks1_again = manager.get_write_access().pop_tasks(client1, 10);
    EXPECT_TRUE(tasks1_again.empty());
}

TEST_F(ClientTaskManagerTest, PendingLimitExceeded) {
    // max_total_pending_tasks=1
    ClientTaskManager manager({/*max_total_finished_tasks=*/10000,
                               /*max_total_pending_tasks=*/1,
                               /*max_total_processing_tasks=*/10000,
                               /*pending_task_timeout_sec=*/0,
                               /*processing_task_timeout_sec=*/0,
                               /*max_retry_attempts=*/3});
    UUID client_id = generate_uuid();

    auto first =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{
                           .key = "k1", .source = "seg1", .targets = {"seg2"}});
    ASSERT_TRUE(first.has_value());

    auto second =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{
                           .key = "k2", .source = "seg1", .targets = {"seg2"}});
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error(), ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
}

TEST_F(ClientTaskManagerTest, ProcessingLimitCapsPop) {
    // max_total_processing_tasks=1
    ClientTaskManager manager({/*max_total_finished_tasks=*/10000,
                               /*max_total_pending_tasks=*/10000,
                               /*max_total_processing_tasks=*/1,
                               /*pending_task_timeout_sec=*/0,
                               /*processing_task_timeout_sec=*/0,
                               /*max_retry_attempts=*/3});
    UUID client_id = generate_uuid();

    auto t1 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{
                           .key = "k1", .source = "seg1", .targets = {"seg2"}});
    auto t2 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{
                           .key = "k2", .source = "seg2", .targets = {"seg1"}});
    ASSERT_TRUE(t1.has_value());
    ASSERT_TRUE(t2.has_value());

    auto tasks = manager.get_write_access().pop_tasks(client_id, 10);
    ASSERT_EQ(tasks.size(), 1u);
}

TEST_F(ClientTaskManagerTest, PruneExpiredTasksPendingTimeout) {
    ClientTaskManager manager({/*max_total_finished_tasks=*/10000,
                               /*max_total_pending_tasks=*/1,
                               /*max_total_processing_tasks=*/10000,
                               /*pending_task_timeout_sec=*/1,
                               /*processing_task_timeout_sec=*/0,
                               /*max_retry_attempts=*/3});
    UUID client_id = generate_uuid();

    auto t1 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{
                           .key = "k1", .source = "seg1", .targets = {"seg2"}});
    ASSERT_TRUE(t1.has_value());
    const UUID t1_id = t1.value();

    std::this_thread::sleep_for(std::chrono::seconds(2));
    manager.get_write_access().prune_expired_tasks();

    auto task_opt = manager.get_read_access().find_task_by_id(t1_id);
    ASSERT_TRUE(task_opt.has_value());
    EXPECT_EQ(task_opt->status, TaskStatus::FAILED);
    EXPECT_EQ(task_opt->message, "pending timeout");

    // Expired pending task should not be popped.
    auto popped = manager.get_write_access().pop_tasks(client_id, 10);
    EXPECT_TRUE(popped.empty());

    // Pending limit should be freed after pruning.
    auto t2 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{
                           .key = "k2", .source = "seg1", .targets = {"seg2"}});
    ASSERT_TRUE(t2.has_value());
}

TEST_F(ClientTaskManagerTest, PruneExpiredTasksProcessingTimeoutFreesSlot) {
    ClientTaskManager manager({/*max_total_finished_tasks=*/10000,
                               /*max_total_pending_tasks=*/10000,
                               /*max_total_processing_tasks=*/1,
                               /*pending_task_timeout_sec=*/0,
                               /*processing_task_timeout_sec=*/1,
                               /*max_retry_attempts=*/3});
    UUID client_id = generate_uuid();

    auto t1 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{
                           .key = "k1", .source = "seg1", .targets = {"seg2"}});
    auto t2 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{
                           .key = "k2", .source = "seg1", .targets = {"seg2"}});
    ASSERT_TRUE(t1.has_value());
    ASSERT_TRUE(t2.has_value());
    const UUID t1_id = t1.value();
    const UUID t2_id = t2.value();

    // Pop first task into PROCESSING; second stays pending due to processing
    // cap.
    auto first = manager.get_write_access().pop_tasks(client_id, 10);
    ASSERT_EQ(first.size(), 1u);
    EXPECT_EQ(first[0].id, t1_id);
    EXPECT_EQ(first[0].status, TaskStatus::PROCESSING);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    manager.get_write_access().prune_expired_tasks();

    auto task1_opt = manager.get_read_access().find_task_by_id(t1_id);
    ASSERT_TRUE(task1_opt.has_value());
    EXPECT_EQ(task1_opt->status, TaskStatus::FAILED);
    EXPECT_EQ(task1_opt->message, "processing timeout");

    // Now processing slot should be freed; we should be able to pop the second.
    auto second = manager.get_write_access().pop_tasks(client_id, 10);
    ASSERT_EQ(second.size(), 1u);
    EXPECT_EQ(second[0].id, t2_id);
    EXPECT_EQ(second[0].status, TaskStatus::PROCESSING);
}

TEST_F(ClientTaskManagerTest, CancelPendingTaskReleasesQuotaAndSkipsDispatch) {
    ClientTaskManager manager({10, 1, 1, 0, 0, 3});
    const UUID client = generate_uuid();
    const ReplicaMovePayload payload{.tenant_id = "tenant-a",
                                     .key = "key",
                                     .source = "source",
                                     .target = "target"};
    auto submit = [&] {
        return manager.get_write_access()
            .submit_task_typed<TaskType::REPLICA_MOVE>(client, payload);
    };
    auto task = submit();
    ASSERT_TRUE(task);
    EXPECT_EQ(
        manager.get_write_access().cancel_task(*task, TenantId("tenant-b")),
        ErrorCode::TASK_NOT_FOUND);
    EXPECT_EQ(
        manager.get_write_access().cancel_task(*task, TenantId("tenant-a")),
        ErrorCode::OK);
    ASSERT_TRUE(submit());
    auto dispatched = manager.get_write_access().pop_tasks(client, 10);
    ASSERT_EQ(dispatched.size(), 1);
    EXPECT_NE(dispatched.front().id, *task);

    auto page = manager.get_read_access().list_tasks(
        {.tenant_id = "tenant-a", .status = TaskStatus::CANCELED});
    ASSERT_TRUE(page);
    ASSERT_EQ(page->tasks.size(), 1);
    EXPECT_EQ(page->tasks.front().id, *task);
}

TEST_F(ClientTaskManagerTest, CancelProcessingTaskWaitsForExecutorResult) {
    ClientTaskManager manager({10, 10, 1, 0, 0, 3});
    const UUID client = generate_uuid();
    auto task =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client, {.key = "key", .source = "source", .targets = {"target"}});
    ASSERT_TRUE(task);
    ASSERT_EQ(manager.get_write_access().pop_tasks(client, 1).size(), 1);
    EXPECT_EQ(
        manager.get_write_access().cancel_task(*task, TenantId::Default()),
        ErrorCode::OK);
    EXPECT_EQ(manager.get_read_access().find_task_by_id(*task)->status,
              TaskStatus::PROCESSING);
    auto requested =
        manager.get_read_access().is_cancellation_requested(client, *task);
    ASSERT_TRUE(requested);
    EXPECT_TRUE(*requested);
    EXPECT_EQ(manager.get_read_access()
                  .is_cancellation_requested(generate_uuid(), *task)
                  .error(),
              ErrorCode::ILLEGAL_CLIENT);

    // A commit that raced with the request still reports success.
    EXPECT_EQ(manager.get_write_access().complete_task(
                  client, *task, TaskStatus::SUCCESS, "done"),
              ErrorCode::OK);
    EXPECT_EQ(manager.get_read_access().find_task_by_id(*task)->status,
              TaskStatus::SUCCESS);
    EXPECT_EQ(
        manager.get_write_access().cancel_task(*task, TenantId::Default()),
        ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
}

TEST_F(ClientTaskManagerTest, DrainTasksAwaitCompletionAndObservation) {
    ClientTaskManager manager({0, 10, 10, 0, 1, 3});
    const UUID client = generate_uuid();
    const ReplicaMovePayload payload{
        .key = "key", .source = "source", .target = "target"};
    auto task =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_MOVE>(
            client, payload, true);
    ASSERT_TRUE(task);
    ASSERT_EQ(manager.get_write_access().pop_tasks(client, 1).size(), 1u);
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    manager.get_write_access().prune_expired_tasks();
    auto observed = manager.get_read_access().find_task_by_id(*task);
    ASSERT_TRUE(observed);
    EXPECT_EQ(observed->status, TaskStatus::PROCESSING);
    EXPECT_TRUE(manager.get_read_access()
                    .is_cancellation_requested(client, *task)
                    .value());
    EXPECT_EQ(
        manager.get_write_access().cancel_task(*task, TenantId::Default()),
        ErrorCode::OK);
    EXPECT_EQ(manager.get_write_access().complete_task(
                  client, *task, TaskStatus::CANCELED, "canceled"),
              ErrorCode::OK);
    auto ordinary =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_MOVE>(
            client, payload);
    ASSERT_TRUE(ordinary);
    EXPECT_EQ(
        manager.get_write_access().cancel_task(*ordinary, TenantId::Default()),
        ErrorCode::OK);
    manager.get_write_access().prune_finished_tasks();
    ASSERT_TRUE(manager.get_read_access().find_task_by_id(*task));
    EXPECT_FALSE(manager.get_read_access().find_task_by_id(*ordinary));
    manager.get_write_access().release_drain_task(*task);
    manager.get_write_access().prune_finished_tasks();
    EXPECT_FALSE(manager.get_read_access().find_task_by_id(*task));
}

TEST_F(ClientTaskManagerTest, FailPendingTaskSkipsQueuedAssignment) {
    ClientTaskManager manager({10000, 10000, 10000, 0, 0, 3});
    UUID client_id = generate_uuid();

    auto task_id =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{.tenant_id = "default",
                                          .key = "failed-pending-key",
                                          .source = "source",
                                          .targets = {"target"}});
    ASSERT_TRUE(task_id.has_value());

    auto fail_result = manager.get_write_access().fail_task_if_pending(
        *task_id, "test failure");
    EXPECT_EQ(fail_result, ErrorCode::OK);

    auto tasks = manager.get_write_access().pop_tasks(client_id, 10);
    EXPECT_TRUE(tasks.empty());
}

TEST_F(ClientTaskManagerTest, PrunedPendingFailuresPreserveOtherClientsQuota) {
    ClientTaskManager manager({1, 2, 2, 0, 0, 3});
    const UUID failed_client = generate_uuid();
    const UUID waiting_client = generate_uuid();
    const ReplicaCopyPayload payload{
        .key = "key", .source = "source", .targets = {"target"}};

    for (int i = 0; i < 2; ++i) {
        auto task = manager.get_write_access()
                        .submit_task_typed<TaskType::REPLICA_COPY>(
                            failed_client, payload);
        ASSERT_TRUE(task.has_value());
        ASSERT_EQ(manager.get_write_access().fail_task_if_pending(
                      *task, "dynamic replica lease expired"),
                  ErrorCode::OK);
    }
    // Match the maintenance order with pending-task expiration disabled.
    manager.get_write_access().prune_expired_tasks();
    manager.get_write_access().prune_finished_tasks();

    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(manager.get_write_access()
                        .submit_task_typed<TaskType::REPLICA_COPY>(
                            waiting_client, payload)
                        .has_value());
    }
    EXPECT_TRUE(
        manager.get_write_access().pop_tasks(failed_client, 10).empty());

    auto rejected =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            waiting_client, payload);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);

    // Only dispatching a genuinely pending task releases a slot.
    ASSERT_EQ(manager.get_write_access().pop_tasks(waiting_client, 1).size(),
              1u);
    ASSERT_TRUE(
        manager.get_write_access()
            .submit_task_typed<TaskType::REPLICA_COPY>(waiting_client, payload)
            .has_value());
    rejected =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            waiting_client, payload);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
}

TEST_F(ClientTaskManagerTest, ExpirySweepPreservesQuotaAfterTerminalPruning) {
    ClientTaskManager manager({1, 2, 2, 3600, 0, 3});
    const UUID failed_client = generate_uuid();
    const UUID waiting_client = generate_uuid();
    const ReplicaMovePayload payload{
        .key = "key", .source = "source", .target = "target"};

    for (int i = 0; i < 2; ++i) {
        auto task = manager.get_write_access()
                        .submit_task_typed<TaskType::REPLICA_MOVE>(
                            failed_client, payload);
        ASSERT_TRUE(task.has_value());
        ASSERT_EQ(manager.get_write_access().fail_task_if_pending(
                      *task, "failed before dispatch"),
                  ErrorCode::OK);
    }
    manager.get_write_access().prune_finished_tasks();
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(manager.get_write_access()
                        .submit_task_typed<TaskType::REPLICA_MOVE>(
                            waiting_client, payload)
                        .has_value());
    }
    manager.get_write_access().prune_expired_tasks();
    manager.get_write_access().prune_expired_tasks();

    auto rejected =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_MOVE>(
            waiting_client, payload);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
    EXPECT_EQ(manager.get_write_access().pop_tasks(waiting_client, 10).size(),
              2u);
}

class PendingTaskCompletionQuotaTest
    : public ClientTaskManagerTest,
      public ::testing::WithParamInterface<std::tuple<TaskStatus, bool>> {};

TEST_P(PendingTaskCompletionQuotaTest, ReleasesPendingQuotaExactlyOnce) {
    const auto [status, sweep_expired] = GetParam();
    ClientTaskManager manager({0, 2, 2, 3600, 0, 3});
    const UUID completing_client = generate_uuid();
    const UUID waiting_client = generate_uuid();
    const ReplicaMovePayload payload{
        .key = "key", .source = "source", .target = "target"};
    auto completing =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_MOVE>(
            completing_client, payload);
    ASSERT_TRUE(completing.has_value());
    ASSERT_TRUE(
        manager.get_write_access()
            .submit_task_typed<TaskType::REPLICA_MOVE>(waiting_client, payload)
            .has_value());

    EXPECT_EQ(manager.get_write_access().complete_task(
                  waiting_client, *completing, status, "wrong client"),
              ErrorCode::ILLEGAL_CLIENT);
    EXPECT_EQ(manager.get_write_access().complete_task(
                  completing_client, *completing, TaskStatus::PENDING,
                  "not a terminal status"),
              ErrorCode::INVALID_PARAMS);
    auto rejected =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_MOVE>(
            waiting_client, payload);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);

    // The completion API accepts a pending task. Its queued ID may outlive
    // the terminal record, but its quota must be released immediately.
    ASSERT_EQ(manager.get_write_access().complete_task(
                  completing_client, *completing, status, "before fetch"),
              ErrorCode::OK);
    ASSERT_EQ(manager.get_write_access().complete_task(
                  completing_client, *completing, status, "duplicate"),
              ErrorCode::OK);
    ASSERT_TRUE(
        manager.get_write_access()
            .submit_task_typed<TaskType::REPLICA_MOVE>(waiting_client, payload)
            .has_value());

    manager.get_write_access().prune_finished_tasks();
    if (sweep_expired) {
        manager.get_write_access().prune_expired_tasks();
    } else {
        EXPECT_TRUE(manager.get_write_access()
                        .pop_tasks(completing_client, 10)
                        .empty());
    }
    rejected =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_MOVE>(
            waiting_client, payload);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
    const auto processing =
        manager.get_write_access().pop_tasks(waiting_client, 10);
    ASSERT_EQ(processing.size(), 2u);
    for (int i = 0; i < 2; ++i) {
        ASSERT_TRUE(manager.get_write_access()
                        .submit_task_typed<TaskType::REPLICA_MOVE>(
                            waiting_client, payload)
                        .has_value());
    }
    ASSERT_EQ(manager.get_write_access().complete_task(
                  waiting_client, processing.front().id, status, "after fetch"),
              ErrorCode::OK);
    rejected =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_MOVE>(
            waiting_client, payload);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
}

INSTANTIATE_TEST_SUITE_P(
    TerminalStatusAndCleanup, PendingTaskCompletionQuotaTest,
    ::testing::Combine(::testing::Values(TaskStatus::SUCCESS,
                                         TaskStatus::FAILED),
                       ::testing::Bool()));

}  // namespace mooncake
