#include <gtest/gtest.h>
#include <glog/logging.h>
#include "task_manager.h"
#include <thread>

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
    ClientTaskManager manager({10000, 10000, 10000, 0, 0});
    UUID client_id = generate_uuid();
    ReplicaCopyPayload payload{.key = "test_key", .targets = {"seg1"}};

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

TEST_F(ClientTaskManagerTest, MarkTaskComplete) {
    ClientTaskManager manager({10000, 10000, 10000, 0, 0});
    UUID client_id = generate_uuid();

    auto task_id_exp =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{.key = "key1", .targets = {"seg1"}});
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
    ClientTaskManager manager({max_tasks, 10000, 10000, 0, 0});
    UUID client_id = generate_uuid();

    std::vector<UUID> task_ids;
    for (size_t i = 0; i < max_tasks + 2; ++i) {
        auto id_exp = manager.get_write_access()
                          .submit_task_typed<TaskType::REPLICA_COPY>(
                              client_id, ReplicaCopyPayload{
                                             .key = "key" + std::to_string(i),
                                             .targets = {"seg1"}});
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
    ClientTaskManager manager({10000, 10000, 10000, 0, 0});
    UUID client1 = generate_uuid();
    UUID client2 = generate_uuid();

    auto id1_exp =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client1, ReplicaCopyPayload{.key = "key1", .targets = {"seg1"}});
    auto id2_exp =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client2, ReplicaCopyPayload{.key = "key2", .targets = {"seg2"}});
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
                               /*processing_task_timeout_sec=*/0});
    UUID client_id = generate_uuid();

    auto first =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{.key = "k1", .targets = {"seg1"}});
    ASSERT_TRUE(first.has_value());

    auto second =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{.key = "k2", .targets = {"seg1"}});
    ASSERT_FALSE(second.has_value());
    EXPECT_EQ(second.error(), ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
}

TEST_F(ClientTaskManagerTest, ProcessingLimitCapsPop) {
    // max_total_processing_tasks=1
    ClientTaskManager manager({/*max_total_finished_tasks=*/10000,
                               /*max_total_pending_tasks=*/10000,
                               /*max_total_processing_tasks=*/1,
                               /*pending_task_timeout_sec=*/0,
                               /*processing_task_timeout_sec=*/0});
    UUID client_id = generate_uuid();

    auto t1 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{.key = "k1", .targets = {"seg1"}});
    auto t2 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{.key = "k2", .targets = {"seg1"}});
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
                               /*processing_task_timeout_sec=*/0});
    UUID client_id = generate_uuid();

    auto t1 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{.key = "k1", .targets = {"seg2"}});
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
            client_id, ReplicaCopyPayload{.key = "k2", .targets = {"seg2"}});
    ASSERT_TRUE(t2.has_value());
}

TEST_F(ClientTaskManagerTest, PruneExpiredTasksProcessingTimeoutFreesSlot) {
    ClientTaskManager manager({/*max_total_finished_tasks=*/10000,
                               /*max_total_pending_tasks=*/10000,
                               /*max_total_processing_tasks=*/1,
                               /*pending_task_timeout_sec=*/0,
                               /*processing_task_timeout_sec=*/1});
    UUID client_id = generate_uuid();

    auto t1 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{.key = "k1", .targets = {"seg2"}});
    auto t2 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{.key = "k2", .targets = {"seg2"}});
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

TEST_F(ClientTaskManagerTest, SerializerRoundTrip) {
    ClientTaskManager manager({10000, 10000, 10000, 0, 0});
    UUID client_id1 = generate_uuid();
    UUID client_id2 = generate_uuid();

    // Create task
    auto t1 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id1,
            ReplicaCopyPayload{.key = "pending_key", .targets = {"seg1"}});
    auto t2 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id2,
            ReplicaCopyPayload{.key = "processing_key", .targets = {"seg2"}});
    auto t3 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_MOVE>(
            client_id1, ReplicaMovePayload{
                            .key = "key3", .source = "seg1", .target = "seg4"});
    auto t4 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_MOVE>(
            client_id2, ReplicaMovePayload{
                            .key = "key4", .source = "seg2", .target = "seg5"});

    ASSERT_TRUE(t1.has_value());
    ASSERT_TRUE(t2.has_value());
    ASSERT_TRUE(t3.has_value());
    ASSERT_TRUE(t4.has_value());

    // Pop t1 from client_id1 to make task processing
    manager.get_write_access().pop_tasks(client_id1, 1);

    // Pop t2 t4 from client_id2 to make task processing
    manager.get_write_access().pop_tasks(client_id2, 2);

    // Complete t1 as SUCCESS, t2 as FAILED
    auto ec1 = manager.get_write_access().complete_task(
        client_id1, t1.value(), TaskStatus::SUCCESS, "Done");
    auto ec2 = manager.get_write_access().complete_task(
        client_id2, t2.value(), TaskStatus::FAILED, "Failed");

    // Get original tasks
    auto task1_before = manager.get_read_access().find_task_by_id(t1.value());
    auto task2_before = manager.get_read_access().find_task_by_id(t2.value());
    auto task3_before = manager.get_read_access().find_task_by_id(t3.value());
    auto task4_before = manager.get_read_access().find_task_by_id(t4.value());

    // Serialize
    TaskManagerSerializer serializer(&manager);
    auto serialized = serializer.Serialize();
    ASSERT_TRUE(serialized.has_value());

    // Deserialize into new manager
    ClientTaskManager manager2({10000, 10000, 10000, 0, 0});
    TaskManagerSerializer serializer2(&manager2);
    auto result = serializer2.Deserialize(serialized.value());
    ASSERT_TRUE(result.has_value());

    // Verify statuses
    auto task1 = manager2.get_read_access().find_task_by_id(t1.value());
    auto task2 = manager2.get_read_access().find_task_by_id(t2.value());
    auto task3 = manager2.get_read_access().find_task_by_id(t3.value());
    auto task4 = manager2.get_read_access().find_task_by_id(t4.value());

    ASSERT_TRUE(task1.has_value());
    ASSERT_TRUE(task2.has_value());
    ASSERT_TRUE(task3.has_value());
    ASSERT_TRUE(task4.has_value());

    auto time_diff = [](auto time1, auto time2) {
        return std::abs(
            std::chrono::duration_cast<std::chrono::seconds>(time1 - time2)
                .count());
    };

    EXPECT_EQ(task1->id, task1_before->id);
    EXPECT_EQ(task1->type, TaskType::REPLICA_COPY);
    EXPECT_EQ(task1->status, TaskStatus::SUCCESS);
    EXPECT_FALSE(task1->payload.empty());
    EXPECT_EQ(task1->assigned_client, client_id1);
    EXPECT_LE(time_diff(task1->created_at, task1_before->created_at), 1);
    EXPECT_LE(time_diff(task1->last_updated_at, task1_before->last_updated_at),
              1);
    EXPECT_EQ(task2->id, task2_before->id);
    EXPECT_EQ(task2->type, TaskType::REPLICA_COPY);
    EXPECT_EQ(task2->status, TaskStatus::FAILED);
    EXPECT_LE(time_diff(task2->created_at, task2_before->created_at), 1);
    EXPECT_LE(time_diff(task2->last_updated_at, task2_before->last_updated_at),
              1);
    EXPECT_FALSE(task2->payload.empty());
    EXPECT_EQ(task2->assigned_client, client_id2);
    EXPECT_EQ(task3->id, task3_before->id);
    EXPECT_EQ(task3->type, TaskType::REPLICA_MOVE);
    EXPECT_EQ(task3->status, TaskStatus::PENDING);
    EXPECT_LE(time_diff(task3->created_at, task3_before->created_at), 1);
    EXPECT_LE(time_diff(task3->last_updated_at, task3_before->last_updated_at),
              1);
    EXPECT_FALSE(task3->payload.empty());
    EXPECT_EQ(task3->assigned_client, client_id1);
    EXPECT_EQ(task4->id, task4_before->id);
    EXPECT_EQ(task4->type, TaskType::REPLICA_MOVE);
    EXPECT_EQ(task4->status, TaskStatus::PROCESSING);
    EXPECT_LE(time_diff(task4->created_at, task4_before->created_at), 1);
    EXPECT_LE(time_diff(task4->last_updated_at, task4_before->last_updated_at),
              1);
    EXPECT_FALSE(task4->payload.empty());
    EXPECT_EQ(task4->assigned_client, client_id2);
}

TEST_F(ClientTaskManagerTest, SerializerEmptyManager) {
    ClientTaskManager manager({10000, 10000, 10000, 0, 0});

    TaskManagerSerializer serializer(&manager);
    auto serialized = serializer.Serialize();
    ASSERT_TRUE(serialized.has_value());

    ClientTaskManager manager2({10000, 10000, 10000, 0, 0});
    TaskManagerSerializer serializer2(&manager2);
    auto result = serializer2.Deserialize(serialized.value());
    ASSERT_TRUE(result.has_value());

    // Verify empty
    auto tasks = manager2.get_read_access();
    EXPECT_EQ(tasks.size(), 0u);
}

TEST_F(ClientTaskManagerTest, SerializerReset) {
    ClientTaskManager manager({10000, 10000, 10000, 0, 0});
    UUID client_id = generate_uuid();

    auto t1 =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client_id, ReplicaCopyPayload{.key = "key1", .targets = {"seg1"}});
    ASSERT_TRUE(t1.has_value());

    TaskManagerSerializer serializer(&manager);
    serializer.Reset();

    // Verify all data cleared
    auto task = manager.get_read_access().find_task_by_id(t1.value());
    EXPECT_FALSE(task.has_value());

    auto tasks = manager.get_read_access();
    EXPECT_EQ(tasks.size(), 0u);
}
}  // namespace mooncake
