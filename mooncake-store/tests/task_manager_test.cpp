#include <gtest/gtest.h>
#include <glog/logging.h>
#include "task_manager.h"
#include <thread>

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
    ClientTaskManager manager;
    UUID client_id = generate_uuid();
    std::string payload = "test_payload";
    
    UUID task_id = manager.get_write_access().submit_task(client_id, TaskType::REPLICA_COPY, payload);
    
    auto tasks = manager.get_write_access().pop_tasks(client_id, 10);
    ASSERT_EQ(tasks.size(), 1);
    EXPECT_EQ(tasks[0].id, task_id);
    EXPECT_EQ(tasks[0].payload, payload);
    EXPECT_EQ(tasks[0].status, TaskStatus::PROCESSING);
}

TEST_F(ClientTaskManagerTest, TaskStatusLifecycle) {
    ClientTaskManager manager;
    UUID client_id = generate_uuid();
    
    UUID task_id = manager.get_write_access().submit_task(client_id, TaskType::REPLICA_COPY, "payload");
    
    // Initially pending
    auto task_opt = manager.get_read_access().find_task_by_id(task_id);
    ASSERT_TRUE(task_opt.has_value());
    EXPECT_EQ(task_opt->status, TaskStatus::PENDING);
    
    // Pop to make it processing
    manager.get_write_access().pop_tasks(client_id, 1);
    task_opt = manager.get_read_access().find_task_by_id(task_id);
    EXPECT_EQ(task_opt->status, TaskStatus::PROCESSING);
    
    // Mark success
    manager.get_write_access().mark_success(client_id, task_id);
    task_opt = manager.get_read_access().find_task_by_id(task_id);
    EXPECT_EQ(task_opt->status, TaskStatus::SUCCESS);
}

TEST_F(ClientTaskManagerTest, PruningLogic) {
    size_t max_tasks = 5;
    ClientTaskManager manager(max_tasks);
    UUID client_id = generate_uuid();
    
    std::vector<UUID> task_ids;
    for (size_t i = 0; i < max_tasks + 2; ++i) {
        UUID id = manager.get_write_access().submit_task(client_id, TaskType::REPLICA_COPY, "payload");
        task_ids.push_back(id);
        manager.get_write_access().pop_tasks(client_id, 1);
        manager.get_write_access().mark_success(client_id, id);
    }
    
    // The first 2 tasks should have been pruned
    EXPECT_FALSE(manager.get_read_access().find_task_by_id(task_ids[0]).has_value());
    EXPECT_FALSE(manager.get_read_access().find_task_by_id(task_ids[1]).has_value());
    
    // The last 5 tasks should still exist
    for (size_t i = 2; i < task_ids.size(); ++i) {
        EXPECT_TRUE(manager.get_read_access().find_task_by_id(task_ids[i]).has_value());
    }
}

TEST_F(ClientTaskManagerTest, MultipleClients) {
    ClientTaskManager manager;
    UUID client1 = generate_uuid();
    UUID client2 = generate_uuid();
    
    UUID id1 = manager.get_write_access().submit_task(client1, TaskType::REPLICA_COPY, "p1");
    UUID id2 = manager.get_write_access().submit_task(client2, TaskType::REPLICA_COPY, "p2");
    
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

TEST_F(ClientTaskManagerTest, MarkFailed) {
    ClientTaskManager manager;
    UUID client_id = generate_uuid();
    UUID task_id = manager.get_write_access().submit_task(client_id, TaskType::REPLICA_COPY, "payload");
    
    manager.get_write_access().pop_tasks(client_id, 1);
    manager.get_write_access().mark_failed(client_id, task_id, "error occurred");
    
    auto task_opt = manager.get_read_access().find_task_by_id(task_id);
    ASSERT_TRUE(task_opt.has_value());
    EXPECT_EQ(task_opt->status, TaskStatus::FAILED);
    EXPECT_EQ(task_opt->error_message, "error occurred");
}

} // namespace mooncake
