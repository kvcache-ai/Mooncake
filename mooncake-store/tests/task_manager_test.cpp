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

namespace {
Task listing_task(uint64_t id, TaskStatus status = TaskStatus::PENDING,
                  TaskType type = TaskType::REPLICA_COPY,
                  const std::string& tenant = "default",
                  const std::string& key = "key", UUID client = {1, 1}) {
    Task task{};
    task.id = {id, 0};
    task.type = type;
    task.status = status;
    task.assigned_client = client;
    task.created_at = task.last_updated_at = std::chrono::system_clock::now();
    task.payload =
        type == TaskType::REPLICA_COPY
            ? serialize_payload(ReplicaCopyPayload{.tenant_id = tenant,
                                                   .key = key,
                                                   .source = "source",
                                                   .targets = {"target"}})
            : serialize_payload(ReplicaMovePayload{.tenant_id = tenant,
                                                   .key = key,
                                                   .source = "source",
                                                   .target = "target"});
    return task;
}
}  // namespace

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

TEST_F(ClientTaskManagerTest, ListTasksReturnsTasksWithoutDispatching) {
    ClientTaskManager manager({10, 1, 1, 0, 0, 3});
    const UUID client = generate_uuid();
    const ReplicaCopyPayload payload{
        .key = "key", .source = "source", .targets = {"target"}};
    const auto id =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client, payload);
    ASSERT_TRUE(id.has_value());
    const auto page = manager.get_read_access().list_tasks(TaskListRequest{});
    ASSERT_TRUE(page.has_value());
    ASSERT_EQ(page->tasks.size(), 1u);
    EXPECT_EQ(page->tasks.front().id, *id);
    EXPECT_EQ(page->tasks.front().status, TaskStatus::PENDING);
    EXPECT_TRUE(page->next_page_token.empty());
    const auto rejected =
        manager.get_write_access().submit_task_typed<TaskType::REPLICA_COPY>(
            client, payload);
    ASSERT_FALSE(rejected.has_value());
    EXPECT_EQ(rejected.error(), ErrorCode::TASK_PENDING_LIMIT_EXCEEDED);
    const auto popped = manager.get_write_access().pop_tasks(client, 1);
    ASSERT_EQ(popped.size(), 1u);
    EXPECT_EQ(popped.front().id, *id);
    const auto processing =
        manager.get_read_access().list_tasks(TaskListRequest{});
    ASSERT_TRUE(processing.has_value());
    ASSERT_EQ(processing->tasks.size(), 1u);
    EXPECT_EQ(processing->tasks.front().status, TaskStatus::PROCESSING);
    ASSERT_TRUE(manager.get_write_access()
                    .submit_task_typed<TaskType::REPLICA_COPY>(client, payload)
                    .has_value());
    EXPECT_TRUE(manager.get_write_access().pop_tasks(client, 1).empty());
}

TEST_F(ClientTaskManagerTest, ListTasksFiltersAndPagesInUuidOrder) {
    ClientTaskManager manager({100, 100, 100, 0, 0, 3});
    for (uint64_t id : {7, 1, 5, 3}) {
        manager.get_write_access().restore_task(listing_task(id));
    }
    manager.get_write_access().restore_task(listing_task(
        2, TaskStatus::PROCESSING, TaskType::REPLICA_MOVE, "tenant-b", "key"));
    manager.get_write_access().restore_task(listing_task(
        4, TaskStatus::SUCCESS, TaskType::REPLICA_COPY, "default", "other"));
    manager.get_write_access().restore_task(
        listing_task(6, TaskStatus::FAILED, TaskType::REPLICA_MOVE, "default",
                     "key", {2, 2}));
    TaskListRequest request;
    request.status = TaskStatus::PENDING;
    request.type = TaskType::REPLICA_COPY;
    request.assigned_client = UUID{1, 1};
    request.key = "key";
    request.limit = 2;
    auto page = manager.get_read_access().list_tasks(request);
    ASSERT_TRUE(page.has_value());
    ASSERT_EQ(page->tasks.size(), 2u);
    EXPECT_EQ(page->tasks[0].id, (UUID{1, 0}));
    EXPECT_EQ(page->tasks[1].id, (UUID{3, 0}));
    EXPECT_FALSE(page->next_page_token.empty());
    request.page_token = page->next_page_token;
    page = manager.get_read_access().list_tasks(request);
    ASSERT_TRUE(page.has_value());
    ASSERT_EQ(page->tasks.size(), 2u);
    EXPECT_EQ(page->tasks[0].id, (UUID{5, 0}));
    EXPECT_EQ(page->tasks[1].id, (UUID{7, 0}));
    EXPECT_TRUE(page->next_page_token.empty());
    request = TaskListRequest{};
    request.tenant_id = "tenant-b";
    page = manager.get_read_access().list_tasks(request);
    ASSERT_TRUE(page.has_value());
    ASSERT_EQ(page->tasks.size(), 1u);
    EXPECT_EQ(page->tasks.front().id, (UUID{2, 0}));
}

TEST_F(ClientTaskManagerTest, ListTasksRejectsInvalidRequests) {
    ClientTaskManager manager({100, 100, 100, 0, 0, 3});
    manager.get_write_access().restore_task(listing_task(1));
    const auto invalid = [&](const TaskListRequest& request) {
        const auto result = manager.get_read_access().list_tasks(request);
        return !result.has_value() &&
               result.error() == ErrorCode::INVALID_PARAMS;
    };
    TaskListRequest request;
    request.limit = 1001;
    ASSERT_TRUE(invalid(request));
    request.limit = 0;
    ASSERT_TRUE(invalid(request));
    request = TaskListRequest{};
    request.status = static_cast<TaskStatus>(-1);
    ASSERT_TRUE(invalid(request));
    request.status = static_cast<TaskStatus>(4);
    ASSERT_TRUE(invalid(request));
    request.status.reset();
    request.type = static_cast<TaskType>(-1);
    ASSERT_TRUE(invalid(request));
    request.type = static_cast<TaskType>(2);
    ASSERT_TRUE(invalid(request));
    request = TaskListRequest{};
    for (const std::string& tenant : std::vector<std::string>{
             "_internal", "bad\ntenant", std::string("bad\0tenant", 10)}) {
        request.tenant_id = tenant;
        ASSERT_TRUE(invalid(request));
    }
    request = TaskListRequest{};
    for (const std::string token :
         {"v2:1-0", "1-0", "v1:", "v1:1", "v1:-1-0", "v1:+1-0", "v1:01-0",
          "v1:1-00", "v1:1-0x", "v1:1-0 ", "v1:1-0:extra",
          "v1:18446744073709551616-0"}) {
        request.page_token = token;
        ASSERT_TRUE(invalid(request));
    }
    request = TaskListRequest{};
    request.limit = 1000;
    EXPECT_TRUE(manager.get_read_access().list_tasks(request).has_value());
    request.page_token = "v1:18446744073709551615-18446744073709551615";
    const auto empty = manager.get_read_access().list_tasks(request);
    ASSERT_TRUE(empty.has_value());
    EXPECT_TRUE(empty->tasks.empty());
    EXPECT_TRUE(empty->next_page_token.empty());
}

TEST_F(ClientTaskManagerTest, ListTasksIsolatesLegacyAndMalformedPayloads) {
    ClientTaskManager manager({100, 100, 100, 0, 0, 3});
    Task legacy = listing_task(1);
    legacy.payload = R"({"key":"key","source":"source","targets":["target"]})";
    manager.get_write_access().restore_task(std::move(legacy));
    manager.get_write_access().restore_task(listing_task(
        2, TaskStatus::PENDING, TaskType::REPLICA_MOVE, "tenant-b"));
    uint64_t id = 3;
    for (const std::string payload :
         {"", "{", "[]", "not json", R"({"tenant_id":3,"key":"key"})",
          R"({"tenant_id":"tenant-b","key":4})",
          R"({"tenant_id":"tenant-b","key":"key"})garbage",
             R"({"tenant_id":"tenant-b","tenant_id":"default","key":"key"})",
          R"({"key":"key","source":"source","targets":[])"}) {
        Task malformed = listing_task(id++);
        malformed.payload = payload;
        manager.get_write_access().restore_task(std::move(malformed));
    }
    auto page = manager.get_read_access().list_tasks(TaskListRequest{});
    ASSERT_TRUE(page.has_value());
    ASSERT_EQ(page->tasks.size(), 1u);
    EXPECT_EQ(page->tasks.front().id, (UUID{1, 0}));
    TaskListRequest request;
    request.tenant_id = "tenant-b";
    page = manager.get_read_access().list_tasks(request);
    ASSERT_TRUE(page.has_value());
    ASSERT_EQ(page->tasks.size(), 1u);
    EXPECT_EQ(page->tasks.front().id, (UUID{2, 0}));
    request.tenant_id = "";
    page = manager.get_read_access().list_tasks(request);
    ASSERT_TRUE(page.has_value());
    ASSERT_EQ(page->tasks.size(), 1u);
    EXPECT_EQ(page->tasks.front().id, (UUID{1, 0}));
}

TEST_F(ClientTaskManagerTest, ListTasksSupportsEveryStatusAndTypeFilter) {
    ClientTaskManager manager({100, 100, 100, 0, 0, 3});
    uint64_t id = 0;
    for (const auto status : {TaskStatus::PENDING, TaskStatus::PROCESSING,
                              TaskStatus::FAILED, TaskStatus::SUCCESS}) {
        for (const auto type :
             {TaskType::REPLICA_COPY, TaskType::REPLICA_MOVE}) {
            manager.get_write_access().restore_task(
                listing_task(++id, status, type));
        }
    }
    for (const auto status : {TaskStatus::PENDING, TaskStatus::PROCESSING,
                              TaskStatus::FAILED, TaskStatus::SUCCESS}) {
        TaskListRequest request;
        request.status = status;
        auto page = manager.get_read_access().list_tasks(request);
        ASSERT_TRUE(page.has_value());
        ASSERT_EQ(page->tasks.size(), 2u);
        for (const auto& task : page->tasks) {
            EXPECT_EQ(task.status, status);
        }
        for (const auto type :
             {TaskType::REPLICA_COPY, TaskType::REPLICA_MOVE}) {
            request.type = type;
            page = manager.get_read_access().list_tasks(request);
            ASSERT_TRUE(page.has_value());
            ASSERT_EQ(page->tasks.size(), 1u);
            EXPECT_EQ(page->tasks.front().type, type);
        }
    }
    for (const auto type : {TaskType::REPLICA_COPY, TaskType::REPLICA_MOVE}) {
        TaskListRequest request;
        request.type = type;
        const auto page = manager.get_read_access().list_tasks(request);
        ASSERT_TRUE(page.has_value());
        ASSERT_EQ(page->tasks.size(), 4u);
        for (const auto& task : page->tasks) {
            EXPECT_EQ(task.type, type);
        }
    }
    TaskListRequest request;
    request.assigned_client = UUID{99, 99};
    EXPECT_TRUE(manager.get_read_access().list_tasks(request)->tasks.empty());
    request = TaskListRequest{};
    request.key = "";
    EXPECT_TRUE(manager.get_read_access().list_tasks(request)->tasks.empty());
}

TEST_F(ClientTaskManagerTest,
       ListTasksContinuationSurvivesPruneAndSeesChanges) {
    ClientTaskManager manager({0, 100, 100, 0, 0, 3});
    for (uint64_t id : {10, 30, 40}) {
        manager.get_write_access().restore_task(listing_task(id));
    }
    manager.get_write_access().restore_task(
        listing_task(20, TaskStatus::PROCESSING));
    TaskListRequest request;
    request.limit = 1;
    const auto first = manager.get_read_access().list_tasks(request);
    ASSERT_TRUE(first.has_value());
    ASSERT_EQ(first->tasks.size(), 1u);
    EXPECT_EQ(first->tasks.front().id, (UUID{10, 0}));
    ASSERT_EQ(manager.get_write_access().complete_task(
                  {1, 1}, {10, 0}, TaskStatus::SUCCESS, "done"),
              ErrorCode::OK);
    manager.get_write_access().prune_finished_tasks();
    EXPECT_FALSE(
        manager.get_read_access().find_task_by_id({10, 0}).has_value());
    ASSERT_EQ(manager.get_write_access().complete_task(
                  {1, 1}, {20, 0}, TaskStatus::SUCCESS, "new status"),
              ErrorCode::OK);
    manager.get_write_access().restore_task(listing_task(5));
    manager.get_write_access().restore_task(listing_task(15));
    request.limit = 100;
    request.page_token = first->next_page_token;
    const auto next = manager.get_read_access().list_tasks(request);
    ASSERT_TRUE(next.has_value());
    ASSERT_EQ(next->tasks.size(), 4u);
    EXPECT_EQ(next->tasks[0].id, (UUID{15, 0}));
    EXPECT_EQ(next->tasks[1].id, (UUID{20, 0}));
    EXPECT_EQ(next->tasks[1].status, TaskStatus::SUCCESS);
    EXPECT_EQ(next->tasks[1].message, "new status");
    EXPECT_EQ(next->tasks[2].id, (UUID{30, 0}));
    EXPECT_EQ(next->tasks[3].id, (UUID{40, 0}));
    EXPECT_TRUE(next->next_page_token.empty());
}

TEST_F(ClientTaskManagerTest, ListTasksPreservesSerializerAndRestoredRecords) {
    ClientTaskManager manager({100, 100, 100, 0, 0, 3});
    for (uint64_t id : {3, 1, 2}) {
        manager.get_write_access().restore_task(listing_task(id));
    }
    TaskManagerSerializer serializer(&manager);
    const auto before = serializer.Serialize();
    ASSERT_TRUE(before.has_value());
    const auto page = manager.get_read_access().list_tasks(TaskListRequest{});
    ASSERT_TRUE(page.has_value());
    const auto after = serializer.Serialize();
    ASSERT_TRUE(after.has_value());
    EXPECT_EQ(*before, *after);
    ClientTaskManager restored({100, 100, 100, 0, 0, 3});
    TaskManagerSerializer restored_serializer(&restored);
    ASSERT_TRUE(restored_serializer.Deserialize(*before).has_value());
    const auto restored_page =
        restored.get_read_access().list_tasks(TaskListRequest{});
    ASSERT_TRUE(restored_page.has_value());
    ASSERT_EQ(restored_page->tasks.size(), page->tasks.size());
    for (size_t i = 0; i < page->tasks.size(); ++i) {
        EXPECT_EQ(restored_page->tasks[i].id, page->tasks[i].id);
        EXPECT_EQ(restored_page->tasks[i].payload, page->tasks[i].payload);
        EXPECT_EQ(restored_page->tasks[i].status, page->tasks[i].status);
        EXPECT_EQ(restored_page->tasks[i].assigned_client,
                  page->tasks[i].assigned_client);
    }
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
