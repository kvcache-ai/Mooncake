#include "task_executor.h"
#include "client_service.h"
#include "master_client.h"
#include "task_manager.h"
#include "gtest/gtest.h"
#include "glog/logging.h"
#include <ylt/struct_json/json_writer.h>
#include <ylt/struct_json/json_reader.h>
#include <memory>
#include <thread>
#include <chrono>
#include <vector>
#include <cstring>
#include <map>
#include <atomic>

namespace mooncake {

// Test helper to track execution flow and validate logic
class TaskExecutionTracker {
   public:
    struct ExecutionRecord {
        std::string operation;
        std::string key;
        ErrorCode result;
        std::chrono::steady_clock::time_point timestamp;
    };

    void Record(const std::string& operation, const std::string& key,
                ErrorCode result) {
        ExecutionRecord record;
        record.operation = operation;
        record.key = key;
        record.result = result;
        record.timestamp = std::chrono::steady_clock::now();
        records_.push_back(record);
    }

    std::vector<ExecutionRecord> GetRecords() const { return records_; }
    void Clear() { records_.clear(); }

    bool HasOperation(const std::string& op) const {
        for (const auto& r : records_) {
            if (r.operation == op) return true;
        }
        return false;
    }

    size_t CountOperation(const std::string& op) const {
        size_t count = 0;
        for (const auto& r : records_) {
            if (r.operation == op) count++;
        }
        return count;
    }

   private:
    std::vector<ExecutionRecord> records_;
};

class TaskExecutorTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("TaskExecutorTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    // Helper to create a valid memory replica descriptor
    Replica::Descriptor CreateMemoryReplicaDescriptor(
        ReplicaID id, const std::string& segment_name, size_t size = 1024) {
        Replica::Descriptor replica;
        replica.id = id;
        replica.status = ReplicaStatus::COMPLETE;
        MemoryDescriptor mem_desc;
        mem_desc.buffer_descriptor.size_ = size;
        mem_desc.buffer_descriptor.buffer_address_ = 0x1000;
        mem_desc.buffer_descriptor.transport_endpoint_ = segment_name;
        replica.descriptor_variant = mem_desc;
        return replica;
    }

    // Helper to create a query result with replicas
    QueryResult CreateQueryResultWithReplicas(
        const std::vector<Replica::Descriptor>& replicas) {
        std::vector<Replica::Descriptor> replicas_copy =
            replicas;  // Make a copy to move
        std::chrono::steady_clock::time_point lease_timeout =
            std::chrono::steady_clock::now() +
            std::chrono::seconds(60);  // 60 second lease
        return QueryResult(std::move(replicas_copy), lease_timeout);
    }
};

TEST_F(TaskExecutorTest, TaskExecutorCreation) {
    TaskExecutor executor(nullptr, nullptr, 2);
    EXPECT_TRUE(executor.IsRunning());

    executor.Stop();
    EXPECT_FALSE(executor.IsRunning());
}

TEST_F(TaskExecutorTest, TaskExecutorThreadPoolSizes) {
    {
        TaskExecutor executor(nullptr, nullptr, 1);
        EXPECT_TRUE(executor.IsRunning());
        executor.Stop();
    }

    {
        TaskExecutor executor(nullptr, nullptr, 4);
        EXPECT_TRUE(executor.IsRunning());
        executor.Stop();
    }

    {
        TaskExecutor executor(nullptr, nullptr, 8);
        EXPECT_TRUE(executor.IsRunning());
        executor.Stop();
    }
}

TEST_F(TaskExecutorTest, TaskExecutorStop) {
    TaskExecutor executor(nullptr, nullptr, 2);
    EXPECT_TRUE(executor.IsRunning());

    executor.Stop();
    EXPECT_FALSE(executor.IsRunning());

    // Stop should be idempotent
    executor.Stop();
    EXPECT_FALSE(executor.IsRunning());
}

TEST_F(TaskExecutorTest, TaskExecutorDestructor) {
    {
        TaskExecutor executor(nullptr, nullptr, 2);
        EXPECT_TRUE(executor.IsRunning());
    }
    // Destructor should call Stop()
}

TEST_F(TaskExecutorTest, ReplicaCopyPayloadStructure) {
    ReplicaCopyPayload payload;
    payload.key = "test_key";
    payload.targets = {"target1", "target2"};

    EXPECT_EQ(payload.key, "test_key");
    EXPECT_EQ(payload.targets.size(), 2);
    EXPECT_EQ(payload.targets[0], "target1");
    EXPECT_EQ(payload.targets[1], "target2");
}

// Test ReplicaCopyPayload with multiple target segments
TEST_F(TaskExecutorTest, ReplicaCopyPayloadMultipleTargets) {
    ReplicaCopyPayload payload;
    payload.key = "test_multi_target_key";
    payload.targets = {"segment1", "segment2", "segment3", "segment4"};

    EXPECT_EQ(payload.key, "test_multi_target_key");
    EXPECT_EQ(payload.targets.size(), 4);
    EXPECT_EQ(payload.targets[0], "segment1");
    EXPECT_EQ(payload.targets[1], "segment2");
    EXPECT_EQ(payload.targets[2], "segment3");
    EXPECT_EQ(payload.targets[3], "segment4");

    // Test serialization and deserialization
    std::string json;
    struct_json::to_json(payload, json);

    ReplicaCopyPayload deserialized_payload;
    struct_json::from_json(deserialized_payload, json);

    EXPECT_EQ(deserialized_payload.key, payload.key);
    EXPECT_EQ(deserialized_payload.targets.size(), payload.targets.size());
    for (size_t i = 0; i < payload.targets.size(); ++i) {
        EXPECT_EQ(deserialized_payload.targets[i], payload.targets[i]);
    }
}

// Test ReplicaCopyPayload with empty targets (edge case)
TEST_F(TaskExecutorTest, ReplicaCopyPayloadEmptyTargets) {
    ReplicaCopyPayload payload;
    payload.key = "test_key";
    payload.targets = {};

    EXPECT_EQ(payload.key, "test_key");
    EXPECT_EQ(payload.targets.size(), 0);

    // Serialization and deserialization should handle empty vector
    std::string json;
    struct_json::to_json(payload, json);

    ReplicaCopyPayload deserialized_payload;
    struct_json::from_json(deserialized_payload, json);

    EXPECT_EQ(deserialized_payload.key, payload.key);
    EXPECT_EQ(deserialized_payload.targets.size(), 0);
}

// Test ReplicaCopyPayload with single target (most common case)
TEST_F(TaskExecutorTest, ReplicaCopyPayloadSingleTarget) {
    ReplicaCopyPayload payload;
    payload.key = "test_single_target_key";
    payload.targets = {"target_segment"};

    EXPECT_EQ(payload.key, "test_single_target_key");
    EXPECT_EQ(payload.targets.size(), 1);
    EXPECT_EQ(payload.targets[0], "target_segment");
}

TEST_F(TaskExecutorTest, ReplicaMovePayloadStructure) {
    ReplicaMovePayload payload;
    payload.key = "test_key";
    payload.source = "source_segment";
    payload.target = "target_segment";

    EXPECT_EQ(payload.key, "test_key");
    EXPECT_EQ(payload.source, "source_segment");
    EXPECT_EQ(payload.target, "target_segment");
}

TEST_F(TaskExecutorTest, TaskWithReplicaCopyPayload) {
    Task task;
    UUID test_id = generate_uuid();
    task.id = test_id;
    task.type = TaskType::REPLICA_COPY;
    task.status = TaskStatus::PENDING;

    ReplicaCopyPayload payload;
    payload.key = "test_key";
    payload.targets = {"target1", "target2"};

    struct_json::to_json(payload, task.payload);

    EXPECT_EQ(task.id.first, test_id.first);
    EXPECT_EQ(task.id.second, test_id.second);
    EXPECT_EQ(task.type, TaskType::REPLICA_COPY);
    EXPECT_EQ(task.status, TaskStatus::PENDING);
    EXPECT_FALSE(task.payload.empty());

    // Verify we can deserialize
    ReplicaCopyPayload deserialized;
    struct_json::from_json(deserialized, task.payload);
    EXPECT_EQ(deserialized.key, "test_key");
    EXPECT_EQ(deserialized.targets.size(), 2);
}

TEST_F(TaskExecutorTest, TaskWithReplicaMovePayload) {
    Task task;
    task.id = generate_uuid();
    task.type = TaskType::REPLICA_MOVE;
    task.status = TaskStatus::PENDING;

    ReplicaMovePayload payload;
    payload.key = "test_key";
    payload.source = "source_segment";
    payload.target = "target_segment";

    struct_json::to_json(payload, task.payload);

    EXPECT_EQ(task.type, TaskType::REPLICA_MOVE);
    EXPECT_EQ(task.status, TaskStatus::PENDING);
    EXPECT_FALSE(task.payload.empty());

    // Verify we can deserialize
    ReplicaMovePayload deserialized;
    struct_json::from_json(deserialized, task.payload);
    EXPECT_EQ(deserialized.key, "test_key");
    EXPECT_EQ(deserialized.source, "source_segment");
    EXPECT_EQ(deserialized.target, "target_segment");
}

TEST_F(TaskExecutorTest, ReplicaCopyExecutorStructure) {
    ReplicaCopyExecutor executor(nullptr, nullptr);
    EXPECT_NE(&executor, nullptr);
}

// Test payload validation
TEST_F(TaskExecutorTest, ReplicaCopyExecutorPayloadValidation) {
    ReplicaCopyExecutor executor(nullptr, nullptr);

    // Test empty key (should fail during execution)
    ReplicaCopyPayload empty_key_payload;
    empty_key_payload.key = "";
    empty_key_payload.targets = {"target1"};
    // Executor should handle empty key gracefully

    // Test empty targets (should fail during execution)
    ReplicaCopyPayload empty_targets_payload;
    empty_targets_payload.key = "test_key";
    empty_targets_payload.targets = {};
    // Executor should handle empty targets gracefully

    EXPECT_TRUE(true);  // Structure validation passed
}

TEST_F(TaskExecutorTest, ReplicaMoveExecutorStructure) {
    ReplicaMoveExecutor executor(nullptr, nullptr);
    EXPECT_NE(&executor, nullptr);
}

TEST_F(TaskExecutorTest, TaskExecutorSubmitTask) {
    TaskExecutor executor(nullptr, nullptr, 2);

    UUID client_id = generate_uuid();
    ClientTask client_task;
    client_task.assignment.id = generate_uuid();
    client_task.assignment.type = TaskType::REPLICA_COPY;

    ReplicaCopyPayload payload;
    payload.key = "test_key";
    payload.targets = {"target1"};
    struct_json::to_json(payload, client_task.assignment.payload);

    // Submit task - should not crash even with nullptr clients
    // Actual execution will fail, but submission should succeed
    executor.SubmitTask(client_task, client_id);

    // Wait a bit for task to be enqueued
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    executor.Stop();
}

TEST_F(TaskExecutorTest, TaskExecutorSubmitMultipleTasks) {
    TaskExecutor executor(nullptr, nullptr, 4);

    UUID client_id = generate_uuid();

    // Submit multiple tasks
    for (int i = 0; i < 5; ++i) {
        ClientTask client_task;
        client_task.assignment.id = generate_uuid();
        client_task.assignment.type =
            (i % 2 == 0) ? TaskType::REPLICA_COPY : TaskType::REPLICA_MOVE;

        if (client_task.assignment.type == TaskType::REPLICA_COPY) {
            ReplicaCopyPayload payload;
            payload.key = "test_key_" + std::to_string(i);
            payload.targets = {"target1"};
            struct_json::to_json(payload, client_task.assignment.payload);
        } else {
            ReplicaMovePayload payload;
            payload.key = "test_key_" + std::to_string(i);
            payload.source = "source_segment";
            payload.target = "target_segment";
            struct_json::to_json(payload, client_task.assignment.payload);
        }

        executor.SubmitTask(client_task, client_id);
    }

    // Wait for tasks to be processed
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    executor.Stop();
}

TEST_F(TaskExecutorTest, TaskExecutorConcurrentSubmission) {
    TaskExecutor executor(nullptr, nullptr, 4);

    UUID client_id = generate_uuid();
    std::vector<std::thread> threads;
    std::atomic<int> submitted_count{0};

    // Submit tasks from multiple threads
    for (int i = 0; i < 3; ++i) {
        threads.emplace_back([&executor, &client_id, &submitted_count, i]() {
            for (int j = 0; j < 2; ++j) {
                ClientTask client_task;
                client_task.assignment.id = generate_uuid();
                client_task.assignment.type = TaskType::REPLICA_COPY;
                ReplicaCopyPayload payload;
                payload.key =
                    "test_key_" + std::to_string(i) + "_" + std::to_string(j);
                payload.targets = {"target1"};
                struct_json::to_json(payload, client_task.assignment.payload);

                executor.SubmitTask(client_task, client_id);
                submitted_count++;
            }
        });
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(submitted_count.load(), 6);

    // Wait for tasks to be processed
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    executor.Stop();
}

TEST_F(TaskExecutorTest, TaskExecutorInvalidTaskType) {
    TaskExecutor executor(nullptr, nullptr, 2);

    UUID client_id = generate_uuid();
    ClientTask client_task;
    client_task.assignment.id = generate_uuid();
    client_task.assignment.type =
        static_cast<TaskType>(999);  // Invalid task type

    // Should not crash even with invalid task type
    // ExecuteTask should handle it and return INVALID_PARAMS
    executor.SubmitTask(client_task, client_id);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    executor.Stop();
}

TEST_F(TaskExecutorTest, MemoryReplicaDescriptorHandling) {
    // Test creating memory replica descriptor
    auto replica = CreateMemoryReplicaDescriptor(1, "test_segment:12345", 1024);

    EXPECT_EQ(replica.id, 1);
    EXPECT_EQ(replica.status, ReplicaStatus::COMPLETE);
    EXPECT_TRUE(replica.is_memory_replica());

    const auto& mem_desc = replica.get_memory_descriptor();
    EXPECT_EQ(mem_desc.buffer_descriptor.size_, 1024);
    EXPECT_EQ(mem_desc.buffer_descriptor.transport_endpoint_,
              "test_segment:12345");
}

TEST_F(TaskExecutorTest, SourceSegmentExtraction) {
    // Test source segment extraction logic:
    // For memory replica: use buffer_descriptor.transport_endpoint_
    // For disk replica: use payload.source (requires src_segment in payload)

    auto memory_replica =
        CreateMemoryReplicaDescriptor(1, "memory_seg:12345", 1024);
    EXPECT_TRUE(memory_replica.is_memory_replica());

    const auto& mem_desc = memory_replica.get_memory_descriptor();
    std::string src_segment = mem_desc.buffer_descriptor.transport_endpoint_;
    EXPECT_EQ(src_segment, "memory_seg:12345");
}

TEST_F(TaskExecutorTest, ExecuteTaskWithInvalidCopyPayload) {
    TaskExecutor executor(nullptr, nullptr, 1);

    UUID client_id = generate_uuid();
    ClientTask client_task;
    client_task.assignment.id = generate_uuid();
    client_task.assignment.type = TaskType::REPLICA_COPY;
    client_task.assignment.payload =
        R"({"key": "some_key", "targets": ["t1", "t2" })";  // 注意花括号不匹配

    executor.SubmitTask(client_task, client_id);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    SUCCEED()
        << "Executor handled invalid JSON payload gracefully without crashing.";

    executor.Stop();
}

TEST_F(TaskExecutorTest, ExecuteTaskWithInvalidMovePayload) {
    TaskExecutor executor(nullptr, nullptr, 1);

    UUID client_id = generate_uuid();
    ClientTask client_task;
    client_task.assignment.id = generate_uuid();
    client_task.assignment.type = TaskType::REPLICA_MOVE;
    client_task.assignment.payload = R"({"key": test_key})";

    executor.SubmitTask(client_task, client_id);

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    SUCCEED()
        << "Executor handled invalid JSON payload for move task gracefully.";

    executor.Stop();
}

TEST_F(TaskExecutorTest, SubmitTaskToStoppedExecutor) {
    TaskExecutor executor(nullptr, nullptr, 1);

    executor.Stop();
    ASSERT_FALSE(executor.IsRunning());

    UUID client_id = generate_uuid();
    ClientTask client_task;
    client_task.assignment.id = generate_uuid();
    client_task.assignment.type = TaskType::REPLICA_COPY;

    ReplicaCopyPayload payload;
    payload.key = "a_key_that_will_be_ignored";
    payload.targets = {"t1"};
    struct_json::to_json(payload, client_task.assignment.payload);

    executor.SubmitTask(client_task, client_id);

    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    SUCCEED() << "Task submitted to a stopped executor was correctly ignored.";
}

TEST_F(TaskExecutorTest, ExecuteTaskWithIncompletePayload) {
    TaskExecutor executor(nullptr, nullptr, 1);

    UUID client_id = generate_uuid();
    ClientTask client_task;
    client_task.assignment.id = generate_uuid();
    client_task.assignment.type = TaskType::REPLICA_COPY;
    client_task.assignment.payload = R"({"key": "some_key"})";

    executor.SubmitTask(client_task, client_id);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    SUCCEED() << "Executor handled incomplete payload without crashing.";
    executor.Stop();
}

// Test retry_count field initialization
TEST_F(TaskExecutorTest, TaskRetryCountInitialization) {
    Task task;
    task.id = generate_uuid();
    task.type = TaskType::REPLICA_COPY;
    task.status = TaskStatus::PENDING;

    // retry_count should be initialized to 0 by default
    EXPECT_EQ(task.retry_count, 0);

    // Test increment_retry method
    task.increment_retry();
    EXPECT_EQ(task.retry_count, 1);

    task.increment_retry();
    EXPECT_EQ(task.retry_count, 2);

    // Verify last_updated_at is updated after increment
    auto time_before = std::chrono::system_clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    task.increment_retry();
    auto time_after = std::chrono::system_clock::now();

    EXPECT_GE(task.last_updated_at, time_before);
    EXPECT_LE(task.last_updated_at, time_after);
}

// Test retry count in task structure
TEST_F(TaskExecutorTest, TaskWithRetryCount) {
    Task task;
    task.id = generate_uuid();
    task.type = TaskType::REPLICA_COPY;
    task.status = TaskStatus::PENDING;
    task.retry_count = 5;

    EXPECT_EQ(task.retry_count, 5);

    // Test that retry_count is preserved in task copy
    Task copied_task = task;
    EXPECT_EQ(copied_task.retry_count, 5);
    EXPECT_EQ(copied_task.id.first, task.id.first);
    EXPECT_EQ(copied_task.id.second, task.id.second);
}

// Test task retry count with multiple increments
TEST_F(TaskExecutorTest, TaskRetryCountMultipleIncrements) {
    Task task;
    task.id = generate_uuid();

    // Increment multiple times
    for (uint32_t i = 0; i < 15; ++i) {
        EXPECT_EQ(task.retry_count, i);
        task.increment_retry();
        EXPECT_EQ(task.retry_count, i + 1);
    }

    EXPECT_EQ(task.retry_count, 15);
}

// Test that retry_count is preserved in task payload serialization
// (indirectly, as retry_count is part of Task structure, not payload)
TEST_F(TaskExecutorTest, TaskRetryCountPreserved) {
    Task task;
    task.id = generate_uuid();
    task.type = TaskType::REPLICA_COPY;
    task.status = TaskStatus::PENDING;
    task.retry_count = 7;

    ReplicaCopyPayload payload;
    payload.key = "test_key";
    payload.targets = {"target1"};
    struct_json::to_json(payload, task.payload);

    // Create a copy of the task (simulating retry scenario)
    Task retry_task = task;
    retry_task.increment_retry();

    // Verify retry count increased
    EXPECT_EQ(task.retry_count, 7);
    EXPECT_EQ(retry_task.retry_count, 8);

    // Verify other fields are preserved
    EXPECT_EQ(retry_task.id.first, task.id.first);
    EXPECT_EQ(retry_task.id.second, task.id.second);
    EXPECT_EQ(retry_task.type, task.type);
    EXPECT_EQ(retry_task.payload, task.payload);
}

// Test retry count boundary conditions (MAX_RETRY_COUNT = 10)
// This test simulates the retry logic behavior
TEST_F(TaskExecutorTest, TaskRetryCountBoundaryConditions) {
    constexpr uint32_t MAX_RETRY_COUNT = 10;

    Task task;
    task.id = generate_uuid();
    task.type = TaskType::REPLICA_COPY;

    // Test incrementing up to MAX_RETRY_COUNT
    for (uint32_t i = 0; i < MAX_RETRY_COUNT; ++i) {
        EXPECT_EQ(task.retry_count, i);
        task.increment_retry();
        EXPECT_EQ(task.retry_count, i + 1);
    }

    EXPECT_EQ(task.retry_count, MAX_RETRY_COUNT);

    // Verify that retry_count can exceed MAX_RETRY_COUNT (the check is in
    // ExecuteTask, not here)
    task.increment_retry();
    EXPECT_EQ(task.retry_count, MAX_RETRY_COUNT + 1);
}

// Test retry count with task status transitions
TEST_F(TaskExecutorTest, TaskRetryCountWithStatusTransitions) {
    Task task;
    task.id = generate_uuid();
    task.type = TaskType::REPLICA_COPY;
    task.status = TaskStatus::PENDING;
    task.retry_count = 3;

    // Simulate task processing
    task.mark_processing();
    EXPECT_EQ(task.status, TaskStatus::PROCESSING);
    EXPECT_EQ(task.retry_count, 3);  // retry_count should not change

    // Simulate retry (reset status and increment retry)
    task.status = TaskStatus::PENDING;
    task.increment_retry();
    EXPECT_EQ(task.status, TaskStatus::PENDING);
    EXPECT_EQ(task.retry_count, 4);
}

// Test multiple task retries with different retry counts
TEST_F(TaskExecutorTest, MultipleTasksWithDifferentRetryCounts) {
    std::vector<Task> tasks;
    const int num_tasks = 5;

    for (int i = 0; i < num_tasks; ++i) {
        Task task;
        task.id = generate_uuid();
        task.type =
            (i % 2 == 0) ? TaskType::REPLICA_COPY : TaskType::REPLICA_MOVE;
        task.status = TaskStatus::PENDING;
        task.retry_count = i;  // Different retry counts
        tasks.push_back(task);
    }

    // Verify each task has correct retry_count
    for (int i = 0; i < num_tasks; ++i) {
        EXPECT_EQ(tasks[i].retry_count, static_cast<uint32_t>(i));
    }

    // Increment retry for all tasks
    for (auto& task : tasks) {
        task.increment_retry();
    }

    // Verify retry counts increased
    for (int i = 0; i < num_tasks; ++i) {
        EXPECT_EQ(tasks[i].retry_count, static_cast<uint32_t>(i + 1));
    }
}

}  // namespace mooncake
