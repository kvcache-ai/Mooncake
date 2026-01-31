#include "client_service.h"
#include "master_client.h"
#include "task_manager.h"
#include "rpc_types.h"
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
#include <set>
#include <string>

namespace mooncake {

// Test class for Client Copy/Move and task execution functionality
// Note: TaskExecutor has been merged into Client class.
// These are unit tests for the logic that can be tested without full
// integration. Full end-to-end tests are in task_integration_test.cpp
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

// Payload Structure Tests
TEST_F(TaskExecutorTest, ReplicaCopyPayloadStructure) {
    ReplicaCopyPayload payload;
    payload.key = "test_key";
    payload.targets = {"target1", "target2"};

    EXPECT_EQ(payload.key, "test_key");
    EXPECT_EQ(payload.targets.size(), 2);
    EXPECT_EQ(payload.targets[0], "target1");
    EXPECT_EQ(payload.targets[1], "target2");
}

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

// Task Payload Tests
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

// Replica Descriptor Tests
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

// ClientTask and TaskAssignment Tests
TEST_F(TaskExecutorTest, ClientTaskStructure) {
    // ClientTask is now a private struct in Client class, but we can test
    // TaskAssignment which is used to construct ClientTask
    TaskAssignment assignment;
    assignment.id = generate_uuid();
    assignment.type = TaskType::REPLICA_COPY;

    ReplicaCopyPayload payload;
    payload.key = "test_key";
    payload.targets = {"target1"};
    struct_json::to_json(payload, assignment.payload);

    EXPECT_EQ(assignment.type, TaskType::REPLICA_COPY);
    EXPECT_FALSE(assignment.payload.empty());

    // Verify we can deserialize
    ReplicaCopyPayload deserialized;
    struct_json::from_json(deserialized, assignment.payload);
    EXPECT_EQ(deserialized.key, "test_key");
    EXPECT_EQ(deserialized.targets.size(), 1);
}

TEST_F(TaskExecutorTest, TaskAssignmentToClientTaskConversion) {
    TaskAssignment assignment;
    assignment.id = generate_uuid();
    assignment.type = TaskType::REPLICA_COPY;

    ReplicaCopyPayload payload;
    payload.key = "test_key";
    payload.targets = {"target1", "target2"};
    struct_json::to_json(payload, assignment.payload);

    // Simulate ClientTask construction (ClientTask is private, so we test the
    // pattern) In actual Client code: ClientTask client_task;
    // client_task.assignment = assignment; client_task.retry_count = 0;
    EXPECT_EQ(assignment.type, TaskType::REPLICA_COPY);
    EXPECT_FALSE(assignment.payload.empty());

    // Verify payload can be deserialized
    ReplicaCopyPayload deserialized;
    struct_json::from_json(deserialized, assignment.payload);
    EXPECT_EQ(deserialized.key, "test_key");
    EXPECT_EQ(deserialized.targets.size(), 2);
}

// Copy/Move Method Parameter Validation Tests
TEST_F(TaskExecutorTest, CopyMethodEmptyKey) {
    // This test documents expected behavior for empty key
    // Actual implementation will fail at Query stage
    ReplicaCopyPayload payload;
    payload.key = "";
    payload.targets = {"target1"};

    // Serialization should work even with empty key
    std::string json;
    struct_json::to_json(payload, json);
    EXPECT_FALSE(json.empty());

    ReplicaCopyPayload deserialized;
    struct_json::from_json(deserialized, json);
    EXPECT_TRUE(deserialized.key.empty());
    EXPECT_EQ(deserialized.targets.size(), 1);
}

TEST_F(TaskExecutorTest, CopyMethodEmptyTargets) {
    ReplicaCopyPayload payload;
    payload.key = "test_key";
    payload.targets = {};

    // Serialization should handle empty targets
    std::string json;
    struct_json::to_json(payload, json);

    ReplicaCopyPayload deserialized;
    struct_json::from_json(deserialized, json);
    EXPECT_EQ(deserialized.key, "test_key");
    EXPECT_TRUE(deserialized.targets.empty());
}

TEST_F(TaskExecutorTest, MoveMethodEmptyKey) {
    ReplicaMovePayload payload;
    payload.key = "";
    payload.source = "source_segment";
    payload.target = "target_segment";

    std::string json;
    struct_json::to_json(payload, json);

    ReplicaMovePayload deserialized;
    struct_json::from_json(deserialized, json);
    EXPECT_TRUE(deserialized.key.empty());
    EXPECT_EQ(deserialized.source, "source_segment");
    EXPECT_EQ(deserialized.target, "target_segment");
}

TEST_F(TaskExecutorTest, MoveMethodEmptySegments) {
    ReplicaMovePayload payload;
    payload.key = "test_key";
    payload.source = "";
    payload.target = "target_segment";

    std::string json;
    struct_json::to_json(payload, json);

    ReplicaMovePayload deserialized;
    struct_json::from_json(deserialized, json);
    EXPECT_EQ(deserialized.key, "test_key");
    EXPECT_TRUE(deserialized.source.empty());
    EXPECT_EQ(deserialized.target, "target_segment");
}

// Source Segment Selection Logic Tests
TEST_F(TaskExecutorTest, SourceSegmentSelectionLogic) {
    // Create multiple replicas with different segments
    std::vector<Replica::Descriptor> replicas;
    replicas.push_back(CreateMemoryReplicaDescriptor(1, "segment1", 1024));
    replicas.push_back(CreateMemoryReplicaDescriptor(2, "segment2", 1024));
    replicas.push_back(CreateMemoryReplicaDescriptor(3, "segment3", 1024));

    // Test finding source when targets are segment2 and segment3
    std::vector<std::string> targets = {"segment2", "segment3"};
    std::set<std::string> target_set(targets.begin(), targets.end());

    bool found_source = false;
    std::string selected_segment;
    for (const auto& replica : replicas) {
        if (replica.is_memory_replica()) {
            const auto& mem_desc = replica.get_memory_descriptor();
            std::string replica_segment =
                mem_desc.buffer_descriptor.transport_endpoint_;
            if (target_set.find(replica_segment) == target_set.end()) {
                selected_segment = replica_segment;
                found_source = true;
                break;
            }
        }
    }

    EXPECT_TRUE(found_source);
    EXPECT_EQ(selected_segment, "segment1");
}

TEST_F(TaskExecutorTest, SourceSegmentSelectionAllInTargets) {
    std::vector<Replica::Descriptor> replicas;
    replicas.push_back(CreateMemoryReplicaDescriptor(1, "segment1", 1024));
    replicas.push_back(CreateMemoryReplicaDescriptor(2, "segment2", 1024));

    std::vector<std::string> targets = {"segment1", "segment2"};
    std::set<std::string> target_set(targets.begin(), targets.end());

    bool found_source = false;
    for (const auto& replica : replicas) {
        if (replica.is_memory_replica()) {
            const auto& mem_desc = replica.get_memory_descriptor();
            std::string replica_segment =
                mem_desc.buffer_descriptor.transport_endpoint_;
            if (target_set.find(replica_segment) == target_set.end()) {
                found_source = true;
                break;
            }
        }
    }

    EXPECT_FALSE(found_source);
}

// Retry Logic Tests
TEST_F(TaskExecutorTest, RetryCountIncrement) {
    // Simulate ClientTask retry logic
    uint32_t retry_count = 0;
    constexpr uint32_t MAX_RETRY_COUNT = 10;

    // Test incrementing retry count
    for (uint32_t i = 0; i < MAX_RETRY_COUNT; ++i) {
        EXPECT_EQ(retry_count, i);
        retry_count++;
        EXPECT_EQ(retry_count, i + 1);
    }

    EXPECT_EQ(retry_count, MAX_RETRY_COUNT);

    // Test that retry count can exceed MAX_RETRY_COUNT
    retry_count++;
    EXPECT_EQ(retry_count, MAX_RETRY_COUNT + 1);
}

TEST_F(TaskExecutorTest, RetryDecisionLogic) {
    constexpr uint32_t MAX_RETRY_COUNT = 10;

    // Test cases for retry decision
    struct RetryTestCase {
        ErrorCode error;
        uint32_t retry_count;
        bool should_retry;
    };

    std::vector<RetryTestCase> test_cases = {
        {ErrorCode::NO_AVAILABLE_HANDLE, 0, true},    // Should retry
        {ErrorCode::NO_AVAILABLE_HANDLE, 5, true},    // Should retry
        {ErrorCode::NO_AVAILABLE_HANDLE, 9, true},    // Should retry
        {ErrorCode::NO_AVAILABLE_HANDLE, 10, false},  // Max retries reached
        {ErrorCode::NO_AVAILABLE_HANDLE, 11, false},  // Exceeded max
        {ErrorCode::OBJECT_NOT_FOUND, 0, false},      // Should not retry
        {ErrorCode::REPLICA_NOT_FOUND, 0, false},     // Should not retry
        {ErrorCode::INTERNAL_ERROR, 0, false},        // Should not retry
    };

    for (const auto& test_case : test_cases) {
        bool should_retry =
            (test_case.error == ErrorCode::NO_AVAILABLE_HANDLE) &&
            (test_case.retry_count < MAX_RETRY_COUNT);
        EXPECT_EQ(should_retry, test_case.should_retry)
            << "Error: " << static_cast<int>(test_case.error)
            << ", Retry count: " << test_case.retry_count;
    }
}

TEST_F(TaskExecutorTest, RetryDelayCalculation) {
    constexpr uint32_t MAX_RETRY_COUNT = 10;

    for (uint32_t retry_count = 0; retry_count < MAX_RETRY_COUNT;
         ++retry_count) {
        auto retry_delay = std::chrono::milliseconds(50 * (retry_count + 1));
        EXPECT_EQ(retry_delay.count(), 50 * (retry_count + 1));
    }
}

// Task Type and Error Handling Tests
TEST_F(TaskExecutorTest, TaskTypeHandling) {
    // Test valid task types
    EXPECT_EQ(static_cast<int>(TaskType::REPLICA_COPY), 0);
    EXPECT_EQ(static_cast<int>(TaskType::REPLICA_MOVE), 1);

    // Test invalid task type (should be handled gracefully)
    TaskType invalid_type = static_cast<TaskType>(999);
    EXPECT_NE(invalid_type, TaskType::REPLICA_COPY);
    EXPECT_NE(invalid_type, TaskType::REPLICA_MOVE);
}

TEST_F(TaskExecutorTest, PayloadDeserializationErrorHandling) {
    TaskAssignment assignment;
    assignment.id = generate_uuid();
    assignment.type = TaskType::REPLICA_COPY;
    assignment.payload = R"({"invalid": "json"})";  // Missing required fields

    // This should be caught by exception handler in ExecuteTask
    ReplicaCopyPayload payload;
    bool exception_caught = false;
    try {
        struct_json::from_json(payload, assignment.payload);
    } catch (const std::exception& e) {
        exception_caught = true;
    }

    // struct_json does not throw, but field will be default
    EXPECT_FALSE(exception_caught);
    EXPECT_TRUE(payload.key.empty());
    EXPECT_TRUE(payload.targets.empty());
}

// Multiple Target Segments Handling Tests
TEST_F(TaskExecutorTest, MultipleTargetSegmentsHandling) {
    ReplicaCopyPayload payload;
    payload.key = "test_key";
    payload.targets = {"target1", "target2", "target3", "target4", "target5"};

    EXPECT_EQ(payload.targets.size(), 5);
    EXPECT_EQ(payload.targets[0], "target1");
    EXPECT_EQ(payload.targets[4], "target5");

    // Test serialization with multiple targets
    std::string json;
    struct_json::to_json(payload, json);

    ReplicaCopyPayload deserialized;
    struct_json::from_json(deserialized, json);
    EXPECT_EQ(deserialized.targets.size(), 5);
}

}  // namespace mooncake
