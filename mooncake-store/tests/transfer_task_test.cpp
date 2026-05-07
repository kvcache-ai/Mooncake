// transfer_task_test.cpp
#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "allocator.h"
#include "replica.h"
#include "types.h"

namespace mooncake {

// Test fixture for TransferTask tests
// TODO: Currently, this test does not cover TransferSubmitter and
// TransferEngine integration. Will add more tests in the future.
class TransferTaskTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize glog for logging
        google::InitGoogleLogging("TransferTaskTest");
        FLAGS_logtostderr = 1;  // Output logs to stderr
    }

    void TearDown() override {
        // Cleanup glog
        google::ShutdownGoogleLogging();
    }

    using ScatterRangeForTest = std::tuple<size_t, size_t, size_t>;
    using ScatterKeyRangesForTest = std::vector<
        std::pair<Replica::Descriptor, std::vector<ScatterRangeForTest>>>;

    std::optional<TransferSubmitter::ScatterReadBuildResult>
    buildScatterReadRequestsForTest(void* dest_buffer,
                                    const ScatterKeyRangesForTest& key_ranges,
                                    bool enable_task_grouping) {
        return TransferSubmitter::buildScatterReadRequests(
            dest_buffer, key_ranges, enable_task_grouping,
            "buildScatterReadRequestsForTest",
            [](const std::string& endpoint) -> SegmentHandle {
                return endpoint == "remote-a" ? 1 : 2;
            });
    }

    Replica::Descriptor makeMemoryReplica(const std::string& endpoint,
                                          uintptr_t base, size_t size) {
        AllocatedBuffer::Descriptor buffer_desc{};
        buffer_desc.size_ = size;
        buffer_desc.buffer_address_ = base;
        buffer_desc.protocol_ = "rdma";
        buffer_desc.transport_endpoint_ = endpoint;

        MemoryDescriptor memory_desc{};
        memory_desc.buffer_descriptor = buffer_desc;

        Replica::Descriptor replica{};
        replica.id = 1;
        replica.descriptor_variant = memory_desc;
        replica.status = ReplicaStatus::COMPLETE;
        return replica;
    }
};

// Test basic MemcpyOperation functionality
TEST_F(TransferTaskTest, MemcpyOperationBasic) {
    const size_t data_size = 1024;
    std::vector<char> src_data(data_size, 'A');
    std::vector<char> dest_data(data_size, 'B');

    // Create memcpy operation
    MemcpyOperation op(dest_data.data(), src_data.data(), data_size);

    // Verify operation parameters
    EXPECT_EQ(op.dest, dest_data.data());
    EXPECT_EQ(op.src, src_data.data());
    EXPECT_EQ(op.size, data_size);

    // Perform memcpy manually to test
    std::memcpy(op.dest, op.src, op.size);

    // Verify data was copied correctly
    EXPECT_EQ(dest_data, src_data);
    for (size_t i = 0; i < data_size; ++i) {
        EXPECT_EQ(dest_data[i], 'A');
    }
}

// Test MemcpyOperationState functionality
TEST_F(TransferTaskTest, MemcpyOperationState) {
    auto state = std::make_shared<MemcpyOperationState>();

    // Initially not completed
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(state->get_strategy(), TransferStrategy::LOCAL_MEMCPY);

    // Set completed with success
    state->set_completed(ErrorCode::OK);
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
}

// Test MemcpyWorkerPool basic functionality
TEST_F(TransferTaskTest, MemcpyWorkerPoolBasic) {
    MemcpyWorkerPool pool;

    const size_t data_size = 512;
    std::vector<char> src_data(data_size, 'X');
    std::vector<char> dest_data(data_size, 'Y');

    auto state = std::make_shared<MemcpyOperationState>();

    // Create memcpy operations
    std::vector<MemcpyOperation> operations;
    operations.emplace_back(dest_data.data(), src_data.data(), data_size);

    // Create and submit task
    MemcpyTask task(std::move(operations), state);
    pool.submitTask(std::move(task));

    // Wait for completion
    state->wait_for_completion();

    // Verify completion and result
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);

    // Verify data was copied correctly
    for (size_t i = 0; i < data_size; ++i) {
        EXPECT_EQ(dest_data[i], 'X');
    }
}

// Test multiple memcpy operations in one task
TEST_F(TransferTaskTest, MemcpyWorkerPoolMultipleOperations) {
    MemcpyWorkerPool pool;

    const size_t num_ops = 3;
    const size_t data_size = 256;

    std::vector<std::vector<char>> src_buffers(num_ops);
    std::vector<std::vector<char>> dest_buffers(num_ops);

    // Initialize source buffers with different patterns
    for (size_t i = 0; i < num_ops; ++i) {
        src_buffers[i].resize(data_size, 'A' + i);
        dest_buffers[i].resize(data_size, 'Z');
    }

    auto state = std::make_shared<MemcpyOperationState>();

    // Create multiple memcpy operations
    std::vector<MemcpyOperation> operations;
    for (size_t i = 0; i < num_ops; ++i) {
        operations.emplace_back(dest_buffers[i].data(), src_buffers[i].data(),
                                data_size);
    }

    // Create and submit task
    MemcpyTask task(std::move(operations), state);
    pool.submitTask(std::move(task));

    // Wait for completion
    state->wait_for_completion();

    // Verify completion and result
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);

    // Verify all data was copied correctly
    for (size_t i = 0; i < num_ops; ++i) {
        for (size_t j = 0; j < data_size; ++j) {
            EXPECT_EQ(dest_buffers[i][j], 'A' + i);
        }
    }
}

TEST_F(TransferTaskTest, EmptyScatterReadBuildHasNoTasks) {
    char dest[1];
    ScatterKeyRangesForTest key_ranges;

    auto grouped = buildScatterReadRequestsForTest(dest, key_ranges, true);
    ASSERT_TRUE(grouped.has_value());
    EXPECT_TRUE(grouped->flat_requests.empty());
    EXPECT_EQ(grouped->logical_task_count, 0u);
}

TEST_F(TransferTaskTest, ScatterReadGroupingReducesLogicalTasks) {
    char dest[4096];
    auto replica = makeMemoryReplica("remote", 0x100000, sizeof(dest));
    ScatterKeyRangesForTest key_ranges{
        {replica, {{0, 0, 256}, {512, 1024, 128}, {1024, 2048, 512}}},
    };

    auto ungrouped = buildScatterReadRequestsForTest(dest, key_ranges, false);
    ASSERT_TRUE(ungrouped.has_value());
    EXPECT_EQ(ungrouped->flat_requests.size(), 3u);
    EXPECT_EQ(ungrouped->logical_task_count, 3u);
    for (const auto& request : ungrouped->flat_requests) {
        EXPECT_EQ(request.task_group_id, TransferRequest::kNoTaskGroup);
    }

    auto grouped = buildScatterReadRequestsForTest(dest, key_ranges, true);
    ASSERT_TRUE(grouped.has_value());
    EXPECT_EQ(grouped->flat_requests.size(), 3u);
    EXPECT_EQ(grouped->logical_task_count, 1u);
    for (const auto& request : grouped->flat_requests) {
        EXPECT_NE(request.task_group_id, TransferRequest::kNoTaskGroup);
        EXPECT_EQ(request.target_id, grouped->flat_requests[0].target_id);
    }
}

TEST_F(TransferTaskTest, ScatterReadGroupingKeepsSingleRangeUngrouped) {
    char dest[4096];
    auto replica = makeMemoryReplica("remote", 0x100000, sizeof(dest));
    ScatterKeyRangesForTest key_ranges{
        {replica, {{0, 0, 256}}},
    };

    auto grouped = buildScatterReadRequestsForTest(dest, key_ranges, true);
    ASSERT_TRUE(grouped.has_value());
    ASSERT_EQ(grouped->flat_requests.size(), 1u);
    EXPECT_EQ(grouped->logical_task_count, 1u);
    EXPECT_EQ(grouped->flat_requests[0].task_group_id,
              TransferRequest::kNoTaskGroup);
}

TEST_F(TransferTaskTest, ScatterReadGroupingSeparatesReplicaGroups) {
    char dest[4096];
    auto replica_a = makeMemoryReplica("remote-a", 0x100000, sizeof(dest));
    auto replica_b = makeMemoryReplica("remote-b", 0x200000, sizeof(dest));
    ScatterKeyRangesForTest key_ranges{
        {replica_a, {{0, 0, 256}, {256, 256, 256}}},
        {replica_b, {{512, 0, 128}, {640, 128, 128}}},
    };

    auto grouped = buildScatterReadRequestsForTest(dest, key_ranges, true);
    ASSERT_TRUE(grouped.has_value());
    EXPECT_EQ(grouped->flat_requests.size(), 4u);
    EXPECT_EQ(grouped->logical_task_count, 2u);
    EXPECT_EQ(grouped->flat_requests[0].task_group_id,
              grouped->flat_requests[1].task_group_id);
    EXPECT_EQ(grouped->flat_requests[2].task_group_id,
              grouped->flat_requests[3].task_group_id);
    EXPECT_NE(grouped->flat_requests[0].task_group_id,
              grouped->flat_requests[2].task_group_id);
}

// Test TransferStrategy enum and stream operator
TEST_F(TransferTaskTest, TransferStrategyEnum) {
    // Test enum values
    EXPECT_EQ(static_cast<int>(TransferStrategy::LOCAL_MEMCPY), 0);
    EXPECT_EQ(static_cast<int>(TransferStrategy::TRANSFER_ENGINE), 1);

    // Test stream operator
    std::ostringstream oss;
    oss << TransferStrategy::LOCAL_MEMCPY;
    EXPECT_EQ(oss.str(), "LOCAL_MEMCPY");

    oss.str("");
    oss << TransferStrategy::TRANSFER_ENGINE;
    EXPECT_EQ(oss.str(), "TRANSFER_ENGINE");
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
