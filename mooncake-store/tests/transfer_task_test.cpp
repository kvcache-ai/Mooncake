// transfer_task_test.cpp
#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

#include "types.h"
#ifdef USE_CUDA
#include <cuda_runtime_api.h>
#endif

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

#ifdef USE_CUDA
TEST_F(TransferTaskTest, TransferScatterWritesGpuDestinationDirectly) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "CUDA device is unavailable";
    }

    constexpr size_t kDestSize = 32;
    std::vector<char> source(64);
    std::iota(source.begin(), source.end(), 0);
    void* gpu_destination = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_destination, kDestSize), cudaSuccess);

    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17932"), 0);
    ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    ASSERT_EQ(engine.registerLocalMemory(source.data(), source.size(), "cpu:0"),
              0);
    ASSERT_EQ(engine.registerLocalMemory(gpu_destination, kDestSize, "cuda:0"),
              0);

    std::vector<size_t> destination_offsets{1, 9, 20},
        source_offsets{3, 17, 41}, lengths{4, 6, 7};
    TransferEngine::ScatterTransferRange transfer{
        .opcode = TransferRequest::READ,
        .remote_segment = engine.getLocalIpAndPort(),
        .remote_base_offset = reinterpret_cast<uintptr_t>(source.data()),
        .remote_size = source.size(),
        .local_buffer = gpu_destination,
        .local_capacity = kDestSize,
        .local_offsets = destination_offsets,
        .remote_offsets = source_offsets,
        .lengths = lengths,
        .on_fragment_complete = {},
    };

    std::shared_ptr<StorageBackend> backend;
    TransferSubmitter submitter(engine, backend, "localhost");
    ASSERT_TRUE(submitter.transferScatter({transfer}).ok());
    std::vector<char> actual(kDestSize);
    ASSERT_EQ(cudaMemcpy(actual.data(), gpu_destination, kDestSize,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    for (size_t i = 0; i < lengths.size(); ++i) {
        EXPECT_EQ(std::memcmp(actual.data() + destination_offsets[i],
                              source.data() + source_offsets[i], lengths[i]),
                  0);
    }

    EXPECT_EQ(engine.unregisterLocalMemory(gpu_destination), 0);
    EXPECT_EQ(engine.unregisterLocalMemory(source.data()), 0);
    EXPECT_EQ(cudaFree(gpu_destination), cudaSuccess);
}
#endif

// Test the locality decision used by TransferSubmitter::isLocalTransfer.
// Same-host different-process pairs share an IP but have distinct ports;
// they must NOT be treated as locally addressable, otherwise memcpy in the
// caller process would dereference a virtual address belonging to a peer
// process and segfault.
TEST_F(TransferTaskTest, IsSameProcessEndpoint) {
    // Empty inputs -> not same-process (cannot prove locality).
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint("", ""));
    EXPECT_FALSE(
        TransferSubmitter::isSameProcessEndpoint("", "192.168.1.10:12345"));
    EXPECT_FALSE(
        TransferSubmitter::isSameProcessEndpoint("192.168.1.10:12345", ""));

    // Identical ip:port -> same process.
    EXPECT_TRUE(TransferSubmitter::isSameProcessEndpoint("192.168.1.10:12345",
                                                         "192.168.1.10:12345"));

    // Same host, different port -> different process, NOT local.
    // This is the regression case fixed by this change.
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint(
        "192.168.1.10:12345", "192.168.1.10:12346"));

    // Different hosts -> not local.
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint(
        "192.168.1.10:12345", "192.168.1.11:12345"));

    // Hostname endpoints (non-P2P metadata mode) compare as full strings.
    EXPECT_TRUE(TransferSubmitter::isSameProcessEndpoint("host-a", "host-a"));
    EXPECT_FALSE(TransferSubmitter::isSameProcessEndpoint("host-a", "host-b"));
}

// Test TransferStrategy enum and stream operator
TEST_F(TransferTaskTest, TransferStrategyEnum) {
    // Test enum values
    EXPECT_EQ(static_cast<int>(TransferStrategy::LOCAL_MEMCPY), 0);
    EXPECT_EQ(static_cast<int>(TransferStrategy::TRANSFER_ENGINE), 1);
    EXPECT_EQ(static_cast<int>(TransferStrategy::FILE_READ), 2);
    EXPECT_EQ(static_cast<int>(TransferStrategy::EMPTY), 3);
    EXPECT_EQ(static_cast<int>(TransferStrategy::SPDK_NVMF), 4);

    // Test stream operator
    std::ostringstream oss;
    oss << TransferStrategy::LOCAL_MEMCPY;
    EXPECT_EQ(oss.str(), "LOCAL_MEMCPY");

    oss.str("");
    oss << TransferStrategy::TRANSFER_ENGINE;
    EXPECT_EQ(oss.str(), "TRANSFER_ENGINE");

    oss.str("");
    oss << TransferStrategy::SPDK_NVMF;
    EXPECT_EQ(oss.str(), "SPDK_NVMF");

    oss.str("");
    oss << TransferStrategy::FILE_READ;
    EXPECT_EQ(oss.str(), "FILE_READ");

    oss.str("");
    oss << TransferStrategy::EMPTY;
    EXPECT_EQ(oss.str(), "EMPTY");
}

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
