// transfer_task_test.cpp
#include "transfer_task.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <limits>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

#include "types.h"
#include "pinned_buffer_pool.h"
#if defined(USE_CUDA) || defined(MOONCAKE_TEST_CUDA_H2D)
#include <cuda_runtime_api.h>
#endif

namespace mooncake {

// Test fixture for TransferTask tests
// TODO: Currently, this test does not cover TransferSubmitter and
// TransferEngine integration. Will add more tests in the future.
class ScopedEnvVar {
   public:
    ScopedEnvVar(const char* name, const char* value) : name_(name) {
        if (const char* old_value = std::getenv(name)) {
            had_old_value_ = true;
            old_value_ = old_value;
        }
        if (value) {
            setenv(name_.c_str(), value, 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

    ~ScopedEnvVar() {
        if (had_old_value_) {
            setenv(name_.c_str(), old_value_.c_str(), 1);
        } else {
            unsetenv(name_.c_str());
        }
    }

   private:
    std::string name_;
    bool had_old_value_ = false;
    std::string old_value_;
};

class TransferTaskTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // Initialize glog for logging
        google::InitGoogleLogging("TransferTaskTest");
        FLAGS_logtostderr = 1;  // Output logs to stderr
        unsetenv("MC_STORE_MEMCPY");
    }

    void TearDown() override {
        unsetenv("MC_STORE_MEMCPY");
        // Cleanup glog
        google::ShutdownGoogleLogging();
    }
};

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

TEST_F(TransferTaskTest, TransferScatterHandlesFragmentedCpuBuffers) {
    constexpr size_t kBufferSize = 512;
    constexpr size_t kFragmentCount = 128;
    std::vector<char> source(kBufferSize), destination(kBufferSize, 0);
    std::iota(source.begin(), source.end(), 0);
    std::vector<size_t> completions(kFragmentCount, 0);
    std::vector<size_t> destination_offsets, source_offsets,
        lengths(kFragmentCount, 1);
    for (size_t i = 0; i < kFragmentCount; ++i) {
        destination_offsets.push_back(i * 2);
        source_offsets.push_back(i * 3);
    }

    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17931"), 0);
    if (!engine.isUsingTent()) {
        ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    }
    ASSERT_EQ(engine.registerLocalMemory(source.data(), source.size(), "cpu:0"),
              0);
    ASSERT_EQ(engine.registerLocalMemory(destination.data(), destination.size(),
                                         "cpu:0"),
              0);

    ASSERT_TRUE(engine
                    .transferScatter({{
                        .opcode = TransferRequest::READ,
                        .remote_segment = engine.getLocalIpAndPort(),
                        .remote_base_offset =
                            reinterpret_cast<uintptr_t>(source.data()),
                        .remote_size = source.size(),
                        .local_buffer = destination.data(),
                        .local_capacity = destination.size(),
                        .local_offsets = destination_offsets,
                        .remote_offsets = source_offsets,
                        .lengths = lengths,
                        .on_fragment_complete =
                            [&](size_t i, const Status& status) {
                                EXPECT_TRUE(status.ok());
                                ++completions[i];
                            },
                    }})
                    .ok());
    for (size_t i = 0; i < kFragmentCount; ++i) {
        EXPECT_EQ(destination[destination_offsets[i]],
                  source[source_offsets[i]]);
        EXPECT_EQ(completions[i], 1u);
    }
}

#ifdef USE_CUDA
TEST_F(TransferTaskTest, TransferScatterHandlesFragmentedGpuBuffers) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "CUDA device is unavailable";
    }

    constexpr size_t kBufferSize = 512;
    constexpr size_t kFragmentCount = 128;
    std::vector<char> source(kBufferSize), cpu_destination(kBufferSize, 0);
    std::iota(source.begin(), source.end(), 0);
    void *gpu_source = nullptr, *gpu_destination = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_source, kBufferSize), cudaSuccess);
    ASSERT_EQ(cudaMalloc(&gpu_destination, kBufferSize), cudaSuccess);
    ASSERT_EQ(cudaMemcpy(gpu_source, source.data(), kBufferSize,
                         cudaMemcpyHostToDevice),
              cudaSuccess);

    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17932"), 0);
    if (!engine.isUsingTent()) {
        ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);
    }
    ASSERT_EQ(engine.registerLocalMemory(source.data(), source.size(), "cpu:0"),
              0);
    ASSERT_EQ(engine.registerLocalMemory(cpu_destination.data(),
                                         cpu_destination.size(), "cpu:0"),
              0);
    ASSERT_EQ(engine.registerLocalMemory(gpu_source, kBufferSize, "cuda:0"), 0);
    ASSERT_EQ(
        engine.registerLocalMemory(gpu_destination, kBufferSize, "cuda:0"), 0);

    std::vector<size_t> destination_offsets, source_offsets,
        lengths(kFragmentCount, 1);
    std::vector<char> expected(kBufferSize, 0), actual(kBufferSize);
    for (size_t i = 0; i < kFragmentCount; ++i) {
        destination_offsets.push_back(i * 2);
        source_offsets.push_back(i * 3);
        expected[i * 2] = source[i * 3];
    }
    auto make_transfer = [&](void* remote_source, void* local_destination) {
        return TransferEngine::ScatterTransferRange{
            .opcode = TransferRequest::READ,
            .remote_segment = engine.getLocalIpAndPort(),
            .remote_base_offset = reinterpret_cast<uintptr_t>(remote_source),
            .remote_size = kBufferSize,
            .local_buffer = local_destination,
            .local_capacity = kBufferSize,
            .local_offsets = destination_offsets,
            .remote_offsets = source_offsets,
            .lengths = lengths,
            .on_fragment_complete = {},
        };
    };

    auto invalid_transfer = make_transfer(source.data(), gpu_destination);
    invalid_transfer.remote_offsets = {};
    EXPECT_TRUE(engine.transferScatter({invalid_transfer}).IsInvalidArgument());

    ASSERT_EQ(cudaMemset(gpu_destination, 0, kBufferSize), cudaSuccess);
    ASSERT_TRUE(
        engine.transferScatter({make_transfer(source.data(), gpu_destination)})
            .ok());
    ASSERT_EQ(cudaMemcpy(actual.data(), gpu_destination, kBufferSize,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(actual, expected);

    ASSERT_TRUE(engine
                    .transferScatter(
                        {make_transfer(gpu_source, cpu_destination.data())})
                    .ok());
    EXPECT_EQ(cpu_destination, expected);

    ASSERT_EQ(cudaMemset(gpu_destination, 0, kBufferSize), cudaSuccess);
    auto operation =
        engine.submitScatter({make_transfer(gpu_source, gpu_destination)});
    ASSERT_EQ(engine.freeEngine(), 0);
    ASSERT_TRUE(operation.wait().ok());
    ASSERT_EQ(cudaMemcpy(actual.data(), gpu_destination, kBufferSize,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(actual, expected);

    EXPECT_EQ(cudaFree(gpu_source), cudaSuccess);
    EXPECT_EQ(cudaFree(gpu_destination), cudaSuccess);
}
#endif

// Same-host endpoints from different processes are not locally addressable.
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

TEST_F(TransferTaskTest, BatchGetOffloadObjectHonorsLocalMemcpySetting) {
    setenv("MC_STORE_MEMCPY", "1", 1);
    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17933"), 0);
    const std::string endpoint = engine.getLocalIpAndPort();

    std::vector<char> source(512, 'A');
    std::vector<char> destination(512, 0);
    const std::vector<std::string> keys{"key"};
    const std::vector<uint64_t> pointers{
        reinterpret_cast<uintptr_t>(source.data())};
    const std::unordered_map<std::string, std::vector<Slice>> slices{
        {"key", {{nullptr, 0}, {destination.data(), destination.size()}}}};

    {
        std::shared_ptr<StorageBackend> backend;
        TransferSubmitter submitter(engine, backend, endpoint);
        auto future = submitter.submit_batch_get_offload_object(
            endpoint, keys, pointers, slices,
            OffloadBufferAccess::kLocalAddress);
        ASSERT_TRUE(future);
        EXPECT_EQ(future->strategy(), TransferStrategy::LOCAL_MEMCPY);
        EXPECT_EQ(future->get(), ErrorCode::OK);
        EXPECT_FALSE(submitter.submit_batch_get_offload_object(
            endpoint, keys, {std::numeric_limits<uint64_t>::max() - 7},
            {{"key", {{destination.data(), 16}}}},
            OffloadBufferAccess::kLocalAddress));
    }
    EXPECT_EQ(destination, source);

    setenv("MC_STORE_MEMCPY", "0", 1);
    std::shared_ptr<StorageBackend> backend;
    TransferSubmitter submitter(engine, backend, endpoint);
    EXPECT_FALSE(submitter.submit_batch_get_offload_object(
        endpoint, keys, pointers, slices, OffloadBufferAccess::kLocalAddress));
    EXPECT_EQ(engine.freeEngine(), 0);
}

#ifdef MOONCAKE_TEST_CUDA_H2D
TEST_F(TransferTaskTest, BatchGetOffloadObjectCopiesPinnedHostToGpu) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "CUDA device is unavailable";
    }

    setenv("MC_STORE_MEMCPY", "1", 1);
    constexpr size_t kSourceOffset = 128;
    constexpr size_t kSize = 4096;
    auto pinned_buffer =
        PinnedBufferPool::AllocatePinned(kSourceOffset + kSize);
    ASSERT_NE(pinned_buffer.pinned_host.addr, nullptr);
    void* pinned_source = pinned_buffer.data;
    void* gpu_destination = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_destination, kSize), cudaSuccess);
    std::memset(static_cast<char*>(pinned_source) + kSourceOffset, 0x5a, kSize);

    TransferEngine engine(false);
    ASSERT_EQ(engine.init("P2PHANDSHAKE", "localhost:17934"), 0);
    const std::string endpoint = engine.getLocalIpAndPort();
    {
        std::shared_ptr<StorageBackend> backend;
        TransferSubmitter submitter(engine, backend, endpoint);
        auto future = submitter.submit_batch_get_offload_object(
            endpoint, {"gpu"},
            {reinterpret_cast<uintptr_t>(static_cast<char*>(pinned_source) +
                                         kSourceOffset)},
            {{"gpu", {{gpu_destination, kSize}}}},
            OffloadBufferAccess::kLocalAddress);
        ASSERT_TRUE(future);
        EXPECT_EQ(future->get(), ErrorCode::OK);
    }

    std::vector<unsigned char> actual(kSize);
    EXPECT_EQ(cudaMemcpy(actual.data(), gpu_destination, kSize,
                         cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(actual, std::vector<unsigned char>(kSize, 0x5a));
    EXPECT_EQ(engine.freeEngine(), 0);
    EXPECT_EQ(cudaFree(gpu_destination), cudaSuccess);
}
#endif
TEST_F(TransferTaskTest, CanUseLocalMemcpyRequiresSameProcessEndpoint) {
    ScopedEnvVar memcpy_enabled("MC_STORE_MEMCPY", "1");

    TransferEngine engine(false);
    ASSERT_EQ(
        engine.init("P2PHANDSHAKE", "127.0.0.1:30991", "127.0.0.1", 30991), 0);
    ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);

    std::shared_ptr<StorageBackend> storage_backend;
    TransferSubmitter submitter(engine, storage_backend, "127.0.0.1:30991");

    const auto local_endpoint = engine.getLocalIpAndPort();
    ASSERT_FALSE(local_endpoint.empty());
    EXPECT_TRUE(submitter.canUseLocalMemcpy(local_endpoint));

    EXPECT_FALSE(submitter.canUseLocalMemcpy("127.0.0.1:30992"));
    EXPECT_FALSE(submitter.canUseLocalMemcpy(""));
}

TEST_F(TransferTaskTest, CanUseLocalMemcpyHonorsMemcpyEnv) {
    ScopedEnvVar memcpy_disabled("MC_STORE_MEMCPY", "0");

    TransferEngine engine(false);
    ASSERT_EQ(
        engine.init("P2PHANDSHAKE", "127.0.0.1:30993", "127.0.0.1", 30993), 0);
    ASSERT_NE(engine.installTransport("tcp", nullptr), nullptr);

    std::shared_ptr<StorageBackend> storage_backend;
    TransferSubmitter submitter(engine, storage_backend, "127.0.0.1:30993");

    EXPECT_FALSE(submitter.canUseLocalMemcpy(engine.getLocalIpAndPort()));
}

TEST_F(TransferTaskTest, BatchWriteHonorsLocalMemcpySetting) {
    ScopedEnvVar memcpy_enabled("MC_STORE_MEMCPY", "1");

    TransferEngine engine(false);
    ASSERT_EQ(
        engine.init("P2PHANDSHAKE", "127.0.0.1:30995", "127.0.0.1", 30995), 0);

    std::vector<char> source(512, 'A');
    std::vector<char> destination(source.size(), 0);
    MemoryDescriptor memory;
    memory.buffer_descriptor.buffer_address_ =
        reinterpret_cast<uintptr_t>(destination.data());
    memory.buffer_descriptor.size_ = destination.size();
    memory.buffer_descriptor.transport_endpoint_ = engine.getLocalIpAndPort();
    memory.buffer_descriptor.protocol_ = "tcp";
    Replica::Descriptor replica;
    replica.descriptor_variant = memory;
    replica.status = ReplicaStatus::PROCESSING;

    std::shared_ptr<StorageBackend> storage_backend;
    {
        TransferSubmitter submitter(engine, storage_backend,
                                    engine.getLocalIpAndPort());
        std::vector<std::vector<Slice>> slices{
            {{source.data(), source.size()}}};
        auto future =
            submitter.submit_batch({replica}, slices, TransferRequest::WRITE);

        ASSERT_TRUE(future);
        EXPECT_EQ(future->strategy(), TransferStrategy::LOCAL_MEMCPY);
        EXPECT_EQ(future->get(), ErrorCode::OK);
    }
    EXPECT_EQ(destination, source);
    EXPECT_EQ(engine.freeEngine(), 0);
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
