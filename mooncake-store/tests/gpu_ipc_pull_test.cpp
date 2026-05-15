// gpu_ipc_pull_test.cpp
// Unit tests for GPU IPC pull feature (same-node GPU->CPU optimization)

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <memory>
#include <thread>
#include <vector>

#include "transfer_task.h"
#include "rpc_types.h"
#include "allocator.h"
#include "gpu_ipc_utils.h"
#include "gpu_staging_utils.h"

#ifdef USE_CUDA
#include <cuda_runtime.h>
#include "common/serialization.h"
#endif

namespace mooncake {

class GpuIpcPullTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("GpuIpcPullTest");
        FLAGS_logtostderr = 1;
    }
    void TearDown() override { google::ShutdownGoogleLogging(); }
};

// ============================================================
// 1. GpuIpcOperationState (no GPU required)
// ============================================================

TEST_F(GpuIpcPullTest, OperationStateBasic) {
    auto state = std::make_shared<GpuIpcOperationState>();

    // Initially not completed
    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(state->get_strategy(), TransferStrategy::GPU_IPC);

    // Set completed with success
    state->set_completed(ErrorCode::OK);
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
}

TEST_F(GpuIpcPullTest, OperationStateError) {
    auto state = std::make_shared<GpuIpcOperationState>();

    state->set_completed(ErrorCode::TRANSFER_FAIL);
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::TRANSFER_FAIL);
}

TEST_F(GpuIpcPullTest, OperationStateWaitCompletion) {
    auto state = std::make_shared<GpuIpcOperationState>();

    // Complete from another thread
    std::thread t([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        state->set_completed(ErrorCode::OK);
    });

    state->wait_for_completion();
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
    t.join();
}

TEST_F(GpuIpcPullTest, TransferFutureWithGpuIpcPull) {
    auto state = std::make_shared<GpuIpcOperationState>();
    state->set_completed(ErrorCode::OK);

    TransferFuture future(state);
    EXPECT_EQ(future.strategy(), TransferStrategy::GPU_IPC);
    EXPECT_EQ(future.wait(), ErrorCode::OK);
}

// ============================================================
// 2. TransferStrategy enum
// ============================================================

TEST_F(GpuIpcPullTest, TransferStrategyEnum) {
    // Verify enum values don't collide
    EXPECT_EQ(static_cast<int>(TransferStrategy::LOCAL_MEMCPY), 0);
    EXPECT_EQ(static_cast<int>(TransferStrategy::TRANSFER_ENGINE), 1);
    EXPECT_EQ(static_cast<int>(TransferStrategy::FILE_READ), 2);
    EXPECT_EQ(static_cast<int>(TransferStrategy::GPU_IPC), 4);
    EXPECT_EQ(static_cast<int>(TransferStrategy::EMPTY), 3);

    // Verify operator<< works
    std::ostringstream oss;
    oss << TransferStrategy::GPU_IPC;
    EXPECT_EQ(oss.str(), "GPU_IPC");

    std::ostringstream oss2;
    oss2 << TransferStrategy::EMPTY;
    EXPECT_EQ(oss2.str(), "EMPTY");
}

// ============================================================
// 3. RPC message structs
// ============================================================

TEST_F(GpuIpcPullTest, GpuIpcTransferRequestD2H) {
    GpuIpcTransferRequest req;
    req.direction = GpuIpcDirection::DEVICE_TO_HOST;
    req.gpu_ipc_handle = "test_handle_data";
    req.gpu_device_id = 0;
    req.gpu_offsets = {0, 4096};
    req.cpu_addrs = {0x1000, 0x2000};
    req.sizes = {4096, 4096};

    EXPECT_EQ(req.direction, GpuIpcDirection::DEVICE_TO_HOST);
    EXPECT_EQ(req.gpu_ipc_handle, "test_handle_data");
    EXPECT_EQ(req.gpu_device_id, 0);
    EXPECT_EQ(req.gpu_offsets.size(), 2u);
    EXPECT_EQ(req.cpu_addrs.size(), 2u);
    EXPECT_EQ(req.sizes.size(), 2u);
}

TEST_F(GpuIpcPullTest, GpuIpcTransferResponseDefault) {
    GpuIpcTransferResponse resp;
    EXPECT_EQ(resp.overall_status, 0);
    EXPECT_TRUE(resp.per_slice_status.empty());
}

// ============================================================
// 4. AllocatedBuffer::Descriptor transport_endpoint_
// ============================================================

TEST_F(GpuIpcPullTest, DescriptorRpcEndpoint) {
    AllocatedBuffer::Descriptor desc;
    desc.size_ = 4096;
    desc.buffer_address_ = 0x7f000000;
    desc.protocol_ = "tcp";
    desc.transport_endpoint_ = "192.168.1.1:12345";
    // rpc_endpoint_ field removed — not part of current API

    EXPECT_EQ(desc.transport_endpoint_, "192.168.1.1:12345");

    // Empty transport_endpoint_ is the default
    AllocatedBuffer::Descriptor desc2;
    EXPECT_TRUE(desc2.transport_endpoint_.empty());
}

// ============================================================
// 5. gpu_ipc_utils.h (compile-time checks + GPU runtime tests)
// ============================================================

TEST_F(GpuIpcPullTest, IpcHandleSize) {
#if defined(USE_CUDA) || defined(USE_MUSA) || defined(USE_MACA)
    EXPECT_EQ(gpu_ipc::IpcHandleSize(), sizeof(cudaIpcMemHandle_t));
    EXPECT_EQ(gpu_ipc::IpcHandleSize(), 64u);
#elif defined(USE_HIP)
    EXPECT_EQ(gpu_ipc::IpcHandleSize(), sizeof(hipIpcMemHandle_t));
#elif defined(USE_ASCEND) || defined(USE_UBSHMEM)
    EXPECT_EQ(gpu_ipc::IpcHandleSize(), 65u);
#else
    EXPECT_EQ(gpu_ipc::IpcHandleSize(), 0u);
#endif
}

TEST_F(GpuIpcPullTest, OpenIpcHandleInvalidSize) {
    // Wrong size should fail gracefully
    char dummy[4] = {0};
    void* ptr = nullptr;
    EXPECT_FALSE(gpu_ipc::OpenIpcHandle(dummy, sizeof(dummy), &ptr));
    EXPECT_EQ(ptr, nullptr);
}

TEST_F(GpuIpcPullTest, CloseIpcHandleNull) {
    // Should not crash on nullptr
    gpu_ipc::CloseIpcHandle(nullptr);
}

// ============================================================
// 6. GPU-dependent tests (only run if CUDA is available)
// ============================================================

#ifdef USE_CUDA

TEST_F(GpuIpcPullTest, IsDevicePointerDetectsGpu) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    void* gpu_ptr = nullptr;
    cudaError_t err = cudaMalloc(&gpu_ptr, 4096);
    ASSERT_EQ(err, cudaSuccess);
    ASSERT_NE(gpu_ptr, nullptr);

    int dev_id = -1;
    EXPECT_TRUE(gpu_staging::IsDevicePointer(gpu_ptr, &dev_id));
    EXPECT_GE(dev_id, 0);

    // CPU pointer should return false
    char cpu_buf[64];
    EXPECT_FALSE(gpu_staging::IsDevicePointer(cpu_buf, nullptr));

    cudaFree(gpu_ptr);
}

TEST_F(GpuIpcPullTest, IpcHandleRoundTrip) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    // Allocate GPU memory
    void* gpu_ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_ptr, 4096), cudaSuccess);

    // Write known pattern
    std::vector<char> src(4096, 0x42);
    ASSERT_EQ(cudaMemcpy(gpu_ptr, src.data(), 4096, cudaMemcpyHostToDevice),
              cudaSuccess);

    // Get IPC handle
    cudaIpcMemHandle_t handle;
    ASSERT_EQ(cudaIpcGetMemHandle(&handle, gpu_ptr), cudaSuccess);

    // Serialize
    std::string serialized =
        serializeBinaryData(&handle, sizeof(cudaIpcMemHandle_t));

    // Deserialize
    std::vector<uint8_t> buf;
    deserializeBinaryData(serialized, buf);
    ASSERT_EQ(buf.size(), sizeof(cudaIpcMemHandle_t));

    // Open via cross-platform API (same process — this is for unit test only,
    // in production IPC handles are opened in a different process)
    // Note: cudaIpcOpenMemHandle in same process returns cudaErrorInvalidValue
    // on most drivers. We test the serialization round-trip instead.
    cudaIpcMemHandle_t handle2;
    std::memcpy(&handle2, buf.data(), sizeof(handle2));
    EXPECT_EQ(std::memcmp(&handle, &handle2, sizeof(handle)), 0);

    cudaFree(gpu_ptr);
}

TEST_F(GpuIpcPullTest, CopyDeviceToHostWorks) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    void* gpu_ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_ptr, 4096), cudaSuccess);

    // Write pattern to GPU
    std::vector<char> src(4096, 0xAB);
    ASSERT_EQ(cudaMemcpy(gpu_ptr, src.data(), 4096, cudaMemcpyHostToDevice),
              cudaSuccess);

    // Read back via cross-platform API
    std::vector<char> dst(4096, 0);
    EXPECT_TRUE(gpu_staging::CopyDeviceToHost(dst.data(), gpu_ptr, 4096));

    // Verify
    EXPECT_EQ(dst, src);

    cudaFree(gpu_ptr);
}

TEST_F(GpuIpcPullTest, SetDeviceWorks) {
    int device_count = 0;
    cudaGetDeviceCount(&device_count);
    if (device_count == 0) {
        GTEST_SKIP() << "No CUDA device available";
    }

    // Should not crash
    gpu_staging::SetDevice(0);

    int current_device = -1;
    cudaGetDevice(&current_device);
    EXPECT_EQ(current_device, 0);
}

#endif  // USE_CUDA

// ============================================================
// 7. New unified GPU IPC Transfer types (no GPU required)
// ============================================================

TEST_F(GpuIpcPullTest, GpuIpcTransferRequestFields) {
    GpuIpcTransferRequest req;
    req.direction = GpuIpcDirection::DEVICE_TO_HOST;
    req.gpu_ipc_handle = "test_handle";
    req.gpu_device_id = 1;
    req.keys = {"key0", "key1"};
    req.gpu_offsets = {0, 4096};
    req.cpu_addrs = {0x1000, 0x2000};
    req.sizes = {4096, 4096};

    EXPECT_EQ(req.direction, GpuIpcDirection::DEVICE_TO_HOST);
    EXPECT_EQ(req.gpu_device_id, 1);
    EXPECT_EQ(req.keys.size(), 2u);
    EXPECT_EQ(req.gpu_offsets.size(), 2u);
    EXPECT_EQ(req.cpu_addrs.size(), 2u);
    EXPECT_EQ(req.sizes.size(), 2u);

    req.direction = GpuIpcDirection::HOST_TO_DEVICE;
    EXPECT_EQ(req.direction, GpuIpcDirection::HOST_TO_DEVICE);
}

TEST_F(GpuIpcPullTest, GpuIpcTransferResponsePerSlice) {
    GpuIpcTransferResponse resp;
    EXPECT_EQ(resp.overall_status, 0);
    EXPECT_TRUE(resp.per_slice_status.empty());

    resp.per_slice_status = {0, 0, -1, 0};
    resp.overall_status = -1;
    EXPECT_EQ(resp.per_slice_status.size(), 4u);
    EXPECT_EQ(resp.per_slice_status[2], -1);
}

TEST_F(GpuIpcPullTest, GpuIpcOperationStateSyncFallback) {
    // Test the synchronous (non-coro) fallback path — no awaiter set
    auto state = std::make_shared<GpuIpcOperationState>();

    EXPECT_FALSE(state->is_completed());
    EXPECT_EQ(state->get_strategy(), TransferStrategy::GPU_IPC);

    std::thread t([&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(30));
        state->set_completed(ErrorCode::OK);
    });

    state->wait_for_completion();
    EXPECT_TRUE(state->is_completed());
    EXPECT_EQ(state->get_result(), ErrorCode::OK);
    t.join();
}

// ============================================================
// 8. GPU-dependent: CopyHostToDevice + async + NUMA tests
// ============================================================

#ifdef USE_CUDA

TEST_F(GpuIpcPullTest, CopyHostToDeviceWorks) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    void* gpu_ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_ptr, 4096), cudaSuccess);

    std::vector<char> src(4096, 0xCD);
    EXPECT_TRUE(gpu_staging::CopyHostToDevice(gpu_ptr, src.data(), 4096));

    std::vector<char> dst(4096, 0);
    ASSERT_EQ(cudaMemcpy(dst.data(), gpu_ptr, 4096, cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(dst, src);
    cudaFree(gpu_ptr);
}

TEST_F(GpuIpcPullTest, ScopedGpuStreamBasic) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    gpu_staging::SetDevice(0);
    {
        gpu_staging::ScopedGpuStream stream(0);
        EXPECT_TRUE(stream.valid());
        EXPECT_TRUE(stream.sync());
    }
}

TEST_F(GpuIpcPullTest, AsyncCopyD2HRoundTrip) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    void* gpu_ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_ptr, 4096), cudaSuccess);

    std::vector<char> src(4096, 0xEF);
    ASSERT_EQ(cudaMemcpy(gpu_ptr, src.data(), 4096, cudaMemcpyHostToDevice),
              cudaSuccess);

    std::vector<char> dst(4096, 0);
    {
        gpu_staging::ScopedGpuStream stream(0);
        ASSERT_TRUE(stream.valid());
        EXPECT_TRUE(gpu_staging::CopyDeviceToHostAsync(dst.data(), gpu_ptr,
                                                       4096, stream.get()));
        EXPECT_TRUE(stream.sync());
    }
    EXPECT_EQ(dst, src);
    cudaFree(gpu_ptr);
}

TEST_F(GpuIpcPullTest, AsyncCopyH2DRoundTrip) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    void* gpu_ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_ptr, 4096), cudaSuccess);

    std::vector<char> src(4096, 0xBB);
    {
        gpu_staging::ScopedGpuStream stream(0);
        ASSERT_TRUE(stream.valid());
        EXPECT_TRUE(gpu_staging::CopyHostToDeviceAsync(gpu_ptr, src.data(),
                                                       4096, stream.get()));
        EXPECT_TRUE(stream.sync());
    }

    std::vector<char> dst(4096, 0);
    ASSERT_EQ(cudaMemcpy(dst.data(), gpu_ptr, 4096, cudaMemcpyDeviceToHost),
              cudaSuccess);
    EXPECT_EQ(dst, src);
    cudaFree(gpu_ptr);
}

// GetDeviceNumaNode test removed — function not in gpu_staging API

TEST_F(GpuIpcPullTest, IsDevicePointerDefaultParam) {
    int device_count = 0;
    if (cudaGetDeviceCount(&device_count) != cudaSuccess || device_count == 0) {
        GTEST_SKIP() << "No CUDA device available";
    }
    void* gpu_ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&gpu_ptr, 4096), cudaSuccess);

    // Default param (no out_device_id)
    EXPECT_TRUE(gpu_staging::IsDevicePointer(gpu_ptr));

    char cpu_buf[64];
    EXPECT_FALSE(gpu_staging::IsDevicePointer(cpu_buf));
    cudaFree(gpu_ptr);
}

#endif  // USE_CUDA

}  // namespace mooncake
