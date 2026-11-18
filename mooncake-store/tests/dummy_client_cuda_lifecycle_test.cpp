#include <gtest/gtest.h>
#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

#ifdef USE_CUDA
#include <cuda.h>
#include <cuda_runtime.h>
#endif

#include "dummy_client.h"
#include "uds_transport.h"

namespace mooncake {

class DummyClientCudaLifecycleTest : public ::testing::Test {
   protected:
    static constexpr size_t kSize = 4096;

    void SetUp() override {
        client_.ipc_socket_path_ =
            "@dummy_cuda_lifecycle_" + std::to_string(getpid());
        server_ = std::make_unique<UdsAcceptor>(client_.ipc_socket_path_);
        server_->registerHandler([this](UdsConnection& connection) {
            IpcRequestType type;
            ASSERT_EQ(connection.recvRaw(&type, sizeof(type)), 0);
            CudaRegisterRequest request{};
            if (type == IPC_CUDA_REGISTER) {
                int fd = connection.recvFd(&request, sizeof(request));
                ASSERT_GE(fd, 0);
                close(fd);
            } else {
                ASSERT_TRUE(type == IPC_CUDA_UNREGISTER ||
                            type == IPC_CUDA_UNMAP_CLIENT);
                ASSERT_EQ(connection.recvRaw(&request, sizeof(request)), 0);
            }
            EXPECT_EQ(request.client_id_first, client_.client_id_.first);
            EXPECT_EQ(request.client_id_second, client_.client_id_.second);
            if (type != IPC_CUDA_UNMAP_CLIENT) {
                EXPECT_EQ(request.dummy_addr, address_.load());
                EXPECT_EQ(request.handle.size, kSize);
            }
            // Lose the reply before committing the MR, as can happen when
            // the requester times out during a slow registration.
            const bool delayed =
                type == IPC_CUDA_REGISTER && delay_register_.exchange(false);
            if (delayed) {
                connection.close();
                EXPECT_EQ(finish_register_.get_future().wait_for(
                              std::chrono::seconds(5)),
                          std::future_status::ready);
            }
            {
                std::lock_guard lock(events_mutex_);
                events_.push_back(type);
            }
            if (delayed || drop_reply_.exchange(false)) return;
            int status = 0;
            EXPECT_EQ(connection.sendRaw(&status, sizeof(status)), 0);
        });
        ASSERT_TRUE(server_->start().has_value());
        client_.connected_.store(true);
    }

    void TearDown() override {
        // The fixture has no RPC server. Leave the UDS server available for
        // normal destruction if a failed assertion left a CUDA record behind.
        client_.connected_.store(false);
    }

    void SeedRegistration(void* ptr, size_t references) {
        address_ = reinterpret_cast<uintptr_t>(ptr);
        CudaIpcBufferHandle handle{};
        handle.size = kSize;
        using State = DummyClient::CudaRegistrationState;
        const auto state =
            references ? State::kRegistered : State::kUnconfirmed;
        client_.registered_cuda_buffers_.emplace(
            address_.load(), DummyClient::CudaBufferRegistration{
                                 kSize, handle, references, state});
    }

    int SendRegisterWithTestFd(void* ptr, int fd) {
        auto base = reinterpret_cast<uintptr_t>(ptr);
        const auto& handle = client_.registered_cuda_buffers_.at(base).handle;
        auto result = client_.cuda_buffer_ipc_request(IPC_CUDA_REGISTER, base,
                                                      handle, fd);
        return result.status;
    }

    bool CanTransfer(void* ptr) {
        return client_.registered_cuda_buffer_remaining(ptr).has_value();
    }

    int Replay() { return client_.reregister_cuda_buffers(); }

    std::vector<IpcRequestType> Events() {
        std::lock_guard lock(events_mutex_);
        return events_;
    }

    std::mutex events_mutex_;
    std::vector<IpcRequestType> events_;
    std::atomic<uintptr_t> address_{0};
    std::atomic<bool> drop_reply_{false};
    std::atomic<bool> delay_register_{false};
    std::promise<void> finish_register_;
    std::unique_ptr<UdsAcceptor> server_;
    DummyClient client_{true};
};

TEST_F(DummyClientCudaLifecycleTest,
       LostFinalUnregisterIsRetriedWithoutReplay) {
    int placeholder;
    SeedRegistration(&placeholder, 2);
    ASSERT_EQ(client_.unregister_buffer(&placeholder), 0);
    EXPECT_TRUE(CanTransfer(&placeholder));
    EXPECT_TRUE(Events().empty());

    drop_reply_ = true;
    EXPECT_NE(client_.unregister_buffer(&placeholder), 0);
    EXPECT_FALSE(CanTransfer(&placeholder));
    EXPECT_EQ(Replay(), 0);
    EXPECT_EQ(client_.unregister_buffer(&placeholder), 0);
    EXPECT_FALSE(CanTransfer(&placeholder));
    EXPECT_EQ(Events(), (std::vector<IpcRequestType>{IPC_CUDA_UNREGISTER,
                                                     IPC_CUDA_UNREGISTER}));
}

TEST_F(DummyClientCudaLifecycleTest, LostBulkUnmapIsRetriedOnTheOrderedSocket) {
    int placeholder;
    SeedRegistration(&placeholder, 1);
    drop_reply_ = true;
    EXPECT_NE(client_.tearDownAll(), 0);
    EXPECT_EQ(Replay(), 0);
    EXPECT_EQ(client_.tearDownAll(), 0);
    EXPECT_EQ(Events(), (std::vector<IpcRequestType>{IPC_CUDA_UNMAP_CLIENT,
                                                     IPC_CUDA_UNMAP_CLIENT}));
}

TEST_F(DummyClientCudaLifecycleTest, CloseWaitsForPendingRegisterToFinish) {
    int placeholder;
    SeedRegistration(&placeholder, 0);
    // The fake daemon only checks descriptor delivery. This exercises the
    // production socket on a CPU-only host.
    int fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    ASSERT_GE(fd, 0);
    delay_register_ = true;
    const int status = SendRegisterWithTestFd(&placeholder, fd);
    close(fd);
    EXPECT_NE(status, 0);
    EXPECT_TRUE(Events().empty());

    std::promise<void> closing;
    auto started = closing.get_future();
    auto closed = std::async(std::launch::async, [&] {
        closing.set_value();
        return client_.tearDownAll();
    });
    started.wait();
    EXPECT_EQ(closed.wait_for(std::chrono::milliseconds(100)),
              std::future_status::timeout);
    finish_register_.set_value();
    EXPECT_EQ(closed.get(), 0);
    EXPECT_EQ(Events(), (std::vector<IpcRequestType>{IPC_CUDA_REGISTER,
                                                     IPC_CUDA_UNMAP_CLIENT}));
}

#ifdef USE_CUDA
TEST_F(DummyClientCudaLifecycleTest, OnlyAcknowledgedRegistrationsAreReplayed) {
    int count = 0;
    if (cudaGetDeviceCount(&count) != cudaSuccess || count == 0)
        GTEST_SKIP() << "No CUDA device";
    CUdevice device;
    int dmabuf_supported = 0;
    ASSERT_EQ(cuDeviceGet(&device, 0), CUDA_SUCCESS);
    ASSERT_EQ(
        cuDeviceGetAttribute(&dmabuf_supported,
                             CU_DEVICE_ATTRIBUTE_DMA_BUF_SUPPORTED, device),
        CUDA_SUCCESS);
    if (!dmabuf_supported) GTEST_SKIP() << "CUDA device cannot export DMA-BUFs";
    ASSERT_EQ(cudaSetDevice(0), cudaSuccess);
    void* ptr = nullptr;
    ASSERT_EQ(cudaMalloc(&ptr, kSize), cudaSuccess);
    std::unique_ptr<void, decltype(&cudaFree)> allocation(ptr, cudaFree);
    address_ = reinterpret_cast<uintptr_t>(ptr);

    drop_reply_ = true;
    EXPECT_NE(client_.register_buffer(ptr, kSize), 0);
    EXPECT_FALSE(CanTransfer(ptr));
    EXPECT_EQ(Replay(), 0);
    EXPECT_FALSE(CanTransfer(ptr));
    EXPECT_EQ(Events(), (std::vector<IpcRequestType>{IPC_CUDA_REGISTER}));

    ASSERT_EQ(client_.register_buffer(ptr, kSize), 0);
    ASSERT_EQ(client_.register_buffer(ptr, kSize), 0);
    ASSERT_EQ(Replay(), 0);
    EXPECT_TRUE(CanTransfer(ptr));
    ASSERT_EQ(client_.unregister_buffer(ptr), 0);
    EXPECT_TRUE(CanTransfer(ptr));
    ASSERT_EQ(client_.unregister_buffer(ptr), 0);
    EXPECT_FALSE(CanTransfer(ptr));
    EXPECT_EQ(Events(),
              (std::vector<IpcRequestType>{
                  IPC_CUDA_REGISTER, IPC_CUDA_UNREGISTER, IPC_CUDA_REGISTER,
                  IPC_CUDA_REGISTER, IPC_CUDA_UNREGISTER}));
}
#endif

}  // namespace mooncake
