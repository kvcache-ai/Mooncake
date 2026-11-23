#include <gtest/gtest.h>
#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <vector>

#ifdef USE_CUDA
#include <cuda.h>
#include <cuda_runtime.h>
#endif

#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include "dummy_client.h"
#include "real_client.h"
#include "uds_transport.h"

namespace mooncake {
using namespace std::chrono_literals;

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
                EXPECT_EQ(finish_register_.get_future().wait_for(5s),
                          std::future_status::ready);
            }
            {
                std::lock_guard lock(events_mutex_);
                events_.push_back(type);
            }
            int status = 0;
            if (type == IPC_CUDA_UNMAP_CLIENT && daemon_) {
                if (!draining_.exchange(true)) drain_requested_.set_value();
                auto drained = daemon_->unmap_shm_internal(client_.client_id_);
                if (!drained) status = toInt(drained.error());
            }
            if (delayed || drop_reply_.exchange(false)) return;
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

    // Drop an actual TCP connection after admission. The worker keeps the
    // same shared mapping lock as the production range handler until its
    // delayed device access finishes. The UDS handler calls the real drain.
    void LoseRangeReply(coro_rpc::context<std::vector<int>> reply,
                        const std::vector<std::string>& keys,
                        const std::vector<std::vector<uint64_t>>& addresses,
                        const std::vector<std::vector<size_t>>& sizes,
                        const std::vector<std::vector<size_t>>& offsets,
                        int32_t, const UUID& id, bool put) {
        ASSERT_EQ(id, client_.client_id_);
        ASSERT_EQ(keys, std::vector<std::string>{"key"});
        ASSERT_EQ(sizes, (std::vector<std::vector<size_t>>{{1}}));
        ASSERT_EQ(offsets, (std::vector<std::vector<size_t>>{{0}}));
        ASSERT_EQ(addresses,
                  (std::vector<std::vector<uint64_t>>{{address_.load()}}));
        auto* ptr = reinterpret_cast<void*>(addresses.front().front());
        worker_ =
            std::async(std::launch::async, [this, reply, ptr, put]() mutable {
                std::shared_lock lock(daemon_->dummy_client_mutex_);
                reply.get_context_info()->close();
                ASSERT_EQ(finish_io_.get_future().wait_for(10s),
                          std::future_status::ready);
                if (put) {
                    CopyByte(&consumed_, ptr);
                } else {
                    const char incoming = 'b';
                    CopyByte(ptr, &incoming);
                }
            });
    }

    std::vector<tl::expected<int64_t, ErrorCode>> BusyBatch(
        const std::vector<std::string>&, const std::vector<uint64_t>& addresses,
        const std::vector<size_t>&, int32_t, const UUID&) {
        EXPECT_EQ(addresses, std::vector<uint64_t>{address_.load()});
        return {tl::unexpected(ErrorCode::CUDA_BUFFER_IN_USE)};
    }

    void StartDataRpc(coro_rpc::coro_rpc_server& rpc) {
        rpc.register_handler<&DummyClientCudaLifecycleTest::BusyBatch>(
            this,
            coro_rpc::func_id<&RealClient::batch_get_into_dummy_helper>());
        rpc.register_handler<&DummyClientCudaLifecycleTest::LoseRangeReply>(
            this,
            coro_rpc::func_id<&RealClient::execute_dummy_session_ranges>());
        ASSERT_FALSE(rpc.async_start().hasResult());
        client_.client_accessor_.GetOrCreateClientPool(
            "127.0.0.1:" + std::to_string(rpc.port()));
    }

    static void CopyByte(void* out, const void* in) {
#ifdef USE_CUDA
        ASSERT_EQ(cudaMemcpy(out, in, 1, cudaMemcpyDefault), cudaSuccess);
#else
        std::memcpy(out, in, 1);
#endif
    }

    void CheckLostRangeReply(bool put, bool lose_drain_ack = false) {
#ifdef USE_CUDA
        int devices = 0;
        if (cudaGetDeviceCount(&devices) != cudaSuccess || devices == 0)
            GTEST_SKIP() << "No CUDA device";
        void* buffer = nullptr;
        ASSERT_EQ(cudaMalloc(&buffer, kSize), cudaSuccess);
        std::unique_ptr<void, decltype(&cudaFree)> gpu(buffer, cudaFree);
#else
        char allocation[kSize];
        void* buffer = allocation;
#endif
        SeedRegistration(buffer, 1);
        daemon_ = std::make_unique<RealClient>();
        daemon_->shm_contexts_.try_emplace(client_.client_id_);
        const char original = 'a', reused = 'c';
        CopyByte(buffer, &original);
        coro_rpc::coro_rpc_server rpc(2, 0, "127.0.0.1");
        StartDataRpc(rpc);
        drop_reply_ = lose_drain_ack;
        auto transfer = std::async(std::launch::async, [&] {
            return put ? client_.batch_put_from_multi_buffer_ranges(
                             {"key"}, {{buffer}}, {{1}}, {{0}})
                       : client_.batch_get_into_multi_buffer_ranges(
                             {"key"}, {{buffer}}, {{1}}, {{0}});
        });
        EXPECT_EQ(drain_requested_.get_future().wait_for(5s),
                  std::future_status::ready);
        const bool returned_early =
            transfer.wait_for(0ms) == std::future_status::ready;
        EXPECT_FALSE(returned_early);
        if (returned_early) CopyByte(buffer, &reused);
        finish_io_.set_value();
        const int expected =
            toInt(lose_drain_ack ? ErrorCode::CUDA_BUFFER_IN_USE
                                 : ErrorCode::RPC_FAIL);
        EXPECT_EQ(transfer.get(), std::vector<int>{expected});
        if (lose_drain_ack) {
            EXPECT_TRUE(client_.cuda_transfer_pending());
            drop_reply_ = true;
            EXPECT_EQ(client_.tearDownAll(),
                      toInt(ErrorCode::CUDA_BUFFER_IN_USE));
            EXPECT_TRUE(client_.cuda_transfer_pending());
            EXPECT_EQ(client_.tearDownAll(), 0);
        }
        EXPECT_FALSE(client_.cuda_transfer_pending());
        EXPECT_EQ(daemon_->shm_contexts_.count(client_.client_id_), 0u);
        EXPECT_EQ(client_.setup_dummy(0, 0, "", ""),
                  toInt(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS));
        char current = 0;
        if (!returned_early) {
            if (!put) {
                CopyByte(&current, buffer);
                EXPECT_EQ(current, 'b');
            }
            CopyByte(buffer, &reused);
        }
        if (worker_.valid()) worker_.get();
        CopyByte(&current, buffer);
        EXPECT_EQ(current, reused);
        if (put) {
            EXPECT_EQ(consumed_, original);
        }
    }

    std::unique_ptr<RealClient> daemon_;
    std::atomic<bool> draining_{false};
    std::promise<void> drain_requested_, finish_io_;
    char consumed_ = 0;
    std::future<void> worker_;
    std::mutex events_mutex_;
    std::vector<IpcRequestType> events_;
    std::atomic<uintptr_t> address_{0};
    std::atomic<bool> drop_reply_{false};
    std::atomic<bool> delay_register_{false};
    std::promise<void> finish_register_;
    std::unique_ptr<UdsAcceptor> server_;
    DummyClient client_{true};
};

TEST_F(DummyClientCudaLifecycleTest, LostPutReplyDrainsBeforeSourceReuse) {
    CheckLostRangeReply(true);
}

TEST_F(DummyClientCudaLifecycleTest, LostGetReplyDrainsBeforeDestinationReuse) {
    CheckLostRangeReply(false);
}

TEST_F(DummyClientCudaLifecycleTest, LostDrainAcknowledgementRequiresClose) {
    CheckLostRangeReply(false, true);
}

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

TEST_F(DummyClientCudaLifecycleTest, BusyBatchReplyRequiresConfirmedDrain) {
    int buffer;
    SeedRegistration(&buffer, 1);
    coro_rpc::coro_rpc_server rpc(2, 0, "127.0.0.1");
    StartDataRpc(rpc);
    drop_reply_ = true;
    EXPECT_EQ(client_.batch_get_into({"key"}, {&buffer}, {1}),
              std::vector<int64_t>{toInt(ErrorCode::CUDA_BUFFER_IN_USE)});
    EXPECT_TRUE(client_.cuda_transfer_pending());
    EXPECT_EQ(client_.tearDownAll(), 0);
    EXPECT_FALSE(client_.cuda_transfer_pending());
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
    EXPECT_EQ(closed.wait_for(100ms), std::future_status::timeout);
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
