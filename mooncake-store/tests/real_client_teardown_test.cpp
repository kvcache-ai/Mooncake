#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "real_client.h"
#include "test_server_helpers.h"
#include "transfer_engine_impl.h"
#include "transport/shm_transport/shm_transport.h"
#include "uds_transport.h"

namespace mooncake {
class TransferEngineImplTestPeer {
   public:
    // The Store capability check queries the engine mode and installed names;
    // neither a verbs device nor a TENT backend is needed for that query.
    static std::shared_ptr<TransferEngine> makeEngine(bool use_tent,
                                                      const char *name) {
        auto engine = std::make_shared<TransferEngine>(false);
        engine->use_tent_ = use_tent;
        if (!use_tent) {
            engine->impl_ = std::make_shared<TransferEngineImpl>();
            auto &impl = *engine->impl_;
            impl.multi_transports_ = std::make_shared<MultiTransport>(
                nullptr, impl.local_server_name_);
            impl.multi_transports_->transport_map_[name] =
                std::make_shared<ShmTransport>();
        }
        return engine;
    }
};
}  // namespace mooncake

namespace mooncake::test {
namespace {

struct ShutdownProbe {
    std::promise<void> client_destructor_started;
    std::atomic<bool> worker_saw_mapping{false};
};

class TeardownClient : public Client {
   public:
    explicit TeardownClient(std::shared_ptr<ShutdownProbe> probe)
        : Client("localhost", "", "tcp"), probe_(std::move(probe)) {}

    ~TeardownClient() override {
        probe_->client_destructor_started.set_value();
    }

   private:
    std::shared_ptr<ShutdownProbe> probe_;
};

bool IsMapped(void *address) {
    unsigned char residency = 0;
    return mincore(address, static_cast<size_t>(sysconf(_SC_PAGESIZE)),
                   &residency) == 0;
}

}  // namespace

class RealClientTeardownTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                      .set_rpc_port(0)
                                      .set_http_metrics_port(0)
                                      .set_http_metadata_port(0)
                                      .build()));

        // Accept the real master handshake but leave BatchPutRevoke
        // unregistered, producing an immediate RPC_FAIL without a timeout.
        const int port = getFreeTcpPort();
        failing_master_ = std::make_unique<coro_rpc::coro_rpc_server>(
            1, port, "127.0.0.1", std::chrono::seconds(0), true);
        failing_master_->register_handler<&WrappedMasterService::ServiceReady>(
            master_.service().get());
        ASSERT_FALSE(failing_master_->async_start().hasResult());

        probe_ = std::make_shared<ShutdownProbe>();
        client_ = std::make_shared<RealClient>();
        client_->client_ = std::make_shared<TeardownClient>(probe_);
        ASSERT_EQ(client_->client_->master_client_.Connect(
                      "127.0.0.1:" + std::to_string(port)),
                  ErrorCode::OK);
        client_->real_sessions_.put_sessions.emplace(
            kPendingKey, RealClient::PutSessionEntry{});
        ASSERT_TRUE(client_->claim_session_put_owner(client_->real_sessions_,
                                                     kPendingKey));

        // Exercise the production mmap owner, using its full release alignment
        // even when huge pages are enabled in the test environment.
        mapping_size_ = std::max(static_cast<size_t>(sysconf(_SC_PAGESIZE)),
                                 get_hugepage_size_from_env());
        mapping_ = mmap(nullptr, mapping_size_, PROT_READ | PROT_WRITE,
                        MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        ASSERT_NE(mapping_, MAP_FAILED);
        client_->hugepage_segment_ptrs_.emplace_back(
            mapping_, RealClient::HugepageSegmentDeleter{mapping_size_});
    }

    void TearDown() override {
        client_.reset();
        if (failing_master_) failing_master_->stop();
        master_.Stop();
        if (!storage_path_.empty()) {
            std::filesystem::remove_all(storage_path_);
        }
    }

    void AddFileStorage() {
        char path[] = "/tmp/mooncake_teardown_XXXXXX";
        ASSERT_NE(mkdtemp(path), nullptr);
        storage_path_ = path;
        FileStorageConfig config;
        config.storage_backend_type = StorageBackendType::kFilePerKey;
        config.storage_filepath = storage_path_;
        config.local_buffer_size = 4096;
        config.total_keys_limit = 16;
        config.total_size_limit = 4096;
        // Initialization is unnecessary: the constructor already owns Client.
        client_->file_storage_ =
            std::make_shared<FileStorage>(config, client_->client_, "");
    }

    void CheckDestructorOrdering() {
        std::promise<void> release_worker;
        auto release = release_worker.get_future().share();
        auto arrived = std::make_shared<std::promise<void>>();
        auto worker_started = arrived->get_future();
        auto client_destructor_started =
            probe_->client_destructor_started.get_future();
        client_->client_->task_thread_pool_.enqueue(
            [arrived, release, probe = probe_, mapping = mapping_] {
                arrived->set_value();
                release.wait();
                probe->worker_saw_mapping.store(IsMapped(mapping));
            });
        ASSERT_EQ(worker_started.wait_for(std::chrono::seconds(10)),
                  std::future_status::ready);

        std::thread destroyer(
            [client = std::move(client_)]() mutable { client.reset(); });
        EXPECT_EQ(client_destructor_started.wait_for(std::chrono::seconds(10)),
                  std::future_status::ready);
        // Client's destructor now has to join the gated production worker.
        // mincore detects the old ordering safely, without reading freed
        // memory.
        EXPECT_TRUE(IsMapped(mapping_));
        release_worker.set_value();
        destroyer.join();

        EXPECT_TRUE(probe_->worker_saw_mapping.load());
        EXPECT_FALSE(IsMapped(mapping_));
    }

    bool HasPendingSession() const {
        return client_->real_sessions_.put_sessions.count(kPendingKey) == 1 &&
               client_->session_put_owners_.count(kPendingKey) == 1;
    }

    bool PendingStateEmpty() const {
        return client_->real_sessions_.put_sessions.empty() &&
               client_->session_put_owners_.empty();
    }

    ErrorCode ReconnectMaster() {
        return client_->client_->master_client_.Connect(
            master_.master_address());
    }

    std::weak_ptr<ClientBufferAllocator> AddCudaAllocation(
        const UUID &id, CudaIpcBufferHandle &handle) {
        handle.has_device_uuid = true;
        handle.size = 4096;
        const std::string key(handle.device_uuid.size() + handle.handle.size(),
                              '\0');
        auto &context = client_->shm_contexts_[id];
        auto &allocation = client_->cuda_allocations_[key];
        // Exercise MR bookkeeping and the production cleanup paths without
        // requiring a CUDA device or verbs provider.
        allocation = std::make_shared<RealClient::CudaRegisteredAllocation>();
        auto allocator = ClientBufferAllocator::create(mapping_, mapping_size_);
        std::weak_ptr<ClientBufferAllocator> allocator_observer = allocator;
        context.client_buffer_allocator = std::move(allocator);
        return allocator_observer;
    }

    void CheckCudaQuarantine() {
        const UUID id = generate_uuid();
        client_->ipc_socket_path_ =
            "mc_quarantined_cuda_" + std::to_string(id.first);
        ASSERT_EQ(client_->start_ipc_server(), 0);
        UdsAcceptor replacement(client_->ipc_socket_path_);
        replacement.registerHandler([](UdsConnection &) {});
        CudaIpcBufferHandle handle{};
        auto allocator = AddCudaAllocation(id, handle);
        // A second request for an allocation with an ambiguous initial
        // rollback must preserve the negative acknowledgement for this id.
        ASSERT_FALSE(client_->register_cuda_buffer_with_fd(kCudaAddress, handle,
                                                           id, -1));
        EXPECT_FALSE(client_->unmap_shm_internal(id));
        EXPECT_TRUE(allocator.expired());
        EXPECT_EQ(client_->shm_contexts_.count(id), 0U);
        EXPECT_FALSE(
            client_->unregister_cuda_buffer_internal(kCudaAddress, handle, id));
        EXPECT_FALSE(client_->unmap_shm_internal(id));
        EXPECT_TRUE(client_->unmap_shm_internal(generate_uuid()));
        ASSERT_EQ(ReconnectMaster(), ErrorCode::OK);
        EXPECT_NE(client_->tearDownAll(), 0);
        EXPECT_EQ(client_->client_, nullptr);
        EXPECT_FALSE(UdsConnector(client_->ipc_socket_path_).connect());
        EXPECT_FALSE(replacement.start());
        EXPECT_NE(client_->tearDownAll(), 0);
        EXPECT_EQ(client_->cuda_allocations_.size(), 1U);
        // There is no actual MR or CUDA import in this synthetic test.
        client_->cuda_allocations_.clear();
    }

    void CheckIpcNameDuringTransfer() {
        using namespace std::chrono_literals;
        ASSERT_EQ(ReconnectMaster(), ErrorCode::OK);
        client_->ipc_socket_path_ =
            "mc_active_cuda_" + std::to_string(generate_uuid().first);
        ASSERT_EQ(client_->start_ipc_server(), 0);
        UdsAcceptor replacement(client_->ipc_socket_path_);
        replacement.registerHandler([](UdsConnection &) {});

        // A direct range handler holds this lock from address lookup through
        // transfer completion. A replacement cannot acknowledge its drain.
        std::shared_lock transfer(client_->dummy_client_mutex_);
        auto closing = std::async(std::launch::async,
                                  [this] { return client_->tearDownAll(); });
        const auto deadline = std::chrono::steady_clock::now() + 5s;
        while (!client_->closed_.load() &&
               std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(1ms);
        EXPECT_TRUE(client_->closed_.load());
        EXPECT_EQ(closing.wait_for(100ms), std::future_status::timeout);
        EXPECT_FALSE(replacement.start());
        transfer.unlock();
        EXPECT_EQ(closing.get(), 0);
        EXPECT_TRUE(replacement.start());
    }

    void CheckCudaDeregistrationRetry() {
        const UUID id = generate_uuid();
        CudaIpcBufferHandle handle{};
        auto allocator = AddCudaAllocation(id, handle);
        auto &allocation = client_->cuda_allocations_.begin()->second;
        allocation->state = RealClient::CudaRegistrationState::kRegistered;
        client_->shm_contexts_[id].cuda_buffers.emplace(
            kCudaAddress, RealClient::CudaRegisteredBuffer{handle, allocation});
        client_->registered_buffer_sizes_[allocation->mapping.base()] = {
            handle.size};
        int attempts = 0;
        struct ResetHook {
            ~ResetHook() { RealClient::SetTeUnregisterHookForTesting(nullptr); }
        } reset_hook;
        RealClient::SetTeUnregisterHookForTesting(
            [&](void *) -> tl::expected<void, ErrorCode> {
                // The first attempt fails after removing the MR. The retry
                // confirms that TE no longer has it and clears the Store
                // record.
                return tl::unexpected(++attempts == 1
                                          ? ErrorCode::INVALID_PARAMS
                                          : ErrorCode::NOT_REGISTERED);
            });
        auto result = client_->unmap_shm_internal(id);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
        EXPECT_TRUE(allocator.expired());
        EXPECT_EQ(allocation->state,
                  RealClient::CudaRegistrationState::kUnregisterPending);
        EXPECT_TRUE(client_->unmap_shm_internal(id));
        EXPECT_TRUE(client_->unmap_shm_internal(id));
        EXPECT_EQ(attempts, 2);
        EXPECT_TRUE(client_->cuda_allocations_.empty());
    }

    void CheckUnsupportedCudaBackend(bool tent, const char *transport) {
        client_->client_->transfer_engine_ =
            TransferEngineImplTestPeer::makeEngine(tent, transport);
        const UUID id = generate_uuid();
        client_->shm_contexts_.try_emplace(id);
        CudaIpcBufferHandle handle{};
        handle.has_device_uuid = true;
        handle.size = 4096;
        const bool previous =
            std::exchange(globalConfig().rdma_data_direct, !tent);
        auto result =
            client_->register_cuda_buffer_with_fd(kCudaAddress, handle, id, -1);
        globalConfig().rdma_data_direct = previous;
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::NOT_SUPPORTED);
        EXPECT_TRUE(client_->cuda_allocations_.empty());
        EXPECT_TRUE(client_->shm_contexts_[id].cuda_buffers.empty());
        EXPECT_TRUE(
            client_->unregister_cuda_buffer_internal(kCudaAddress, handle, id));
    }

    void CheckCudaClosePending() {
        const UUID id = generate_uuid();
        CudaIpcBufferHandle handle{};
        AddCudaAllocation(id, handle);
        auto &allocation = client_->cuda_allocations_.begin()->second;
        // A failed CUDA close retains the logical registration, but its
        // Store/TE registration was already removed on the first attempt.
        allocation->state = RealClient::CudaRegistrationState::kClosePending;
        client_->shm_contexts_[id].cuda_buffers.emplace(
            kCudaAddress, RealClient::CudaRegisteredBuffer{handle, allocation});
        EXPECT_FALSE(client_->register_cuda_buffer_with_fd(kCudaAddress, handle,
                                                           id, -1));
        EXPECT_TRUE(
            client_->unregister_cuda_buffer_internal(kCudaAddress, handle, id));
        EXPECT_TRUE(client_->cuda_allocations_.empty());
        EXPECT_TRUE(client_->shm_contexts_[id].cuda_buffers.empty());
        EXPECT_TRUE(client_->unmap_shm_internal(id));
    }

    static constexpr uint64_t kCudaAddress = 0x1000;
    static constexpr const char *kPendingKey = "teardown_pending_put";
    testing::InProcMaster master_;
    std::unique_ptr<coro_rpc::coro_rpc_server> failing_master_;
    std::shared_ptr<RealClient> client_;
    std::shared_ptr<ShutdownProbe> probe_;
    void *mapping_ = MAP_FAILED;
    size_t mapping_size_ = 0;
    std::string storage_path_;
};

TEST_F(RealClientTeardownTest, DestructorJoinsWorkersAfterSessionCleanupFails) {
    CheckDestructorOrdering();
}

TEST_F(RealClientTeardownTest,
       DestructorDropsFileStorageReferenceBeforeReleasingSegments) {
    AddFileStorage();
    ASSERT_NE(client_->file_storage_, nullptr);
    CheckDestructorOrdering();
}

TEST_F(RealClientTeardownTest, ExplicitCloseRetainsFailedSessionForRetry) {
    EXPECT_EQ(client_->tearDownAll(), static_cast<int>(ErrorCode::RPC_FAIL));
    EXPECT_EQ(client_->tearDownAll(), static_cast<int>(ErrorCode::RPC_FAIL));
    EXPECT_NE(client_->client_, nullptr);
    EXPECT_TRUE(HasPendingSession());
    EXPECT_TRUE(IsMapped(mapping_));

    ASSERT_EQ(ReconnectMaster(), ErrorCode::OK);
    // The synthetic pending key is absent, so OBJECT_NOT_FOUND confirms that
    // the retry can release its session ownership and complete shutdown.
    EXPECT_EQ(client_->tearDownAll(), 0);
    EXPECT_EQ(client_->client_, nullptr);
    EXPECT_TRUE(PendingStateEmpty());
    EXPECT_FALSE(IsMapped(mapping_));
}

TEST_F(RealClientTeardownTest, CudaQuarantineSurvivesDummyAndRealClose) {
    CheckCudaQuarantine();
}

TEST_F(RealClientTeardownTest, HoldsIpcNameUntilActiveTransferDrains) {
    CheckIpcNameDuringTransfer();
}

TEST_F(RealClientTeardownTest, CudaDeregistrationRetriesAfterShmCleanup) {
    CheckCudaDeregistrationRetry();
}

TEST_F(RealClientTeardownTest, CudaCloseRetrySkipsCompletedMrDeregistration) {
    CheckCudaClosePending();
}

TEST_F(RealClientTeardownTest, TentRejectsPersistentCudaBeforeImport) {
    CheckUnsupportedCudaBackend(true, "rdma");
}

TEST_F(RealClientTeardownTest, DataDirectRejectsPersistentCudaBeforeImport) {
    CheckUnsupportedCudaBackend(false, "rdma");
    CheckUnsupportedCudaBackend(false, "rdma_twosided");
}

}  // namespace mooncake::test
