#include <gtest/gtest.h>
#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <future>
#include <memory>
#include <string>

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

class TeardownClient : public Client {
   public:
    explicit TeardownClient(std::promise<void> &destroying)
        : Client("localhost", "", "tcp"), destroying_(destroying) {}
    ~TeardownClient() override { destroying_.set_value(); }

   private:
    std::promise<void> &destroying_;
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
        failing_master_->register_handler<&WrappedMasterService::ServiceReady,
                                          &WrappedMasterService::BatchPutStart,
                                          &WrappedMasterService::BatchPutEnd>(
            master_.service().get());
        ASSERT_FALSE(failing_master_->async_start().hasResult());

        client_ = std::make_shared<RealClient>();
        client_->client_ = std::make_shared<TeardownClient>(destroying_);
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
        bool worker_saw_mapping = false;
        auto destroying = destroying_.get_future().share();
        client_->client_->task_thread_pool_.enqueue([&, destroying] {
            if (destroying.wait_for(std::chrono::seconds(10)) ==
                std::future_status::ready) {
                worker_saw_mapping = IsMapped(mapping_);
            }
        });
        client_.reset();  // Joins the worker before releasing segment memory.
        EXPECT_TRUE(worker_saw_mapping);
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

#ifdef USE_NOF
    void CheckNoFCleanup() {
        const auto id = client_->client_->getClientId();
        Segment mem;
        mem.id = generate_uuid();
        mem.name = mem.te_endpoint = "mem";
        mem.protocol = "tcp";
        mem.base = 0x300000000;
        mem.size = 16 << 20;
        NoFSegment nof;
        nof.id = generate_uuid();
        nof.name = nof.te_endpoint = "nof";
        nof.base = 0x400000000;
        nof.size = 16 << 20;
        auto master = master_.service();
        ASSERT_TRUE(master->MountSegment(mem, id));
        ASSERT_TRUE(master->MountNoFSegment(nof, id));

        const std::vector<std::string> keys{"end_failed", "end_lost",
                                            "revoke_lost"};
        ReplicateConfig config;
        config.replica_num = config.nof_replica_num = 1;
        ASSERT_EQ(client_->batch_put_session_start(keys, {64, 64, 64}, config),
                  std::vector<int>(3, 0));
        // The proxy acknowledges MEMORY End for two keys, then fails NoF
        // revoke. Simulate lost End and revoke replies directly at Master.
        EXPECT_EQ(client_->batch_put_session_end({keys[0], keys[2]}),
                  std::vector<int>(2, toInt(ErrorCode::RPC_FAIL)));
        ASSERT_TRUE(master->PutEnd(id, ObjectMeta{keys[1], std::nullopt},
                                   ReplicaType::MEMORY));
        ASSERT_TRUE(master->PutRevoke(id, keys[2], ReplicaType::NOF_SSD));
        EXPECT_EQ(client_->tearDownAll(), toInt(ErrorCode::RPC_FAIL));
        ASSERT_EQ(ReconnectMaster(), ErrorCode::OK);
        EXPECT_EQ(client_->tearDownAll(), 0);
        EXPECT_TRUE(PendingStateEmpty());
        for (const auto &key : keys) {
            SCOPED_TRACE(key);
            auto replicas = master->GetReplicaList(key);
            ASSERT_TRUE(replicas);
            ASSERT_EQ(replicas->replicas.size(), 1u);
            EXPECT_TRUE(replicas->replicas.front().is_memory_replica());
            EXPECT_EQ(replicas->replicas.front().status,
                      ReplicaStatus::COMPLETE);
            // GetReplicaList hides PROCESSING replicas; probe NoF directly.
            auto revoke = master->PutRevoke(id, key, ReplicaType::NOF_SSD);
            ASSERT_FALSE(revoke);
            EXPECT_EQ(revoke.error(), ErrorCode::INVALID_WRITE);
        }
    }
#endif

    using CudaState = RealClient::CudaRegistrationState;
    const UUID cuda_id_ = generate_uuid();
    const CudaIpcBufferHandle handle_{.size = 4096, .has_device_uuid = true};

    std::weak_ptr<ClientBufferAllocator> AddCudaAllocation(CudaState state) {
        const std::string key(
            handle_.device_uuid.size() + handle_.handle.size(), '\0');
        auto &context = client_->shm_contexts_[cuda_id_];
        auto &allocation = client_->cuda_allocations_[key];
        allocation = std::make_shared<RealClient::CudaRegisteredAllocation>();
        allocation->state = state;
        if (state != CudaState::kQuarantined) {
            context.cuda_buffers.emplace(
                kCudaAddress,
                RealClient::CudaRegisteredBuffer{handle_, allocation});
        }
        context.client_buffer_allocator =
            ClientBufferAllocator::create(mapping_, mapping_size_);
        return context.client_buffer_allocator;
    }

    void StartIpcWithReplacement(UdsAcceptor &replacement) {
        client_->ipc_socket_path_ = std::to_string(cuda_id_.first);
        replacement.registerHandler([](UdsConnection &) {});
        ASSERT_EQ(client_->start_ipc_server(), 0);
    }

    void CheckCudaQuarantine() {
        UdsAcceptor replacement(std::to_string(cuda_id_.first));
        StartIpcWithReplacement(replacement);
        auto allocator = AddCudaAllocation(CudaState::kQuarantined);
        // A second request for an allocation with an ambiguous initial
        // rollback must preserve the negative acknowledgement for this client.
        ASSERT_FALSE(client_->register_cuda_buffer_with_fd(
            kCudaAddress, handle_, cuda_id_, -1));
        EXPECT_FALSE(client_->unmap_shm_internal(cuda_id_));
        EXPECT_TRUE(allocator.expired());
        EXPECT_EQ(client_->shm_contexts_.count(cuda_id_), 0U);
        EXPECT_FALSE(client_->unregister_cuda_buffer_internal(
            kCudaAddress, handle_, cuda_id_));
        EXPECT_FALSE(client_->unmap_shm_internal(cuda_id_));
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
        UdsAcceptor replacement(std::to_string(cuda_id_.first));
        StartIpcWithReplacement(replacement);

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
        auto allocator = AddCudaAllocation(CudaState::kRegistered);
        auto &allocation = client_->cuda_allocations_.begin()->second;
        client_->registered_buffer_sizes_[allocation->mapping.base()] = {
            handle_.size};
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
        auto result = client_->unmap_shm_internal(cuda_id_);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
        EXPECT_TRUE(allocator.expired());
        EXPECT_EQ(allocation->state, CudaState::kUnregisterPending);
        EXPECT_TRUE(client_->unmap_shm_internal(cuda_id_));
        EXPECT_TRUE(client_->unmap_shm_internal(cuda_id_));
        EXPECT_EQ(attempts, 2);
        EXPECT_TRUE(client_->cuda_allocations_.empty());
    }

    void CheckUnsupportedCudaBackend(bool tent, const char *transport) {
        client_->client_->transfer_engine_ =
            TransferEngineImplTestPeer::makeEngine(tent, transport);
        client_->shm_contexts_.try_emplace(cuda_id_);
        const bool previous =
            std::exchange(globalConfig().rdma_data_direct, !tent);
        auto result = client_->register_cuda_buffer_with_fd(
            kCudaAddress, handle_, cuda_id_, -1);
        globalConfig().rdma_data_direct = previous;
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::NOT_SUPPORTED);
        EXPECT_TRUE(client_->cuda_allocations_.empty());
        EXPECT_TRUE(client_->shm_contexts_[cuda_id_].cuda_buffers.empty());
        EXPECT_TRUE(client_->unregister_cuda_buffer_internal(
            kCudaAddress, handle_, cuda_id_));
    }

    void CheckCudaClosePending() {
        AddCudaAllocation(CudaState::kClosePending);
        EXPECT_FALSE(client_->register_cuda_buffer_with_fd(
            kCudaAddress, handle_, cuda_id_, -1));
        EXPECT_TRUE(client_->unregister_cuda_buffer_internal(
            kCudaAddress, handle_, cuda_id_));
        EXPECT_TRUE(client_->cuda_allocations_.empty());
        EXPECT_TRUE(client_->shm_contexts_[cuda_id_].cuda_buffers.empty());
        EXPECT_TRUE(client_->unmap_shm_internal(cuda_id_));
    }

    static constexpr uint64_t kCudaAddress = 0x1000;
    static constexpr const char *kPendingKey = "teardown_pending_put";
    testing::InProcMaster master_;
    std::unique_ptr<coro_rpc::coro_rpc_server> failing_master_;
    std::promise<void> destroying_;
    std::shared_ptr<RealClient> client_;
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
    CheckDestructorOrdering();
}

TEST_F(RealClientTeardownTest, ExplicitCloseRetainsFailedSessionForRetry) {
    EXPECT_EQ(client_->tearDownAll(), static_cast<int>(ErrorCode::RPC_FAIL));
    EXPECT_EQ(client_->tearDownAll(), static_cast<int>(ErrorCode::RPC_FAIL));
    EXPECT_TRUE(HasPendingSession());
    EXPECT_TRUE(IsMapped(mapping_));

    ASSERT_EQ(ReconnectMaster(), ErrorCode::OK);
    // The synthetic pending key is absent, so OBJECT_NOT_FOUND confirms that
    // the retry can release its session ownership and complete shutdown.
    EXPECT_EQ(client_->tearDownAll(), 0);
    EXPECT_TRUE(PendingStateEmpty());
    EXPECT_FALSE(IsMapped(mapping_));
}

#ifdef USE_NOF
TEST_F(RealClientTeardownTest, CleanupAfterLostMemoryEndAndNoFRevokeReplies) {
    CheckNoFCleanup();
}
#endif
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
