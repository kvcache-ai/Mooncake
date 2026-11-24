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

}  // namespace mooncake::test
