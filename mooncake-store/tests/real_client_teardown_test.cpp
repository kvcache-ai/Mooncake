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

}  // namespace mooncake::test
