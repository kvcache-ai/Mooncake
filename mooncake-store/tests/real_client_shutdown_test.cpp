#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <filesystem>
#include <future>
#include <memory>

#include "file_storage.h"
#include "real_client.h"
#include "test_server_helpers.h"

namespace mooncake {
#ifdef MOONCAKE_TEST_WRAP_UNREGISTER
namespace {
std::atomic<void*> failed_unregister_address{nullptr};
}
extern "C" tl::expected<void, ErrorCode>
__real__ZN8mooncake6Client21unregisterLocalMemoryEPvb(Client*, void*, bool);
extern "C" tl::expected<void, ErrorCode>
__wrap__ZN8mooncake6Client21unregisterLocalMemoryEPvb(Client* client,
                                                      void* addr, bool update) {
    if (addr == failed_unregister_address.load())
        return tl::unexpected(ErrorCode::INVALID_PARAMS);
    return __real__ZN8mooncake6Client21unregisterLocalMemoryEPvb(client, addr,
                                                                 update);
}
#endif
class FileStorageShutdownTestPeer {
   public:
    static auto Allocator(FileStorage& s) {
        return std::weak_ptr<ClientBufferAllocator>(s.client_buffer_allocator_);
    }
    static auto Register(FileStorage& s) { return s.RegisterLocalMemory(); }
    static void* Staging(FileStorage& s) {
        return s.client_buffer_allocator_->getBase();
    }
    static void SetBackend(FileStorage& s,
                           std::shared_ptr<StorageBackendInterface> backend) {
        s.storage_backend_ = std::move(backend);
    }
    static void SetRescan(FileStorage& storage, std::future<void> future) {
        storage.rescan_future_ = std::move(future);
    }
};
class RealClientShutdownTestPeer {
   public:
    static std::unique_ptr<RpcDrainGuard::ScopedCall> Enter(RealClient& c) {
        return std::make_unique<RpcDrainGuard::ScopedCall>(
            c.offload_rpc_drain_);
    }
};
namespace {
using namespace std::chrono_literals;

class RealClientShutdownTest : public ::testing::Test {
   protected:
    void SetUp() override {
        ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
        client = RealClient::create();
        config = {
            {CONFIG_KEY_LOCAL_HOSTNAME, "127.0.0.1"},
            {CONFIG_KEY_METADATA_SERVER, "P2PHANDSHAKE"},
            {CONFIG_KEY_GLOBAL_SEGMENT_SIZE, "16777216"},
            {CONFIG_KEY_LOCAL_BUFFER_SIZE, "16777216"},
            {CONFIG_KEY_PROTOCOL, "tcp"},
            {CONFIG_KEY_MASTER_SERVER_ADDR, master.master_address()},
        };
        ASSERT_TRUE(client->setup_internal(config));
        path = std::filesystem::temp_directory_path() /
               ("mooncake_shutdown_" + std::to_string(getFreeTcpPort()));
        std::filesystem::create_directories(path);
        FileStorageConfig storage_config = FileStorageConfig::FromEnvironment();
        storage_config.storage_filepath = path.string();
        storage_config.local_buffer_size = 1024 * 1024;
        client->file_storage_ = std::make_shared<FileStorage>(
            storage_config, client->client_, "127.0.0.1:0");
        client->offload_rpc_server_ =
            std::make_unique<coro_rpc::coro_rpc_server>(1, 0, "127.0.0.1");
        client->offload_rpc_server_
            ->register_handler<&RealClient::release_offload_buffer>(
                client.get());
        client->offload_rpc_server_->async_start();
    }
    void TearDown() override {
        client.reset();
        master.Stop();
        std::filesystem::remove_all(path);
    }
    testing::InProcMaster master;
    std::shared_ptr<RealClient> client;
    std::filesystem::path path;
    ConfigDict config;
};

TEST_F(RealClientShutdownTest, ExplicitCloseReleasesStorageAndClientOwners) {
    std::weak_ptr<Client> backend = client->client_;
    std::weak_ptr<FileStorage> storage = client->file_storage_;
    ASSERT_NE(client->offload_rpc_server_, nullptr);
    EXPECT_EQ(client->tearDownAll(), 0);
    EXPECT_EQ(client->offload_rpc_server_, nullptr);
    EXPECT_TRUE(storage.expired());
    EXPECT_TRUE(backend.expired());
    EXPECT_EQ(client->tearDownAll(), 0);
}

TEST_F(RealClientShutdownTest, ConcurrentCloseWaitsForStorageRescan) {
    std::promise<void> release;
    FileStorageShutdownTestPeer::SetRescan(*client->file_storage_,
                                           release.get_future());
    std::promise<void> started;
    auto first = std::async(std::launch::async, [&] {
        started.set_value();
        return client->tearDownAll();
    });
    started.get_future().wait();
    auto second =
        std::async(std::launch::async, [&] { return client->tearDownAll(); });
    auto first_status = first.wait_for(100ms);
    auto second_status = second.wait_for(100ms);
    release.set_value();  // Always release before any assertion or future
                          // destruction.
    EXPECT_EQ(first.get(), 0);
    EXPECT_EQ(second.get(), 0);
    EXPECT_EQ(first_status, std::future_status::timeout);
    EXPECT_EQ(second_status, std::future_status::timeout);
}

#ifdef MOONCAKE_TEST_WRAP_UNREGISTER
TEST_F(RealClientShutdownTest,
       FailedStagingUnregisterRetainsOnlyItsArenaAndReportsFailure) {
    ASSERT_TRUE(FileStorageShutdownTestPeer::Register(*client->file_storage_));
    auto allocator =
        FileStorageShutdownTestPeer::Allocator(*client->file_storage_);
    std::weak_ptr<Client> backend = client->client_;
    failed_unregister_address =
        FileStorageShutdownTestPeer::Staging(*client->file_storage_);
    const auto result = client->tearDownAll();
    failed_unregister_address = nullptr;
    EXPECT_EQ(result, static_cast<int>(ErrorCode::INVALID_PARAMS));
    EXPECT_TRUE(backend.expired());
    EXPECT_EQ(client->file_storage_, nullptr);
    EXPECT_FALSE(allocator.expired());
    EXPECT_EQ(client->tearDownAll(), result);
}
#endif

class BlockedReadBackend : public BucketStorageBackend {
   public:
    BlockedReadBackend(const FileStorageConfig& config,
                       std::shared_future<void> release)
        : BucketStorageBackend(config, BucketBackendConfig{}),
          release_(std::move(release)) {}
    tl::expected<void, ErrorCode> BatchLoad(
        std::unordered_map<std::string, Slice>&) override {
        entered.set_value();
        release_.wait();
        return tl::unexpected(ErrorCode::FILE_READ_FAIL);
    }
    std::promise<void> entered;

   private:
    std::shared_future<void> release_;
};

TEST_F(RealClientShutdownTest, CloseDrainsOffloadCallsBeforeStoppingExecutor) {
    std::promise<void> release;
    auto backend = std::make_shared<BlockedReadBackend>(
        client->file_storage_->config_, release.get_future().share());
    FileStorageShutdownTestPeer::SetBackend(*client->file_storage_, backend);
    const std::vector<std::string> keys{"blocked"};
    const std::vector<int64_t> sizes{64};
    auto read = std::async(std::launch::async, [&] {
        return async_simple::coro::syncAwait(
            client->batch_get_offload_object(keys, sizes));
    });
    auto entered = backend->entered.get_future().wait_for(5s);
    if (entered != std::future_status::ready) {
        release.set_value();
        read.wait();
        FAIL() << "Read did not reach the backend gate";
    }
    const auto port = client->offload_rpc_server_->port();
    auto closed =
        std::async(std::launch::async, [&] { return client->tearDownAll(); });
    const auto status = closed.wait_for(100ms);
    // The server must still run continuations while posted SSD work drains.
    coro_rpc::coro_rpc_client probe;
    auto connected = async_simple::coro::syncAwait(
        probe.connect("127.0.0.1", std::to_string(port)));
    auto response = async_simple::coro::syncAwait(
        probe.call<&RealClient::release_offload_buffer>(uint64_t{0}));
    release.set_value();
    EXPECT_FALSE(read.get().has_value());
    EXPECT_EQ(closed.get(), 0);
    EXPECT_EQ(status, std::future_status::timeout);
    EXPECT_FALSE(connected);
    EXPECT_TRUE(response.has_value());
}

TEST_F(RealClientShutdownTest, ReinitializationAcceptsOffloadCalls) {
    ASSERT_EQ(client->tearDownAll(), 0);
    ASSERT_TRUE(client->setup_internal(config));
    auto inflight = RealClientShutdownTestPeer::Enter(*client);
    EXPECT_TRUE(inflight->ok());
}

TEST(FileStorageShutdownTest, UnregistersStagingWithAnExternalTransferEngine) {
    testing::InProcMaster master;
    ASSERT_TRUE(master.Start(InProcMasterConfigBuilder().build()));
    auto engine = std::make_shared<TransferEngine>(false);
    const auto endpoint = "127.0.0.1:" + std::to_string(getFreeTcpPort());
    ASSERT_EQ(engine->init("P2PHANDSHAKE", endpoint), 0);
    ASSERT_NE(engine->installTransport("tcp", nullptr), nullptr);
    auto created = Client::Create(endpoint, "P2PHANDSHAKE", "tcp", std::nullopt,
                                  master.master_address(), engine);
    ASSERT_TRUE(created);
    auto client = *created;
    FileStorageConfig config = FileStorageConfig::FromEnvironment();
    config.local_buffer_size = 1024 * 1024;
    const auto path = std::filesystem::temp_directory_path() /
                      ("mooncake_staging_" + std::to_string(getFreeTcpPort()));
    config.storage_filepath = path.string();
    std::filesystem::create_directories(path);
    FileStorage storage(config, client, endpoint);
    const auto addr = reinterpret_cast<uint64_t>(
        FileStorageShutdownTestPeer::Staging(storage));
    ASSERT_TRUE(FileStorageShutdownTestPeer::Register(storage));
    auto contains = [&] {
        auto desc = engine->getMetadata()->getSegmentDescByID(LOCAL_SEGMENT_ID);
        return desc && std::any_of(desc->buffers.begin(), desc->buffers.end(),
                                   [&](const auto& buffer) {
                                       return buffer.addr == addr;
                                   });
    };
    ASSERT_TRUE(contains());
    EXPECT_TRUE(storage.Shutdown());
    EXPECT_FALSE(contains());
    EXPECT_TRUE(storage.Shutdown());
    client.reset();
    created.reset();
    engine.reset();
    master.Stop();
    std::filesystem::remove_all(path);
}
}  // namespace
}  // namespace mooncake
