#include <gtest/gtest.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include <algorithm>
#include <functional>
#include <memory>
#include <set>

#include "client_service.h"
#include "common/network.h"
#include "rpc_service.h"
#include "storage/distributed/object_storage_backend.h"
#include "version.h"

namespace mooncake {

// Inject only the object backend and drive the real notification poller
// explicitly, so upload/finalize races do not depend on the one-second timer.
class ClientObjectStorageTestPeer {
   public:
    static std::unique_ptr<Client> Create(
        std::shared_ptr<ObjectStorageAdapter> adapter) {
        auto client =
            std::unique_ptr<Client>(new Client("localhost:9003", "", "tcp"));
        client->transfer_engine_ = std::make_shared<TransferEngine>();
        const auto ports = getFreeTcpPorts(1);
        if (ports.empty() ||
            client->InitTransferEngine(
                "127.0.0.1:" + std::to_string(ports.front()), "P2PHANDSHAKE",
                "tcp", std::nullopt) != ErrorCode::OK)
            return nullptr;
        client->InitTransferSubmitter();
        client->object_storage_backend_ =
            std::make_shared<ObjectStorageBackend>(ObjectStorageBackendConfig{},
                                                   std::move(adapter));
        client->object_storage_config_current_ = true;
        client->object_storage_namespace_ = "test";
        client->task_poll_running_ = true;
        client->storage_heartbeat_running_ = true;
        return client;
    }
    static ErrorCode Connect(Client& client, const std::string& address) {
        return client.master_client_.Connect(address);
    }
    static void Poll(Client& client) { client.PollObjectStorage(); }
    static size_t Pending(Client& client) {
        std::lock_guard lock(client.object_storage_completions_mutex_);
        return client.object_storage_completions_.size();
    }
    static void Report(Client& client, const std::string& name) {
        client.ReportObjectStorageUpload(name, ObjectUploadOutcome::UNKNOWN);
    }
};

namespace {
class UploadAdapter : public ObjectStorageAdapter {
   public:
    std::function<void()> before_upload;
    bool SupportsSinglePutVAttempt() const override { return true; }
    tl::expected<void, ErrorCode> Put(const std::string&,
                                      std::span<const char>) override {
        return {};
    }
    tl::expected<void, ErrorCode> PutV(const std::string&, const iovec*,
                                       int) override {
        if (before_upload) before_upload();
        return {};
    }
    tl::expected<size_t, ErrorCode> Get(const std::string&, void*,
                                        size_t) override {
        return tl::make_unexpected(ErrorCode::NOT_SUPPORTED);
    }
    tl::expected<bool, ErrorCode> Exists(const std::string&) override {
        return false;
    }
    tl::expected<void, ErrorCode> Delete(const std::string&) override {
        return {};
    }
    tl::expected<std::vector<KeyInfo>, ErrorCode> ListKeys() override {
        return std::vector<KeyInfo>{};
    }
    tl::expected<void, ErrorCode> Init() override { return {}; }
    const char* GetName() const override { return "test"; }
};

class CompletionMaster {
   public:
    std::set<std::string> failed_keys;
    std::vector<std::string> notifications;
    ErrorCode notification_error{ErrorCode::OK};
    bool truncate_batch{false};
    std::function<void()> before_end;

    tl::expected<std::string, ErrorCode> Ready() {
        return GetMooncakeStoreVersion();
    }
    tl::expected<std::vector<Replica::Descriptor>, ErrorCode> Start(
        const UUID&, const std::string& key, uint64_t size,
        const ReplicateConfig&, const std::string&) {
        Replica replica(
            ObjectStorageDescriptor{key, size, ObjectStorageWriteMode::SYNC,
                                    "test"},
            ReplicaStatus::PROCESSING);
        auto descriptor = replica.get_descriptor();
        descriptor.object_write_attempt =
            ObjectWriteAttempt{descriptor.id, key};
        return std::vector<Replica::Descriptor>{descriptor};
    }
    std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
    BatchStart(const UUID& writer, const std::vector<std::string>& keys,
               const std::vector<uint64_t>& sizes,
               const ReplicateConfig& config, const std::string& tenant) {
        std::vector<tl::expected<std::vector<Replica::Descriptor>, ErrorCode>>
            result;
        for (size_t i = 0; i < keys.size(); ++i)
            result.push_back(Start(writer, keys[i], sizes[i], config, tenant));
        return result;
    }
    tl::expected<void, ErrorCode> End(const UUID&, const ObjectMeta& meta,
                                      ReplicaType, const std::string&) {
        if (before_end) before_end();
        if (failed_keys.contains(meta.key))
            return tl::make_unexpected(ErrorCode::INVALID_WRITE);
        return {};
    }
    std::vector<tl::expected<void, ErrorCode>> BatchEnd(
        const UUID& writer, const std::vector<ObjectMeta>& metas,
        ReplicaType type, const std::string& tenant) {
        if (truncate_batch) return {};
        std::vector<tl::expected<void, ErrorCode>> result;
        for (const auto& meta : metas)
            result.push_back(End(writer, meta, type, tenant));
        return result;
    }
    std::vector<tl::expected<void, ErrorCode>> BatchUpsertEnd(
        const UUID& writer, const std::vector<ObjectMeta>& metas,
        const std::string& tenant) {
        return BatchEnd(writer, metas, ReplicaType::ALL, tenant);
    }
    tl::expected<void, ErrorCode> Finish(const UUID&, const std::string& name,
                                         ObjectUploadOutcome,
                                         const std::string&) {
        notifications.push_back(name);
        if (notification_error != ErrorCode::OK)
            return tl::make_unexpected(notification_error);
        return {};
    }
};

class ClientObjectStorageTest : public ::testing::Test {
   protected:
    void SetUp() override {
        const auto ports = getFreeTcpPorts(1);
        ASSERT_EQ(ports.size(), 1u);
        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            2, ports.front(), "127.0.0.1", std::chrono::seconds(0), true);
        Register<&CompletionMaster::Ready,
                 &WrappedMasterService::ServiceReady>();
        Register<&CompletionMaster::Start, &WrappedMasterService::PutStart>();
        Register<&CompletionMaster::Start,
                 &WrappedMasterService::UpsertStart>();
        Register<&CompletionMaster::BatchStart,
                 &WrappedMasterService::BatchPutStart>();
        Register<&CompletionMaster::BatchStart,
                 &WrappedMasterService::BatchUpsertStart>();
        Register<&CompletionMaster::End, &WrappedMasterService::PutEnd>();
        Register<&CompletionMaster::End, &WrappedMasterService::UpsertEnd>();
        Register<&CompletionMaster::BatchEnd,
                 &WrappedMasterService::BatchPutEnd>();
        Register<&CompletionMaster::BatchUpsertEnd,
                 &WrappedMasterService::BatchUpsertEnd>();
        Register<&CompletionMaster::Finish,
                 &WrappedMasterService::FinishObjectStorageUpload>();
        auto started = server_->async_start();
        ASSERT_FALSE(started.hasResult());
        adapter_ = std::make_shared<UploadAdapter>();
        client_ = ClientObjectStorageTestPeer::Create(adapter_);
        ASSERT_TRUE(client_);
        ASSERT_EQ(ClientObjectStorageTestPeer::Connect(
                      *client_, "127.0.0.1:" + std::to_string(ports.front())),
                  ErrorCode::OK);
        config_.replica_num = 0;
        config_.os_replica_num = 1;
        adapter_->before_upload = [&] {
            // On the second batch item this polls after the first upload has
            // completed, while BatchPutEnd/BatchUpsertEnd has not been called.
            ClientObjectStorageTestPeer::Poll(*client_);
            EXPECT_TRUE(master_.notifications.empty());
        };
        master_.before_end = [&] {
            EXPECT_EQ(ClientObjectStorageTestPeer::Pending(*client_), 0u);
        };
    }
    void TearDown() override {
        client_.reset();
        server_->stop();
    }
    template <auto Local, auto Remote>
    void Register() {
        server_->register_handler<Local>(&master_, coro_rpc::func_id<Remote>());
    }
    CompletionMaster master_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::shared_ptr<UploadAdapter> adapter_;
    std::unique_ptr<Client> client_;
    ReplicateConfig config_;
    char data_[4] = {'t', 'e', 's', 't'};
};

TEST_F(ClientObjectStorageTest, SingleWritesNotifyOnlyAfterFailedEnd) {
    for (const bool upsert : {false, true}) {
        for (const bool fail : {false, true}) {
            SCOPED_TRACE(upsert);
            SCOPED_TRACE(fail);
            master_.notifications.clear();
            master_.failed_keys =
                fail ? std::set<std::string>{"key"} : std::set<std::string>{};
            std::vector<Slice> slices{{data_, sizeof(data_)}};
            auto result = upsert ? client_->Upsert("key", slices, config_)
                                 : client_->Put("key", slices, config_);
            EXPECT_EQ(result.has_value(), !fail);
            EXPECT_EQ(ClientObjectStorageTestPeer::Pending(*client_),
                      fail ? 1u : 0u);
            ClientObjectStorageTestPeer::Poll(*client_);
            EXPECT_EQ(master_.notifications,
                      fail ? std::vector<std::string>{"key"}
                           : std::vector<std::string>{});
            EXPECT_EQ(ClientObjectStorageTestPeer::Pending(*client_), 0u);
        }
    }
}

TEST_F(ClientObjectStorageTest, BatchesNotifyOnlyFailedItemsAfterEnd) {
    for (const bool upsert : {false, true}) {
        for (const bool truncate : {false, true}) {
            SCOPED_TRACE(upsert);
            SCOPED_TRACE(truncate);
            master_.notifications.clear();
            master_.failed_keys = {"second"};
            master_.truncate_batch = truncate;
            std::vector<std::vector<Slice>> slices(2, {{data_, sizeof(data_)}});
            auto results =
                upsert
                    ? client_->BatchUpsert({"first", "second"}, slices, config_)
                    : client_->BatchPut({"first", "second"}, slices, config_);
            ASSERT_EQ(results.size(), 2u);
            EXPECT_EQ(results[0].has_value(), !truncate);
            EXPECT_FALSE(results[1]);
            ClientObjectStorageTestPeer::Poll(*client_);
            std::sort(master_.notifications.begin(),
                      master_.notifications.end());
            EXPECT_EQ(master_.notifications,
                      truncate ? (std::vector<std::string>{"first", "second"})
                               : (std::vector<std::string>{"second"}));
            EXPECT_EQ(ClientObjectStorageTestPeer::Pending(*client_), 0u);
        }
    }
}

TEST_F(ClientObjectStorageTest,
       PermanentErrorsAreDroppedAndTransientErrorsRetry) {
    for (auto error : {ErrorCode::ILLEGAL_CLIENT, ErrorCode::INVALID_PARAMS}) {
        master_.notification_error = error;
        ClientObjectStorageTestPeer::Report(*client_, "permanent");
        EXPECT_EQ(ClientObjectStorageTestPeer::Pending(*client_), 0u);
    }
    for (auto terminal : {ErrorCode::OK, ErrorCode::ILLEGAL_CLIENT,
                          ErrorCode::INVALID_PARAMS}) {
        master_.notification_error = ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
        ClientObjectStorageTestPeer::Report(*client_, "retry");
        EXPECT_EQ(ClientObjectStorageTestPeer::Pending(*client_), 1u);
        ClientObjectStorageTestPeer::Poll(*client_);
        EXPECT_EQ(ClientObjectStorageTestPeer::Pending(*client_), 1u);
        master_.notification_error = terminal;
        ClientObjectStorageTestPeer::Poll(*client_);
        EXPECT_EQ(ClientObjectStorageTestPeer::Pending(*client_), 0u);
        const auto calls = master_.notifications.size();
        ClientObjectStorageTestPeer::Poll(*client_);
        EXPECT_EQ(master_.notifications.size(), calls);
    }
}
}  // namespace
}  // namespace mooncake
