#include "p2p/ha/oplog/p2p_hot_standby_service.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <gtest/gtest.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <future>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <xxhash.h>
#include <ylt/util/utils.hpp>

#include "p2p/ha/ha_status.h"
#include "p2p/ha/oplog/localfs_oplog_store.h"
#include "p2p/ha/oplog/polling_oplog_change_notifier.h"
#include "mock_oplog_store.h"
#include "p2p/master/p2p_master_service.h"
#include "p2p/master/p2p_rpc_service.h"
#include "p2p/common/p2p_rpc_types.h"

namespace mooncake::test {

struct P2PHotStandbyServiceTestAccess {
    static void StartRecoveryWorker(P2PHotStandbyService& service) {
        std::lock_guard<std::mutex> lock(service.mutex_);
        service.StartRecoveryWorker();
    }

    static std::unique_lock<std::mutex> StartWorkerWithMetadataLocked(
        P2PHotStandbyService& service) {
        std::unique_lock<std::mutex> lock(service.mutex_);
        service.StartRecoveryWorker();
        return lock;
    }

    static bool RecoveryRequested(P2PHotStandbyService& service) {
        std::lock_guard<std::mutex> lock(service.recovery_mutex_);
        return service.recovery_requested_;
    }

    static void StopRecoveryWorker(P2PHotStandbyService& service) {
        service.StopRecoveryWorker();
    }

    static bool RecoveryStopping(P2PHotStandbyService& service) {
        return service.recovery_stopping_.load();
    }

    static ErrorCode ResyncFromSnapshot(P2PHotStandbyService& service) {
        std::lock_guard<std::mutex> lock(service.mutex_);
        return service.ResyncFromSnapshotLocked();
    }
};

namespace {

class ScopedRecoveryLogCapture : public google::LogSink {
   public:
    ScopedRecoveryLogCapture() { google::AddLogSink(this); }
    ~ScopedRecoveryLogCapture() override { google::RemoveLogSink(this); }

    void send(google::LogSeverity severity, const char*, const char*, int,
              const struct ::tm*, const char* message, size_t length) override {
        if (severity >= google::GLOG_WARNING) {
            std::lock_guard<std::mutex> lock(mutex_);
            messages_.emplace_back(severity, std::string(message, length));
        }
    }

    std::vector<std::pair<google::LogSeverity, std::string>> Messages() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return messages_;
    }

   private:
    mutable std::mutex mutex_;
    std::vector<std::pair<google::LogSeverity, std::string>> messages_;
};

struct ControlledNotifierState {
    std::mutex mutex;
    std::condition_variable cv;
    OpLogChangeNotifier::ErrorCallback on_error;
    uint64_t start_sequence_id{0};
    size_t poll_batch_size{0};
    int active_callbacks{0};
    bool healthy_on_start{true};
    bool healthy{false};
    std::function<void()> on_health_check;
};

class ControlledNotifier : public OpLogChangeNotifier {
   public:
    explicit ControlledNotifier(std::shared_ptr<ControlledNotifierState> state)
        : state_(std::move(state)) {}

    ErrorCode Start(uint64_t start_sequence_id, EntryCallback,
                    ErrorCallback on_error, MaintenanceCallback = {}) override {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->start_sequence_id = start_sequence_id;
        state_->on_error = std::move(on_error);
        state_->healthy = state_->healthy_on_start;
        return ErrorCode::OK;
    }

    void Stop() override {
        std::unique_lock<std::mutex> lock(state_->mutex);
        state_->healthy = false;
        state_->cv.wait(lock, [this] { return state_->active_callbacks == 0; });
    }

    bool IsHealthy() const override {
        if (state_->on_health_check) {
            state_->on_health_check();
        }
        std::lock_guard<std::mutex> lock(state_->mutex);
        return state_->healthy;
    }

   private:
    std::shared_ptr<ControlledNotifierState> state_;
};

class ControlledReaderStore : public MockOpLogStore {
   public:
    explicit ControlledReaderStore(
        std::shared_ptr<ControlledNotifierState> state)
        : state_(std::move(state)) {}

    std::unique_ptr<OpLogChangeNotifier> CreateChangeNotifier(
        const std::string&, size_t poll_batch_size) override {
        std::lock_guard<std::mutex> lock(state_->mutex);
        state_->poll_batch_size = poll_batch_size;
        return std::make_unique<ControlledNotifier>(state_);
    }

   private:
    std::shared_ptr<ControlledNotifierState> state_;
};

class ForcedTrimLocalFsOpLogStore : public LocalFsOpLogStore {
   public:
    ForcedTrimLocalFsOpLogStore(const std::string& cluster_id,
                                const std::string& root_dir,
                                uint64_t trimmed_sequence_id)
        : LocalFsOpLogStore(cluster_id, root_dir,
                            /*enable_batch_write=*/false,
                            /*poll_interval_ms=*/10),
          trimmed_sequence_id_(trimmed_sequence_id) {}

    ErrorCode GetTrimmedSequenceId(uint64_t& sequence_id) override {
        sequence_id = trimmed_sequence_id_;
        return ErrorCode::OK;
    }

   private:
    uint64_t trimmed_sequence_id_;
};

void InjectNotifierErrors(const std::shared_ptr<ControlledNotifierState>& state,
                          ErrorCode error, int count = 1) {
    OpLogChangeNotifier::ErrorCallback callback;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        callback = state->on_error;
        ++state->active_callbacks;
    }
    if (!callback) {
        {
            std::lock_guard<std::mutex> lock(state->mutex);
            --state->active_callbacks;
        }
        state->cv.notify_all();
        ADD_FAILURE() << "Notifier error callback is not installed";
        return;
    }
    for (int i = 0; i < count; ++i) {
        callback(error);
    }
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        --state->active_callbacks;
    }
    state->cv.notify_all();
}

class FlakySnapshotRpcService {
   public:
    BeginStandbySnapshotResponse BeginSnapshot(
        const BeginStandbySnapshotRequest& request) {
        BeginStandbySnapshotResponse response;
        if (request.cluster_id != kClusterId) {
            response.error_code = toInt(ErrorCode::INVALID_PARAMS);
            return response;
        }
        response.session_id = kSessionId;
        response.baseline_sequence_id = kBaselineSequenceId;
        response.object_count = 1;
        response.client_count = 1;
        return response;
    }

    StandbySnapshotChunkResponse GetSnapshotChunk(
        const StandbySnapshotChunkRequest& request) {
        EXPECT_EQ(request.session_id, kSessionId);
        EXPECT_EQ(request.object_offset, 0u);
        EXPECT_EQ(request.client_offset, 0u);
        ++chunk_requests;
        if (fail_first_chunk.exchange(false)) {
            StandbySnapshotChunkResponse response;
            response.error_code = toInt(ErrorCode::INTERNAL_ERROR);
            return response;
        }

        StandbySnapshotChunkResponse response;
        response.done = true;
        response.next_object_offset = 1;
        response.next_client_offset = 1;

        P2PStandbyClientInfo client;
        client.client_id = kClientId;
        client.ip_address = "127.0.0.1";
        client.rpc_port = 50051;
        client.segments.push_back(kSegment);
        response.clients.push_back({kClientId, std::move(client)});

        P2PStandbyRouteEntry route;
        route.object_size = kObjectSize;
        route.last_sequence_id = kBaselineSequenceId;
        response.objects.push_back({kObjectKey, std::move(route)});
        return response;
    }

    int32_t EndSnapshot(const EndStandbySnapshotRequest& request) {
        EXPECT_EQ(request.session_id, kSessionId);
        ++end_requests;
        return toInt(ErrorCode::OK);
    }

    static constexpr const char* kClusterId = "p2p-hot-standby-test";
    static constexpr uint64_t kBaselineSequenceId = 123;
    static constexpr size_t kObjectSize = 8192;
    static inline const std::string kSessionId = "flaky-session";
    static inline const std::string kObjectKey = "flaky-snapshot-key";
    static inline const UUID kClientId{101, 101};
    static inline const UUID kSegmentId{102, 102};
    static inline const P2PSegment kSegment{
        .id = kSegmentId,
        .name = "segment-102-102",
        .size = 1024 * 1024,
        .priority = 1,
        .tags = {},
    };

    std::atomic<bool> fail_first_chunk{true};
    std::atomic<int> chunk_requests{0};
    std::atomic<int> end_requests{0};
};

// Bind once on port 0 and keep the listener alive for the entire test. A
// PID-derived port can be in use by another test or an outgoing connection.
class SnapshotServer {
   public:
    explicit SnapshotServer(P2PHotStandbyService& source)
        : service_(&source), server_(1, 0) {
        server_.register_handler<&P2PStandbySnapshotService::BeginSnapshot,
                                 &P2PStandbySnapshotService::GetSnapshotChunk,
                                 &P2PStandbySnapshotService::EndSnapshot>(
            &service_);
    }

    ~SnapshotServer() { server_.stop(); }

    bool Start() { return !server_.async_start().hasResult(); }
    uint16_t Port() const { return server_.port(); }
    std::string Endpoint() const {
        return "127.0.0.1:" + std::to_string(Port());
    }

   private:
    P2PStandbySnapshotService service_;
    coro_rpc::coro_rpc_server server_;
};

class SnapshotSourceServer {
   public:
    SnapshotSourceServer(uint16_t port,
                         P2PStandbySnapshotService::Source source)
        : service_(std::move(source)), server_(1, port), port_(port) {
        RegisterP2PStandbySnapshotRpcHandlers(server_, &service_);
    }

    ~SnapshotSourceServer() {
        if (started_) {
            server_.stop();
        }
    }

    bool Start() {
        auto start_result = server_.async_start();
        started_ = !start_result.hasResult();
        return started_;
    }

    std::string Endpoint() const {
        return "127.0.0.1:" + std::to_string(port_);
    }

   private:
    P2PStandbySnapshotService service_;
    coro_rpc::coro_rpc_server server_;
    uint16_t port_{0};
    bool started_{false};
};

uint16_t AllocateLoopbackPort() {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return 0;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        ::close(fd);
        return 0;
    }
    socklen_t len = sizeof(addr);
    if (::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) != 0) {
        ::close(fd);
        return 0;
    }
    const uint16_t port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

class P2PHotStandbyServiceTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ =
            std::filesystem::temp_directory_path() /
            ("mooncake_p2p_hot_standby_test_" + std::to_string(::getpid()) +
             "_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(test_dir_);
    }

    void TearDown() override { std::filesystem::remove_all(test_dir_); }

    P2PMasterConfig MakeMasterConfig() const {
        P2PMasterConfig config;
        config.oplog.enabled = true;
        config.cluster_id = kClusterId;
        config.oplog.store_type = "localfs";
        config.oplog.data_dir = test_dir_.string();
        config.routes.max_clients_per_key = 0;
        return config;
    }

    P2PMasterConfig MakeWrappedMasterConfig() const {
        auto config = MakeMasterConfig();
        config.metrics.enable_reporting = false;
        return config;
    }

    P2PHotStandbyConfig MakeStandbyConfig() const {
        P2PHotStandbyConfig config;
        config.cluster_id = kClusterId;
        config.oplog_store_type = OpLogStoreType::LOCAL_FS;
        config.oplog_store_root_dir = test_dir_.string();
        config.oplog_poll_interval_ms = 10;
        return config;
    }

    P2PSegment MakeSegment(const UUID& segment_id) const {
        P2PSegment segment;
        segment.id = segment_id;
        segment.name = "segment-" + std::to_string(segment_id.first) + "-" +
                       std::to_string(segment_id.second);
        segment.size = 1024 * 1024;
        segment.priority = 1;
        segment.memory_type = MemoryType::DRAM;
        return segment;
    }

    bool WaitForPersistedEntry(uint64_t sequence_id,
                               std::chrono::milliseconds timeout) const {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (std::chrono::steady_clock::now() < deadline) {
            LocalFsOpLogStore reader(kClusterId, test_dir_.string(),
                                     /*enable_batch_write=*/false);
            if (reader.Init() == ErrorCode::OK) {
                OpLogEntry entry;
                if (reader.ReadOpLog(sequence_id, entry) == ErrorCode::OK) {
                    return true;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return false;
    }

    void RegisterClient(P2PMasterService& service, const UUID& client_id,
                        const P2PSegment& segment) const {
        P2PRegisterClientRequest req;
        req.client_id = client_id;
        req.ip_address = "127.0.0.1";
        req.rpc_port = 50051;
        req.segments = {segment};
        auto result = service.RegisterClient(req);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
        ASSERT_TRUE(
            service
                .Heartbeat({.client_id = client_id,
                            .service_state = P2PClientServiceState::ONLINE})
                .has_value());
    }

    void PublishRoute(P2PMasterService& service, const std::string& key,
                      const UUID& client_id, const UUID& segment_id,
                      size_t size = 4096) const {
        P2PPublishRouteRequest req;
        req.key = key;
        req.client_id = client_id;
        req.segment_id = segment_id;
        req.object_size = size;
        auto result = service.PublishRoute(req);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    void WithdrawRoute(P2PMasterService& service, const std::string& key,
                       const UUID& client_id, const UUID& segment_id) const {
        P2PWithdrawRouteRequest req;
        req.key = key;
        req.client_id = client_id;
        req.segment_id = segment_id;
        auto result = service.WithdrawRoute(req);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    static constexpr const char* kClusterId = "p2p-hot-standby-test";
    std::filesystem::path test_dir_;
};

TEST_F(P2PHotStandbyServiceTest, UsesConfiguredOpLogPollBatchSize) {
    auto state = std::make_shared<ControlledNotifierState>();
    auto config = MakeStandbyConfig();
    config.oplog_poll_batch_size = 7;
    P2PHotStandbyService standby(
        config, [&] { return std::make_unique<ControlledReaderStore>(state); });

    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        EXPECT_EQ(7u, state->poll_batch_size);
    }
    standby.Stop();
}

TEST_F(P2PHotStandbyServiceTest, SyncStatusDistinguishesUnknownLatestFromZero) {
    class UnknownLatestReaderStore : public ControlledReaderStore {
       public:
        UnknownLatestReaderStore(std::shared_ptr<ControlledNotifierState> state,
                                 bool& fail)
            : ControlledReaderStore(std::move(state)), fail_(fail) {}

        ErrorCode GetLatestSequenceId(uint64_t& latest) override {
            return fail_ ? ErrorCode::INTERNAL_ERROR
                         : MockOpLogStore::GetLatestSequenceId(latest);
        }

       private:
        bool& fail_;
    };

    bool fail_latest = false;
    auto state = std::make_shared<ControlledNotifierState>();
    P2PHotStandbyService standby(MakeStandbyConfig(), [&] {
        return std::make_unique<UnknownLatestReaderStore>(state, fail_latest);
    });
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    EXPECT_TRUE(standby.GetSyncStatus().primary_seq_id_known);
    EXPECT_EQ(standby.GetSyncStatus().primary_seq_id, 0u);
    fail_latest = true;
    EXPECT_FALSE(standby.GetSyncStatus().primary_seq_id_known);
    fail_latest = false;
    EXPECT_TRUE(standby.GetSyncStatus().primary_seq_id_known);
    standby.Stop();
    EXPECT_FALSE(standby.GetSyncStatus().primary_seq_id_known);
}

TEST_F(P2PHotStandbyServiceTest, ReplicatesMasterWrittenP2POpLog) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{1, 1};
    const UUID segment_id{2, 2};

    RegisterClient(master, client_id, MakeSegment(segment_id));
    PublishRoute(master, "key-a", client_id, segment_id, 1234);

    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        standby.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));
    standby.Stop();

    auto exported = standby.ExportMetadata();
    auto client_it = exported.clients.find(client_id);
    ASSERT_NE(client_it, exported.clients.end());
    EXPECT_EQ(client_it->second.ip_address, "127.0.0.1");
    EXPECT_EQ(client_it->second.rpc_port, 50051);
    ASSERT_EQ(client_it->second.segments.size(), 1);
    EXPECT_EQ(client_it->second.segments[0].id, segment_id);

    auto object_it = exported.routes.find("key-a");
    ASSERT_NE(object_it, exported.routes.end());
    EXPECT_EQ(object_it->second.object_size, 1234);
    ASSERT_EQ(object_it->second.locations.size(), 1);
    EXPECT_EQ(object_it->second.locations[0].client_id, client_id);
    EXPECT_EQ(object_it->second.locations[0].segment_id, segment_id);
}

TEST_F(P2PHotStandbyServiceTest, BootstrapsFromStandbySnapshotSource) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{11, 11};
    const UUID segment_id{12, 12};
    RegisterClient(master, client_id, MakeSegment(segment_id));
    PublishRoute(master, "snapshot-key", client_id, segment_id, 8192);

    P2PHotStandbyService source(MakeStandbyConfig());
    ASSERT_EQ(source.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        source.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));
    SnapshotServer snapshot(source);
    ASSERT_TRUE(snapshot.Start());

    auto target_config = MakeStandbyConfig();
    target_config.snapshot_source_endpoints = {snapshot.Endpoint()};
    target_config.snapshot_chunk_size = 1;
    P2PHotStandbyService target(target_config);
    ASSERT_EQ(target.Start(), ErrorCode::OK);
    ASSERT_GE(target.GetLatestAppliedSequenceId(), 2);

    auto exported = target.ExportMetadata();
    ASSERT_NE(exported.clients.find(client_id), exported.clients.end());
    auto object = exported.routes.find("snapshot-key");
    ASSERT_NE(object, exported.routes.end());
    EXPECT_EQ(object->second.object_size, 8192);
    ASSERT_EQ(object->second.locations.size(), 1);

    target.Stop();
    source.Stop();
}

TEST_F(P2PHotStandbyServiceTest,
       SnapshotBootstrapSucceedsAfterTransientChunkError) {
    FlakySnapshotRpcService rpc_service;
    coro_rpc::coro_rpc_server server(1, 0, "127.0.0.1");
    server.register_handler<&FlakySnapshotRpcService::BeginSnapshot>(
        &rpc_service,
        coro_rpc::func_id<&P2PStandbySnapshotService::BeginSnapshot>());
    server.register_handler<&FlakySnapshotRpcService::GetSnapshotChunk>(
        &rpc_service,
        coro_rpc::func_id<&P2PStandbySnapshotService::GetSnapshotChunk>());
    server.register_handler<&FlakySnapshotRpcService::EndSnapshot>(
        &rpc_service,
        coro_rpc::func_id<&P2PStandbySnapshotService::EndSnapshot>());
    auto start_result = server.async_start();
    ASSERT_FALSE(start_result.hasResult());

    P2PStandbyMetadataStore target;
    uint64_t baseline_sequence_id = 0;
    P2PStandbySnapshotClient client;
    const auto result = client.Bootstrap(
        "127.0.0.1:" + std::to_string(server.port()), kClusterId, &target,
        baseline_sequence_id, /*chunk_size=*/1);
    server.stop();

    EXPECT_EQ(result, ErrorCode::OK);
    EXPECT_EQ(baseline_sequence_id,
              FlakySnapshotRpcService::kBaselineSequenceId);
    EXPECT_EQ(rpc_service.chunk_requests.load(), 2);
    EXPECT_EQ(rpc_service.end_requests.load(), 1);

    auto exported = target.ExportMetadata();
    ASSERT_NE(exported.clients.find(FlakySnapshotRpcService::kClientId),
              exported.clients.end());
    auto object = exported.routes.find(FlakySnapshotRpcService::kObjectKey);
    ASSERT_NE(object, exported.routes.end());
    EXPECT_EQ(object->second.object_size, FlakySnapshotRpcService::kObjectSize);
}

TEST_F(P2PHotStandbyServiceTest,
       HandoffBehindTrimFallsBackToPrimarySnapshotAndReplacesInitialMetadata) {
    P2PMasterService primary(MakeMasterConfig());
    const UUID client_id{13, 13};
    const UUID segment_id{14, 14};
    RegisterClient(primary, client_id, MakeSegment(segment_id));
    PublishRoute(primary, "primary-snapshot-key", client_id, segment_id, 8192);
    ASSERT_EQ(primary.GetLatestOpLogSequenceId(), 2u);

    P2PStandbyMetadataStore stale_store;
    const UUID stale_client_id{15, 15};
    const UUID stale_segment_id{16, 16};
    stale_store.RegisterClient(stale_client_id, "127.0.0.1", 50052,
                               {MakeSegment(stale_segment_id)});
    stale_store.PublishRoute("stale-handoff-key",
                             {stale_client_id, stale_segment_id}, 1024, 1);
    {
        auto stale_metadata = stale_store.ExportMetadata();
        P2PHotStandbyService current_target(
            MakeStandbyConfig(), [&]() -> std::unique_ptr<OpLogStore> {
                auto store = std::make_unique<ForcedTrimLocalFsOpLogStore>(
                    kClusterId, test_dir_.string(), /*trimmed_sequence_id=*/2);
                if (store->Init() != ErrorCode::OK) {
                    return nullptr;
                }
                return store;
            });
        ASSERT_EQ(current_target.Start(/*baseline_sequence_id=*/2u,
                                       std::move(stale_metadata)),
                  ErrorCode::OK);
        auto exported = current_target.ExportMetadata();
        EXPECT_NE(exported.routes.find("stale-handoff-key"),
                  exported.routes.end());
        EXPECT_EQ(exported.routes.find("primary-snapshot-key"),
                  exported.routes.end());
        current_target.Stop();
    }

    const auto port = AllocateLoopbackPort();
    ASSERT_NE(port, 0u);
    SnapshotSourceServer server(
        port, MakeP2PPrimarySnapshotSource(kClusterId, &primary));
    ASSERT_TRUE(server.Start());

    auto target_config = MakeStandbyConfig();
    target_config.snapshot_source_endpoints = {server.Endpoint()};
    target_config.snapshot_chunk_size = 1;
    P2PHotStandbyService target(target_config, [&]() {
        auto store = std::make_unique<ForcedTrimLocalFsOpLogStore>(
            kClusterId, test_dir_.string(), /*trimmed_sequence_id=*/2);
        if (store->Init() != ErrorCode::OK) {
            return std::unique_ptr<OpLogStore>();
        }
        return std::unique_ptr<OpLogStore>(std::move(store));
    });

    auto stale_metadata = stale_store.ExportMetadata();
    ASSERT_EQ(
        target.Start(/*baseline_sequence_id=*/1u, std::move(stale_metadata)),
        ErrorCode::OK);
    EXPECT_GE(target.GetLatestAppliedSequenceId(), 2u);
    auto exported = target.ExportMetadata();
    EXPECT_EQ(exported.routes.find("stale-handoff-key"), exported.routes.end());
    EXPECT_EQ(exported.clients.find(stale_client_id), exported.clients.end());
    EXPECT_NE(exported.routes.find("primary-snapshot-key"),
              exported.routes.end());
    EXPECT_NE(exported.clients.find(client_id), exported.clients.end());

    target.Stop();
}

TEST_F(P2PHotStandbyServiceTest,
       SnapshotBootstrapRetriesUntilPrimarySourceBecomesAvailable) {
    P2PMasterService primary(MakeMasterConfig());
    const UUID client_id{17, 17};
    const UUID segment_id{18, 18};
    RegisterClient(primary, client_id, MakeSegment(segment_id));
    PublishRoute(primary, "delayed-primary-snapshot-key", client_id, segment_id,
                 4096);

    const auto port = AllocateLoopbackPort();
    ASSERT_NE(port, 0u);
    SnapshotSourceServer server(
        port, MakeP2PPrimarySnapshotSource(kClusterId, &primary));
    auto target_config = MakeStandbyConfig();
    target_config.snapshot_source_endpoints = {server.Endpoint()};
    P2PHotStandbyService target(target_config);
    auto start_future =
        std::async(std::launch::async, [&] { return target.Start(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    ASSERT_NE(start_future.wait_for(std::chrono::milliseconds(0)),
              std::future_status::ready);

    ASSERT_TRUE(server.Start());
    ASSERT_EQ(start_future.wait_for(std::chrono::seconds(10)),
              std::future_status::ready);
    ASSERT_EQ(start_future.get(), ErrorCode::OK);
    auto exported = target.ExportMetadata();
    EXPECT_NE(exported.routes.find("delayed-primary-snapshot-key"),
              exported.routes.end());

    target.Stop();
}

TEST_F(P2PHotStandbyServiceTest,
       SnapshotKeepsFixedInventoryWhileSourceContinuesApplying) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{21, 21};
    const UUID segment_id{22, 22};
    RegisterClient(master, client_id, MakeSegment(segment_id));
    PublishRoute(master, "before-snapshot", client_id, segment_id, 4096);

    P2PHotStandbyService source(MakeStandbyConfig());
    ASSERT_EQ(source.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        source.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));

    P2PStandbySnapshotService snapshot(&source);
    auto begin = snapshot.BeginSnapshot({kClusterId});
    ASSERT_EQ(fromInt(begin.error_code), ErrorCode::OK);
    EXPECT_EQ(begin.baseline_sequence_id, 2);

    PublishRoute(master, "after-snapshot", client_id, segment_id, 8192);
    ASSERT_TRUE(
        source.WaitForAppliedSequence(3, std::chrono::milliseconds(2000)));

    auto chunk = snapshot.GetSnapshotChunk(
        {begin.session_id, /*object_offset=*/0, /*client_offset=*/0,
         /*limit=*/100});
    ASSERT_EQ(fromInt(chunk.error_code), ErrorCode::OK);
    ASSERT_TRUE(chunk.done);
    ASSERT_EQ(chunk.objects.size(), 1);
    EXPECT_EQ(chunk.objects.front().key, "before-snapshot");
    EXPECT_EQ(snapshot.EndSnapshot({begin.session_id}), toInt(ErrorCode::OK));
}

TEST_F(P2PHotStandbyServiceTest,
       PrimarySnapshotFallbackReplaysWriteAfterSnapshotBegin) {
    P2PMasterService primary(MakeMasterConfig());
    const UUID client_id{21, 21};
    const UUID segment_id{22, 22};
    RegisterClient(primary, client_id, MakeSegment(segment_id));
    PublishRoute(primary, "before-primary-snapshot", client_id, segment_id,
                 4096);

    std::atomic<bool> wrote_after_begin{false};
    P2PStandbySnapshotService::Source source;
    source.get_cluster_id = [] { return std::string(kClusterId); };
    source.is_ready = [] { return true; };
    source.get_baseline_sequence_id = [&primary] {
        return primary.GetLatestOpLogSequenceId();
    };
    source.list_object_keys = [&] {
        auto keys = primary.ListSnapshotObjectKeys();
        bool expected = false;
        if (wrote_after_begin.compare_exchange_strong(expected, true)) {
            P2PPublishRouteRequest req;
            req.key = "after-primary-snapshot";
            req.client_id = client_id;
            req.segment_id = segment_id;
            req.object_size = 8192;
            auto result = primary.PublishRoute(req);
            EXPECT_TRUE(result.has_value()) << toString(result.error());
        }
        return keys;
    };
    source.list_client_ids = [&primary] {
        return primary.ListSnapshotClientIds();
    };
    source.get_object_metadata = [&primary](const std::string& key) {
        return primary.GetSnapshotObjectMetadata(key);
    };
    source.get_client_info = [&primary](const UUID& id) {
        return primary.GetSnapshotClientInfo(id);
    };

    const auto port = AllocateLoopbackPort();
    ASSERT_NE(port, 0u);
    SnapshotSourceServer server(port, std::move(source));
    ASSERT_TRUE(server.Start());

    auto target_config = MakeStandbyConfig();
    target_config.snapshot_source_endpoints = {server.Endpoint()};
    target_config.snapshot_chunk_size = 1;
    P2PHotStandbyService target(target_config);
    ASSERT_EQ(target.Start(), ErrorCode::OK);
    ASSERT_TRUE(wrote_after_begin.load());
    ASSERT_TRUE(
        target.WaitForAppliedSequence(3, std::chrono::milliseconds(2000)));

    auto exported = target.ExportMetadata();
    auto before = exported.routes.find("before-primary-snapshot");
    ASSERT_NE(before, exported.routes.end());
    EXPECT_EQ(before->second.object_size, 4096u);
    auto after = exported.routes.find("after-primary-snapshot");
    ASSERT_NE(after, exported.routes.end());
    EXPECT_EQ(after->second.object_size, 8192u);

    target.Stop();
}

TEST_F(P2PHotStandbyServiceTest, PrimarySnapshotSourceServesCurrentMetadata) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{23, 23};
    const UUID segment_id{24, 24};
    RegisterClient(master, client_id, MakeSegment(segment_id));
    PublishRoute(master, "primary-snapshot-key", client_id, segment_id, 16384);

    P2PStandbySnapshotService::Source source =
        MakeP2PPrimarySnapshotSource(kClusterId, &master);
    P2PStandbySnapshotService snapshot(std::move(source));
    auto begin = snapshot.BeginSnapshot({kClusterId});
    ASSERT_EQ(fromInt(begin.error_code), ErrorCode::OK);
    EXPECT_EQ(begin.baseline_sequence_id, 2);
    EXPECT_EQ(begin.object_count, 1);
    EXPECT_EQ(begin.client_count, 1);

    auto chunk = snapshot.GetSnapshotChunk(
        {begin.session_id, /*object_offset=*/0, /*client_offset=*/0,
         /*limit=*/1});
    ASSERT_EQ(fromInt(chunk.error_code), ErrorCode::OK);
    ASSERT_TRUE(chunk.done);
    ASSERT_EQ(chunk.clients.size(), 1);
    EXPECT_EQ(chunk.clients.front().client_id, client_id);
    EXPECT_EQ(chunk.clients.front().info.ip_address, "127.0.0.1");
    EXPECT_EQ(chunk.clients.front().info.rpc_port, 50051);
    ASSERT_EQ(chunk.clients.front().info.segments.size(), 1);
    EXPECT_EQ(chunk.clients.front().info.segments.front().id, segment_id);

    ASSERT_EQ(chunk.objects.size(), 1);
    EXPECT_EQ(chunk.objects.front().key, "primary-snapshot-key");
    EXPECT_EQ(chunk.objects.front().route.object_size, 16384);
    ASSERT_EQ(chunk.objects.front().route.locations.size(), 1);
    EXPECT_EQ(chunk.objects.front().route.locations.front().client_id,
              client_id);
    EXPECT_EQ(chunk.objects.front().route.locations.front().segment_id,
              segment_id);
}

TEST_F(P2PHotStandbyServiceTest,
       StartsFromDemotedPrimaryMetadataAndCatchesUpFromOpLog) {
    P2PMasterService primary(MakeMasterConfig());
    const UUID client_id{25, 25};
    const UUID segment_id{26, 26};
    RegisterClient(primary, client_id, MakeSegment(segment_id));
    PublishRoute(primary, "handoff-before", client_id, segment_id, 4096);

    auto handoff_metadata = primary.ExportMetadataForStandby();
    const uint64_t handoff_sequence_id = primary.GetLatestOpLogSequenceId();
    ASSERT_GT(handoff_sequence_id, 0u);
    ASSERT_NE(handoff_metadata.clients.find(client_id),
              handoff_metadata.clients.end());
    ASSERT_NE(handoff_metadata.routes.find("handoff-before"),
              handoff_metadata.routes.end());

    PublishRoute(primary, "handoff-after", client_id, segment_id, 8192);
    const uint64_t catch_up_target = primary.GetLatestOpLogSequenceId();
    ASSERT_GT(catch_up_target, handoff_sequence_id);

    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(handoff_sequence_id, std::move(handoff_metadata)),
              ErrorCode::OK);
    ASSERT_TRUE(standby.WaitForAppliedSequence(
        catch_up_target, std::chrono::milliseconds(2000)));

    auto exported = standby.ExportMetadata();
    ASSERT_NE(exported.clients.find(client_id), exported.clients.end());
    auto before = exported.routes.find("handoff-before");
    ASSERT_NE(before, exported.routes.end());
    EXPECT_EQ(before->second.object_size, 4096);
    auto after = exported.routes.find("handoff-after");
    ASSERT_NE(after, exported.routes.end());
    EXPECT_EQ(after->second.object_size, 8192);

    standby.Stop();
}

TEST_F(P2PHotStandbyServiceTest,
       LocalCommittedHandoffRollsBackReversibleUncommittedMetadata) {
    P2PMasterService primary(MakeMasterConfig());
    const UUID base_client_id{101, 101};
    const UUID base_segment_id{102, 102};
    const UUID uncommitted_segment_id{103, 103};
    const UUID uncommitted_client_id{104, 104};
    const UUID uncommitted_client_segment_id{105, 105};

    RegisterClient(primary, base_client_id, MakeSegment(base_segment_id));
    PublishRoute(primary, "committed-key", base_client_id, base_segment_id,
                 2048);
    const uint64_t committed_sequence_id = primary.GetLatestOpLogSequenceId();
    ASSERT_EQ(committed_sequence_id, 2u);

    auto mount_result = primary.MountSegment(
        MakeSegment(uncommitted_segment_id), base_client_id);
    ASSERT_TRUE(mount_result.has_value()) << toString(mount_result.error());
    PublishRoute(primary, "uncommitted-segment-key", base_client_id,
                 uncommitted_segment_id, 4096);
    RegisterClient(primary, uncommitted_client_id,
                   MakeSegment(uncommitted_client_segment_id));
    PublishRoute(primary, "uncommitted-client-key", uncommitted_client_id,
                 uncommitted_client_segment_id, 8192);
    ASSERT_GT(primary.GetLatestOpLogSequenceId(), committed_sequence_id);

    auto handoff =
        primary.ExportLocalCommittedMetadataForStandby(committed_sequence_id);
    ASSERT_TRUE(handoff.has_value()) << toString(handoff.error());

    ASSERT_EQ(handoff->clients.size(), 1u);
    auto base_client = handoff->clients.find(base_client_id);
    ASSERT_NE(base_client, handoff->clients.end());
    ASSERT_EQ(base_client->second.segments.size(), 1u);
    EXPECT_EQ(base_client->second.segments.front().id, base_segment_id);
    EXPECT_EQ(handoff->clients.find(uncommitted_client_id),
              handoff->clients.end());

    ASSERT_EQ(handoff->routes.size(), 1u);
    auto committed_object = handoff->routes.find("committed-key");
    ASSERT_NE(committed_object, handoff->routes.end());
    EXPECT_EQ(committed_object->second.object_size, 2048u);
    EXPECT_EQ(committed_object->second.last_sequence_id, committed_sequence_id);
    ASSERT_EQ(committed_object->second.locations.size(), 1u);
    const auto& location = committed_object->second.locations.front();
    EXPECT_EQ(location.client_id, base_client_id);
    EXPECT_EQ(location.segment_id, base_segment_id);
}

TEST_F(P2PHotStandbyServiceTest,
       LocalCommittedHandoffRejectsUncommittedWithdrawLikeOpLog) {
    P2PMasterService primary(MakeMasterConfig());
    const UUID client_id{111, 111};
    const UUID segment_id{112, 112};

    RegisterClient(primary, client_id, MakeSegment(segment_id));
    PublishRoute(primary, "withdraw-like-key", client_id, segment_id, 2048);
    const uint64_t committed_sequence_id = primary.GetLatestOpLogSequenceId();
    ASSERT_EQ(committed_sequence_id, 2u);

    WithdrawRoute(primary, "withdraw-like-key", client_id, segment_id);
    ASSERT_EQ(primary.GetLatestOpLogSequenceId(), committed_sequence_id + 1);

    auto handoff =
        primary.ExportLocalCommittedMetadataForStandby(committed_sequence_id);
    ASSERT_FALSE(handoff.has_value());
    EXPECT_EQ(handoff.error(), ErrorCode::INTERNAL_ERROR);
}

TEST_F(P2PHotStandbyServiceTest,
       LocalCommittedHandoffRejectsWhenBufferedEntriesAreUnavailable) {
    P2PMasterService primary(MakeMasterConfig());
    auto* manager = primary.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    manager->SetOpLogStore(nullptr);

    constexpr size_t kEntriesToExhaustBoundedBuffer = 100001;
    for (size_t i = 0; i < kEntriesToExhaustBoundedBuffer; ++i) {
        manager->Append(OpType_PUBLISH_ROUTE, "buffered-entry");
    }

    auto handoff =
        primary.ExportLocalCommittedMetadataForStandby(/*sequence_id=*/0);
    ASSERT_FALSE(handoff.has_value());
    EXPECT_EQ(handoff.error(), ErrorCode::OPLOG_ENTRY_NOT_FOUND);
}

#ifdef STORE_USE_REDIS
TEST_F(P2PHotStandbyServiceTest,
       SelectSnapshotSourcesPrefersStandbyAndFallsBackToPrimary) {
    auto make_source = [](std::string instance_id, std::string role,
                          bool snapshot_ready, uint64_t applied_sequence_id,
                          std::string snapshot_endpoint) {
        RedisMasterRegistryEntry entry;
        entry.instance_id = std::move(instance_id);
        entry.master_endpoint = entry.instance_id + ":50051";
        entry.snapshot_endpoint = std::move(snapshot_endpoint);
        entry.role = std::move(role);
        entry.snapshot_ready = snapshot_ready;
        entry.applied_sequence_id = applied_sequence_id;
        return entry;
    };

    auto standby_selection = SelectP2PSnapshotSources(
        {make_source("primary-high", "primary", false, 900, "primary:52051"),
         make_source("standby-low", "standby", true, 300, "standby-low:52051"),
         make_source("standby-high", "standby", true, 500,
                     "standby-high:52051"),
         make_source("self", "standby", true, 700, "self:52051"),
         make_source("standby-not-ready", "standby", false, 800,
                     "not-ready:52051"),
         make_source("standby-stale", "standby", true, 99, "stale:52051")},
        "self", "self:50051", "self:52051", 100);
    EXPECT_FALSE(standby_selection.used_primary_fallback);
    EXPECT_EQ(
        standby_selection.endpoints,
        (std::vector<std::string>{"standby-high:52051", "standby-low:52051"}));
    EXPECT_EQ(standby_selection.alive_count, 6u);
    EXPECT_EQ(standby_selection.qualified_standby_count, 2u);
    EXPECT_EQ(standby_selection.primary_fallback_count, 0u);
    EXPECT_EQ(standby_selection.excluded_count, 4u);
    EXPECT_EQ(standby_selection.excluded_self_count, 1u);
    EXPECT_EQ(standby_selection.excluded_unavailable_count, 1u);
    EXPECT_EQ(standby_selection.excluded_stale_count, 1u);
    EXPECT_EQ(standby_selection.excluded_primary_not_used_count, 1u);
    EXPECT_EQ(standby_selection.excluded_role_count, 0u);

    auto primary_selection = SelectP2PSnapshotSources(
        {make_source("primary-low", "primary", false, 200, "primary-low:52051"),
         make_source("primary-high", "primary", false, 600,
                     "primary-high:52051"),
         make_source("self", "primary", false, 800, "self:52051"),
         make_source("standby-not-ready", "standby", false, 900,
                     "not-ready:52051"),
         make_source("standby-no-endpoint", "standby", true, 1000, ""),
         make_source("standby-stale", "standby", true, 99, "stale:52051")},
        "self", "self:50051", "self:52051", 100);
    EXPECT_TRUE(primary_selection.used_primary_fallback);
    EXPECT_EQ(
        primary_selection.endpoints,
        (std::vector<std::string>{"primary-high:52051", "primary-low:52051"}));
    EXPECT_EQ(primary_selection.alive_count, 6u);
    EXPECT_EQ(primary_selection.qualified_standby_count, 0u);
    EXPECT_EQ(primary_selection.primary_fallback_count, 2u);
    EXPECT_EQ(primary_selection.excluded_count, 4u);
    EXPECT_EQ(primary_selection.excluded_self_count, 1u);
    EXPECT_EQ(primary_selection.excluded_unavailable_count, 2u);
    EXPECT_EQ(primary_selection.excluded_stale_count, 1u);
    EXPECT_EQ(primary_selection.excluded_primary_not_used_count, 0u);
    EXPECT_EQ(primary_selection.excluded_role_count, 0u);

    auto restarted_self_by_endpoint =
        make_source("previous-self", "primary", false, 1000, "self:52051");
    restarted_self_by_endpoint.master_endpoint = "self:50051";
    auto restart_selection = SelectP2PSnapshotSources(
        {restarted_self_by_endpoint,
         make_source("remote-primary", "primary", false, 900, "remote:52051")},
        "new-self", "self:50051", "self:52051", 100);
    EXPECT_TRUE(restart_selection.used_primary_fallback);
    EXPECT_EQ(restart_selection.endpoints,
              (std::vector<std::string>{"remote:52051"}));
    EXPECT_EQ(restart_selection.alive_count, 2u);
    EXPECT_EQ(restart_selection.primary_fallback_count, 1u);
    EXPECT_EQ(restart_selection.excluded_count, 1u);
    EXPECT_EQ(restart_selection.excluded_self_count, 1u);

    // Multiple exclusion conditions still account for each source once;
    // transitional and unknown roles must not disappear from diagnostics.
    auto unavailable_selection = SelectP2PSnapshotSources(
        {make_source("self", "standby", false, 0, ""),
         make_source("no-endpoint", "standby", false, 0, ""),
         make_source("stale", "standby", false, 99, "stale:52051"),
         make_source("starting", "starting", true, 100, "starting:52051"),
         make_source("promoting", "promoting", true, 100, "promoting:52051"),
         make_source("unknown", "unknown", true, 100, "unknown:52051")},
        "self", "self:50051", "self:52051", 100);
    EXPECT_TRUE(unavailable_selection.endpoints.empty());
    EXPECT_FALSE(unavailable_selection.used_primary_fallback);
    EXPECT_EQ(unavailable_selection.alive_count, 6u);
    EXPECT_EQ(unavailable_selection.excluded_count, 6u);
    EXPECT_EQ(unavailable_selection.excluded_self_count, 1u);
    EXPECT_EQ(unavailable_selection.excluded_unavailable_count, 1u);
    EXPECT_EQ(unavailable_selection.excluded_stale_count, 1u);
    EXPECT_EQ(unavailable_selection.excluded_primary_not_used_count, 0u);
    EXPECT_EQ(unavailable_selection.excluded_role_count, 3u);

    auto empty_selection = SelectP2PSnapshotSources({}, "self", "", "", 100);
    EXPECT_TRUE(empty_selection.endpoints.empty());
    EXPECT_EQ(empty_selection.alive_count, 0u);
    EXPECT_EQ(empty_selection.excluded_count, 0u);
}
#endif

TEST_F(P2PHotStandbyServiceTest,
       SnapshotResyncFailuresAggregateSourcesWithoutDuplicateErrors) {
    auto config = MakeStandbyConfig();
    config.snapshot_source_endpoints = {"invalid-source-a", "invalid-source-b"};
    P2PHotStandbyService standby(config);
    ScopedRecoveryLogCapture logs;
    // Exercise a whole log interval without relying on counters left by other
    // tests. No network or retry sleeps are needed for invalid endpoints.
    for (int i = 0; i < 51; ++i) {
        ASSERT_EQ(P2PHotStandbyServiceTestAccess::ResyncFromSnapshot(standby),
                  ErrorCode::INTERNAL_ERROR);
    }
    const auto messages = logs.Messages();
    ASSERT_GE(messages.size(), 1u);
    ASSERT_LE(messages.size(), 2u);
    for (const auto& [severity, message] : messages) {
        EXPECT_EQ(severity, google::GLOG_WARNING);
        EXPECT_NE(message.find("all snapshot sources failed"),
                  std::string::npos);
        EXPECT_NE(message.find("invalid-source-a: INVALID_PARAMS"),
                  std::string::npos);
        EXPECT_NE(message.find("invalid-source-b: INVALID_PARAMS"),
                  std::string::npos);
        EXPECT_NE(message.find("source_count=2"), std::string::npos);
        EXPECT_NE(message.find("omitted_errors=0"), std::string::npos);
    }
}

TEST_F(P2PHotStandbyServiceTest, SnapshotFailureSummaryBoundsReportedSources) {
    auto config = MakeStandbyConfig();
    for (int i = 0; i < 10; ++i) {
        config.snapshot_source_endpoints.push_back("invalid-" +
                                                   std::to_string(i));
    }
    P2PHotStandbyService standby(config);
    ScopedRecoveryLogCapture logs;
    for (int i = 0; i < 51; ++i) {
        ASSERT_EQ(P2PHotStandbyServiceTestAccess::ResyncFromSnapshot(standby),
                  ErrorCode::INTERNAL_ERROR);
    }
    const auto messages = logs.Messages();
    ASSERT_GE(messages.size(), 1u);
    ASSERT_LE(messages.size(), 2u);
    for (const auto& [severity, message] : messages) {
        EXPECT_EQ(severity, google::GLOG_WARNING);
        EXPECT_NE(message.find("source_count=10"), std::string::npos);
        EXPECT_NE(message.find("omitted_errors=2"), std::string::npos);
        size_t error_count = 0;
        for (size_t pos = message.find("INVALID_PARAMS");
             pos != std::string::npos;
             pos = message.find("INVALID_PARAMS", pos + 1)) {
            ++error_count;
        }
        EXPECT_EQ(error_count, 8u);
    }
}

TEST_F(P2PHotStandbyServiceTest, MissingSnapshotSourcesAreRateLimited) {
    P2PHotStandbyService standby(MakeStandbyConfig());
    ScopedRecoveryLogCapture logs;
    for (int i = 0; i < 51; ++i) {
        ASSERT_EQ(P2PHotStandbyServiceTestAccess::ResyncFromSnapshot(standby),
                  ErrorCode::INTERNAL_ERROR);
    }
    const auto messages = logs.Messages();
    ASSERT_GE(messages.size(), 1u);
    ASSERT_LE(messages.size(), 2u);
    for (const auto& [severity, message] : messages) {
        EXPECT_EQ(severity, google::GLOG_WARNING);
        EXPECT_NE(message.find("no snapshot source available"),
                  std::string::npos);
    }
}

TEST_F(P2PHotStandbyServiceTest,
       SnapshotRejectsInvalidChunksAndLimitsSessions) {
    P2PHotStandbyService source(MakeStandbyConfig());
    ASSERT_EQ(source.Start(), ErrorCode::OK);

    P2PStandbySnapshotService snapshot(&source);
    std::vector<std::string> session_ids;
    for (size_t i = 0; i < kMaxStandbySnapshotSessions; ++i) {
        auto begin = snapshot.BeginSnapshot({kClusterId});
        ASSERT_EQ(fromInt(begin.error_code), ErrorCode::OK);
        session_ids.push_back(std::move(begin.session_id));
    }

    auto excess_session = snapshot.BeginSnapshot({kClusterId});
    EXPECT_EQ(fromInt(excess_session.error_code),
              ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    auto oversized_chunk = snapshot.GetSnapshotChunk(
        {session_ids.front(), 0, 0, kMaxStandbySnapshotChunkSize + 1});
    EXPECT_EQ(fromInt(oversized_chunk.error_code), ErrorCode::INVALID_PARAMS);

    auto invalid_offset =
        snapshot.GetSnapshotChunk({session_ids.front(), 1, 0, 1});
    EXPECT_EQ(fromInt(invalid_offset.error_code), ErrorCode::INVALID_PARAMS);

    ASSERT_EQ(snapshot.EndSnapshot({session_ids.front()}),
              toInt(ErrorCode::OK));
    auto replacement_session = snapshot.BeginSnapshot({kClusterId});
    EXPECT_EQ(fromInt(replacement_session.error_code), ErrorCode::OK);
    source.Stop();
}

TEST_F(P2PHotStandbyServiceTest, SnapshotReadinessRequiresEnabledServer) {
    P2PHotStandbyService standby(MakeStandbyConfig());
    EXPECT_FALSE(standby.CanServeSnapshot());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    EXPECT_TRUE(standby.IsReadyForSnapshot());
    EXPECT_FALSE(standby.CanServeSnapshot());
    standby.Stop();
    EXPECT_FALSE(standby.CanServeSnapshot());
}

TEST_F(P2PHotStandbyServiceTest, SnapshotReadinessFollowsServerLifecycle) {
    auto config = MakeStandbyConfig();
    config.snapshot_service_port = AllocateLoopbackPort();
    ASSERT_NE(config.snapshot_service_port, 0);
    P2PHotStandbyService standby(config);
    EXPECT_FALSE(standby.CanServeSnapshot());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    EXPECT_TRUE(standby.CanServeSnapshot());

    P2PStandbySnapshotClient client;
    P2PStandbyMetadataStore metadata;
    uint64_t baseline = 0;
    ASSERT_EQ(client.Bootstrap(
                  "127.0.0.1:" + std::to_string(config.snapshot_service_port),
                  kClusterId, &metadata, baseline, 256),
              ErrorCode::OK);
    ASSERT_EQ(standby.Promote(), ErrorCode::OK);
    EXPECT_FALSE(standby.CanServeSnapshot());
    standby.Stop();
    EXPECT_FALSE(standby.CanServeSnapshot());

    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    EXPECT_TRUE(standby.CanServeSnapshot());
    standby.Stop();
    EXPECT_FALSE(standby.CanServeSnapshot());
}

TEST_F(P2PHotStandbyServiceTest, SnapshotServerRejectsOccupiedPort) {
    P2PHotStandbyService first(MakeStandbyConfig());
    ASSERT_EQ(first.Start(), ErrorCode::OK);
    SnapshotServer snapshot(first);
    ASSERT_TRUE(snapshot.Start());

    auto config = MakeStandbyConfig();
    config.snapshot_service_port = snapshot.Port();
    P2PHotStandbyService second(config);
    EXPECT_EQ(second.Start(), ErrorCode::INTERNAL_ERROR);
    EXPECT_FALSE(second.CanServeSnapshot());

    second.Stop();
    EXPECT_FALSE(second.CanServeSnapshot());
    first.Stop();
}

TEST_F(P2PHotStandbyServiceTest, TrimSignalSwitchesToSnapshotResync) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{31, 31};
    const UUID segment_id{32, 32};
    RegisterClient(master, client_id, MakeSegment(segment_id));
    PublishRoute(master, "trim-resync-key", client_id, segment_id, 4096);

    P2PHotStandbyService source(MakeStandbyConfig());
    ASSERT_EQ(source.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        source.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));
    SnapshotServer snapshot(source);
    ASSERT_TRUE(snapshot.Start());

    std::mutex states_mutex;
    std::vector<std::shared_ptr<ControlledNotifierState>> states;
    auto factory = [&]() -> std::unique_ptr<OpLogStore> {
        auto state = std::make_shared<ControlledNotifierState>();
        {
            std::lock_guard<std::mutex> lock(states_mutex);
            states.push_back(state);
        }
        return std::make_unique<ControlledReaderStore>(state);
    };

    auto target_config = MakeStandbyConfig();
    target_config.snapshot_source_endpoints = {snapshot.Endpoint()};
    P2PHotStandbyService target(target_config, factory);
    ASSERT_EQ(target.Start(), ErrorCode::OK);

    std::shared_ptr<ControlledNotifierState> initial_state;
    {
        std::lock_guard<std::mutex> lock(states_mutex);
        ASSERT_FALSE(states.empty());
        initial_state = states.front();
    }
    InjectNotifierErrors(initial_state, ErrorCode::OPLOG_TRIMMED);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (std::chrono::steady_clock::now() < deadline) {
        size_t state_count = 0;
        {
            std::lock_guard<std::mutex> lock(states_mutex);
            state_count = states.size();
        }
        if (state_count >= 3 && target.GetState() == StandbyState::WATCHING) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(target.GetState(), StandbyState::WATCHING);
    auto exported = target.ExportMetadata();
    EXPECT_NE(exported.routes.find("trim-resync-key"), exported.routes.end());
}

TEST_F(P2PHotStandbyServiceTest, RestartedWorkerResumesPendingSnapshotResync) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{35, 35};
    const UUID segment_id{36, 36};
    RegisterClient(master, client_id, MakeSegment(segment_id));
    PublishRoute(master, "before-resync", client_id, segment_id);

    P2PHotStandbyService source(MakeStandbyConfig());
    ASSERT_EQ(source.Start(), ErrorCode::OK);
    ASSERT_TRUE(source.WaitForAppliedSequence(2));
    SnapshotServer snapshot(source);
    ASSERT_TRUE(snapshot.Start());

    auto initial_state = std::make_shared<ControlledNotifierState>();
    auto config = MakeStandbyConfig();
    config.snapshot_source_endpoints = {snapshot.Endpoint()};
    P2PHotStandbyService standby(config, [&] {
        return std::make_unique<ControlledReaderStore>(initial_state);
    });
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    ASSERT_TRUE(standby.GetMetadataStore()->RouteExists("before-resync"));

    // A trim event can arrive while promotion has stopped the recovery worker.
    // Restart must recover from the retained state without another
    // notification.
    P2PHotStandbyServiceTestAccess::StopRecoveryWorker(standby);
    PublishRoute(master, "after-resync", client_id, segment_id);
    ASSERT_TRUE(source.WaitForAppliedSequence(3));
    InjectNotifierErrors(initial_state, ErrorCode::OPLOG_TRIMMED);
    ASSERT_EQ(standby.GetState(), StandbyState::SYNCING);
    ASSERT_FALSE(standby.GetMetadataStore()->RouteExists("after-resync"));
    P2PHotStandbyServiceTestAccess::StartRecoveryWorker(standby);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (standby.GetState() != StandbyState::WATCHING &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(standby.GetState(), StandbyState::WATCHING);
    EXPECT_EQ(standby.GetLatestAppliedSequenceId(), 3);
    EXPECT_TRUE(standby.GetMetadataStore()->RouteExists("before-resync"));
    EXPECT_TRUE(standby.GetMetadataStore()->RouteExists("after-resync"));
}

TEST_F(P2PHotStandbyServiceTest, RestartedWorkerResumesPendingReconnect) {
    auto initial_state = std::make_shared<ControlledNotifierState>();
    std::atomic<int> readers_created{0};
    P2PHotStandbyService standby(MakeStandbyConfig(), [&] {
        ++readers_created;
        return std::make_unique<ControlledReaderStore>(initial_state);
    });
    ASSERT_EQ(standby.Start(/*baseline_sequence_id=*/7), ErrorCode::OK);
    P2PHotStandbyServiceTestAccess::StopRecoveryWorker(standby);
    EXPECT_EQ(readers_created.load(), 1);
    InjectNotifierErrors(initial_state, ErrorCode::INTERNAL_ERROR);
    ASSERT_EQ(standby.GetState(), StandbyState::RECONNECTING);
    P2PHotStandbyServiceTestAccess::StartRecoveryWorker(standby);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (standby.GetState() != StandbyState::WATCHING &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_EQ(standby.GetState(), StandbyState::WATCHING);
    EXPECT_EQ(readers_created.load(), 2);
    std::lock_guard<std::mutex> lock(initial_state->mutex);
    EXPECT_EQ(initial_state->start_sequence_id, 7);
    EXPECT_TRUE(initial_state->healthy);
}

TEST_F(P2PHotStandbyServiceTest, StopBeforeRecoveryAcquiresMetadataLock) {
    auto initial_state = std::make_shared<ControlledNotifierState>();
    std::atomic<int> readers_created{0};
    P2PHotStandbyService standby(MakeStandbyConfig(), [&] {
        ++readers_created;
        return std::make_unique<ControlledReaderStore>(initial_state);
    });
    ASSERT_EQ(standby.Start(/*baseline_sequence_id=*/7), ErrorCode::OK);
    P2PHotStandbyServiceTestAccess::StopRecoveryWorker(standby);
    InjectNotifierErrors(initial_state, ErrorCode::INTERNAL_ERROR);
    ASSERT_EQ(standby.GetState(), StandbyState::RECONNECTING);

    auto wait_until = [](auto predicate) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!predicate() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        return predicate();
    };
    auto metadata_lock =
        P2PHotStandbyServiceTestAccess::StartWorkerWithMetadataLocked(standby);
    ASSERT_TRUE(wait_until([&] {
        return !P2PHotStandbyServiceTestAccess::RecoveryRequested(standby);
    }));
    // The worker consumed its startup request but cannot begin recovery yet.
    std::thread stop_worker(
        [&] { P2PHotStandbyServiceTestAccess::StopRecoveryWorker(standby); });
    EXPECT_TRUE(wait_until([&] {
        return P2PHotStandbyServiceTestAccess::RecoveryStopping(standby);
    }));
    metadata_lock.unlock();
    stop_worker.join();

    EXPECT_EQ(readers_created.load(), 1);
    EXPECT_EQ(standby.GetState(), StandbyState::RECONNECTING);
    EXPECT_EQ(standby.GetLatestAppliedSequenceId(), 7);
}

TEST_F(P2PHotStandbyServiceTest, ReconnectsFromLastAppliedSequence) {
    auto& ha_status = HAStatusManager::instance();
    ha_status.Reset();
    ha_status.Configure(true, kClusterId, "test-standby");
    std::mutex states_mutex;
    std::vector<std::shared_ptr<ControlledNotifierState>> states;
    std::atomic<int> factory_calls{0};
    std::atomic<bool> allow_reconnect{false};
    auto factory = [&]() -> std::unique_ptr<OpLogStore> {
        const int call = ++factory_calls;
        auto state = std::make_shared<ControlledNotifierState>();
        if (call > 1 && !allow_reconnect.load()) {
            state->healthy_on_start = false;
        }
        {
            std::lock_guard<std::mutex> lock(states_mutex);
            states.push_back(state);
        }
        return std::make_unique<ControlledReaderStore>(state);
    };

    auto config = MakeStandbyConfig();
    config.reconnect_initial_backoff_ms = 1;
    config.reconnect_max_backoff_ms = 5;
    P2PHotStandbyService standby(config, factory);
    ASSERT_EQ(standby.Start(/*baseline_sequence_id=*/7), ErrorCode::OK);
    std::shared_ptr<ControlledNotifierState> first_state;
    {
        std::lock_guard<std::mutex> lock(states_mutex);
        ASSERT_EQ(states.size(), 1);
        first_state = states.front();
    }
    InjectNotifierErrors(first_state, ErrorCode::INTERNAL_ERROR);

    const auto failure_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < failure_deadline &&
           ha_status.SerializeDoctorJson().find("OPLOG_RECONNECT_FAILED") ==
               std::string::npos) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_NE(ha_status.SerializeDoctorJson().find("OPLOG_RECONNECT_FAILED"),
              std::string::npos);
    allow_reconnect.store(true);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        size_t state_count = 0;
        {
            std::lock_guard<std::mutex> lock(states_mutex);
            state_count = states.size();
        }
        if (state_count >= 2 && standby.GetState() == StandbyState::WATCHING &&
            ha_status.SerializeDoctorJson().find("OPLOG_RECONNECT_FAILED") ==
                std::string::npos) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::shared_ptr<ControlledNotifierState> second_state;
    {
        std::lock_guard<std::mutex> lock(states_mutex);
        ASSERT_GE(states.size(), 3);
        second_state = states.back();
    }
    EXPECT_GE(factory_calls.load(), 3);
    {
        std::lock_guard<std::mutex> lock(second_state->mutex);
        EXPECT_EQ(second_state->start_sequence_id, 7);
        EXPECT_TRUE(second_state->healthy);
    }
    EXPECT_EQ(standby.GetState(), StandbyState::WATCHING);
    EXPECT_EQ(ha_status.SerializeDoctorJson().find("OPLOG_RECONNECT_FAILED"),
              std::string::npos);
    standby.Stop();
    ha_status.Reset();
}

TEST_F(P2PHotStandbyServiceTest,
       ReadFailureSurvivesReconnectUntilSuccessfulPoll) {
    class PollingReaderStore : public MockOpLogStore {
       public:
        explicit PollingReaderStore(std::atomic<bool>& reads_succeed)
            : reads_succeed_(reads_succeed) {}
        ErrorCode ReadOpLogSince(uint64_t, size_t,
                                 std::vector<OpLogEntry>& entries) override {
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            entries.clear();
            return reads_succeed_.load() ? ErrorCode::OK
                                         : ErrorCode::INTERNAL_ERROR;
        }
        std::unique_ptr<OpLogChangeNotifier> CreateChangeNotifier(
            const std::string&, size_t poll_batch_size) override {
            return std::make_unique<PollingOpLogChangeNotifier>(
                this, 10, poll_batch_size);
        }

       private:
        std::atomic<bool>& reads_succeed_;
    };

    auto& status = HAStatusManager::instance();
    status.Reset();
    status.Configure(true, kClusterId, "test-standby");
    status.SetRoleState("standby", "WATCHING");
    std::atomic<bool> reads_succeed{true};
    std::atomic<int> readers_created{0};
    P2PHotStandbyService standby(MakeStandbyConfig(), [&] {
        ++readers_created;
        return std::make_unique<PollingReaderStore>(reads_succeed);
    });
    auto has_read_issue = [&] {
        return status.SerializeDoctorJson().find("OPLOG_READ_FAILED") !=
               std::string::npos;
    };
    auto wait_until = [](auto predicate) {
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (!predicate() && std::chrono::steady_clock::now() < deadline) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return predicate();
    };

    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    reads_succeed.store(false);
    EXPECT_TRUE(wait_until([&] { return readers_created.load() >= 3; }));
    EXPECT_TRUE(has_read_issue());
    EXPECT_NE(status.SerializeDoctorJson().find("degraded"), std::string::npos);

    const int before_reconnect = readers_created.load();
    EXPECT_TRUE(
        wait_until([&] { return readers_created.load() > before_reconnect; }));
    EXPECT_TRUE(has_read_issue());

    reads_succeed.store(true);
    EXPECT_TRUE(wait_until([&] {
        return standby.GetState() == StandbyState::WATCHING &&
               !has_read_issue() &&
               status.SerializeDoctorJson().find("\"ok\"") != std::string::npos;
    }));

    reads_succeed.store(false);
    EXPECT_TRUE(wait_until(has_read_issue));
    standby.Stop();
    EXPECT_FALSE(has_read_issue());
    status.Reset();
}

TEST_F(P2PHotStandbyServiceTest, StoppedRecoveryClearsIssuesBeforePromotion) {
    auto& status = HAStatusManager::instance();
    status.Reset();
    status.Configure(true, kClusterId, "test-standby");
    auto seed_issues = [&] {
        status.SetDoctorIssue("OPLOG_RECONNECT_FAILED", "previous reconnect");
        status.SetDoctorIssue("SNAPSHOT_RECOVERY_FAILED", "previous resync");
    };
    auto expect_no_recovery_issues = [&] {
        const auto report = status.SerializeDoctorJson();
        EXPECT_EQ(report.find("OPLOG_RECONNECT_FAILED"), std::string::npos);
        EXPECT_EQ(report.find("SNAPSHOT_RECOVERY_FAILED"), std::string::npos);
    };

    std::promise<void> watching;
    std::promise<void> release_recovery;
    auto watching_future = watching.get_future();
    auto release_future = release_recovery.get_future();
    auto first_state = std::make_shared<ControlledNotifierState>();
    int readers_created = 0;
    P2PHotStandbyService standby(MakeStandbyConfig(), [&] {
        auto state = first_state;
        if (++readers_created > 1) {
            state = std::make_shared<ControlledNotifierState>();
            if (readers_created == 2) {
                state->on_health_check = [&] {
                    watching.set_value();
                    release_future.wait();
                };
            }
        }
        return std::make_unique<ControlledReaderStore>(state);
    });

    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    seed_issues();
    InjectNotifierErrors(first_state, ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(watching_future.wait_for(std::chrono::seconds(2)),
              std::future_status::ready);
    EXPECT_EQ(standby.GetState(), StandbyState::WATCHING);

    std::thread stop_worker(
        [&] { P2PHotStandbyServiceTestAccess::StopRecoveryWorker(standby); });
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!P2PHotStandbyServiceTestAccess::RecoveryStopping(standby) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(P2PHotStandbyServiceTestAccess::RecoveryStopping(standby));
    release_recovery.set_value();
    stop_worker.join();
    expect_no_recovery_issues();

    seed_issues();
    ASSERT_EQ(standby.Promote(), ErrorCode::OK);
    expect_no_recovery_issues();
    status.SetLeader("test-standby", 1);
    status.SetRoleState("primary", "RUNNING");
    EXPECT_NE(status.SerializeDoctorJson().find("\"ok\""), std::string::npos);
    standby.Stop();
    status.Reset();
}

TEST_F(P2PHotStandbyServiceTest, CoalescesRepeatedWatchErrors) {
    std::mutex states_mutex;
    std::vector<std::shared_ptr<ControlledNotifierState>> states;
    std::atomic<int> factory_calls{0};
    auto factory = [&]() -> std::unique_ptr<OpLogStore> {
        ++factory_calls;
        auto state = std::make_shared<ControlledNotifierState>();
        {
            std::lock_guard<std::mutex> lock(states_mutex);
            states.push_back(state);
        }
        return std::make_unique<ControlledReaderStore>(state);
    };

    P2PHotStandbyService standby(MakeStandbyConfig(), factory);
    ASSERT_EQ(standby.Start(), ErrorCode::OK);

    std::shared_ptr<ControlledNotifierState> first_state;
    {
        std::lock_guard<std::mutex> lock(states_mutex);
        first_state = states.front();
    }
    InjectNotifierErrors(first_state, ErrorCode::INTERNAL_ERROR, 2);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline &&
           (factory_calls.load() < 2 ||
            standby.GetState() != StandbyState::WATCHING)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    EXPECT_EQ(factory_calls.load(), 2);
    EXPECT_EQ(standby.GetState(), StandbyState::WATCHING);
    standby.Stop();
}

TEST_F(P2PHotStandbyServiceTest, StopInterruptsReconnectBackoff) {
    std::shared_ptr<ControlledNotifierState> first_state;
    std::atomic<int> factory_calls{0};
    auto factory = [&]() -> std::unique_ptr<OpLogStore> {
        const int call = ++factory_calls;
        if (call > 1) {
            return nullptr;
        }
        first_state = std::make_shared<ControlledNotifierState>();
        return std::make_unique<ControlledReaderStore>(first_state);
    };

    auto config = MakeStandbyConfig();
    config.reconnect_initial_backoff_ms = 1000;
    config.reconnect_max_backoff_ms = 1000;
    P2PHotStandbyService standby(config, factory);
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    InjectNotifierErrors(first_state, ErrorCode::INTERNAL_ERROR);

    const auto reconnect_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < reconnect_deadline &&
           factory_calls.load() < 2) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_GE(factory_calls.load(), 2);

    const auto stop_start = std::chrono::steady_clock::now();
    standby.Stop();
    const auto stop_elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - stop_start);

    EXPECT_LT(stop_elapsed, std::chrono::milliseconds(250));
    EXPECT_EQ(standby.GetState(), StandbyState::STOPPED);
    const int calls_after_stop = factory_calls.load();
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    EXPECT_EQ(factory_calls.load(), calls_after_stop);
}

TEST_F(P2PHotStandbyServiceTest, CapsInitialReconnectBackoffAtMaximum) {
    std::shared_ptr<ControlledNotifierState> first_state;
    std::atomic<int> factory_calls{0};
    auto factory = [&]() -> std::unique_ptr<OpLogStore> {
        if (++factory_calls > 1) {
            return nullptr;
        }
        first_state = std::make_shared<ControlledNotifierState>();
        return std::make_unique<ControlledReaderStore>(first_state);
    };

    auto config = MakeStandbyConfig();
    config.reconnect_initial_backoff_ms = 1000;
    config.reconnect_max_backoff_ms = 1;
    P2PHotStandbyService standby(config, factory);
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    InjectNotifierErrors(first_state, ErrorCode::INTERNAL_ERROR);

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(250);
    while (std::chrono::steady_clock::now() < deadline &&
           factory_calls.load() < 3) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_GE(factory_calls.load(), 3);
    standby.Stop();
}

TEST_F(P2PHotStandbyServiceTest, SerializesConcurrentPromoteAndStop) {
    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);

    ErrorCode promote_result = ErrorCode::INTERNAL_ERROR;
    std::thread promote_thread([&] { promote_result = standby.Promote(); });
    std::thread stop_thread([&] { standby.Stop(); });
    promote_thread.join();
    stop_thread.join();

    EXPECT_TRUE(promote_result == ErrorCode::OK ||
                promote_result == ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);
    EXPECT_EQ(standby.GetState(), StandbyState::STOPPED);
}

TEST_F(P2PHotStandbyServiceTest, UnmountSegmentCascadeIsReplayed) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{3, 3};
    const UUID segment_id{4, 4};

    RegisterClient(master, client_id, MakeSegment(segment_id));
    PublishRoute(master, "key-cascade", client_id, segment_id);
    auto unmount = master.UnmountSegment(segment_id, client_id);
    ASSERT_TRUE(unmount.has_value()) << toString(unmount.error());

    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        standby.WaitForAppliedSequence(3, std::chrono::milliseconds(2000)));
    standby.Stop();

    auto exported = standby.ExportMetadata();
    EXPECT_EQ(exported.routes.find("key-cascade"), exported.routes.end());
    auto client_it = exported.clients.find(client_id);
    ASSERT_NE(client_it, exported.clients.end());
    EXPECT_TRUE(client_it->second.segments.empty());
}

TEST_F(P2PHotStandbyServiceTest, PromoteFinalCatchUpExportsLateEntry) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{5, 5};
    const UUID segment_id{6, 6};

    RegisterClient(master, client_id, MakeSegment(segment_id));

    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        standby.WaitForAppliedSequence(1, std::chrono::milliseconds(2000)));

    PublishRoute(master, "key-late", client_id, segment_id, 8192);
    ASSERT_TRUE(WaitForPersistedEntry(2, std::chrono::milliseconds(2000)));
    ASSERT_EQ(standby.Promote(), ErrorCode::OK);
    EXPECT_EQ(standby.GetState(), StandbyState::PROMOTED);
    EXPECT_GE(standby.GetLatestAppliedSequenceId(), 2);

    auto exported = standby.ExportMetadata();
    auto object_it = exported.routes.find("key-late");
    ASSERT_NE(object_it, exported.routes.end());
    EXPECT_EQ(object_it->second.object_size, 8192);
}

TEST_F(P2PHotStandbyServiceTest, PromotionFailsOnFinalCatchUpApplyFailure) {
    LocalFsOpLogStore writer(kClusterId, test_dir_.string(),
                             /*enable_batch_write=*/true,
                             /*poll_interval_ms=*/10);
    ASSERT_EQ(writer.Init(), ErrorCode::OK);

    auto config = MakeStandbyConfig();
    config.oplog_poll_interval_ms = 10000;
    P2PHotStandbyService standby(std::move(config));
    ASSERT_EQ(standby.Start(), ErrorCode::OK);

    OpLogEntry invalid_entry;
    invalid_entry.sequence_id = 1;
    invalid_entry.op_type = OpType_REGISTER_CLIENT;
    invalid_entry.payload = "invalid-payload";
    invalid_entry.checksum =
        XXH32(invalid_entry.payload.data(), invalid_entry.payload.size(), 0);
    ASSERT_EQ(writer.WriteOpLog(invalid_entry, /*sync=*/true), ErrorCode::OK);

    EXPECT_EQ(standby.Promote(), ErrorCode::INTERNAL_ERROR);
    EXPECT_EQ(standby.GetState(), StandbyState::FAILED);
    EXPECT_FALSE(standby.GetSyncStatus().apply_healthy);
}

TEST_F(P2PHotStandbyServiceTest, ForcePromoteAfterApplyFailure) {
    LocalFsOpLogStore writer(kClusterId, test_dir_.string(),
                             /*enable_batch_write=*/true,
                             /*poll_interval_ms=*/10);
    ASSERT_EQ(writer.Init(), ErrorCode::OK);

    OpLogEntry invalid_entry;
    invalid_entry.sequence_id = 1;
    invalid_entry.op_type = OpType_REGISTER_CLIENT;
    invalid_entry.payload = "invalid-payload";
    invalid_entry.checksum =
        XXH32(invalid_entry.payload.data(), invalid_entry.payload.size(), 0);
    ASSERT_EQ(writer.WriteOpLog(invalid_entry, /*sync=*/true), ErrorCode::OK);

    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    for (int i = 0; i < 100 && standby.GetState() != StandbyState::FAILED;
         ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    auto status = standby.GetSyncStatus();
    ASSERT_EQ(status.state, StandbyState::FAILED);
    EXPECT_FALSE(status.apply_healthy);
    EXPECT_EQ(status.failed_sequence_id, 1u);
    EXPECT_EQ(status.failed_op_type, static_cast<int>(OpType_REGISTER_CLIENT));
    EXPECT_EQ(status.failure_reason, "operation apply failed");
    EXPECT_EQ(standby.Promote(), ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS);

    EXPECT_EQ(standby.Promote(/*force=*/true), ErrorCode::OK);
    EXPECT_EQ(standby.GetState(), StandbyState::PROMOTED);
    EXPECT_EQ(standby.GetLatestAppliedSequenceId(), 0u);
}

TEST_F(P2PHotStandbyServiceTest, RestoreExportedMetadataIntoP2PMasterService) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{7, 7};
    const UUID segment_id{8, 8};
    const auto segment = MakeSegment(segment_id);

    RegisterClient(master, client_id, segment);
    PublishRoute(master, "key-restore", client_id, segment_id, 2048);

    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        standby.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));
    ASSERT_EQ(standby.Promote(), ErrorCode::OK);
    const uint64_t promoted_sequence_id = standby.GetLatestAppliedSequenceId();

    P2PMasterService restored_master(MakeMasterConfig());
    ASSERT_EQ(restored_master.RestoreFromStandbyMetadata(
                  standby.ExportMetadata(), promoted_sequence_id),
              ErrorCode::OK);

    // Promotion restores metadata, but does not infer a live client's
    // readiness.
    auto blocked = restored_master.GetReadRoute("key-restore");
    ASSERT_FALSE(blocked.has_value());
    EXPECT_EQ(blocked.error(), ErrorCode::REPLICA_IS_NOT_READY);

    P2PGetWriteRouteRequest route_req;
    route_req.client_id = {99, 99};
    route_req.key = "new-key-after-restore";
    route_req.object_size = 1024;

    // Another handoff must retain unready clients and their routes without
    // making them available before the client confirms ONLINE.
    const auto exported_before_heartbeat =
        restored_master.ExportMetadataForStandby();
    ASSERT_TRUE(exported_before_heartbeat.clients.contains(client_id));
    ASSERT_TRUE(exported_before_heartbeat.routes.contains("key-restore"));
    {
        P2PMasterService next_restored_master(MakeMasterConfig());
        ASSERT_EQ(next_restored_master.RestoreFromStandbyMetadata(
                      exported_before_heartbeat, promoted_sequence_id),
                  ErrorCode::OK);
        auto next_read = next_restored_master.GetReadRoute("key-restore");
        ASSERT_FALSE(next_read.has_value());
        EXPECT_EQ(next_read.error(), ErrorCode::REPLICA_IS_NOT_READY);
        auto next_write = next_restored_master.GetWriteRoute(route_req);
        ASSERT_FALSE(next_write.has_value());
        EXPECT_EQ(next_write.error(), ErrorCode::NO_AVAILABLE_CANDIDATE);

        auto next_heartbeat = next_restored_master.Heartbeat(
            {.client_id = client_id, .tasks = {}});
        ASSERT_TRUE(next_heartbeat.has_value());
        EXPECT_EQ(next_heartbeat->status, P2PClientStatus::HEALTH);
        next_read = next_restored_master.GetReadRoute("key-restore");
        ASSERT_FALSE(next_read.has_value());
        EXPECT_EQ(next_read.error(), ErrorCode::REPLICA_IS_NOT_READY);
        next_write = next_restored_master.GetWriteRoute(route_req);
        ASSERT_FALSE(next_write.has_value());
        EXPECT_EQ(next_write.error(), ErrorCode::NO_AVAILABLE_CANDIDATE);

        ASSERT_TRUE(
            next_restored_master
                .Heartbeat({.client_id = client_id,
                            .tasks = {},
                            .service_state = P2PClientServiceState::ONLINE})
                .has_value());
        next_read = next_restored_master.GetReadRoute("key-restore");
        ASSERT_TRUE(next_read.has_value());
        ASSERT_EQ(next_read->size(), 1);
        EXPECT_EQ(next_read->front().client_id, client_id);
        EXPECT_EQ(next_read->front().segment_id, segment_id);
        EXPECT_EQ(next_read->front().object_size, 2048);
        next_write = next_restored_master.GetWriteRoute(route_req);
        ASSERT_TRUE(next_write.has_value());
        ASSERT_EQ(next_write->size(), 1);
        EXPECT_EQ(next_write->front().client_id, client_id);
    }

    auto heartbeat = restored_master.Heartbeat({.client_id = client_id});
    ASSERT_TRUE(heartbeat.has_value());
    ASSERT_TRUE(restored_master
                    .Heartbeat({.client_id = client_id,
                                .service_state = P2PClientServiceState::ONLINE})
                    .has_value());

    auto ip_result = restored_master.QueryIp(client_id);
    ASSERT_TRUE(ip_result.has_value()) << toString(ip_result.error());
    ASSERT_EQ(ip_result.value().size(), 1);
    EXPECT_EQ(ip_result.value()[0], "127.0.0.1");

    auto segments_result = restored_master.GetClientSegments(client_id);
    ASSERT_TRUE(segments_result.has_value())
        << toString(segments_result.error());
    ASSERT_EQ(segments_result.value().size(), 1);
    EXPECT_EQ(segments_result.value()[0], segment.name);

    auto replica_result = restored_master.GetReadRoute("key-restore");
    ASSERT_TRUE(replica_result.has_value()) << toString(replica_result.error());
    ASSERT_EQ(replica_result.value().size(), 1);
    const auto& route = replica_result.value()[0];
    EXPECT_EQ(route.client_id, client_id);
    EXPECT_EQ(route.segment_id, segment_id);
    EXPECT_EQ(route.ip_address, "127.0.0.1");
    EXPECT_EQ(route.rpc_port, 50051);
    EXPECT_EQ(route.object_size, 2048);

    auto route_result = restored_master.GetWriteRoute(route_req);
    ASSERT_TRUE(route_result.has_value()) << toString(route_result.error());
    ASSERT_FALSE(route_result.value().empty());
    EXPECT_EQ(route_result.value()[0].client_id, client_id);

    PublishRoute(restored_master, "key-after-restore", client_id, segment_id,
                 1024);
    ASSERT_NE(restored_master.GetOpLogManager(), nullptr);
    EXPECT_EQ(restored_master.GetOpLogManager()->GetLastSequenceId(),
              promoted_sequence_id + 1);

    auto added_replica_result =
        restored_master.GetReadRoute("key-after-restore");
    ASSERT_TRUE(added_replica_result.has_value())
        << toString(added_replica_result.error());
    ASSERT_EQ(added_replica_result.value().size(), 1);

    WithdrawRoute(restored_master, "key-after-restore", client_id, segment_id);
    EXPECT_EQ(restored_master.GetOpLogManager()->GetLastSequenceId(),
              promoted_sequence_id + 2);
}

TEST_F(P2PHotStandbyServiceTest, LatestReadFailureAbortsBeforeMetadataRestore) {
    class FailedLatestStore : public MockOpLogStore {
       public:
        ErrorCode GetLatestSequenceId(uint64_t&) override {
            return ErrorCode::INTERNAL_ERROR;
        }
    };

    P2PMasterService source(MakeMasterConfig());
    RegisterClient(source, UUID{71, 71}, MakeSegment(UUID{72, 72}));
    const auto metadata = source.ExportMetadataForStandby();
    ASSERT_EQ(metadata.clients.size(), 1);

    P2PMasterService promoted(MakeMasterConfig());
    promoted.GetOpLogManager()->SetOpLogStore(
        std::make_shared<FailedLatestStore>());
    EXPECT_EQ(promoted.RestoreFromStandbyMetadata(metadata, 1),
              ErrorCode::INTERNAL_ERROR);
    EXPECT_TRUE(promoted.GetClientManager().GetAllClients().empty());
    EXPECT_EQ(promoted.GetOpLogManager()->GetLastSequenceId(), 0);
}

TEST_F(P2PHotStandbyServiceTest, RestorePromotedMetadataIntoWrappedRuntime) {
    P2PMasterService primary_master(MakeMasterConfig());
    const UUID client_id{17, 17};
    const UUID segment_id{18, 18};
    const auto segment = MakeSegment(segment_id);

    RegisterClient(primary_master, client_id, segment);
    PublishRoute(primary_master, "runtime-key", client_id, segment_id, 4096);

    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        standby.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));
    ASSERT_EQ(standby.Promote(), ErrorCode::OK);
    const uint64_t promoted_sequence_id = standby.GetLatestAppliedSequenceId();

    P2PMasterRpcService promoted_runtime(MakeWrappedMasterConfig());
    auto& promoted_master =
        static_cast<P2PMasterService&>(promoted_runtime.GetMasterService());
    ASSERT_EQ(promoted_master.RestoreFromStandbyMetadata(
                  standby.ExportMetadata(), promoted_sequence_id),
              ErrorCode::OK);

    ASSERT_TRUE(promoted_runtime
                    .Heartbeat({.client_id = client_id,
                                .service_state = P2PClientServiceState::ONLINE})
                    .has_value());
    auto replica_result = promoted_runtime.GetReadRoute(
        P2PGetReadRouteRequest{.key = "runtime-key"});
    ASSERT_TRUE(replica_result.has_value()) << toString(replica_result.error());
    ASSERT_EQ(replica_result.value().size(), 1);

    P2PGetWriteRouteRequest route_req;
    route_req.client_id = {99, 99};
    route_req.key = "runtime-key-after-promotion";
    route_req.object_size = 1024;
    auto route_result = promoted_runtime.GetWriteRoute(route_req);
    ASSERT_TRUE(route_result.has_value()) << toString(route_result.error());
    ASSERT_FALSE(route_result.value().empty());
    EXPECT_EQ(route_result.value()[0].client_id, client_id);

    P2PPublishRouteRequest add_req;
    add_req.key = "runtime-key-after-promotion";
    add_req.client_id = client_id;
    add_req.segment_id = segment_id;
    add_req.object_size = 1024;
    ASSERT_TRUE(promoted_runtime.PublishRoute(add_req).has_value());

    ASSERT_NE(promoted_master.GetOpLogManager(), nullptr);
    EXPECT_EQ(promoted_master.GetOpLogManager()->GetLastSequenceId(),
              promoted_sequence_id + 1);
}

TEST_F(P2PHotStandbyServiceTest, PromotedRuntimeContinuesP2PMasterFlow) {
    P2PMasterService primary_master(MakeMasterConfig());
    const UUID original_client_id{27, 27};
    const UUID original_segment_id{28, 28};
    const auto original_segment = MakeSegment(original_segment_id);

    RegisterClient(primary_master, original_client_id, original_segment);
    PublishRoute(primary_master, "flow-key-before-promotion",
                 original_client_id, original_segment_id, 4096);

    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        standby.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));
    ASSERT_EQ(standby.Promote(), ErrorCode::OK);
    const uint64_t promoted_sequence_id = standby.GetLatestAppliedSequenceId();

    P2PMasterRpcService promoted_runtime(MakeWrappedMasterConfig());
    auto& promoted_master =
        static_cast<P2PMasterService&>(promoted_runtime.GetMasterService());
    ASSERT_EQ(promoted_master.RestoreFromStandbyMetadata(
                  standby.ExportMetadata(), promoted_sequence_id),
              ErrorCode::OK);

    ASSERT_TRUE(promoted_runtime
                    .Heartbeat({.client_id = original_client_id,
                                .service_state = P2PClientServiceState::ONLINE})
                    .has_value());
    auto restored_replica = promoted_runtime.GetReadRoute(
        P2PGetReadRouteRequest{.key = "flow-key-before-promotion"});
    ASSERT_TRUE(restored_replica.has_value())
        << toString(restored_replica.error());
    ASSERT_EQ(restored_replica.value().size(), 1);

    const UUID rejoined_client_id{29, 29};
    const UUID rejoined_segment_id{30, 30};
    const auto rejoined_segment = MakeSegment(rejoined_segment_id);
    RegisterClient(promoted_master, rejoined_client_id, rejoined_segment);

    P2PGetWriteRouteRequest route_req;
    route_req.client_id = {31, 31};
    route_req.key = "flow-key-after-promotion";
    route_req.object_size = 1024;
    auto route_result = promoted_runtime.GetWriteRoute(route_req);
    ASSERT_TRUE(route_result.has_value()) << toString(route_result.error());
    ASSERT_FALSE(route_result.value().empty());

    P2PPublishRouteRequest add_req;
    add_req.key = route_req.key;
    add_req.client_id = rejoined_client_id;
    add_req.segment_id = rejoined_segment_id;
    add_req.object_size = route_req.object_size;
    ASSERT_TRUE(promoted_runtime.PublishRoute(add_req).has_value());

    auto added_replica = promoted_runtime.GetReadRoute(
        P2PGetReadRouteRequest{.key = route_req.key});
    ASSERT_TRUE(added_replica.has_value()) << toString(added_replica.error());
    ASSERT_EQ(added_replica.value().size(), 1);
    const auto& p2p_desc = added_replica.value()[0];
    EXPECT_EQ(p2p_desc.client_id, rejoined_client_id);
    EXPECT_EQ(p2p_desc.segment_id, rejoined_segment_id);

    P2PWithdrawRouteRequest remove_req;
    remove_req.key = route_req.key;
    remove_req.client_id = rejoined_client_id;
    remove_req.segment_id = rejoined_segment_id;
    ASSERT_TRUE(promoted_runtime.WithdrawRoute(remove_req).has_value());

    ASSERT_NE(promoted_master.GetOpLogManager(), nullptr);
    EXPECT_EQ(promoted_master.GetOpLogManager()->GetLastSequenceId(),
              promoted_sequence_id + 3);
}

}  // namespace
}  // namespace mooncake::test
