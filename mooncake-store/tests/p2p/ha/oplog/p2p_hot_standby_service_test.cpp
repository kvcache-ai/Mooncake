#include "p2p/ha/oplog/p2p_hot_standby_service.h"

#include <unistd.h>

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <thread>
#include <variant>

#include <xxhash.h>

#include "p2p/ha/oplog/localfs_oplog_store.h"
#include "mock_oplog_store.h"
#include "p2p/master/p2p_master_service.h"
#include "p2p/master/p2p_rpc_service.h"
#include "p2p/master/p2p_rpc_types.h"

namespace mooncake::test {
namespace {

struct ControlledNotifierState {
    std::mutex mutex;
    std::condition_variable cv;
    OpLogChangeNotifier::ErrorCallback on_error;
    uint64_t start_sequence_id{0};
    int active_callbacks{0};
    bool healthy_on_start{true};
    bool healthy{false};
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
        const std::string&) override {
        return std::make_unique<ControlledNotifier>(state_);
    }

   private:
    std::shared_ptr<ControlledNotifierState> state_;
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

    MasterServiceConfig MakeMasterConfig() const {
        return MasterServiceConfig::builder()
            .set_enable_ha(true)
            .set_enable_oplog(true)
            .set_cluster_id(kClusterId)
            .set_oplog_store_type("localfs")
            .set_oplog_data_dir(test_dir_.string())
            .set_max_client_per_key(0)
            .build();
    }

    WrappedMasterServiceConfig MakeWrappedMasterConfig() const {
        auto master_config = MakeMasterConfig();
        WrappedMasterServiceConfig config;
        config.default_kv_lease_ttl = master_config.default_kv_lease_ttl;
        config.default_kv_soft_pin_ttl = master_config.default_kv_soft_pin_ttl;
        config.allow_evict_soft_pinned_objects =
            master_config.allow_evict_soft_pinned_objects;
        config.enable_metric_reporting = false;
        config.enable_ha = master_config.enable_ha;
        config.enable_oplog = master_config.enable_oplog;
        config.oplog_store_type = master_config.oplog_store_type;
        config.oplog_data_dir = master_config.oplog_data_dir;
        config.cluster_id = master_config.cluster_id;
        config.max_client_per_key = master_config.max_client_per_key;
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

    Segment MakeSegment(const UUID& segment_id) const {
        Segment segment;
        segment.id = segment_id;
        segment.name = "segment-" + std::to_string(segment_id.first) + "-" +
                       std::to_string(segment_id.second);
        segment.size = 1024 * 1024;
        segment.extra = P2PSegmentExtraData{
            .priority = 1,
            .tags = {},
            .memory_type = MemoryType::DRAM,
        };
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
                        const Segment& segment) const {
        RegisterClientRequest req;
        req.client_id = client_id;
        req.ip_address = "127.0.0.1";
        req.rpc_port = 50051;
        req.segments = {segment};
        req.deployment_mode = DeploymentMode::P2P;
        auto result = service.RegisterClient(req);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    void AddReplica(P2PMasterService& service, const std::string& key,
                    const UUID& client_id, const UUID& segment_id,
                    size_t size = 4096) const {
        AddReplicaRequest req;
        req.key = key;
        req.client_id = client_id;
        req.segment_id = segment_id;
        req.size = size;
        auto result = service.AddReplica(req);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    void RemoveReplica(P2PMasterService& service, const std::string& key,
                       const UUID& client_id, const UUID& segment_id) const {
        RemoveReplicaRequest req;
        req.key = key;
        req.client_id = client_id;
        req.segment_id = segment_id;
        auto result = service.RemoveReplica(req);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    static constexpr const char* kClusterId = "p2p-hot-standby-test";
    std::filesystem::path test_dir_;
};

TEST_F(P2PHotStandbyServiceTest, ReplicatesMasterWrittenP2POplog) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{1, 1};
    const UUID segment_id{2, 2};

    RegisterClient(master, client_id, MakeSegment(segment_id));
    AddReplica(master, "key-a", client_id, segment_id, 1234);

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

    auto object_it = exported.objects.find("key-a");
    ASSERT_NE(object_it, exported.objects.end());
    EXPECT_EQ(object_it->second.size, 1234);
    ASSERT_EQ(object_it->second.replicas.size(), 1);
    ASSERT_TRUE(std::holds_alternative<P2PProxyDescriptor>(
        object_it->second.replicas[0].descriptor_variant));
    const auto& desc = std::get<P2PProxyDescriptor>(
        object_it->second.replicas[0].descriptor_variant);
    EXPECT_EQ(desc.client_id, client_id);
    EXPECT_EQ(desc.segment_id, segment_id);
    EXPECT_EQ(desc.ip_address, "127.0.0.1");
    EXPECT_EQ(desc.rpc_port, 50051);
}

TEST_F(P2PHotStandbyServiceTest, BootstrapsFromStandbySnapshotSource) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{11, 11};
    const UUID segment_id{12, 12};
    RegisterClient(master, client_id, MakeSegment(segment_id));
    AddReplica(master, "snapshot-key", client_id, segment_id, 8192);

    auto source_config = MakeStandbyConfig();
    source_config.snapshot_service_port =
        static_cast<uint16_t>(49000 + (::getpid() % 1000));
    P2PHotStandbyService source(source_config);
    ASSERT_EQ(source.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        source.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));

    auto target_config = MakeStandbyConfig();
    target_config.snapshot_source_endpoints = {
        "127.0.0.1:" + std::to_string(source_config.snapshot_service_port)};
    target_config.snapshot_chunk_size = 1;
    P2PHotStandbyService target(target_config);
    ASSERT_EQ(target.Start(), ErrorCode::OK);
    ASSERT_GE(target.GetLatestAppliedSequenceId(), 2);

    auto exported = target.ExportMetadata();
    ASSERT_NE(exported.clients.find(client_id), exported.clients.end());
    auto object = exported.objects.find("snapshot-key");
    ASSERT_NE(object, exported.objects.end());
    EXPECT_EQ(object->second.size, 8192);
    ASSERT_EQ(object->second.replicas.size(), 1);

    target.Stop();
    source.Stop();
}

TEST_F(P2PHotStandbyServiceTest,
       SnapshotKeepsFixedInventoryWhileSourceContinuesApplying) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{21, 21};
    const UUID segment_id{22, 22};
    RegisterClient(master, client_id, MakeSegment(segment_id));
    AddReplica(master, "before-snapshot", client_id, segment_id, 4096);

    P2PHotStandbyService source(MakeStandbyConfig());
    ASSERT_EQ(source.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        source.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));

    P2PStandbySnapshotService snapshot(&source);
    auto begin = snapshot.BeginSnapshot({kClusterId});
    ASSERT_EQ(fromInt(begin.error_code), ErrorCode::OK);
    EXPECT_EQ(begin.baseline_sequence_id, 2);

    AddReplica(master, "after-snapshot", client_id, segment_id, 8192);
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

TEST_F(P2PHotStandbyServiceTest, SnapshotServerRejectsOccupiedPort) {
    auto config = MakeStandbyConfig();
    config.snapshot_service_port =
        static_cast<uint16_t>(50000 + (::getpid() % 1000));

    P2PHotStandbyService first(config);
    ASSERT_EQ(first.Start(), ErrorCode::OK);

    P2PHotStandbyService second(config);
    EXPECT_EQ(second.Start(), ErrorCode::INTERNAL_ERROR);

    second.Stop();
    first.Stop();
}

TEST_F(P2PHotStandbyServiceTest, TrimSignalSwitchesToSnapshotResync) {
    P2PMasterService master(MakeMasterConfig());
    const UUID client_id{31, 31};
    const UUID segment_id{32, 32};
    RegisterClient(master, client_id, MakeSegment(segment_id));
    AddReplica(master, "trim-resync-key", client_id, segment_id, 4096);

    auto source_config = MakeStandbyConfig();
    source_config.snapshot_service_port =
        static_cast<uint16_t>(50000 + (::getpid() % 1000));
    P2PHotStandbyService source(source_config);
    ASSERT_EQ(source.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        source.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));

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
    target_config.snapshot_source_endpoints = {
        "127.0.0.1:" + std::to_string(source_config.snapshot_service_port)};
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
    EXPECT_NE(exported.objects.find("trim-resync-key"), exported.objects.end());
}

TEST_F(P2PHotStandbyServiceTest, ReconnectsFromLastAppliedSequence) {
    std::mutex states_mutex;
    std::vector<std::shared_ptr<ControlledNotifierState>> states;
    std::atomic<int> factory_calls{0};
    auto factory = [&]() -> std::unique_ptr<OpLogStore> {
        const int call = ++factory_calls;
        auto state = std::make_shared<ControlledNotifierState>();
        if (call == 2) {
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

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        size_t state_count = 0;
        {
            std::lock_guard<std::mutex> lock(states_mutex);
            state_count = states.size();
        }
        if (state_count >= 2 && standby.GetState() == StandbyState::WATCHING) {
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
    standby.Stop();
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
    AddReplica(master, "key-cascade", client_id, segment_id);
    auto unmount = master.UnmountSegment(segment_id, client_id);
    ASSERT_TRUE(unmount.has_value()) << toString(unmount.error());

    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        standby.WaitForAppliedSequence(3, std::chrono::milliseconds(2000)));
    standby.Stop();

    auto exported = standby.ExportMetadata();
    EXPECT_EQ(exported.objects.find("key-cascade"), exported.objects.end());
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

    AddReplica(master, "key-late", client_id, segment_id, 8192);
    ASSERT_TRUE(WaitForPersistedEntry(2, std::chrono::milliseconds(2000)));
    ASSERT_EQ(standby.Promote(), ErrorCode::OK);
    EXPECT_EQ(standby.GetState(), StandbyState::PROMOTED);
    EXPECT_GE(standby.GetLatestAppliedSequenceId(), 2);

    auto exported = standby.ExportMetadata();
    auto object_it = exported.objects.find("key-late");
    ASSERT_NE(object_it, exported.objects.end());
    EXPECT_EQ(object_it->second.size, 8192);
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
    AddReplica(master, "key-restore", client_id, segment_id, 2048);

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

    auto ip_result = restored_master.QueryIp(client_id);
    ASSERT_TRUE(ip_result.has_value()) << toString(ip_result.error());
    ASSERT_EQ(ip_result.value().size(), 1);
    EXPECT_EQ(ip_result.value()[0], "127.0.0.1");

    auto segments_result = restored_master.GetClientSegments(client_id);
    ASSERT_TRUE(segments_result.has_value())
        << toString(segments_result.error());
    ASSERT_EQ(segments_result.value().size(), 1);
    EXPECT_EQ(segments_result.value()[0], segment.name);

    auto replica_result = restored_master.GetReplicaList("key-restore");
    ASSERT_TRUE(replica_result.has_value()) << toString(replica_result.error());
    ASSERT_EQ(replica_result.value().replicas.size(), 1);
    const auto& replica = replica_result.value().replicas[0];
    ASSERT_TRUE(replica.is_p2p_proxy_replica());
    const auto& p2p_desc = replica.get_p2p_proxy_descriptor();
    EXPECT_EQ(p2p_desc.client_id, client_id);
    EXPECT_EQ(p2p_desc.segment_id, segment_id);
    EXPECT_EQ(p2p_desc.ip_address, "127.0.0.1");
    EXPECT_EQ(p2p_desc.rpc_port, 50051);
    EXPECT_EQ(p2p_desc.object_size, 2048);

    WriteRouteRequest route_req;
    route_req.client_id = {99, 99};
    route_req.key = "new-key-after-restore";
    route_req.size = 1024;
    auto route_result = restored_master.GetWriteRoute(route_req);
    ASSERT_TRUE(route_result.has_value()) << toString(route_result.error());
    ASSERT_FALSE(route_result.value().candidates.empty());
    EXPECT_EQ(route_result.value().candidates[0].client_id, client_id);

    AddReplica(restored_master, "key-after-restore", client_id, segment_id,
               1024);
    ASSERT_NE(restored_master.GetOpLogManager(), nullptr);
    EXPECT_EQ(restored_master.GetOpLogManager()->GetLastSequenceId(),
              promoted_sequence_id + 1);

    auto added_replica_result =
        restored_master.GetReplicaList("key-after-restore");
    ASSERT_TRUE(added_replica_result.has_value())
        << toString(added_replica_result.error());
    ASSERT_EQ(added_replica_result.value().replicas.size(), 1);

    RemoveReplica(restored_master, "key-after-restore", client_id, segment_id);
    EXPECT_EQ(restored_master.GetOpLogManager()->GetLastSequenceId(),
              promoted_sequence_id + 2);
}

TEST_F(P2PHotStandbyServiceTest, RestorePromotedMetadataIntoWrappedRuntime) {
    P2PMasterService primary_master(MakeMasterConfig());
    const UUID client_id{17, 17};
    const UUID segment_id{18, 18};
    const auto segment = MakeSegment(segment_id);

    RegisterClient(primary_master, client_id, segment);
    AddReplica(primary_master, "runtime-key", client_id, segment_id, 4096);

    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        standby.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));
    ASSERT_EQ(standby.Promote(), ErrorCode::OK);
    const uint64_t promoted_sequence_id = standby.GetLatestAppliedSequenceId();

    WrappedP2PMasterService promoted_runtime(MakeWrappedMasterConfig());
    auto& promoted_master =
        static_cast<P2PMasterService&>(promoted_runtime.GetMasterService());
    ASSERT_EQ(promoted_master.RestoreFromStandbyMetadata(
                  standby.ExportMetadata(), promoted_sequence_id),
              ErrorCode::OK);

    auto replica_result =
        promoted_runtime.GetReplicaListInternal("runtime-key");
    ASSERT_TRUE(replica_result.has_value()) << toString(replica_result.error());
    ASSERT_EQ(replica_result.value().replicas.size(), 1);

    WriteRouteRequest route_req;
    route_req.client_id = {99, 99};
    route_req.key = "runtime-key-after-promotion";
    route_req.size = 1024;
    auto route_result = promoted_runtime.GetWriteRoute(route_req);
    ASSERT_TRUE(route_result.has_value()) << toString(route_result.error());
    ASSERT_FALSE(route_result.value().candidates.empty());
    EXPECT_EQ(route_result.value().candidates[0].client_id, client_id);

    AddReplicaRequest add_req;
    add_req.key = "runtime-key-after-promotion";
    add_req.client_id = client_id;
    add_req.segment_id = segment_id;
    add_req.size = 1024;
    ASSERT_TRUE(promoted_runtime.AddReplica(add_req).has_value());

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
    AddReplica(primary_master, "flow-key-before-promotion", original_client_id,
               original_segment_id, 4096);

    P2PHotStandbyService standby(MakeStandbyConfig());
    ASSERT_EQ(standby.Start(), ErrorCode::OK);
    ASSERT_TRUE(
        standby.WaitForAppliedSequence(2, std::chrono::milliseconds(2000)));
    ASSERT_EQ(standby.Promote(), ErrorCode::OK);
    const uint64_t promoted_sequence_id = standby.GetLatestAppliedSequenceId();

    WrappedP2PMasterService promoted_runtime(MakeWrappedMasterConfig());
    auto& promoted_master =
        static_cast<P2PMasterService&>(promoted_runtime.GetMasterService());
    ASSERT_EQ(promoted_master.RestoreFromStandbyMetadata(
                  standby.ExportMetadata(), promoted_sequence_id),
              ErrorCode::OK);

    auto restored_replica =
        promoted_runtime.GetReplicaListInternal("flow-key-before-promotion");
    ASSERT_TRUE(restored_replica.has_value())
        << toString(restored_replica.error());
    ASSERT_EQ(restored_replica.value().replicas.size(), 1);

    const UUID rejoined_client_id{29, 29};
    const UUID rejoined_segment_id{30, 30};
    const auto rejoined_segment = MakeSegment(rejoined_segment_id);
    RegisterClient(promoted_master, rejoined_client_id, rejoined_segment);

    WriteRouteRequest route_req;
    route_req.client_id = {31, 31};
    route_req.key = "flow-key-after-promotion";
    route_req.size = 1024;
    auto route_result = promoted_runtime.GetWriteRoute(route_req);
    ASSERT_TRUE(route_result.has_value()) << toString(route_result.error());
    ASSERT_FALSE(route_result.value().candidates.empty());

    AddReplicaRequest add_req;
    add_req.key = route_req.key;
    add_req.client_id = rejoined_client_id;
    add_req.segment_id = rejoined_segment_id;
    add_req.size = route_req.size;
    ASSERT_TRUE(promoted_runtime.AddReplica(add_req).has_value());

    auto added_replica = promoted_runtime.GetReplicaListInternal(route_req.key);
    ASSERT_TRUE(added_replica.has_value()) << toString(added_replica.error());
    ASSERT_EQ(added_replica.value().replicas.size(), 1);
    const auto& p2p_desc =
        added_replica.value().replicas[0].get_p2p_proxy_descriptor();
    EXPECT_EQ(p2p_desc.client_id, rejoined_client_id);
    EXPECT_EQ(p2p_desc.segment_id, rejoined_segment_id);

    RemoveReplicaRequest remove_req;
    remove_req.key = route_req.key;
    remove_req.client_id = rejoined_client_id;
    remove_req.segment_id = rejoined_segment_id;
    ASSERT_TRUE(promoted_runtime.RemoveReplica(remove_req).has_value());

    ASSERT_NE(promoted_master.GetOpLogManager(), nullptr);
    EXPECT_EQ(promoted_master.GetOpLogManager()->GetLastSequenceId(),
              promoted_sequence_id + 3);
}

}  // namespace
}  // namespace mooncake::test
