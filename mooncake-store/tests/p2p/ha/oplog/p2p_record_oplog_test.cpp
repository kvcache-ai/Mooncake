#include "p2p/master/p2p_master_service.h"

#include <unistd.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "p2p/ha/oplog/localfs_oplog_store.h"
#include "p2p/ha/oplog/oplog_store.h"
#include "p2p/ha/oplog/p2p_oplog_applier.h"
#include "p2p/ha/oplog/p2p_oplog_types.h"
#include "p2p/common/p2p_rpc_types.h"
#include "types.h"
#include "mock_oplog_store.h"

namespace mooncake {
class P2PRecordOpLogTestPeer {
   public:
    static void RunMonitor(P2PClientManager& manager) {
        manager.ClientMonitorFunc();
    }
};
}  // namespace mooncake

namespace mooncake::test {
namespace {

class FailingOpLogStore : public OpLogStore {
   public:
    ErrorCode Init() override { return ErrorCode::OK; }
    ErrorCode WriteOpLog(const OpLogEntry& entry, bool sync) override {
        (void)entry;
        ++write_attempts;
        last_sync = sync;
        return ErrorCode::INTERNAL_ERROR;
    }

    size_t write_attempts{0};
    bool last_sync{true};
    ErrorCode ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) override {
        (void)sequence_id;
        (void)entry;
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }
    ErrorCode ReadOpLogSince(uint64_t start_sequence_id, size_t limit,
                             std::vector<OpLogEntry>& entries) override {
        (void)start_sequence_id;
        (void)limit;
        entries.clear();
        return ErrorCode::OK;
    }
    ErrorCode GetLatestSequenceId(uint64_t& sequence_id) override {
        sequence_id = 0;
        return ErrorCode::OK;
    }
    ErrorCode GetMaxSequenceId(uint64_t& sequence_id) override {
        sequence_id = 0;
        return ErrorCode::OK;
    }
    ErrorCode UpdateLatestSequenceId(uint64_t sequence_id) override {
        (void)sequence_id;
        return ErrorCode::OK;
    }
    ErrorCode RecordSnapshotSequenceId(const std::string& snapshot_id,
                                       uint64_t sequence_id) override {
        (void)snapshot_id;
        (void)sequence_id;
        return ErrorCode::OK;
    }
    ErrorCode GetSnapshotSequenceId(const std::string& snapshot_id,
                                    uint64_t& sequence_id) override {
        (void)snapshot_id;
        sequence_id = 0;
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }
    ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id) override {
        (void)before_sequence_id;
        return ErrorCode::OK;
    }
};

class P2PRecordOpLogTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ =
            std::filesystem::temp_directory_path() /
            ("mooncake_p2p_record_oplog_test_" + std::to_string(::getpid()) +
             "_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(test_dir_);
    }

    void TearDown() override { std::filesystem::remove_all(test_dir_); }

    P2PMasterConfig MakeConfig(bool enable_oplog = true) const {
        P2PMasterConfig config;
        config.oplog.enabled = enable_oplog;
        config.cluster_id = kClusterId;
        config.oplog.store_type = "localfs";
        config.oplog.data_dir = test_dir_.string();
        config.routes.max_clients_per_key = 0;
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

    void RegisterClient(P2PMasterService& service, const UUID& client_id,
                        const P2PSegment& segment,
                        bool send_heartbeat = true) const {
        P2PRegisterClientRequest req;
        req.client_id = client_id;
        req.ip_address = "127.0.0.1";
        req.rpc_port = 50051;
        req.segments = {segment};
        auto result = service.RegisterClient(req);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
        if (!send_heartbeat) return;
        P2PHeartbeatRequest heartbeat;
        heartbeat.client_id = client_id;
        heartbeat.service_state = P2PClientServiceState::ONLINE;
        ASSERT_TRUE(service.Heartbeat(heartbeat).has_value());
    }

    void UnregisterClient(P2PMasterService& service,
                          const UUID& client_id) const {
        auto result = service.UnregisterClient(client_id);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    void MountSegment(P2PMasterService& service, const P2PSegment& segment,
                      const UUID& client_id) const {
        auto result = service.MountSegment(segment, client_id);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    void UnmountSegment(P2PMasterService& service, const UUID& segment_id,
                        const UUID& client_id) const {
        auto result = service.UnmountSegment(segment_id, client_id);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
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

    OpLogEntry ReadEntry(uint64_t sequence_id) const {
        OpLogEntry entry;
        for (int i = 0; i < 100; ++i) {
            LocalFsOpLogStore reader(kClusterId, test_dir_.string(),
                                     /*enable_batch_write=*/false);
            EXPECT_EQ(reader.Init(), ErrorCode::OK);
            if (reader.ReadOpLog(sequence_id, entry) == ErrorCode::OK) {
                return entry;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        LocalFsOpLogStore reader(kClusterId, test_dir_.string(),
                                 /*enable_batch_write=*/false);
        EXPECT_EQ(reader.Init(), ErrorCode::OK);
        EXPECT_EQ(reader.ReadOpLog(sequence_id, entry), ErrorCode::OK);
        return entry;
    }

    void InjectFailingOpLogStore(P2PMasterService& service) const {
        auto* manager = service.GetOpLogManager();
        ASSERT_NE(manager, nullptr);
        manager->SetOpLogStore(std::make_shared<FailingOpLogStore>());
    }

    static constexpr const char* kClusterId = "p2p-record-oplog-test";
    std::filesystem::path test_dir_;
};

TEST_F(P2PRecordOpLogTest, CrashRecordsUnregisterAndCleansStandbyReplicas) {
    auto config = MakeConfig();
    config.client_lifecycle.live_ttl_seconds = 0;
    config.client_lifecycle.crashed_ttl_seconds = 1;
    P2PMasterService service(config);
    service.GetClientManager().Stop();

    const UUID client_id{1, 1};
    const UUID segment_id{2, 2};
    const UUID second_segment_id{3, 3};
    RegisterClient(service, client_id, MakeSegment(segment_id),
                   /*send_heartbeat=*/false);
    MountSegment(service, MakeSegment(second_segment_id), client_id);
    PublishRoute(service, "first", client_id, segment_id);
    PublishRoute(service, "second", client_id, second_segment_id);
    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->GetLastSequenceId(), 4u);

    P2PStandbyMetadataStore standby;
    P2POpLogApplier applier(&standby, kClusterId);
    for (uint64_t sequence_id = 1; sequence_id <= 4; ++sequence_id) {
        ASSERT_TRUE(applier.ApplyOpLogEntry(ReadEntry(sequence_id)));
    }
    auto standby_metadata = standby.ExportMetadata();
    EXPECT_EQ(standby_metadata.routes.size(), 2u);
    ASSERT_NE(standby_metadata.clients.find(client_id),
              standby_metadata.clients.end());
    EXPECT_EQ(standby_metadata.clients.at(client_id).segments.size(), 2u);

    P2PRecordOpLogTestPeer::RunMonitor(service.GetClientManager());
    auto client = service.GetClientManager().GetClient(client_id);
    ASSERT_NE(client, nullptr);
    EXPECT_EQ(client->get_health_state().status,
              P2PClientStatus::DISCONNECTION);
    EXPECT_EQ(manager->GetLastSequenceId(), 4u);
    EXPECT_EQ(service.GetKeyCount(), 2u);

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    P2PRecordOpLogTestPeer::RunMonitor(service.GetClientManager());
    EXPECT_EQ(service.GetClientManager().GetClient(client_id), nullptr);
    EXPECT_EQ(service.GetKeyCount(), 0u);
    ASSERT_EQ(manager->GetLastSequenceId(), 5u);

    tl::expected<uint64_t, ErrorCode> committed_sequence_id;
    for (int attempts = 0; attempts < 1000; ++attempts) {
        committed_sequence_id = manager->GetLocalCommittedSequenceId();
        if (committed_sequence_id.has_value() &&
            committed_sequence_id.value() >= 5) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(committed_sequence_id.has_value());
    ASSERT_EQ(5u, committed_sequence_id.value());

    auto entry = ReadEntry(5);
    EXPECT_EQ(entry.op_type, OpType_UNREGISTER_CLIENT);
    UnregisterClientPayload payload;
    ASSERT_TRUE(DeserializeP2PPayload(entry.payload, payload));
    EXPECT_EQ(payload.client_id, client_id);

    ASSERT_TRUE(applier.ApplyOpLogEntry(entry));
    auto exported = standby.ExportMetadata();
    EXPECT_TRUE(exported.clients.empty());
    EXPECT_TRUE(exported.routes.empty());

    P2PRecordOpLogTestPeer::RunMonitor(service.GetClientManager());
    EXPECT_EQ(manager->GetLastSequenceId(), 5u);
}

TEST_F(P2PRecordOpLogTest, CrashEnqueueFailureStillCleansLocallyWithoutRetry) {
    auto config = MakeConfig();
    config.client_lifecycle.live_ttl_seconds = 0;
    config.client_lifecycle.crashed_ttl_seconds = 0;
    P2PMasterService service(config);
    service.GetClientManager().Stop();

    const UUID client_id{1, 1};
    const UUID segment_id{2, 2};
    RegisterClient(service, client_id, MakeSegment(segment_id),
                   /*send_heartbeat=*/false);
    PublishRoute(service, "key", client_id, segment_id);

    auto failing_store = std::make_shared<FailingOpLogStore>();
    service.GetOpLogManager()->SetOpLogStore(failing_store);
    P2PRecordOpLogTestPeer::RunMonitor(service.GetClientManager());

    EXPECT_EQ(service.GetClientManager().GetClient(client_id), nullptr);
    EXPECT_EQ(service.GetKeyCount(), 0u);
    EXPECT_EQ(failing_store->write_attempts, 1u);
    EXPECT_FALSE(failing_store->last_sync);

    P2PRecordOpLogTestPeer::RunMonitor(service.GetClientManager());
    EXPECT_EQ(failing_store->write_attempts, 1u);
}

TEST_F(P2PRecordOpLogTest, RegisterClientRecordsOpLog) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{1, 1};
    const UUID segment_id{2, 2};
    P2PSegment segment = MakeSegment(segment_id);
    RegisterClient(service, client_id, segment);

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->GetLastSequenceId(), 1);

    OpLogEntry entry = ReadEntry(1);
    EXPECT_EQ(entry.op_type, OpType_REGISTER_CLIENT);
    EXPECT_EQ(entry.object_key, "");

    RegisterClientPayload payload;
    ASSERT_TRUE(DeserializeP2PPayload(entry.payload, payload));
    EXPECT_EQ(payload.client_id, client_id);
    EXPECT_EQ(payload.ip_address, "127.0.0.1");
    EXPECT_EQ(payload.rpc_port, 50051);
    ASSERT_EQ(payload.segments.size(), 1);
    EXPECT_EQ(payload.segments[0].id, segment.id);
    EXPECT_EQ(payload.segments[0].name, segment.name);
}

TEST_F(P2PRecordOpLogTest, DuplicateRegisterClientRecordsSegmentOpLog) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{1, 1};
    const UUID segment_id{2, 2};
    RegisterClient(service, client_id, MakeSegment(segment_id));

    P2PRegisterClientRequest req;
    req.client_id = client_id;
    req.ip_address = "127.0.0.1";
    req.rpc_port = 50051;
    req.segments = {MakeSegment({3, 3})};

    auto duplicate_result = service.RegisterClient(req);
    ASSERT_TRUE(duplicate_result.has_value())
        << toString(duplicate_result.error());

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->GetLastSequenceId(), 2);
    OpLogEntry entry = ReadEntry(2);
    EXPECT_EQ(entry.op_type, OpType_MOUNT_SEGMENT);
    MountSegmentPayload payload;
    ASSERT_TRUE(DeserializeP2PPayload(entry.payload, payload));
    EXPECT_EQ(payload.client_id, client_id);
    EXPECT_EQ(payload.segment.id, req.segments.front().id);
}

TEST_F(P2PRecordOpLogTest, DuplicateRegisterClientRetriesFailedSegmentOpLog) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{1, 1};
    RegisterClient(service, client_id, MakeSegment({2, 2}));

    auto store = std::make_shared<MockOpLogStore>();
    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    manager->SetOpLogStore(store);
    store->SetWriteError(ErrorCode::INTERNAL_ERROR);

    P2PRegisterClientRequest req;
    req.client_id = client_id;
    req.ip_address = "127.0.0.1";
    req.rpc_port = 50051;
    req.segments = {MakeSegment({3, 3})};
    auto failed = service.RegisterClient(req);
    ASSERT_FALSE(failed.has_value());
    EXPECT_EQ(failed.error(), ErrorCode::INTERNAL_ERROR);
    // The mount already succeeded in memory; a retry must still persist it.
    EXPECT_TRUE(service.QuerySegments(req.segments.front().name).has_value());

    store->SetWriteError(ErrorCode::OK);
    ASSERT_TRUE(service.RegisterClient(req).has_value());
    OpLogEntry entry;
    ASSERT_EQ(store->ReadOpLog(manager->GetLastSequenceId(), entry),
              ErrorCode::OK);
    EXPECT_EQ(entry.op_type, OpType_MOUNT_SEGMENT);
    MountSegmentPayload payload;
    ASSERT_TRUE(DeserializeP2PPayload(entry.payload, payload));
    EXPECT_EQ(payload.client_id, client_id);
    EXPECT_EQ(payload.segment.id, req.segments.front().id);
}

TEST_F(P2PRecordOpLogTest, RegisterClientRejectsMissingEndpoint) {
    P2PMasterService service(MakeConfig());
    const UUID segment_id{2, 2};

    P2PRegisterClientRequest missing_ip;
    missing_ip.client_id = {1, 1};
    missing_ip.rpc_port = 50051;
    missing_ip.segments = {MakeSegment(segment_id)};
    auto missing_ip_result = service.RegisterClient(missing_ip);
    ASSERT_FALSE(missing_ip_result.has_value());
    EXPECT_EQ(missing_ip_result.error(), ErrorCode::INVALID_PARAMS);

    P2PRegisterClientRequest missing_port;
    missing_port.client_id = {3, 3};
    missing_port.ip_address = "127.0.0.1";
    missing_port.segments = {MakeSegment(segment_id)};
    auto missing_port_result = service.RegisterClient(missing_port);
    ASSERT_FALSE(missing_port_result.has_value());
    EXPECT_EQ(missing_port_result.error(), ErrorCode::INVALID_PARAMS);

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->GetLastSequenceId(), 0);
}

TEST_F(P2PRecordOpLogTest, MountAndUnmountSegmentRecordOpLog) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{3, 3};
    const UUID initial_segment_id{4, 4};
    const UUID extra_segment_id{5, 5};
    RegisterClient(service, client_id, MakeSegment(initial_segment_id));

    P2PSegment extra_segment = MakeSegment(extra_segment_id);
    MountSegment(service, extra_segment, client_id);
    UnmountSegment(service, extra_segment_id, client_id);

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->GetLastSequenceId(), 3);

    OpLogEntry mount_entry = ReadEntry(2);
    EXPECT_EQ(mount_entry.op_type, OpType_MOUNT_SEGMENT);
    EXPECT_EQ(mount_entry.object_key, "");

    MountSegmentPayload mount_payload;
    ASSERT_TRUE(DeserializeP2PPayload(mount_entry.payload, mount_payload));
    EXPECT_EQ(mount_payload.client_id, client_id);
    EXPECT_EQ(mount_payload.segment.id, extra_segment_id);
    EXPECT_EQ(mount_payload.segment.name, extra_segment.name);

    OpLogEntry unmount_entry = ReadEntry(3);
    EXPECT_EQ(unmount_entry.op_type, OpType_UNMOUNT_SEGMENT);
    EXPECT_EQ(unmount_entry.object_key, "");

    UnmountSegmentPayload unmount_payload;
    ASSERT_TRUE(DeserializeP2PPayload(unmount_entry.payload, unmount_payload));
    EXPECT_EQ(unmount_payload.client_id, client_id);
    EXPECT_EQ(unmount_payload.segment_id, extra_segment_id);
}

TEST_F(P2PRecordOpLogTest, UnregisterClientRecordsOpLog) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{6, 6};
    const UUID segment_id{7, 7};
    RegisterClient(service, client_id, MakeSegment(segment_id));

    UnregisterClient(service, client_id);

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->GetLastSequenceId(), 2);

    OpLogEntry entry = ReadEntry(2);
    EXPECT_EQ(entry.op_type, OpType_UNREGISTER_CLIENT);
    EXPECT_EQ(entry.object_key, "");

    UnregisterClientPayload payload;
    ASSERT_TRUE(DeserializeP2PPayload(entry.payload, payload));
    EXPECT_EQ(payload.client_id, client_id);
}

TEST_F(P2PRecordOpLogTest, AddReplicaRecordsOpLog) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{8, 8};
    const UUID segment_id{9, 9};
    RegisterClient(service, client_id, MakeSegment(segment_id));

    PublishRoute(service, "key-a", client_id, segment_id, 1234);

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->GetLastSequenceId(), 2);

    OpLogEntry entry = ReadEntry(2);
    EXPECT_EQ(entry.op_type, OpType_PUBLISH_ROUTE);
    EXPECT_EQ(entry.object_key, "key-a");

    PublishRoutePayload payload;
    ASSERT_TRUE(DeserializeP2PPayload(entry.payload, payload));
    EXPECT_EQ(payload.object_key, "key-a");
    EXPECT_EQ(payload.client_id, client_id);
    EXPECT_EQ(payload.segment_id, segment_id);
    EXPECT_EQ(payload.size, 1234);
}

TEST_F(P2PRecordOpLogTest, RemoveReplicaRecordsOpLog) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{10, 10};
    const UUID segment_id{11, 11};
    RegisterClient(service, client_id, MakeSegment(segment_id));
    PublishRoute(service, "key-r", client_id, segment_id);

    WithdrawRoute(service, "key-r", client_id, segment_id);

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->GetLastSequenceId(), 3);

    OpLogEntry entry = ReadEntry(3);
    EXPECT_EQ(entry.op_type, OpType_WITHDRAW_ROUTE);
    EXPECT_EQ(entry.object_key, "key-r");

    WithdrawRoutePayload payload;
    ASSERT_TRUE(DeserializeP2PPayload(entry.payload, payload));
    EXPECT_EQ(payload.object_key, "key-r");
    EXPECT_EQ(payload.client_id, client_id);
    EXPECT_EQ(payload.segment_id, segment_id);
}

TEST_F(P2PRecordOpLogTest, BatchSyncRoutesRecordsSuccessfulOps) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{12, 12};
    const UUID segment_id{13, 13};
    RegisterClient(service, client_id, MakeSegment(segment_id));
    PublishRoute(service, "old-key", client_id, segment_id);

    P2PBatchSyncRoutesRequest req;
    req.client_id = client_id;
    req.publish_operations = {{
        .key = "new-key",
        .object_size = 2048,
        .segment_id = segment_id,
    }};
    req.withdraw_operations = {{
        .key = "old-key",
        .segment_id = segment_id,
    }};

    auto response = service.BatchSyncRoutes(req);
    ASSERT_EQ(response.publish_results.size(), 1);
    ASSERT_EQ(response.withdraw_results.size(), 1);
    EXPECT_EQ(response.publish_results[0], ErrorCode::OK);
    EXPECT_EQ(response.withdraw_results[0], ErrorCode::OK);

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->GetLastSequenceId(), 4);

    std::vector<OpLogEntry> entries = {ReadEntry(3), ReadEntry(4)};
    auto has_entry = [&](OpType type, const std::string& key) {
        return std::any_of(
            entries.begin(), entries.end(), [&](const OpLogEntry& entry) {
                return entry.op_type == type && entry.object_key == key;
            });
    };

    EXPECT_TRUE(has_entry(OpType_WITHDRAW_ROUTE, "old-key"));
    EXPECT_TRUE(has_entry(OpType_PUBLISH_ROUTE, "new-key"));
}

TEST_F(P2PRecordOpLogTest,
       BatchSyncRoutesPreservesMutationPolicyWhenPersistenceFails) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{20, 20};
    const UUID segment_id{21, 21};
    RegisterClient(service, client_id, MakeSegment(segment_id));
    PublishRoute(service, "old-key", client_id, segment_id);
    InjectFailingOpLogStore(service);

    P2PBatchSyncRoutesRequest req;
    req.client_id = client_id;
    req.publish_operations = {{
        .key = "new-key",
        .object_size = 2048,
        .segment_id = segment_id,
    }};
    req.withdraw_operations = {{
        .key = "old-key",
        .segment_id = segment_id,
    }};

    auto response = service.BatchSyncRoutes(req);
    ASSERT_EQ(response.publish_results.size(), 1);
    ASSERT_EQ(response.withdraw_results.size(), 1);
    EXPECT_EQ(response.publish_results[0], ErrorCode::OK);
    EXPECT_EQ(response.withdraw_results[0], ErrorCode::INTERNAL_ERROR);

    auto published = service.GetReadRoute("new-key");
    ASSERT_TRUE(published.has_value());
    EXPECT_EQ(published->size(), 1);

    auto retained = service.GetReadRoute("old-key");
    ASSERT_TRUE(retained.has_value());
    EXPECT_EQ(retained->size(), 1);
}

TEST_F(P2PRecordOpLogTest,
       RegisterClientReturnsErrorWhenOpLogPersistenceFails) {
    P2PMasterService service(MakeConfig());
    InjectFailingOpLogStore(service);

    P2PRegisterClientRequest req;
    req.client_id = {30, 30};
    req.ip_address = "127.0.0.1";
    req.rpc_port = 50051;
    req.segments = {MakeSegment({31, 31})};

    auto result = service.RegisterClient(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);

    // Current primary-side policy is apply-memory-then-record for lifecycle
    // operations. Do not silently roll this back in the error path; a follow-up
    // durable retry/outbox or prepare-log-apply flow should make this stronger.
    EXPECT_NE(service.GetClientManager().GetClient(req.client_id), nullptr);
}

TEST_F(P2PRecordOpLogTest, AddReplicaSucceedsWhenOpLogPersistenceFails) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{32, 32};
    const UUID segment_id{33, 33};
    RegisterClient(service, client_id, MakeSegment(segment_id));
    InjectFailingOpLogStore(service);

    P2PPublishRouteRequest req;
    req.key = "failed-add";
    req.client_id = client_id;
    req.segment_id = segment_id;
    req.object_size = 4096;

    auto result = service.PublishRoute(req);
    ASSERT_TRUE(result.has_value());

    auto replicas = service.GetReadRoute(req.key);
    ASSERT_TRUE(replicas.has_value());
    ASSERT_EQ(1u, replicas.value().size());
}

TEST_F(P2PRecordOpLogTest, RemoveReplicaDoesNotApplyWhenOpLogPersistenceFails) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{34, 34};
    const UUID segment_id{35, 35};
    RegisterClient(service, client_id, MakeSegment(segment_id));
    PublishRoute(service, "failed-remove", client_id, segment_id);
    InjectFailingOpLogStore(service);

    P2PWithdrawRouteRequest req;
    req.key = "failed-remove";
    req.client_id = client_id;
    req.segment_id = segment_id;

    auto result = service.WithdrawRoute(req);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INTERNAL_ERROR);

    auto replicas = service.GetReadRoute(req.key);
    ASSERT_TRUE(replicas.has_value()) << toString(replicas.error());
    ASSERT_EQ(replicas.value().size(), 1);
    EXPECT_EQ(replicas.value()[0].client_id, client_id);
}

TEST_F(P2PRecordOpLogTest, EnabledOpLogFailsFastWhenStoreInitFails) {
    std::filesystem::create_directories(test_dir_);
    const auto invalid_root = test_dir_ / "not-a-directory";
    std::ofstream(invalid_root) << "file";
    ASSERT_TRUE(std::filesystem::is_regular_file(invalid_root));

    auto config = MakeConfig();
    config.oplog.data_dir = invalid_root.string();

    EXPECT_THROW(P2PMasterService service(config), std::runtime_error);
}

TEST_F(P2PRecordOpLogTest, DisabledOpLogDoesNotCreateManager) {
    P2PMasterService service(MakeConfig(/*enable_oplog=*/false));
    const UUID client_id{14, 14};
    const UUID segment_id{15, 15};
    RegisterClient(service, client_id, MakeSegment(segment_id));

    PublishRoute(service, "key-disabled", client_id, segment_id);

    EXPECT_EQ(service.GetOpLogManager(), nullptr);
}

}  // namespace
}  // namespace mooncake::test
