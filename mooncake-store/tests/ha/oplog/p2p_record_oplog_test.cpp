#include "p2p_master_service.h"

#include <unistd.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>

#include "ha/oplog/localfs_oplog_store.h"
#include "ha/oplog/p2p_oplog_types.h"
#include "master_config.h"
#include "p2p_rpc_types.h"
#include "types.h"

namespace mooncake::test {
namespace {

class P2PRecordOplogTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ =
            std::filesystem::temp_directory_path() /
            ("mooncake_p2p_record_oplog_test_" + std::to_string(::getpid()) +
             "_" + std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::remove_all(test_dir_);
    }

    void TearDown() override { std::filesystem::remove_all(test_dir_); }

    MasterServiceConfig MakeConfig(bool enable_oplog = true) const {
        return MasterServiceConfig::builder()
            .set_enable_ha(enable_oplog)
            .set_enable_oplog(enable_oplog)
            .set_cluster_id(kClusterId)
            .set_oplog_store_type("localfs")
            .set_oplog_data_dir(test_dir_.string())
            .set_max_replicas_per_key(0)
            .build();
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

    void UnregisterClient(P2PMasterService& service,
                          const UUID& client_id) const {
        UnregisterClientRequest req;
        req.client_id = client_id;
        req.deployment_mode = DeploymentMode::P2P;
        auto result = service.UnregisterClient(req);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    void MountSegment(P2PMasterService& service, const Segment& segment,
                      const UUID& client_id) const {
        auto result = service.MountSegment(segment, client_id);
        ASSERT_TRUE(result.has_value()) << toString(result.error());
    }

    void UnmountSegment(P2PMasterService& service, const UUID& segment_id,
                        const UUID& client_id) const {
        auto result = service.UnmountSegment(segment_id, client_id);
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

    OpLogEntry ReadEntry(uint64_t sequence_id) const {
        LocalFsOpLogStore reader(kClusterId, test_dir_.string(),
                                 /*enable_batch_write=*/false);
        EXPECT_EQ(reader.Init(), ErrorCode::OK);

        OpLogEntry entry;
        EXPECT_EQ(reader.ReadOpLog(sequence_id, entry), ErrorCode::OK);
        return entry;
    }

    static constexpr const char* kClusterId = "p2p-record-oplog-test";
    std::filesystem::path test_dir_;
};

TEST_F(P2PRecordOplogTest, RegisterClientRecordsOplog) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{1, 1};
    const UUID segment_id{2, 2};
    Segment segment = MakeSegment(segment_id);
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

TEST_F(P2PRecordOplogTest, MountAndUnmountSegmentRecordOplog) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{3, 3};
    const UUID initial_segment_id{4, 4};
    const UUID extra_segment_id{5, 5};
    RegisterClient(service, client_id, MakeSegment(initial_segment_id));

    Segment extra_segment = MakeSegment(extra_segment_id);
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

TEST_F(P2PRecordOplogTest, UnregisterClientRecordsOplog) {
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

TEST_F(P2PRecordOplogTest, AddReplicaRecordsOplog) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{8, 8};
    const UUID segment_id{9, 9};
    RegisterClient(service, client_id, MakeSegment(segment_id));

    AddReplica(service, "key-a", client_id, segment_id, 1234);

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->GetLastSequenceId(), 2);

    OpLogEntry entry = ReadEntry(2);
    EXPECT_EQ(entry.op_type, OpType_ADD_REPLICA);
    EXPECT_EQ(entry.object_key, "key-a");

    AddReplicaPayload payload;
    ASSERT_TRUE(DeserializeP2PPayload(entry.payload, payload));
    EXPECT_EQ(payload.object_key, "key-a");
    EXPECT_EQ(payload.client_id, client_id);
    EXPECT_EQ(payload.segment_id, segment_id);
    EXPECT_EQ(payload.size, 1234);
}

TEST_F(P2PRecordOplogTest, RemoveReplicaRecordsOplog) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{10, 10};
    const UUID segment_id{11, 11};
    RegisterClient(service, client_id, MakeSegment(segment_id));
    AddReplica(service, "key-r", client_id, segment_id);

    RemoveReplica(service, "key-r", client_id, segment_id);

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->GetLastSequenceId(), 3);

    OpLogEntry entry = ReadEntry(3);
    EXPECT_EQ(entry.op_type, OpType_REMOVE_REPLICA);
    EXPECT_EQ(entry.object_key, "key-r");

    RemoveReplicaPayload payload;
    ASSERT_TRUE(DeserializeP2PPayload(entry.payload, payload));
    EXPECT_EQ(payload.object_key, "key-r");
    EXPECT_EQ(payload.client_id, client_id);
    EXPECT_EQ(payload.segment_id, segment_id);
}

TEST_F(P2PRecordOplogTest, BatchSyncReplicaRecordsSuccessfulOps) {
    P2PMasterService service(MakeConfig());
    const UUID client_id{12, 12};
    const UUID segment_id{13, 13};
    RegisterClient(service, client_id, MakeSegment(segment_id));
    AddReplica(service, "old-key", client_id, segment_id);

    BatchSyncReplicaRequest req;
    req.client_id = client_id;
    req.add_keys = {"new-key"};
    req.add_segment_ids = {segment_id};
    req.add_sizes = {2048};
    req.remove_keys = {"old-key"};
    req.remove_segment_ids = {segment_id};

    auto response = service.BatchSyncReplica(req);
    ASSERT_EQ(response.add_results.size(), 1);
    ASSERT_EQ(response.remove_results.size(), 1);
    EXPECT_EQ(response.add_results[0], ErrorCode::OK);
    EXPECT_EQ(response.remove_results[0], ErrorCode::OK);

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

    EXPECT_TRUE(has_entry(OpType_REMOVE_REPLICA, "old-key"));
    EXPECT_TRUE(has_entry(OpType_ADD_REPLICA, "new-key"));
}

TEST_F(P2PRecordOplogTest, EnabledOplogFailsFastWhenStoreInitFails) {
    std::filesystem::create_directories(test_dir_);
    const auto invalid_root = test_dir_ / "not-a-directory";
    std::ofstream(invalid_root) << "file";
    ASSERT_TRUE(std::filesystem::is_regular_file(invalid_root));

    auto config = MakeConfig();
    config.oplog_data_dir = invalid_root.string();

    EXPECT_THROW(P2PMasterService service(config), std::runtime_error);
}

TEST_F(P2PRecordOplogTest, DisabledOplogDoesNotCreateManager) {
    P2PMasterService service(MakeConfig(/*enable_oplog=*/false));
    const UUID client_id{14, 14};
    const UUID segment_id{15, 15};
    RegisterClient(service, client_id, MakeSegment(segment_id));

    AddReplica(service, "key-disabled", client_id, segment_id);

    EXPECT_EQ(service.GetOpLogManager(), nullptr);
}

}  // namespace
}  // namespace mooncake::test
