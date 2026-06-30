#include "p2p_master_service.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "ha/oplog/oplog_manager.h"
#include "ha/oplog/oplog_store_factory.h"
#include "ha/oplog/p2p_oplog_types.h"
#include "mock_oplog_store.h"

namespace mooncake::test {

namespace {

// Helper to create a minimal MasterServiceConfig for P2P mode with oplog
// enabled.
MasterServiceConfig MakeP2PConfig(bool enable_oplog = true) {
    MasterServiceConfig config;
    config.enable_metric_reporting = false;
    config.metrics_port = 0;
    config.rpc_port = 0;
    config.rpc_thread_num = 1;
    config.default_kv_lease_ttl = 30000;
    config.default_kv_soft_pin_ttl = 30000;
    config.allow_evict_soft_pinned_objects = false;
    config.eviction_ratio = 0.8;
    config.eviction_high_watermark_ratio = 0.9;
    config.client_live_ttl_sec = 30;
    config.client_crashed_ttl_sec = 90;
    config.enable_ha = enable_oplog;
    config.enable_offload = false;
    config.cluster_id = "test-cluster";
    config.max_replicas_per_key = 0;
    config.enable_oplog = enable_oplog;
    config.oplog_store_type = "localfs";
    config.oplog_data_dir = "/tmp/mooncake_oplog_test";
    return config;
}

// Verify that RecordOplog writes entries to the OpLogManager.
TEST(P2PRecordOplogTest, RecordOplogWritesToManager) {
    MasterServiceConfig config = MakeP2PConfig(/*enable_oplog=*/true);
    P2PMasterService service(config);

    // With oplog enabled, oplog_manager_ should be initialized.
    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);

    // Write an ADD_REPLICA entry via RecordOplog.
    AddReplicaPayload payload;
    payload.object_key = "test-key";
    payload.client_id = {1, 0};
    payload.segment_id = {10, 0};
    payload.size = 1024;
    service.RecordOplog(OpType_ADD_REPLICA, "test-key",
                        SerializeP2PPayload(payload));

    // Verify the entry was written.
    uint64_t latest = 0;
    EXPECT_EQ(manager->GetLastSequenceId(), 1u);
}

// Verify that RecordOplog with sync=true calls AppendAndPersist.
TEST(P2PRecordOplogTest, RecordOplogSyncWritesToManager) {
    MasterServiceConfig config = MakeP2PConfig(/*enable_oplog=*/true);
    P2PMasterService service(config);

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);

    RemoveReplicaPayload payload;
    payload.object_key = "test-key";
    payload.client_id = {1, 0};
    payload.segment_id = {10, 0};
    service.RecordOplog(OpType_REMOVE_REPLICA, "test-key",
                        SerializeP2PPayload(payload), /*sync=*/true);

    EXPECT_EQ(manager->GetLastSequenceId(), 1u);
}

// Verify that RecordOplog is no-op when HA is disabled.
TEST(P2PRecordOplogTest, RecordOplogNoopWhenDisabled) {
    MasterServiceConfig config = MakeP2PConfig(/*enable_oplog=*/false);
    P2PMasterService service(config);

    auto* manager = service.GetOpLogManager();
    EXPECT_EQ(manager, nullptr);

    // Should not crash — just a no-op.
    AddReplicaPayload payload;
    payload.object_key = "test-key";
    service.RecordOplog(OpType_ADD_REPLICA, "test-key",
                        SerializeP2PPayload(payload));
}

// Verify that multiple RecordOplog calls result in sequential sequence IDs.
TEST(P2PRecordOplogTest, MultipleRecordsGetSequentialIds) {
    MasterServiceConfig config = MakeP2PConfig(/*enable_oplog=*/true);
    P2PMasterService service(config);

    auto* manager = service.GetOpLogManager();
    ASSERT_NE(manager, nullptr);

    AddReplicaPayload add_payload;
    add_payload.object_key = "key1";
    add_payload.client_id = {1, 0};
    add_payload.segment_id = {10, 0};
    add_payload.size = 1024;
    service.RecordOplog(OpType_ADD_REPLICA, "key1",
                        SerializeP2PPayload(add_payload));

    RemoveReplicaPayload rm_payload;
    rm_payload.object_key = "key1";
    rm_payload.client_id = {1, 0};
    rm_payload.segment_id = {10, 0};
    service.RecordOplog(OpType_REMOVE_REPLICA, "key1",
                        SerializeP2PPayload(rm_payload));

    uint64_t latest = 0;
    EXPECT_EQ(manager->GetLastSequenceId(), 2u);
}

}  // namespace mooncake::test