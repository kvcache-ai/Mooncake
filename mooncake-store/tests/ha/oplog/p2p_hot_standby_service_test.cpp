#include "ha/oplog/p2p_hot_standby_service.h"

#include <unistd.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <variant>

#include "p2p_master_service.h"
#include "p2p_rpc_service.h"
#include "p2p_rpc_types.h"

namespace mooncake::test {
namespace {

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
            .set_max_replicas_per_key(0)
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
        config.max_replicas_per_key = master_config.max_replicas_per_key;
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
    ASSERT_EQ(standby.Promote(), ErrorCode::OK);
    EXPECT_EQ(standby.GetState(), StandbyState::PROMOTED);
    EXPECT_GE(standby.GetLatestAppliedSequenceId(), 2);

    auto exported = standby.ExportMetadata();
    auto object_it = exported.objects.find("key-late");
    ASSERT_NE(object_it, exported.objects.end());
    EXPECT_EQ(object_it->second.size, 8192);
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
    EXPECT_EQ(route_result.value().candidates[0].replica.client_id, client_id);
    EXPECT_EQ(route_result.value().candidates[0].replica.segment_id,
              segment_id);

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

    auto replica_result = promoted_runtime.GetReplicaList("runtime-key");
    ASSERT_TRUE(replica_result.has_value()) << toString(replica_result.error());
    ASSERT_EQ(replica_result.value().replicas.size(), 1);

    WriteRouteRequest route_req;
    route_req.client_id = {99, 99};
    route_req.key = "runtime-key-after-promotion";
    route_req.size = 1024;
    auto route_result = promoted_runtime.GetWriteRoute(route_req);
    ASSERT_TRUE(route_result.has_value()) << toString(route_result.error());
    ASSERT_FALSE(route_result.value().candidates.empty());
    EXPECT_EQ(route_result.value().candidates[0].replica.client_id, client_id);
    EXPECT_EQ(route_result.value().candidates[0].replica.segment_id,
              segment_id);

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
        promoted_runtime.GetReplicaList("flow-key-before-promotion");
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

    auto added_replica = promoted_runtime.GetReplicaList(route_req.key);
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
