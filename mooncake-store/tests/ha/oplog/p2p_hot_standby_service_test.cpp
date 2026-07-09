#include "ha/oplog/p2p_hot_standby_service.h"

#include <unistd.h>

#include <gtest/gtest.h>

#include <chrono>
#include <cstdint>
#include <filesystem>
#include <string>
#include <variant>

#include "p2p_master_service.h"
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

}  // namespace
}  // namespace mooncake::test
