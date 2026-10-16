// master_client_part_rpc_test.cpp
//
// The part-scoped RPCs over a real connection: a MasterClient writes an object
// in two parts through PutStartForPart and PutEndForPart, reads one part back
// through GetReplicaListForPart, and has a stale revoke refused. What the wire
// carries is the part scope, the replica ids of the attempt and the object's
// part count, so this is the level the suite that drives MasterService directly
// cannot reach.

#include <gtest/gtest.h>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "common/network.h"
#include "master_client.h"
#include "master_config.h"
#include "rpc_service.h"
#include "types.h"

namespace mooncake {
namespace {

std::vector<ReplicaID> ReplicaIdsOf(
    const std::vector<Replica::Descriptor>& descriptors) {
    std::vector<ReplicaID> ids;
    ids.reserve(descriptors.size());
    for (const auto& descriptor : descriptors) {
        ids.push_back(descriptor.id);
    }
    return ids;
}

class MasterClientPartRpcTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("MasterClientPartRpcTest");
        FLAGS_logtostderr = 1;

        WrappedMasterServiceConfig config;
        config.default_kv_lease_ttl = 0;
        config.enable_metric_reporting = false;
        config.enable_multi_part_objects = true;
        service_ = std::make_shared<WrappedMasterService>(config);

        const auto ports = getFreeTcpPorts(1);
        ASSERT_EQ(ports.size(), 1u);
        server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/2, /*port=*/ports.front(), /*address=*/"127.0.0.1",
            std::chrono::seconds(0), /*tcp_no_delay=*/true);
        RegisterRpcService(*server_, *service_);
        ASSERT_FALSE(server_->async_start().hasResult());
        endpoint_ = "127.0.0.1:" + std::to_string(ports.front());
    }

    void TearDown() override {
        if (server_) {
            server_->stop();
        }
        google::ShutdownGoogleLogging();
    }

    // A client whose id owns the segment, so its writes land there.
    void ConnectClient() {
        client_id_ = generate_uuid();
        Segment segment;
        segment.id = generate_uuid();
        segment.name = "part_rpc_segment";
        segment.base = 0x300000000;
        segment.size = 16 * 1024 * 1024;
        EXPECT_TRUE(service_->MountSegment(segment, client_id_).has_value());

        // The listener comes up asynchronously, so the connection is retried
        // instead of guessed at with a delay.
        for (int attempt = 0; attempt < 200; ++attempt) {
            auto client = std::make_unique<MasterClient>(client_id_);
            if (client->Connect(endpoint_) == ErrorCode::OK) {
                client_ = std::move(client);
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        FAIL() << "the master never accepted a connection on " << endpoint_;
    }

    PartWriteRequest PartRequest(uint32_t part_index, uint32_t part_count) {
        PartWriteRequest request;
        request.part_index = part_index;
        request.part_count = part_count;
        request.config.replica_num = 1;
        return request;
    }

    PartEndRequest PartEnd(uint32_t part_index, std::vector<ReplicaID> ids) {
        PartEndRequest request;
        request.key = key_;
        request.part_index = part_index;
        request.replica_ids = std::move(ids);
        return request;
    }

    std::shared_ptr<WrappedMasterService> service_;
    std::unique_ptr<coro_rpc::coro_rpc_server> server_;
    std::string endpoint_;
    UUID client_id_;
    std::unique_ptr<MasterClient> client_;
    const std::string key_ = "part_rpc_key";
};

TEST_F(MasterClientPartRpcTest, WritesAndReadsAnObjectOnePartAtATime) {
    ConnectClient();
    constexpr uint64_t kPartLength = 512;

    auto part_zero =
        client_->PutStartForPart(key_, kPartLength, PartRequest(0, 2));
    ASSERT_TRUE(part_zero.has_value()) << toString(part_zero.error());
    auto part_one =
        client_->PutStartForPart(key_, kPartLength, PartRequest(1, 2));
    ASSERT_TRUE(part_one.has_value()) << toString(part_one.error());
    const std::vector<ReplicaID> part_zero_ids = ReplicaIdsOf(*part_zero);
    const std::vector<ReplicaID> part_one_ids = ReplicaIdsOf(*part_one);

    // The object is not readable until the last part is written.
    ASSERT_TRUE(client_->PutEndForPart(PartEnd(0, part_zero_ids)).has_value());
    EXPECT_FALSE(client_->ExistKey(key_).value_or(true));

    ASSERT_TRUE(client_->PutEndForPart(PartEnd(1, part_one_ids)).has_value());
    EXPECT_TRUE(client_->ExistKey(key_).value_or(false));

    // Each part read answers for its own slice and reports how many parts the
    // object has.
    auto part_one_read = client_->GetReplicaListForPart(key_, 1);
    ASSERT_TRUE(part_one_read.has_value()) << toString(part_one_read.error());
    EXPECT_EQ(part_one_read->part_count.value_or(0), 2u);
    EXPECT_FALSE(part_one_read->replicas.empty());

    // A read that names no part has no single slice to answer with.
    EXPECT_EQ(
        client_->GetReplicaList(key_, std::string(TenantId::kDefaultValue))
            .error(),
        ErrorCode::INVALID_PARAMS);
    // A repeated End for a part that is already complete changes nothing.
    EXPECT_TRUE(client_->PutEndForPart(PartEnd(1, part_one_ids)).has_value());
}

TEST_F(MasterClientPartRpcTest, ARevokeThatNamesAnotherAttemptIsRefused) {
    ConnectClient();
    constexpr uint64_t kPartLength = 512;

    auto part_zero =
        client_->PutStartForPart(key_, kPartLength, PartRequest(0, 2));
    ASSERT_TRUE(part_zero.has_value()) << toString(part_zero.error());
    const std::vector<ReplicaID> part_zero_ids = ReplicaIdsOf(*part_zero);

    // Replica ids of an attempt that is not the one holding the part are
    // answered without touching it, over the wire as well: the attempt that
    // does hold it can still complete.
    EXPECT_EQ(client_->PutRevokeForPart(key_, 0, {0}).error(),
              ErrorCode::INVALID_WRITE);
    EXPECT_TRUE(client_->PutEndForPart(PartEnd(0, part_zero_ids)).has_value());

    // With the part complete there is no attempt left to give up.
    EXPECT_EQ(client_->PutRevokeForPart(key_, 0, part_zero_ids).error(),
              ErrorCode::INVALID_WRITE);
}

}  // namespace
}  // namespace mooncake
