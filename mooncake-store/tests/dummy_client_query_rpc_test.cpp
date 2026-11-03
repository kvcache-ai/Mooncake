// dummy_client_query_rpc_test.cpp
// Verifies the DummyClient -> RealClient query RPCs: get_replica_desc,
// batch_get_replica_desc and batch_query (batch_get_query_results). Checks
// that replica descriptors survive the RPC boundary and that missing keys
// come back empty instead of failing the whole call.
//
// The handler list below is local to this test, mirroring
// dummy_client_probe_key_test.cpp. The production list lives in
// RegisterClientRpcService() in real_client_main.cpp, which tests cannot
// link against; keep the two in sync when adding a DummyClient RPC.

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <memory>
#include <span>
#include <string>
#include <thread>
#include <vector>

#include "dummy_client.h"
#include "real_client.h"
#include "test_server_helpers.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

namespace mooncake {
namespace testing {

// Handlers this test needs: setup/teardown plus the query RPCs under test.
static void RegisterRpcHandlers(coro_rpc::coro_rpc_server &server,
                                RealClient &rc) {
    server.register_handler<&RealClient::service_ready_internal>(&rc);
    server.register_handler<&RealClient::ping>(&rc);
    server.register_handler<&RealClient::map_shm_internal>(&rc);
    server.register_handler<&RealClient::unmap_shm_internal>(&rc);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(&rc);
    server.register_handler<&RealClient::get_replica_desc>(&rc);
    server.register_handler<&RealClient::batch_get_replica_desc>(&rc);
    server.register_handler<&RealClient::batch_get_query_results>(&rc);
}

static constexpr size_t kMB = 1024ULL * 1024;
static constexpr size_t kSegmentSize = 128 * kMB;
static constexpr size_t kLocalBufSize = 64 * kMB;
static constexpr size_t kPoolSize = 64 * kMB;
static constexpr size_t kValueSize = 4096;

// Compare a descriptor received over RPC with the one RealClient sees.
static void ExpectSameMemoryReplica(const Replica::Descriptor &got,
                                    const Replica::Descriptor &want) {
    ASSERT_TRUE(got.is_memory_replica());
    ASSERT_TRUE(want.is_memory_replica());
    EXPECT_EQ(got.id, want.id);
    EXPECT_EQ(got.status, want.status);
    const auto &g = got.get_memory_descriptor().buffer_descriptor;
    const auto &w = want.get_memory_descriptor().buffer_descriptor;
    EXPECT_EQ(g.size_, w.size_);
    EXPECT_EQ(g.buffer_address_, w.buffer_address_);
    EXPECT_EQ(g.transport_endpoint_, w.transport_endpoint_);
}

class DummyClientQueryRpcTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("DummyClientQueryRpcTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
    }

    void TearDown() override {
        // Mute ylt/coro_rpc "operation canceled" noise during shutdown
        easylog::set_min_severity(easylog::Severity::FATAL);
        if (dummy_client_) dummy_client_->tearDownAll();
        if (rpc_server_) rpc_server_->stop();
        if (real_client_) real_client_->tearDownAll();
        master_.Stop();
        easylog::set_min_severity(easylog::Severity::WARN);
    }

    // Bring up master + RealClient + RPC server + DummyClient.
    bool SetupStack() {
        if (!master_.Start(InProcMasterConfigBuilder().build())) return false;

        real_client_ = RealClient::create();
        const std::string rdma_devices =
            (FLAGS_protocol == "rdma") ? FLAGS_device_name : "";
        ipc_path_ = "@query_rpc_test_" + std::to_string(getpid()) + ".sock";
        if (real_client_->setup_real(
                "localhost:17828", "P2PHANDSHAKE", kSegmentSize, kLocalBufSize,
                FLAGS_protocol, rdma_devices, master_.master_address(), nullptr,
                ipc_path_) != 0) {
            return false;
        }

        rpc_port_ = getFreeTcpPort();
        rpc_server_ = std::make_unique<coro_rpc::coro_rpc_server>(
            /*thread_num=*/2, /*port=*/rpc_port_, /*address=*/"127.0.0.1",
            std::chrono::seconds(0), /*tcp_no_delay=*/true);
        RegisterRpcHandlers(*rpc_server_, *real_client_);
        auto ec = rpc_server_->async_start();
        if (ec.hasResult()) return false;
        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        dummy_client_ = std::make_shared<DummyClient>();
        const std::string rpc_addr = "127.0.0.1:" + std::to_string(rpc_port_);
        return dummy_client_->setup_dummy(kPoolSize, kLocalBufSize, rpc_addr,
                                          ipc_path_) == 0;
    }

    void PutData(const std::string &key) {
        const std::string data(kValueSize, 'q');
        std::span<const char> span(data.data(), data.size());
        ReplicateConfig config;
        config.replica_num = 1;
        ASSERT_EQ(real_client_->put(key, span, config), 0);
    }

    mooncake::testing::InProcMaster master_;
    std::shared_ptr<RealClient> real_client_;
    std::shared_ptr<DummyClient> dummy_client_;
    std::unique_ptr<coro_rpc::coro_rpc_server> rpc_server_;
    int rpc_port_ = 0;
    std::string ipc_path_;
};

TEST_F(DummyClientQueryRpcTest, GetReplicaDescOverRpcMatchesRealClient) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "query_rpc_desc_key";
    PutData(key);

    const auto want = real_client_->get_replica_desc(key);
    ASSERT_EQ(want.size(), 1u);
    // An empty list here means the handler is not registered.
    const auto got = dummy_client_->get_replica_desc(key);
    ASSERT_EQ(got.size(), want.size());
    ExpectSameMemoryReplica(got[0], want[0]);
    EXPECT_EQ(got[0].get_memory_descriptor().buffer_descriptor.size_,
              kValueSize);

    EXPECT_TRUE(dummy_client_->get_replica_desc("query_rpc_missing").empty());
}

TEST_F(DummyClientQueryRpcTest, BatchGetReplicaDescOverRpcSkipsMissingKeys) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key_a = "query_rpc_batch_desc_a";
    const std::string key_b = "query_rpc_batch_desc_b";
    PutData(key_a);
    PutData(key_b);

    const std::vector<std::string> keys = {key_a, "query_rpc_batch_missing",
                                           key_b};
    const auto want = real_client_->batch_get_replica_desc(keys);
    ASSERT_EQ(want.size(), 2u);
    const auto got = dummy_client_->batch_get_replica_desc(keys);
    ASSERT_EQ(got.size(), want.size());
    EXPECT_EQ(got.count("query_rpc_batch_missing"), 0u);
    for (const auto &key : {key_a, key_b}) {
        ASSERT_EQ(got.count(key), 1u) << key;
        ASSERT_EQ(got.at(key).size(), 1u) << key;
        ExpectSameMemoryReplica(got.at(key)[0], want.at(key)[0]);
    }
}

TEST_F(DummyClientQueryRpcTest, BatchQueryOverRpcReportsPerKeyResults) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "query_rpc_batch_query_key";
    PutData(key);

    const auto want = real_client_->batch_query({key});
    ASSERT_EQ(want.size(), 1u);
    ASSERT_TRUE(want[0].has_value());

    // An unregistered handler fails every key with RPC_FAIL.
    const auto got =
        dummy_client_->batch_query({key, "query_rpc_batch_query_missing"});
    ASSERT_EQ(got.size(), 2u);
    ASSERT_TRUE(got[0].has_value()) << "error: " << toString(got[0].error());
    ASSERT_EQ(got[0]->replicas.size(), want[0]->replicas.size());
    ExpectSameMemoryReplica(got[0]->replicas[0], want[0]->replicas[0]);
    ASSERT_FALSE(got[1].has_value());
    EXPECT_EQ(got[1].error(), ErrorCode::OBJECT_NOT_FOUND);
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
