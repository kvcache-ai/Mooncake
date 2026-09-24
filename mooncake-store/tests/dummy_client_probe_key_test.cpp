// dummy_client_probe_key_test.cpp
// Verifies DummyClient::probeKey / batchProbeKey over the DummyClient ->
// RealClient RPC boundary: argument/result serialization, the int
// conversion in DummyClient, and the round trip down to the master.
//
// The handler list below is local to this test, mirroring
// dummy_client_get_buffer_test.cpp. The production list lives in
// RegisterClientRpcService() in real_client_main.cpp, which tests cannot
// link against, so this test cannot catch a handler that is missing
// there.

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

// Handlers this test needs. Kept minimal on purpose: setup/teardown plus
// the two probe RPCs under test.
static void RegisterRpcHandlers(coro_rpc::coro_rpc_server &server,
                                RealClient &rc) {
    server.register_handler<&RealClient::service_ready_internal>(&rc);
    server.register_handler<&RealClient::ping>(&rc);
    server.register_handler<&RealClient::map_shm_internal>(&rc);
    server.register_handler<&RealClient::unmap_shm_internal>(&rc);
    server.register_handler<&RealClient::unregister_shm_buffer_internal>(&rc);
    server.register_handler<&RealClient::probeKey_internal>(&rc);
    server.register_handler<&RealClient::batchProbeKey_internal>(&rc);
}

static constexpr size_t kMB = 1024ULL * 1024;
static constexpr size_t kSegmentSize = 128 * kMB;
static constexpr size_t kLocalBufSize = 64 * kMB;
static constexpr size_t kPoolSize = 64 * kMB;

class DummyClientProbeKeyTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("DummyClientProbeKeyTest");
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
        ipc_path_ = "@probe_key_test_" + std::to_string(getpid()) + ".sock";
        if (real_client_->setup_real(
                "localhost:17819", "P2PHANDSHAKE", kSegmentSize, kLocalBufSize,
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

    void PutData(const std::string &key, const std::string &data) {
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

TEST_F(DummyClientProbeKeyTest, ProbeKeyOverRpcReportsExistence) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key = "probe_rpc_key";
    PutData(key, std::string(1024, 'p'));

    // 1 means the object existed at the time of the call. A negative result
    // here means the handler was never registered.
    EXPECT_EQ(dummy_client_->probeKey(key), 1)
        << "probeKey should report an existing key as 1 over the RPC path";

    EXPECT_EQ(dummy_client_->probeKey("probe_rpc_missing_key"), 0)
        << "probeKey should report a missing key as 0, not an error";
}

TEST_F(DummyClientProbeKeyTest, BatchProbeKeyOverRpcReportsExistence) {
    ASSERT_TRUE(SetupStack()) << "Failed to bring up real+dummy stack";

    const std::string key_a = "batch_probe_rpc_key_a";
    const std::string key_b = "batch_probe_rpc_key_b";
    PutData(key_a, std::string(1024, 'a'));
    PutData(key_b, std::string(1024, 'b'));

    const std::vector<std::string> keys = {key_a, "batch_probe_rpc_missing",
                                           key_b};
    auto results = dummy_client_->batchProbeKey(keys);
    ASSERT_EQ(results.size(), keys.size());
    EXPECT_EQ(results[0], 1) << key_a << " should exist";
    EXPECT_EQ(results[1], 0) << "missing key should be 0, not an error";
    EXPECT_EQ(results[2], 1) << key_b << " should exist";
}

TEST_F(DummyClientProbeKeyTest, LastHitOnlyPolicySurvivesBothRpcBoundaries) {
    ASSERT_TRUE(SetupStack());
    for (const auto &key : {"a", "b", "c"}) PutData(key, "state");
    const GrantLeasePolicy policy{ProbeLeaseMode::LastHitOnly, 2};
    EXPECT_EQ(dummy_client_->batchProbeKey({"a", "b", "c", "missing"}, policy),
              (std::vector<int>{1, 1, 0, 0}));
    EXPECT_EQ(dummy_client_->batchProbeKey({"a", "missing", "b", "c"}, policy),
              (std::vector<int>{0, 0, 1, 1}));
    EXPECT_EQ(real_client_->batchProbeKey({"a", "b", "b", "c"}, policy),
              (std::vector<int>{0, 0, 1, 1}));
    EXPECT_EQ(dummy_client_->batchProbeKey({"a", "b", "c"}),
              (std::vector<int>{1, 1, 1}));
    EXPECT_TRUE(dummy_client_->batchProbeKey({}, policy).empty());
    const auto invalid = dummy_client_->batchProbeKey(
        {"a", "b"}, {ProbeLeaseMode::LastHitOnly, 0});
    ASSERT_EQ(invalid.size(), 2u);
    for (int value : invalid) EXPECT_LT(value, 0);
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
