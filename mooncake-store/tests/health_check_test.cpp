#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include <csignal>
#include <ylt/coro_http/coro_http_client.hpp>

#include "client_service.h"
#include "real_client.h"
#include "test_server_helpers.h"
#include "default_config.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

DECLARE_bool(enable_http_server);
DECLARE_int32(http_port);

namespace mooncake {
namespace testing {

class HealthCheckTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("HealthCheckTest");
        FLAGS_logtostderr = 1;
        FLAGS_enable_http_server = true;
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    std::shared_ptr<RealClient> py_client_;
    InProcMaster master_;
    std::string master_address_;

    struct HealthResponse {
        int http_status;
        std::string body;
    };

    HealthResponse fetch_health(int port) {
        coro_http::coro_http_client client;
        std::string url =
            "http://127.0.0.1:" + std::to_string(port) + "/health";
        auto result = client.get(url);
        return {result.status, std::string(result.resp_body)};
    }

    void SetUp() override { py_client_ = RealClient::create(); }

    // Start master and set up the client on the given port.
    // Returns 0 on success.
    int StartMasterAndSetupClient(int port) {
        if (!master_.Start(InProcMasterConfigBuilder().build())) return -1;
        master_address_ = master_.master_address();

        const std::string rdma_devices =
            (FLAGS_protocol == "rdma") ? FLAGS_device_name : "";
        return py_client_->setup_real(
            "localhost:" + std::to_string(port), "P2PHANDSHAKE",
            16 * 1024 * 1024, 16 * 1024 * 1024, FLAGS_protocol, rdma_devices,
            master_address_);
    }
};

// Test 1: health_check returns HC_NOT_INITIALIZED before setup
TEST_F(HealthCheckTest, ReturnsOneBeforeSetup) {
    EXPECT_EQ(py_client_->health_check(), HC_NOT_INITIALIZED);
}

// Test 2: health_check returns HC_HEALTHY after successful setup with master
TEST_F(HealthCheckTest, ReturnsZeroWhenHealthy) {
    ASSERT_EQ(StartMasterAndSetupClient(18900), 0) << "setup_real failed";

    // Wait for ping thread to complete at least one cycle
    std::this_thread::sleep_for(std::chrono::seconds(2));

    EXPECT_EQ(py_client_->health_check(), HC_HEALTHY);

    py_client_->tearDownAll();
    master_.Stop();
}

// Test 3: health_check returns HC_NOT_INITIALIZED after teardown
TEST_F(HealthCheckTest, ReturnsOneAfterTeardown) {
    ASSERT_EQ(StartMasterAndSetupClient(18901), 0);

    py_client_->tearDownAll();
    EXPECT_EQ(py_client_->health_check(), HC_NOT_INITIALIZED);

    master_.Stop();
}

// Test 4: health_check returns HC_MASTER_UNREACHABLE after master stops
TEST_F(HealthCheckTest, ReturnsTwoWhenMasterDown) {
    ASSERT_EQ(StartMasterAndSetupClient(18902), 0);

    // Wait for ping to succeed
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_EQ(py_client_->health_check(), HC_HEALTHY);

    // Stop master, wait for ping to fail
    master_.Stop();
    std::this_thread::sleep_for(std::chrono::seconds(3));

    EXPECT_EQ(py_client_->health_check(), HC_MASTER_UNREACHABLE);

    py_client_->tearDownAll();
}

// Test 5: HTTP /health returns 200 when healthy
TEST_F(HealthCheckTest, HttpReturns200WhenHealthy) {
    int http_port = getFreeTcpPort();
    FLAGS_http_port = http_port;
    ASSERT_EQ(StartMasterAndSetupClient(18910), 0) << "setup_real failed";

    std::this_thread::sleep_for(std::chrono::seconds(2));

    auto resp = fetch_health(http_port);
    EXPECT_EQ(resp.http_status, 200);
    EXPECT_NE(resp.body.find("\"healthy\""), std::string::npos);
    EXPECT_NE(resp.body.find("\"code\":0"), std::string::npos);

    py_client_->tearDownAll();
    master_.Stop();
}

// Test 6: HTTP /health returns 503 when master unreachable
TEST_F(HealthCheckTest, HttpReturns503WhenMasterDown) {
    int http_port = getFreeTcpPort();
    FLAGS_http_port = http_port;
    ASSERT_EQ(StartMasterAndSetupClient(18911), 0);

    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_EQ(py_client_->health_check(), HC_HEALTHY);

    master_.Stop();
    std::this_thread::sleep_for(std::chrono::seconds(3));

    auto resp = fetch_health(http_port);
    EXPECT_EQ(resp.http_status, 503);
    EXPECT_NE(resp.body.find("\"master_unreachable\""), std::string::npos);
    EXPECT_NE(resp.body.find("\"code\":2"), std::string::npos);

    py_client_->tearDownAll();
}

}  // namespace testing
}  // namespace mooncake
