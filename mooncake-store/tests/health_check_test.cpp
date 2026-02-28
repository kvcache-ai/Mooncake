#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "client_service.h"
#include "real_client.h"
#include "test_server_helpers.h"
#include "default_config.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

namespace mooncake {
namespace testing {

class HealthCheckTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("HealthCheckTest");
        FLAGS_logtostderr = 1;
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    std::shared_ptr<RealClient> py_client_;
    InProcMaster master_;
    std::string master_address_;

    void SetUp() override { py_client_ = RealClient::create(); }
};

// Test 1: health_check returns 1 before setup (client_ is nullptr)
TEST_F(HealthCheckTest, ReturnsOneBeforeSetup) {
    // Before setup_real, client_ should be nullptr
    EXPECT_EQ(py_client_->health_check(), 1);
}

// Test 2: health_check returns 0 after successful setup with master
TEST_F(HealthCheckTest, ReturnsZeroWhenHealthy) {
    ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
    master_address_ = master_.master_address();

    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");
    ASSERT_EQ(
        py_client_->setup_real("localhost:18900", "P2PHANDSHAKE",
                               16 * 1024 * 1024, 16 * 1024 * 1024,
                               FLAGS_protocol, rdma_devices, master_address_),
        0) << "setup_real failed";

    // Wait for ping thread to complete at least one cycle
    std::this_thread::sleep_for(std::chrono::seconds(2));

    EXPECT_EQ(py_client_->health_check(), 0);

    py_client_->tearDownAll();
    master_.Stop();
}

// Test 3: health_check returns 1 after teardown (closed_ = true)
TEST_F(HealthCheckTest, ReturnsOneAfterTeardown) {
    ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
    master_address_ = master_.master_address();

    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");
    ASSERT_EQ(
        py_client_->setup_real("localhost:18901", "P2PHANDSHAKE",
                               16 * 1024 * 1024, 16 * 1024 * 1024,
                               FLAGS_protocol, rdma_devices, master_address_),
        0);

    py_client_->tearDownAll();
    EXPECT_EQ(py_client_->health_check(), 1);

    master_.Stop();
}

// Test 4: health_check returns 2 after master stops (ping fails)
TEST_F(HealthCheckTest, ReturnsTwoWhenMasterDown) {
    ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()));
    master_address_ = master_.master_address();

    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");
    ASSERT_EQ(
        py_client_->setup_real("localhost:18902", "P2PHANDSHAKE",
                               16 * 1024 * 1024, 16 * 1024 * 1024,
                               FLAGS_protocol, rdma_devices, master_address_),
        0);

    // Wait for ping to succeed
    std::this_thread::sleep_for(std::chrono::seconds(2));
    EXPECT_EQ(py_client_->health_check(), 0);

    // Stop master, wait for ping to fail
    master_.Stop();
    std::this_thread::sleep_for(std::chrono::seconds(3));

    EXPECT_EQ(py_client_->health_check(), 2);

    py_client_->tearDownAll();
}

}  // namespace testing
}  // namespace mooncake
