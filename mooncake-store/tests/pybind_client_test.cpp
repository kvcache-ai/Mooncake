#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "pybind_client.h"
#include "test_server_helpers.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

namespace mooncake {
namespace testing {

class PyClientTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("PyClientTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() {
        google::ShutdownGoogleLogging();
    }

    void SetUp() override {
        // Override flags from environment variables if present
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name
                  << ", Metadata: P2PHANDSHAKE";
        
        py_client_ = PyClient::create();
    }

    void TearDown() override {
        if (py_client_) {
            py_client_->tearDownAll();
        }
        
        master_.Stop();
    }

    std::shared_ptr<PyClient> py_client_;

    // In-proc master for tests
    mooncake::testing::InProcMaster master_;
    std::string master_address_;
};


// Test basic Put and Get operations
TEST_F(PyClientTest, BasicPutGetOperations) {
    // Start in-proc master
    ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()))
    << "Failed to start in-proc master";
    master_address_ = master_.master_address();
    LOG(INFO) << "Started in-proc master at " << master_address_;

    // Setup the client
    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");
    ASSERT_EQ(py_client_->setup("localhost:17813", "P2PHANDSHAKE",
                                16 * 1024 * 1024, 16 * 1024 * 1024,
                                FLAGS_protocol, rdma_devices, master_address_),
              0);

    const std::string test_data = "Hello, PyClient!";
    const std::string key = "test_key_pyclient";

    // Test Put operation using span
    std::span<const char> data_span(test_data.data(), test_data.size());
    ReplicateConfig config;
    config.replica_num = 1;

    int put_result = py_client_->put(key, data_span, config);
    EXPECT_EQ(put_result, 0) << "Put operation should succeed";

    // Test Get operation using buffer handle
    auto buffer_handle = py_client_->get_buffer(key);
    ASSERT_TRUE(buffer_handle != nullptr) << "Get buffer should succeed";
    EXPECT_EQ(buffer_handle->size(), test_data.size())
        << "Buffer size should match";

    // Verify the data
    std::string retrieved_data(static_cast<const char*>(buffer_handle->ptr()),
                               buffer_handle->size());
    EXPECT_EQ(retrieved_data, test_data)
        << "Retrieved data should match original";

    // Test isExist
    int exist_result = py_client_->isExist(key);
    EXPECT_EQ(exist_result, 1) << "Key should exist";
}

}  // namespace testing

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
