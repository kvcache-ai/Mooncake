#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "pybind_client.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "ibp6s0",
              "Device name to use, valid if protocol=rdma");
DEFINE_string(transfer_engine_metadata_url, "http://localhost:8080/metadata",
              "Metadata connection string for transfer engine");

namespace mooncake {
namespace testing {

class PyClientTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("PyClientTest");
        FLAGS_logtostderr = 1;

        // Override flags from environment variables if present
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
        if (getenv("MC_METADATA_SERVER"))
            FLAGS_transfer_engine_metadata_url = getenv("MC_METADATA_SERVER");

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name
                  << ", Metadata URL: " << FLAGS_transfer_engine_metadata_url;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override { py_client_ = std::make_unique<PyClient>(); }

    void TearDown() override {
        if (py_client_) {
            py_client_->tearDownAll();
        }
    }

    std::unique_ptr<PyClient> py_client_;
};

// Test PyClient construction and setup
TEST_F(PyClientTest, ConstructorAndSetup) {
    ASSERT_TRUE(py_client_ != nullptr);

    // Test setup
    int setup_result = py_client_->setup(
        "localhost:17813",                   // local_hostname
        FLAGS_transfer_engine_metadata_url,  // metadata_server
        16 * 1024 * 1024,                    // global_segment_size (16MB)
        16 * 1024 * 1024,                    // local_buffer_size (16MB)
        FLAGS_protocol,                      // protocol
        FLAGS_device_name,                   // rdma_devices
        "localhost:50051"                    // master_server_addr
    );
    EXPECT_EQ(setup_result, 0) << "Setup should succeed";

    // Verify hostname
    std::string hostname = py_client_->get_hostname();
    EXPECT_EQ(hostname, "localhost:17813");
}

// Test basic Put and Get operations
TEST_F(PyClientTest, BasicPutGetOperations) {
    // Setup the client
    ASSERT_EQ(
        py_client_->setup("localhost:17813", FLAGS_transfer_engine_metadata_url,
                          16 * 1024 * 1024, 16 * 1024 * 1024, FLAGS_protocol,
                          FLAGS_device_name, "localhost:50051"),
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