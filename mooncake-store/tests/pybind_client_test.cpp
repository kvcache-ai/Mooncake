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

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

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

// Test Get Operation will fail if the lease has expired.
// Set the lease time to 1ms and use large data size to ensure the lease will
// expire.
TEST_F(PyClientTest, GetWithLeaseTimeOut) {
    // Start in-proc master
    const uint64_t kv_lease_ttl_ = 1;
    ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder()
                                  .set_default_kv_lease_ttl(kv_lease_ttl_)
                                  .build()))
        << "Failed to start in-proc master";
    master_address_ = master_.master_address();
    LOG(INFO) << "Started in-proc master at " << master_address_;

    // Setup the client
    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");
    ASSERT_EQ(py_client_->setup("localhost:17813", "P2PHANDSHAKE",
                                512 * 1024 * 1024, 512 * 1024 * 1024,
                                FLAGS_protocol, rdma_devices, master_address_),
              0);

    const size_t data_size = 256 * 1024 * 1024;  // 256MB
    std::string test_data(data_size, 'A');       // Fill with 'A' characters
    // Register buffers for zero-copy operations
    int reg_result =
        py_client_->register_buffer(test_data.data(), test_data.size());
    EXPECT_EQ(reg_result, 0) << "Buffer registration should succeed";

    // Test Single Get Operation
    {
        const std::string key = "test_key_pyclient";

        // Put the data
        std::span<const char> data_span(test_data.data(), test_data.size());
        ReplicateConfig config;
        config.replica_num = 1;

        int put_result = py_client_->put(key, data_span, config);
        EXPECT_EQ(put_result, 0) << "Put operation should succeed";

        // Test Get operation using buffer handle
        auto buffer_handle = py_client_->get_buffer(key);
        ASSERT_TRUE(buffer_handle == nullptr) << "Get buffer should fail";

        // Test Get operation using buffer handle
        auto bytes_read =
            py_client_->get_into(key, test_data.data(), test_data.size());
        ASSERT_TRUE(bytes_read < 0) << "Get into should fail";

        // Clear the data for the next test
        std::this_thread::sleep_for(std::chrono::milliseconds(kv_lease_ttl_));
        ASSERT_EQ(py_client_->remove(key), 0)
            << "Remove operation should succeed";
    }

    // Test Batch Get Operation
    {
        const size_t num_slices = 128;
        const size_t slice_size = data_size / num_slices;
        const std::string key_prefix = "batch_test_key_";

        // Prepare batch data
        std::vector<std::string> keys;
        std::vector<std::span<const char>> data_spans;
        std::vector<void*> buffers;
        std::vector<size_t> sizes;

        for (size_t i = 0; i < num_slices; ++i) {
            const std::string key = key_prefix + std::to_string(i);
            keys.push_back(key);

            const char* slice_data = test_data.data() + (i * slice_size);
            data_spans.emplace_back(slice_data, slice_size);
            buffers.push_back(const_cast<char*>(slice_data));
            sizes.push_back(slice_size);
        }

        // Batch Put operation
        ReplicateConfig config;
        config.replica_num = 1;
        int batch_put_result = py_client_->put_batch(keys, data_spans, config);
        EXPECT_EQ(batch_put_result, 0) << "Batch put operation should succeed";

        // Test Batch Get operation using batch_get_buffer
        auto buffer_handles = py_client_->batch_get_buffer(keys);
        ASSERT_EQ(buffer_handles.size(), num_slices)
            << "Should return handles for all keys";
        int fail_count = 0;
        for (size_t i = 0; i < buffer_handles.size(); ++i) {
            if (buffer_handles[i] == nullptr) {
                fail_count++;
            }
        }
        LOG(INFO) << "Batch get buffer " << fail_count << " out of "
                  << num_slices << " keys failed";
        ASSERT_NE(fail_count, 0) << "Should fail for some keys";

        // Test Batch Get operation using batch_get_into
        auto bytes_read_results =
            py_client_->batch_get_into(keys, buffers, sizes);
        ASSERT_EQ(bytes_read_results.size(), num_slices)
            << "Should return results for all keys";
        fail_count = 0;
        for (size_t i = 0; i < bytes_read_results.size(); ++i) {
            if (bytes_read_results[i] < 0) {
                fail_count++;
            }
        }
        LOG(INFO) << "Batch get into " << fail_count << " out of " << num_slices
                  << " keys failed";
        ASSERT_NE(fail_count, 0) << "Should fail for some keys";
    }

    // Unregister buffers
    int unreg_result = py_client_->unregister_buffer(test_data.data());
    EXPECT_EQ(unreg_result, 0) << "Buffer unregistration should succeed";
}

}  // namespace testing

}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
