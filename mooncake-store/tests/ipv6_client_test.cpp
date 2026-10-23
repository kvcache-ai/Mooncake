/**
 * IPv6 Client Tests for Mooncake Store
 *
 * This test file verifies IPv6 support in mooncake-store, specifically:
 * 1. IPv6 loopback address (::1)
 * 2. IPv6 link-local addresses with scope ID (fe80::xxx%interface)
 * 3. IPv6 address parsing and validation
 *
 * Environment variables:
 *   MC_USE_IPV6=1                    - Enable IPv6 mode
 *   SERVER_ADDRESS=[::1]:port        - IPv6 server address
 *   SERVER_ADDRESS_LL=[fe80::x%if]:p - Link-local address for testing
 *   PROTOCOL=tcp|rdma                - Transfer protocol
 *   DEVICE_NAME=                     - RDMA device name (if protocol=rdma)
 */

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>

#include "centralized_client_config_builder.h"
#include "real_client.h"
#include "test_server_helpers.h"
#include "common.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");
DEFINE_string(server_address, "[::1]:17813",
              "Transfer engine endpoint in host:port form, IPv6 needs []");

namespace mooncake {
namespace testing {

//=============================================================================
// Integration tests for IPv6 client operations
//=============================================================================

class IPv6ClientTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("IPv6ClientTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        // Override flags from environment variables if present
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
        if (getenv("SERVER_ADDRESS"))
            FLAGS_server_address = getenv("SERVER_ADDRESS");

        LOG(INFO) << "Protocol: " << FLAGS_protocol
                  << ", Device name: " << FLAGS_device_name
                  << ", Server address: " << FLAGS_server_address
                  << ", Metadata: P2PHANDSHAKE";

        client_ = RealClient::create();
    }

    void TearDown() override {
        if (client_) {
            client_->tearDownAll();
        }
        if (storage_provider_) {
            storage_provider_->tearDownAll();
        }
        master_.Stop();
    }

    bool StartStorageProvider(const std::string& server_address) {
        // The tested client has no storage; provide a separate IPv6 segment.
        const auto host = parseHostNameWithPort(server_address).first;
        const std::string rdma_devices =
            FLAGS_protocol == "rdma" ? FLAGS_device_name : std::string("");
        storage_provider_ = RealClient::create();
        auto config =
            CentralizedClientConfigBuilder::build_centralized_real_client(
                maybeWrapIpV6(host) + ":0", "P2PHANDSHAKE", FLAGS_protocol,
                rdma_devices.empty() ? std::nullopt
                                     : std::optional<std::string>(rdma_devices),
                master_address_, 16 * 1024 * 1024, 0);
        const int result = storage_provider_->setup(config);
        if (result != 0) {
            LOG(ERROR) << "Failed to start IPv6 storage provider at " << host
                       << ": " << result;
            return false;
        }
        return true;
    }

    std::shared_ptr<RealClient> client_;
    std::shared_ptr<RealClient> storage_provider_;
    mooncake::testing::InProcMaster master_;
    std::string master_address_;
};

// Test basic Put/Get operations over IPv6 loopback address
TEST_F(IPv6ClientTest, BasicPutGetOverIPv6Loopback) {
    // Skip if not using IPv6
    const char* use_ipv6 = getenv("MC_USE_IPV6");
    if (!use_ipv6 || std::string(use_ipv6) != "1") {
        GTEST_SKIP() << "MC_USE_IPV6 is not set to 1; skipping IPv6 test";
    }

    // Start in-proc master
    ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()))
        << "Failed to start in-proc master";
    master_address_ = master_.master_address();
    LOG(INFO) << "Started in-proc master at " << master_address_;
    ASSERT_TRUE(StartStorageProvider(FLAGS_server_address))
        << "Failed to start IPv6 storage provider";

    // Setup the client with IPv6 address
    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");

    LOG(INFO) << "Setting up client with server address: "
              << FLAGS_server_address;

    auto config = CentralizedClientConfigBuilder::build_centralized_real_client(
        FLAGS_server_address, "P2PHANDSHAKE", FLAGS_protocol,
        rdma_devices.empty() ? std::nullopt
                             : std::optional<std::string>(rdma_devices),
        master_address_, 0, 16 * 1024 * 1024);
    ASSERT_EQ(client_->setup(config), 0)
        << "Client setup should succeed with IPv6 address";

    // Test Put operation
    const std::string test_data = "Hello, IPv6 World!";
    const std::string key = "ipv6_test_key";

    std::span<const char> data_span(test_data.data(), test_data.size());
    ReplicateConfig replicate_config;
    replicate_config.replica_num = 1;

    int put_result = client_->put(key, data_span, replicate_config);
    EXPECT_EQ(put_result, 0) << "Put operation should succeed over IPv6";

    // Test Get operation
    auto buffer_handle = client_->get_buffer(key);
    ASSERT_TRUE(buffer_handle != nullptr) << "Get buffer should succeed";
    EXPECT_EQ(buffer_handle->size(), test_data.size())
        << "Buffer size should match";

    // Verify the data
    std::string retrieved_data(static_cast<const char*>(buffer_handle->ptr()),
                               buffer_handle->size());
    EXPECT_EQ(retrieved_data, test_data)
        << "Retrieved data should match original";

    // Test isExist
    int exist_result = client_->isExist(key);
    EXPECT_EQ(exist_result, 1) << "Key should exist";

    // Cleanup - remove may return error if lease expired, that's ok
    client_->remove(key);
}

// Test Put/Get over link-local IPv6 address with scope ID
TEST_F(IPv6ClientTest, BasicPutGetOverLinkLocalIPv6) {
    // Skip if link-local address not provided
    const char* ll_addr = getenv("SERVER_ADDRESS_LL");
    if (!ll_addr || std::string(ll_addr).empty()) {
        GTEST_SKIP()
            << "SERVER_ADDRESS_LL is not set; skipping link-local test";
    }

    const char* use_ipv6 = getenv("MC_USE_IPV6");
    if (!use_ipv6 || std::string(use_ipv6) != "1") {
        GTEST_SKIP() << "MC_USE_IPV6 is not set to 1; skipping IPv6 test";
    }

    std::string server_address = ll_addr;
    LOG(INFO) << "Testing with link-local address: " << server_address;

    // Start in-proc master
    ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()))
        << "Failed to start in-proc master";
    master_address_ = master_.master_address();
    LOG(INFO) << "Started in-proc master at " << master_address_;
    ASSERT_TRUE(StartStorageProvider(server_address))
        << "Failed to start link-local IPv6 storage provider";

    // Setup client with link-local address
    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");

    auto config = CentralizedClientConfigBuilder::build_centralized_real_client(
        server_address, "P2PHANDSHAKE", FLAGS_protocol,
        rdma_devices.empty() ? std::nullopt
                             : std::optional<std::string>(rdma_devices),
        master_address_, 0, 16 * 1024 * 1024);
    ASSERT_EQ(client_->setup(config), 0)
        << "Client setup should succeed with link-local IPv6 address";

    // Test Put operation
    const std::string test_data = "Hello, Link-Local IPv6!";
    const std::string key = "ipv6_linklocal_test_key";

    std::span<const char> data_span(test_data.data(), test_data.size());
    ReplicateConfig replicate_config;
    replicate_config.replica_num = 1;

    int put_result = client_->put(key, data_span, replicate_config);
    EXPECT_EQ(put_result, 0)
        << "Put operation should succeed over link-local IPv6";

    // Test Get operation
    auto buffer_handle = client_->get_buffer(key);
    ASSERT_TRUE(buffer_handle != nullptr) << "Get buffer should succeed";
    EXPECT_EQ(buffer_handle->size(), test_data.size())
        << "Buffer size should match";

    // Verify the data
    std::string retrieved_data(static_cast<const char*>(buffer_handle->ptr()),
                               buffer_handle->size());
    EXPECT_EQ(retrieved_data, test_data)
        << "Retrieved data should match original";

    // Cleanup - remove may return error if lease expired, that's ok
    client_->remove(key);
}

// Test batch operations over IPv6
TEST_F(IPv6ClientTest, BatchOperationsOverIPv6) {
    // Skip if not using IPv6
    const char* use_ipv6 = getenv("MC_USE_IPV6");
    if (!use_ipv6 || std::string(use_ipv6) != "1") {
        GTEST_SKIP() << "MC_USE_IPV6 is not set to 1; skipping IPv6 test";
    }

    // Start in-proc master
    ASSERT_TRUE(master_.Start(InProcMasterConfigBuilder().build()))
        << "Failed to start in-proc master";
    master_address_ = master_.master_address();
    ASSERT_TRUE(StartStorageProvider(FLAGS_server_address))
        << "Failed to start IPv6 storage provider";

    // Setup the client
    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");

    auto config = CentralizedClientConfigBuilder::build_centralized_real_client(
        FLAGS_server_address, "P2PHANDSHAKE", FLAGS_protocol,
        rdma_devices.empty() ? std::nullopt
                             : std::optional<std::string>(rdma_devices),
        master_address_, 0, 16 * 1024 * 1024);
    ASSERT_EQ(client_->setup(config), 0);

    // Prepare batch data
    const int num_keys = 10;
    const size_t data_size = 1024;

    std::vector<std::string> keys;
    std::vector<std::string> test_data;
    std::vector<std::span<const char>> data_spans;

    for (int i = 0; i < num_keys; ++i) {
        keys.push_back("ipv6_batch_key_" + std::to_string(i));
        test_data.push_back(std::string(data_size, 'A' + i));
    }

    for (int i = 0; i < num_keys; ++i) {
        data_spans.emplace_back(test_data[i].data(), test_data[i].size());
    }

    // Batch Put
    ReplicateConfig replicate_config;
    replicate_config.replica_num = 1;

    int batch_put_result =
        client_->put_batch(keys, data_spans, replicate_config);
    EXPECT_EQ(batch_put_result, 0) << "Batch put should succeed over IPv6";

    // Batch Get
    auto buffer_handles = client_->batch_get_buffer(keys);
    ASSERT_EQ(buffer_handles.size(), static_cast<size_t>(num_keys))
        << "Should return handles for all keys";

    for (int i = 0; i < num_keys; ++i) {
        ASSERT_TRUE(buffer_handles[i] != nullptr)
            << "Buffer handle " << i << " should not be null";
        EXPECT_EQ(buffer_handles[i]->size(), data_size)
            << "Buffer " << i << " size should match";

        std::string retrieved(
            static_cast<const char*>(buffer_handles[i]->ptr()),
            buffer_handles[i]->size());
        EXPECT_EQ(retrieved, test_data[i]) << "Data " << i << " should match";
    }

    // Cleanup - removeAll may return different count if lease expired
    client_->removeAll();
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
