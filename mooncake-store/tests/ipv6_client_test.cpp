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
#include <random>
#include <barrier>

#include "real_client.h"
#include "test_server_helpers.h"
#include "common.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");
DEFINE_string(server_address, "[::1]:17813",
              "Transfer engine endpoint in host:port form, IPv6 needs []");

namespace mooncake {
namespace testing {

// Helper class to temporarily mute glog output by setting log level to FATAL
class GLogMuter {
   public:
    GLogMuter() : original_log_level_(FLAGS_minloglevel) {
        FLAGS_minloglevel = google::GLOG_FATAL;
    }

    ~GLogMuter() { FLAGS_minloglevel = original_log_level_; }

   private:
    int original_log_level_;
};

//=============================================================================
// Unit tests for IPv6 address parsing functions
//=============================================================================

class IPv6ParsingTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("IPv6ParsingTest");
        FLAGS_logtostderr = 1;
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }
};

// Test isValidIpV6 function with various IPv6 address formats
TEST_F(IPv6ParsingTest, IsValidIpV6) {
    // Valid IPv6 addresses
    EXPECT_TRUE(isValidIpV6("::1")) << "Loopback address should be valid";
    EXPECT_TRUE(isValidIpV6("::")) << "Any address should be valid";
    EXPECT_TRUE(isValidIpV6("2001:db8::1")) << "Global unicast should be valid";
    EXPECT_TRUE(isValidIpV6("fe80::1"))
        << "Link-local without scope should be valid";
    EXPECT_TRUE(isValidIpV6("fe80::a236:bcff:fecb:a1be"))
        << "Full link-local should be valid";

    // Valid IPv6 addresses with scope ID
    EXPECT_TRUE(isValidIpV6("fe80::1%eth0"))
        << "Link-local with scope ID should be valid";
    EXPECT_TRUE(isValidIpV6("fe80::a236:bcff:fecb:a1be%eno2"))
        << "Full link-local with scope ID should be valid";

    // Invalid: IPv6 with port (should not be considered valid pure IPv6)
    EXPECT_FALSE(isValidIpV6("fe80::1%eth0:12345"))
        << "IPv6 with scope ID and port should be invalid";
    EXPECT_FALSE(isValidIpV6("fe80::a236:bcff:fecb:a1be%eno2:17813"))
        << "Full address with scope and port should be invalid";

    // Invalid addresses
    EXPECT_FALSE(isValidIpV6("192.168.1.1")) << "IPv4 should be invalid";
    EXPECT_FALSE(isValidIpV6("localhost")) << "Hostname should be invalid";
    EXPECT_FALSE(isValidIpV6("")) << "Empty string should be invalid";
    EXPECT_FALSE(isValidIpV6("not-an-ip")) << "Random string should be invalid";
}

// Test parseHostNameWithPort function with IPv6 addresses
TEST_F(IPv6ParsingTest, ParseHostNameWithPort) {
    // Test bracketed IPv6 with port
    {
        auto [host, port] = parseHostNameWithPort("[::1]:17813");
        EXPECT_EQ(host, "::1") << "Should extract loopback address";
        EXPECT_EQ(port, 17813) << "Should extract port 17813";
    }

    // Test bracketed link-local with scope ID and port
    {
        auto [host, port] =
            parseHostNameWithPort("[fe80::a236:bcff:fecb:a1be%eno2]:17813");
        EXPECT_EQ(host, "fe80::a236:bcff:fecb:a1be%eno2")
            << "Should preserve scope ID";
        EXPECT_EQ(port, 17813) << "Should extract port 17813";
    }

    // Test unbracketed IPv6 with scope ID and port (common in internal usage)
    {
        auto [host, port] =
            parseHostNameWithPort("fe80::a236:bcff:fecb:a1be%eno2:15773");
        EXPECT_EQ(host, "fe80::a236:bcff:fecb:a1be%eno2")
            << "Should correctly parse host with scope ID";
        EXPECT_EQ(port, 15773) << "Should extract correct port";
    }

    // Test pure IPv6 without port (should use default handshake port)
    {
        auto [host, port] = parseHostNameWithPort("::1");
        EXPECT_EQ(host, "::1") << "Should return loopback address";
        EXPECT_EQ(port, getDefaultHandshakePort())
            << "Should use default handshake port";
    }

    // Test link-local with scope ID but no port
    {
        auto [host, port] =
            parseHostNameWithPort("fe80::a236:bcff:fecb:a1be%eno2");
        EXPECT_EQ(host, "fe80::a236:bcff:fecb:a1be%eno2")
            << "Should preserve full address with scope";
        EXPECT_EQ(port, getDefaultHandshakePort())
            << "Should use default handshake port";
    }

    // Test IPv4 address (should still work)
    {
        auto [host, port] = parseHostNameWithPort("192.168.1.1:8080");
        EXPECT_EQ(host, "192.168.1.1") << "Should extract IPv4 address";
        EXPECT_EQ(port, 8080) << "Should extract port";
    }

    // Test hostname with port
    {
        auto [host, port] = parseHostNameWithPort("localhost:17813");
        EXPECT_EQ(host, "localhost") << "Should extract hostname";
        EXPECT_EQ(port, 17813) << "Should extract port";
    }
}

// Test maybeWrapIpV6 function
TEST_F(IPv6ParsingTest, MaybeWrapIpV6) {
    // IPv6 addresses should be wrapped
    EXPECT_EQ(maybeWrapIpV6("::1"), "[::1]") << "Loopback should be wrapped";
    EXPECT_EQ(maybeWrapIpV6("fe80::1%eth0"), "[fe80::1%eth0]")
        << "Link-local with scope should be wrapped";
    EXPECT_EQ(maybeWrapIpV6("fe80::a236:bcff:fecb:a1be%eno2"),
              "[fe80::a236:bcff:fecb:a1be%eno2]")
        << "Full link-local should be wrapped";

    // Non-IPv6 should not be wrapped
    EXPECT_EQ(maybeWrapIpV6("192.168.1.1"), "192.168.1.1")
        << "IPv4 should not be wrapped";
    EXPECT_EQ(maybeWrapIpV6("localhost"), "localhost")
        << "Hostname should not be wrapped";
}

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
        master_.Stop();
    }

    std::shared_ptr<RealClient> client_;
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

    // Setup the client with IPv6 address
    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");

    LOG(INFO) << "Setting up client with server address: "
              << FLAGS_server_address;

    ASSERT_EQ(
        client_->setup_real(FLAGS_server_address, "P2PHANDSHAKE",
                            16 * 1024 * 1024, 16 * 1024 * 1024, FLAGS_protocol,
                            rdma_devices, master_address_),
        0)
        << "Client setup should succeed with IPv6 address";

    // Test Put operation
    const std::string test_data = "Hello, IPv6 World!";
    const std::string key = "ipv6_test_key";

    std::span<const char> data_span(test_data.data(), test_data.size());
    ReplicateConfig config;
    config.replica_num = 1;

    int put_result = client_->put(key, data_span, config);
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

    // Setup client with link-local address
    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");

    ASSERT_EQ(
        client_->setup_real(server_address, "P2PHANDSHAKE", 16 * 1024 * 1024,
                            16 * 1024 * 1024, FLAGS_protocol, rdma_devices,
                            master_address_),
        0)
        << "Client setup should succeed with link-local IPv6 address";

    // Test Put operation
    const std::string test_data = "Hello, Link-Local IPv6!";
    const std::string key = "ipv6_linklocal_test_key";

    std::span<const char> data_span(test_data.data(), test_data.size());
    ReplicateConfig config;
    config.replica_num = 1;

    int put_result = client_->put(key, data_span, config);
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

    // Setup the client
    const std::string rdma_devices = (FLAGS_protocol == std::string("rdma"))
                                         ? FLAGS_device_name
                                         : std::string("");

    ASSERT_EQ(
        client_->setup_real(FLAGS_server_address, "P2PHANDSHAKE",
                            16 * 1024 * 1024, 16 * 1024 * 1024, FLAGS_protocol,
                            rdma_devices, master_address_),
        0);

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
    ReplicateConfig config;
    config.replica_num = 1;

    int batch_put_result = client_->put_batch(keys, data_spans, config);
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

// Test that IPv6 address with different formats are handled correctly
TEST_F(IPv6ClientTest, IPv6AddressFormatVariations) {
    // Skip if not using IPv6
    const char* use_ipv6 = getenv("MC_USE_IPV6");
    if (!use_ipv6 || std::string(use_ipv6) != "1") {
        GTEST_SKIP() << "MC_USE_IPV6 is not set to 1; skipping IPv6 test";
    }

    // Test different IPv6 address formats that should be parsed correctly
    std::vector<std::pair<std::string, std::pair<std::string, uint16_t>>>
        test_cases = {
            {"[::1]:8080", {"::1", 8080}},
            {"[2001:db8::1]:9000", {"2001:db8::1", 9000}},
            {"[fe80::1%lo]:7000", {"fe80::1%lo", 7000}},
        };

    for (const auto& [input, expected] : test_cases) {
        auto [host, port] = parseHostNameWithPort(input);
        EXPECT_EQ(host, expected.first) << "Host mismatch for input: " << input;
        EXPECT_EQ(port, expected.second)
            << "Port mismatch for input: " << input;
    }
}

}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    return RUN_ALL_TESTS();
}
