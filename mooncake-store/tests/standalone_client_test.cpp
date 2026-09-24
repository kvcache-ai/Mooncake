#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <span>
#include <string>

#include "common/network.h"
#include "embedded_master.h"
#include "real_client.h"
#include "replica.h"
#include "transfer_engine.h"
#include "types.h"

DEFINE_string(protocol, "tcp", "Transfer protocol: rdma|tcp");
DEFINE_string(device_name, "", "Device name to use, valid if protocol=rdma");

namespace mooncake {
namespace testing {

class StandaloneClientTest : public ::testing::Test {
   protected:
    void SetUp() override {
        if (getenv("PROTOCOL")) FLAGS_protocol = getenv("PROTOCOL");
        if (getenv("DEVICE_NAME")) FLAGS_device_name = getenv("DEVICE_NAME");
        client_ = RealClient::create();
    }

    void TearDown() override {
        unsetenv("MOONCAKE_ENABLE_EMBEDDED_MASTER");
        if (client_) {
            client_->tearDownAll();
            client_.reset();
        }
        external_master_.reset();
    }

    std::string rdma_devices() const {
        return (FLAGS_protocol == std::string("rdma")) ? FLAGS_device_name
                                                       : std::string("");
    }

    std::shared_ptr<RealClient> client_;
    std::unique_ptr<EmbeddedMaster> external_master_;
};

TEST_F(StandaloneClientTest, PutGetWithoutExternalMaster) {
    auto setup = client_->setup_internal(
        "localhost", "P2PHANDSHAKE", 16 * 1024 * 1024, 16 * 1024 * 1024,
        FLAGS_protocol, rdma_devices(),
        /*master_server_addr=*/"",
        /*transfer_engine=*/nullptr,
        /*ipc_socket_path=*/"",
        /*local_rpc_port=*/50052,
        /*enable_ssd_offload=*/false,
        /*start_offload_rpc_server=*/true,
        /*ssd_offload_path=*/"",
        /*tenant_id=*/"default",
        /*enable_client_http_server=*/false,
        /*client_http_port=*/DEFAULT_CLIENT_HTTP_PORT,
        /*enable_embedded_master=*/true);
    ASSERT_TRUE(setup.has_value())
        << "Standalone setup should succeed without mooncake_master";

    const std::string key = "standalone_key";
    const std::string test_data = "hello-standalone-store";
    ReplicateConfig config;
    std::span<const char> data_span(test_data.data(), test_data.size());
    ASSERT_EQ(client_->put(key, data_span, config), 0);

    auto buffer_handle = client_->get_buffer(key);
    ASSERT_NE(buffer_handle, nullptr);
    EXPECT_EQ(buffer_handle->size(), test_data.size());
    EXPECT_EQ(std::string(static_cast<const char*>(buffer_handle->ptr()),
                          buffer_handle->size()),
              test_data);
    EXPECT_EQ(client_->isExist(key), 1);
}

TEST_F(StandaloneClientTest, ConfigDictOmitsMasterAddress) {
    ConfigDict config;
    config[CONFIG_KEY_LOCAL_HOSTNAME] = "localhost";
    config[CONFIG_KEY_METADATA_SERVER] = "P2PHANDSHAKE";
    config[CONFIG_KEY_GLOBAL_SEGMENT_SIZE] = "16MB";
    config[CONFIG_KEY_LOCAL_BUFFER_SIZE] = "16MB";
    config[CONFIG_KEY_PROTOCOL] = FLAGS_protocol;
    config[CONFIG_KEY_RDMA_DEVICES] = rdma_devices();
    config[CONFIG_KEY_ENABLE_EMBEDDED_MASTER] = "true";

    auto result = client_->setup_internal(config);
    ASSERT_TRUE(result.has_value())
        << "setup_internal should start an embedded master when "
           "enable_embedded_master=true";

    const std::string key = "standalone_dict_key";
    const std::string test_data = "config-dict-standalone";
    std::span<const char> data_span(test_data.data(), test_data.size());
    ASSERT_EQ(client_->put(key, data_span), 0);
    auto buffer_handle = client_->get_buffer(key);
    ASSERT_NE(buffer_handle, nullptr);
    EXPECT_EQ(std::string(static_cast<const char*>(buffer_handle->ptr()),
                          buffer_handle->size()),
              test_data);
}

TEST_F(StandaloneClientTest, RejectsEmptyMetadataWithoutStandalone) {
    unsetenv("MOONCAKE_ENABLE_EMBEDDED_MASTER");
    ConfigDict config;
    config[CONFIG_KEY_LOCAL_HOSTNAME] = "localhost";
    config[CONFIG_KEY_GLOBAL_SEGMENT_SIZE] = "16MB";
    config[CONFIG_KEY_LOCAL_BUFFER_SIZE] = "16MB";
    config[CONFIG_KEY_PROTOCOL] = FLAGS_protocol;
    auto result = client_->setup_internal(config);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(StandaloneClientTest, RejectsEmptyMetadataWithoutTransferEngine) {
    unsetenv("MOONCAKE_ENABLE_EMBEDDED_MASTER");
    auto result = client_->setup_internal("localhost", "", 0, 0, "tcp", "",
                                          /*master_server_addr=*/"",
                                          /*transfer_engine=*/nullptr);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(StandaloneClientTest, ReusesTransferEngineWithoutMetadataServer) {
    unsetenv("MOONCAKE_ENABLE_EMBEDDED_MASTER");
    external_master_ = std::make_unique<EmbeddedMaster>();
    InProcMasterConfig master_config;
    master_config.http_metadata_port = 0;
    ASSERT_TRUE(external_master_->Start(master_config));

    // Initialize the caller-owned engine separately. Setup must keep its
    // metadata connection and listening endpoint, without initializing it
    // again.
    auto transfer_engine = std::make_shared<TransferEngine>(false);
    const int port = getFreeTcpPort();
    ASSERT_GT(port, 0);
    const auto requested_endpoint = "127.0.0.1:" + std::to_string(port);
    ASSERT_EQ(
        transfer_engine->init("P2PHANDSHAKE", requested_endpoint, "", 0, "tcp"),
        0);
    if (!transfer_engine->isUsingTent()) {
        ASSERT_NE(transfer_engine->installTransport("tcp", nullptr), nullptr);
    }
    const auto endpoint = transfer_engine->getLocalIpAndPort();
    const auto rpc_port = transfer_engine->getRpcPort();
    const auto metadata = transfer_engine->isUsingTent()
                              ? nullptr
                              : transfer_engine->getMetadata();

    auto result = client_->setup_internal(
        endpoint, "", 16 * 1024 * 1024, 16 * 1024 * 1024, "tcp", "",
        external_master_->master_address(), transfer_engine,
        /*ipc_socket_path=*/"",
        /*local_rpc_port=*/50052,
        /*enable_ssd_offload=*/false,
        /*start_offload_rpc_server=*/false,
        /*ssd_offload_path=*/"",
        /*tenant_id=*/"default",
        /*enable_client_http_server=*/false,
        /*client_http_port=*/DEFAULT_CLIENT_HTTP_PORT,
        /*enable_embedded_master=*/false);
    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(transfer_engine->getLocalIpAndPort(), endpoint);
    EXPECT_EQ(transfer_engine->getRpcPort(), rpc_port);
    if (!transfer_engine->isUsingTent()) {
        EXPECT_EQ(transfer_engine->getMetadata(), metadata);
    }

    const std::string key = "reused_transfer_engine_key";
    const std::string test_data = "caller-owned-transfer-engine";
    ASSERT_EQ(client_->put(key, std::span<const char>(test_data)), 0);
    auto buffer_handle = client_->get_buffer(key);
    ASSERT_NE(buffer_handle, nullptr);
    EXPECT_EQ(std::string(static_cast<const char*>(buffer_handle->ptr()),
                          buffer_handle->size()),
              test_data);
}

TEST_F(StandaloneClientTest, EnvVarEnablesStandaloneWithoutKwarg) {
    ASSERT_EQ(setenv("MOONCAKE_ENABLE_EMBEDDED_MASTER", "true", 1), 0);
    ASSERT_EQ(
        client_->setup_real("localhost", "P2PHANDSHAKE", 16 * 1024 * 1024,
                            16 * 1024 * 1024, FLAGS_protocol, rdma_devices(),
                            /*master_server_addr=*/""),
        0)
        << "MOONCAKE_ENABLE_EMBEDDED_MASTER should embed master for "
           "setup() calls that omit enable_embedded_master";

    const std::string key = "standalone_env_key";
    const std::string test_data = "hello-env-standalone";
    std::span<const char> data_span(test_data.data(), test_data.size());
    ASSERT_EQ(client_->put(key, data_span), 0);
    auto buffer_handle = client_->get_buffer(key);
    ASSERT_NE(buffer_handle, nullptr);
    EXPECT_EQ(std::string(static_cast<const char*>(buffer_handle->ptr()),
                          buffer_handle->size()),
              test_data);
    unsetenv("MOONCAKE_ENABLE_EMBEDDED_MASTER");
}

}  // namespace testing
}  // namespace mooncake
