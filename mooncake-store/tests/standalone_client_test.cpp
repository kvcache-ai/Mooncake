#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <memory>
#include <span>
#include <string>

#include "real_client.h"
#include "replica.h"
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
        unsetenv("MOONCAKE_ENABLE_STANDALONE");
        if (client_) {
            client_->tearDownAll();
            client_.reset();
        }
    }

    std::string rdma_devices() const {
        return (FLAGS_protocol == std::string("rdma")) ? FLAGS_device_name
                                                       : std::string("");
    }

    std::shared_ptr<RealClient> client_;
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
        /*enable_standalone=*/true);
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
    config[CONFIG_KEY_ENABLE_STANDALONE] = "true";

    auto result = client_->setup_internal(config);
    ASSERT_TRUE(result.has_value())
        << "setup_internal should start an embedded master when "
           "enable_standalone=true";

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
    unsetenv("MOONCAKE_ENABLE_STANDALONE");
    ConfigDict config;
    config[CONFIG_KEY_LOCAL_HOSTNAME] = "localhost";
    config[CONFIG_KEY_GLOBAL_SEGMENT_SIZE] = "16MB";
    config[CONFIG_KEY_LOCAL_BUFFER_SIZE] = "16MB";
    config[CONFIG_KEY_PROTOCOL] = FLAGS_protocol;
    auto result = client_->setup_internal(config);
    ASSERT_FALSE(result.has_value());
    EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
}

TEST_F(StandaloneClientTest, EnvVarEnablesStandaloneWithoutKwarg) {
    ASSERT_EQ(setenv("MOONCAKE_ENABLE_STANDALONE", "true", 1), 0);
    ASSERT_EQ(
        client_->setup_real("localhost", "P2PHANDSHAKE", 16 * 1024 * 1024,
                            16 * 1024 * 1024, FLAGS_protocol, rdma_devices(),
                            /*master_server_addr=*/"127.0.0.1:50051"),
        0)
        << "MOONCAKE_ENABLE_STANDALONE should embed master for HiCache-style "
           "setup() calls that omit enable_standalone";

    const std::string key = "standalone_env_key";
    const std::string test_data = "hello-env-standalone";
    std::span<const char> data_span(test_data.data(), test_data.size());
    ASSERT_EQ(client_->put(key, data_span), 0);
    auto buffer_handle = client_->get_buffer(key);
    ASSERT_NE(buffer_handle, nullptr);
    EXPECT_EQ(std::string(static_cast<const char*>(buffer_handle->ptr()),
                          buffer_handle->size()),
              test_data);
    unsetenv("MOONCAKE_ENABLE_STANDALONE");
}

}  // namespace testing
}  // namespace mooncake
