#include <gtest/gtest.h>
#include <string>
#include <unordered_map>
#include <utility>

#include "centralized_client_config_builder.h"

namespace mooncake {
namespace {

TEST(ClientConfigBuilderTest, PreservesAscendAddressForTransferEngine) {
    const std::pair<const char*, const char*> addresses[] = {
        {"127.0.0.1:12345:npu_2", "127.0.0.1"},
        {"localhost:12345:npu_2", "localhost"},
        {"[::1]:12345:npu_2", "::1"},
    };
    for (const auto& [input, host] : addresses) {
        SCOPED_TRACE(input);
        const auto config =
            CentralizedClientConfigBuilder::build_centralized_real_client(
                input, "P2PHANDSHAKE", "ascend")
                .value();
        EXPECT_EQ(config.local_hostname, input);
        int device = -1;
        const auto [te_host, te_port] =
            parseHostNameWithPortAscend(config.local_hostname, &device);
        EXPECT_EQ(te_host, host);
        EXPECT_EQ(te_port, 12345);
        EXPECT_EQ(device, 2);
    }
}

TEST(ClientConfigBuilderTest, CentralizedClientRpcPortIsIndependentOfOffload) {
    std::unordered_map<std::string, std::string> config = {
        {"local_hostname", "127.0.0.1:12345"},
        {"metadata_server", "P2PHANDSHAKE"},
        {"client_rpc_port", "12346"},
    };
    auto parsed =
        CentralizedClientConfigBuilder::build_centralized_real_client(config);
    ASSERT_TRUE(parsed);
    EXPECT_EQ(parsed->local_hostname, "127.0.0.1:12345");
    EXPECT_FALSE(parsed->enable_offload);
    EXPECT_EQ(parsed->client_rpc_port, 12346);
    config["enable_offload"] = " True ";
    auto with_offload =
        CentralizedClientConfigBuilder::build_centralized_real_client(config);
    ASSERT_TRUE(with_offload);
    EXPECT_TRUE(with_offload->enable_offload);
    EXPECT_EQ(with_offload->client_rpc_port, parsed->client_rpc_port);
    config["enable_offload"] = "invalid";
    auto invalid_offload =
        CentralizedClientConfigBuilder::build_centralized_real_client(config);
    ASSERT_TRUE(invalid_offload);
    EXPECT_FALSE(invalid_offload->enable_offload);
    EXPECT_EQ(invalid_offload->client_rpc_port, parsed->client_rpc_port);
    config["client_rpc_port"] = "65536";
    auto invalid_port =
        CentralizedClientConfigBuilder::build_centralized_real_client(config);
    ASSERT_FALSE(invalid_port);
    EXPECT_EQ(invalid_port.error(), ErrorCode::INVALID_PARAMS);
}

TEST(ClientConfigBuilderTest, CentralizedDictParsesCapacitySyntax) {
    std::unordered_map<std::string, std::string> values = {
        {"local_hostname", "127.0.0.1"}, {"metadata_server", "P2PHANDSHAKE"}};
    auto config =
        CentralizedClientConfigBuilder::build_centralized_real_client(values);
    ASSERT_TRUE(config);
    EXPECT_EQ(config->global_segment_size, 16ULL * 1024 * 1024);
    EXPECT_EQ(config->local_buffer_size, 16ULL * 1024 * 1024);

    values["global_segment_size"] = "2 TB";
    values["local_buffer_size"] = " 16 MB ";
    config =
        CentralizedClientConfigBuilder::build_centralized_real_client(values);
    ASSERT_TRUE(config);
    EXPECT_EQ(config->global_segment_size, 2ULL * 1024 * 1024 * 1024 * 1024);
    EXPECT_EQ(config->local_buffer_size, 16ULL * 1024 * 1024);

    values["global_segment_size"] = "0";
    values["local_buffer_size"] = "0";
    config =
        CentralizedClientConfigBuilder::build_centralized_real_client(values);
    ASSERT_TRUE(config);
    EXPECT_EQ(config->global_segment_size, 0u);
    EXPECT_EQ(config->local_buffer_size, 0u);

    for (const auto* key : {"global_segment_size", "local_buffer_size"}) {
        auto invalid = values;
        invalid[key] = "invalid";
        auto result =
            CentralizedClientConfigBuilder::build_centralized_real_client(
                invalid);
        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), ErrorCode::INVALID_PARAMS);
    }

    // Both overloads preserve values; Real validates the business boundary.
    values["global_segment_size"] = "1023";
    config =
        CentralizedClientConfigBuilder::build_centralized_real_client(values);
    auto typed = CentralizedClientConfigBuilder::build_centralized_real_client(
        "127.0.0.1", "P2PHANDSHAKE", "tcp", std::nullopt, "127.0.0.1:50051",
        1023, 0);
    ASSERT_TRUE(config);
    ASSERT_TRUE(typed);
    EXPECT_EQ(config->global_segment_size, 1023u);
    EXPECT_EQ(typed->global_segment_size, config->global_segment_size);
    EXPECT_EQ(typed->local_buffer_size, config->local_buffer_size);
}

TEST(ClientConfigBuilderTest, CentralizedDictRequiresHost) {
    std::unordered_map<std::string, std::string> values = {
        {"metadata_server", "P2PHANDSHAKE"}};
    auto missing =
        CentralizedClientConfigBuilder::build_centralized_real_client(values);
    ASSERT_FALSE(missing);
    EXPECT_EQ(missing.error(), ErrorCode::INVALID_PARAMS);
    values["local_hostname"] = "";
    auto empty =
        CentralizedClientConfigBuilder::build_centralized_real_client(values);
    ASSERT_FALSE(empty);
    EXPECT_EQ(empty.error(), ErrorCode::INVALID_PARAMS);
    auto typed = CentralizedClientConfigBuilder::build_centralized_real_client(
        "", "P2PHANDSHAKE");
    ASSERT_FALSE(typed);
    EXPECT_EQ(typed.error(), empty.error());
}

TEST(ClientConfigBuilderTest, CentralizedAutoDiscoveryInBothOverloads) {
    auto typed = CentralizedClientConfigBuilder::build_centralized_real_client(
                     "127.0.0.1", "P2PHANDSHAKE", "rdma", "auto-discovery")
                     .value();
    EXPECT_FALSE(typed.rdma_devices.has_value());

    std::unordered_map<std::string, std::string> values = {
        {"local_hostname", "127.0.0.1"},
        {"metadata_server", "P2PHANDSHAKE"},
        {"rdma_devices", "auto-discovery"}};
    auto config =
        CentralizedClientConfigBuilder::build_centralized_real_client(values);
    ASSERT_TRUE(config);
    EXPECT_FALSE(config->rdma_devices.has_value());
}

}  // namespace
}  // namespace mooncake
