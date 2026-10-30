#include <gtest/gtest.h>
#include <stdexcept>
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
                input, "P2PHANDSHAKE", "ascend");
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
    EXPECT_EQ(parsed.local_hostname, "127.0.0.1:12345");
    EXPECT_FALSE(parsed.enable_offload);
    EXPECT_EQ(parsed.client_rpc_port, 12346);
    config["enable_offload"] = "true";
    auto with_offload =
        CentralizedClientConfigBuilder::build_centralized_real_client(config);
    EXPECT_TRUE(with_offload.enable_offload);
    EXPECT_EQ(with_offload.client_rpc_port, parsed.client_rpc_port);
    config["client_rpc_port"] = "65536";
    EXPECT_THROW(
        CentralizedClientConfigBuilder::build_centralized_real_client(config),
        std::invalid_argument);
}

}  // namespace
}  // namespace mooncake
