#include "config/shared_nvme_kv_config.h"
#include <gtest/gtest.h>
#include <sstream>

namespace mooncake::testing {
namespace {
auto Parse(const char* json) {
    std::istringstream input(json);
    return SharedNvmeKvConfig::Parse(input);
}

TEST(SharedNvmeKvConfigTest, PreservesConfiguredOrderAndPerNamespaceTransport) {
    auto config = Parse(R"({"namespaces":[
        {"device_path":"/dev/ng7n1","transport":"ioctl","runtime_transfer_limit":1048576},
        {"device_path":"/dev/ng0n3","nsid":3,"transport":"io_uring","queue_depth":32},
        {"device_path":"/dev/ng2n1"}]})");
    ASSERT_TRUE(config);
    ASSERT_EQ(config->namespaces.size(), 3);
    EXPECT_EQ(config->namespaces[0].device_path, "/dev/ng7n1");
    EXPECT_EQ(config->namespaces[0].transport, NvmeKvTransport::kIoctl);
    EXPECT_EQ(config->namespaces[0].runtime_transfer_limit, 1048576);
    EXPECT_EQ(config->namespaces[1].nsid, 3);
    EXPECT_EQ(config->namespaces[1].transport, NvmeKvTransport::kIoUring);
    EXPECT_EQ(config->namespaces[1].queue_depth, 32);
    EXPECT_EQ(config->namespaces[2].transport, NvmeKvTransport::kAuto);
}

TEST(SharedNvmeKvConfigTest, UsesLocalDefaultsWithoutAdditionalQueueDepthCap) {
    auto config = Parse(R"({"namespaces":[
        {"device_path":"/dev/ng0n1"},
        {"device_path":"/dev/ng1n1","queue_depth":2048}]})");
    ASSERT_TRUE(config);
    NvmeKvConnectorConfig local;
    EXPECT_EQ(config->namespaces[0].nsid, local.nsid);
    EXPECT_EQ(config->namespaces[0].queue_depth, local.queue_depth);
    EXPECT_EQ(config->namespaces[0].runtime_transfer_limit,
              local.runtime_transfer_limit);
    EXPECT_EQ(config->namespaces[1].queue_depth, 2048);
}

TEST(SharedNvmeKvConfigTest, RejectsAmbiguousAndInvalidConfiguration) {
    for (
        auto json :
        {R"({"namespaces":[]})", R"({"namespaces":[{}]})",
         R"({"namespaces":[{"device_path":""}]})",
         R"({"namespaces":[{"device_path":"/dev/ng0n1","nsid":0}]})",
         R"({"namespaces":[{"device_path":"/dev/ng0n1","nsid":4294967295}]})",
         R"({"namespaces":[{"device_path":"/dev/ng0n1","queue_depth":4294967296}]})",
         R"({"namespaces":[{"device_path":"/dev/ng0n1","runtime_transfer_limit":0}]})",
         R"({"namespaces":[{"device_path":"/dev/ng0n1","transport":"spdk"}]})",
         R"({"environment":{},"namespaces":[{"device_path":"/dev/ng0n1"}]})",
         R"({"namespaces":[{"device_path":"/dev/ng0n1","unknown":true}]})",
         R"({"namespaces":[{"device_path":"/dev/ng0n1","nsid":1,"nsid":2}]})",
         R"({"namespaces":[{"device_path":"/dev/ng0n1"},{"device_path":"/dev/ng0n1"}]})",
         R"({"namespaces":[{"device_path":"/dev/ng0n1"}]} trailing)"}) {
        EXPECT_FALSE(Parse(json)) << json;
    }
}

}  // namespace
}  // namespace mooncake::testing
