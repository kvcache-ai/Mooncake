#include "../../src/nvme_kv/config/connector_config.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>

#include "environ.h"

namespace mooncake::test {
namespace {

class NvmeKvConnectorConfigTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("NvmeKvConnectorConfigTest");
    }

    static void TearDownTestSuite() { google::ShutdownGoogleLogging(); }

    void SetUp() override {
        original_logtostderr_ = FLAGS_logtostderr;
        FLAGS_logtostderr = true;
    }

    void TearDown() override { FLAGS_logtostderr = original_logtostderr_; }

    void SetDevicePath() {
        source_.Set("MOONCAKE_NVME_KV_DEVICE_PATH", "/dev/nvme0n1");
    }

    tl::expected<NvmeKvConnectorConfig, ErrorCode> Load() const {
        return NvmeKvConnectorConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;

   private:
    bool original_logtostderr_ = false;
};

TEST_F(NvmeKvConnectorConfigTest, MissingDevicePathStopsBeforeTransport) {
    source_.Set("MOONCAKE_NVME_KV_TRANSPORT", "invalid");

    testing::internal::CaptureStderr();
    const auto config = Load();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    ASSERT_FALSE(config.has_value());
    EXPECT_EQ(config.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_NE(
        diagnostics.find("MOONCAKE_NVME_KV_DEVICE_PATH must not be empty"),
        std::string::npos);
    EXPECT_EQ(diagnostics.find("Unknown MOONCAKE_NVME_KV_TRANSPORT"),
              std::string::npos);
}

TEST_F(NvmeKvConnectorConfigTest, EmptyDevicePathIsRejected) {
    source_.Set("MOONCAKE_NVME_KV_DEVICE_PATH", "");

    testing::internal::CaptureStderr();
    const auto config = Load();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    ASSERT_FALSE(config.has_value());
    EXPECT_EQ(config.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_NE(
        diagnostics.find("MOONCAKE_NVME_KV_DEVICE_PATH must not be empty"),
        std::string::npos);
}

TEST_F(NvmeKvConnectorConfigTest, UnsetOptionalValuesKeepRealExecutorDefaults) {
    SetDevicePath();

    const auto config = Load();

    ASSERT_TRUE(config.has_value());
    EXPECT_EQ(config->device_path, "/dev/nvme0n1");
    EXPECT_EQ(config->nsid, 1u);
    EXPECT_EQ(config->queue_depth, 256u);
    EXPECT_EQ(config->runtime_transfer_limit, 270336u);
    EXPECT_EQ(config->transport, NvmeKvTransport::kAuto);
}

TEST_F(NvmeKvConnectorConfigTest, PreservesLegacyUnsignedIntegerSyntax) {
    struct Case {
        const char* value;
        uint32_t expected;
    };
    const Case cases[] = {{"0", 0u},
                          {"+17", 17u},
                          {" 17", 17u},
                          {"010", 8u},
                          {"0x10", 16u},
                          {"-0", 0u},
                          {"4294967295", std::numeric_limits<uint32_t>::max()}};
    SetDevicePath();
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        source_.Set("MOONCAKE_NVME_KV_NSID", entry.value);
        source_.Set("MOONCAKE_NVME_KV_QUEUE_DEPTH", entry.value);
        source_.Set("MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT", entry.value);

        const auto config = Load();

        ASSERT_TRUE(config.has_value());
        EXPECT_EQ(config->nsid, entry.expected);
        EXPECT_EQ(config->queue_depth, entry.expected);
        EXPECT_EQ(config->runtime_transfer_limit, entry.expected);
    }
}

TEST_F(NvmeKvConnectorConfigTest, InvalidUnsignedIntegersKeepDefaultsSilently) {
    const char* values[] = {"", "17 ", "17x", "-1", "0x", "4294967296", "abc"};
    SetDevicePath();
    for (const char* value : values) {
        SCOPED_TRACE(value);
        source_.Set("MOONCAKE_NVME_KV_NSID", value);
        source_.Set("MOONCAKE_NVME_KV_QUEUE_DEPTH", value);
        source_.Set("MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT", value);

        testing::internal::CaptureStderr();
        const auto config = Load();
        const auto diagnostics = testing::internal::GetCapturedStderr();

        ASSERT_TRUE(config.has_value());
        EXPECT_EQ(config->nsid, 1u);
        EXPECT_EQ(config->queue_depth, 256u);
        EXPECT_EQ(config->runtime_transfer_limit, 270336u);
        EXPECT_TRUE(diagnostics.empty()) << diagnostics;
    }
}

TEST_F(NvmeKvConnectorConfigTest, AcceptsOnlyExistingTransportTokens) {
    struct Case {
        const char* value;
        NvmeKvTransport expected;
    };
    const Case cases[] = {{"", NvmeKvTransport::kAuto},
                          {"auto", NvmeKvTransport::kAuto},
                          {"io_uring", NvmeKvTransport::kIoUring},
                          {"ioctl", NvmeKvTransport::kIoctl}};
    SetDevicePath();
    for (const auto& entry : cases) {
        SCOPED_TRACE(entry.value);
        source_.Set("MOONCAKE_NVME_KV_TRANSPORT", entry.value);

        const auto config = Load();

        ASSERT_TRUE(config.has_value());
        EXPECT_EQ(config->transport, entry.expected);
    }
}

TEST_F(NvmeKvConnectorConfigTest, InvalidTransportKeepsErrorAndDiagnostic) {
    SetDevicePath();
    for (const char* value : {"AUTO", "io-uring", " ioctl", "invalid"}) {
        SCOPED_TRACE(value);
        source_.Set("MOONCAKE_NVME_KV_TRANSPORT", value);

        testing::internal::CaptureStderr();
        const auto config = Load();
        const auto diagnostics = testing::internal::GetCapturedStderr();

        ASSERT_FALSE(config.has_value());
        EXPECT_EQ(config.error(), ErrorCode::INVALID_PARAMS);
        EXPECT_NE(diagnostics.find(std::string("Unknown ") +
                                   "MOONCAKE_NVME_KV_TRANSPORT: " + value),
                  std::string::npos);
    }
}

TEST_F(NvmeKvConnectorConfigTest, NewConfigsReadCurrentEnvironment) {
    SetDevicePath();
    source_.Set("MOONCAKE_NVME_KV_NSID", "1");
    const auto first = Load();
    source_.Set("MOONCAKE_NVME_KV_NSID", "2");
    const auto second = Load();

    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->nsid, 1u);
    EXPECT_EQ(second->nsid, 2u);
}

}  // namespace
}  // namespace mooncake::test
