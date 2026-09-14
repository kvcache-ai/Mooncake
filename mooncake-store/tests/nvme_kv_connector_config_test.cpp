#include "../src/config/nvme_kv_connector_config.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <optional>
#include <string>

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
        for (size_t i = 0; i < kVariables.size(); ++i) {
            if (const char* value = std::getenv(kVariables[i])) {
                original_[i] = value;
            }
            ASSERT_EQ(unsetenv(kVariables[i]), 0);
        }
    }

    void TearDown() override {
        for (size_t i = 0; i < kVariables.size(); ++i) {
            if (original_[i].has_value()) {
                EXPECT_EQ(setenv(kVariables[i], original_[i]->c_str(), 1), 0);
            } else {
                EXPECT_EQ(unsetenv(kVariables[i]), 0);
            }
        }
        FLAGS_logtostderr = original_logtostderr_;
    }

    void SetDevicePath() {
        ASSERT_EQ(setenv("MOONCAKE_NVME_KV_DEVICE_PATH", "/dev/nvme0n1", 1), 0);
    }

   private:
    inline static constexpr std::array<const char*, 5> kVariables = {
        "MOONCAKE_NVME_KV_DEVICE_PATH", "MOONCAKE_NVME_KV_NSID",
        "MOONCAKE_NVME_KV_QUEUE_DEPTH",
        "MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT",
        "MOONCAKE_NVME_KV_TRANSPORT"};
    std::array<std::optional<std::string>, kVariables.size()> original_;
    bool original_logtostderr_ = false;
};

TEST_F(NvmeKvConnectorConfigTest, MissingDevicePathStopsBeforeTransport) {
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_TRANSPORT", "invalid", 1), 0);

    testing::internal::CaptureStderr();
    const auto config = NvmeKvConnectorConfig::FromEnvironment();
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
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_DEVICE_PATH", "", 1), 0);

    testing::internal::CaptureStderr();
    const auto config = NvmeKvConnectorConfig::FromEnvironment();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    ASSERT_FALSE(config.has_value());
    EXPECT_EQ(config.error(), ErrorCode::INVALID_PARAMS);
    EXPECT_NE(
        diagnostics.find("MOONCAKE_NVME_KV_DEVICE_PATH must not be empty"),
        std::string::npos);
}

TEST_F(NvmeKvConnectorConfigTest, UnsetOptionalValuesKeepRealExecutorDefaults) {
    SetDevicePath();

    const auto config = NvmeKvConnectorConfig::FromEnvironment();

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
        ASSERT_EQ(setenv("MOONCAKE_NVME_KV_NSID", entry.value, 1), 0);
        ASSERT_EQ(setenv("MOONCAKE_NVME_KV_QUEUE_DEPTH", entry.value, 1), 0);
        ASSERT_EQ(
            setenv("MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT", entry.value, 1),
            0);

        const auto config = NvmeKvConnectorConfig::FromEnvironment();

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
        ASSERT_EQ(setenv("MOONCAKE_NVME_KV_NSID", value, 1), 0);
        ASSERT_EQ(setenv("MOONCAKE_NVME_KV_QUEUE_DEPTH", value, 1), 0);
        ASSERT_EQ(setenv("MOONCAKE_NVME_KV_RUNTIME_TRANSFER_LIMIT", value, 1),
                  0);

        testing::internal::CaptureStderr();
        const auto config = NvmeKvConnectorConfig::FromEnvironment();
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
        ASSERT_EQ(setenv("MOONCAKE_NVME_KV_TRANSPORT", entry.value, 1), 0);

        const auto config = NvmeKvConnectorConfig::FromEnvironment();

        ASSERT_TRUE(config.has_value());
        EXPECT_EQ(config->transport, entry.expected);
    }
}

TEST_F(NvmeKvConnectorConfigTest, InvalidTransportKeepsErrorAndDiagnostic) {
    SetDevicePath();
    for (const char* value : {"AUTO", "io-uring", " ioctl", "invalid"}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MOONCAKE_NVME_KV_TRANSPORT", value, 1), 0);

        testing::internal::CaptureStderr();
        const auto config = NvmeKvConnectorConfig::FromEnvironment();
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
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_NSID", "1", 1), 0);
    const auto first = NvmeKvConnectorConfig::FromEnvironment();
    ASSERT_EQ(setenv("MOONCAKE_NVME_KV_NSID", "2", 1), 0);
    const auto second = NvmeKvConnectorConfig::FromEnvironment();

    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->nsid, 1u);
    EXPECT_EQ(second->nsid, 2u);
}

}  // namespace
}  // namespace mooncake::test
