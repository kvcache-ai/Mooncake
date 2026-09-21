#include "../src/config/spdk_controller_config.h"

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

class SpdkControllerConfigTest : public ::testing::Test {
   protected:
    static void SetUpTestSuite() {
        google::InitGoogleLogging("SpdkControllerConfigTest");
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

    inline static constexpr std::array<const char*, 8> kVariables = {
        "MC_NVME_NUM_IO_QUEUES",     "MC_NVME_IO_QUEUE_SIZE",
        "MC_NVME_IO_QUEUE_REQUESTS", "MC_NVME_TRANSPORT_ACK_TIMEOUT",
        "MC_NVME_ADMIN_QUEUE_SIZE",  "MC_NVME_FABRICS_CONNECT_TIMEOUT_US",
        "MC_NVME_HEADER_DIGEST",     "MC_NVME_DATA_DIGEST",
    };
    std::array<std::optional<std::string>, kVariables.size()> original_;
    bool original_logtostderr_ = false;
};

TEST_F(SpdkControllerConfigTest, UnsetAndEmptyValuesLeaveOverridesAbsent) {
    ASSERT_EQ(setenv("MC_NVME_NUM_IO_QUEUES", "", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_HEADER_DIGEST", "", 1), 0);

    testing::internal::CaptureStderr();
    const auto config = SpdkControllerConfig::FromEnvironment();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.num_io_queues.has_value());
    EXPECT_FALSE(config.io_queue_size.has_value());
    EXPECT_FALSE(config.io_queue_requests.has_value());
    EXPECT_FALSE(config.transport_ack_timeout.has_value());
    EXPECT_FALSE(config.admin_queue_size.has_value());
    EXPECT_FALSE(config.fabrics_connect_timeout_us.has_value());
    EXPECT_FALSE(config.header_digest.has_value());
    EXPECT_FALSE(config.data_digest.has_value());
    EXPECT_TRUE(diagnostics.empty()) << diagnostics;
}

TEST_F(SpdkControllerConfigTest, ReadsIndependentValidOverrides) {
    ASSERT_EQ(setenv("MC_NVME_NUM_IO_QUEUES", "11", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_IO_QUEUE_SIZE", "22", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_IO_QUEUE_REQUESTS", "33", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_TRANSPORT_ACK_TIMEOUT", "44", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_ADMIN_QUEUE_SIZE", "55", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_FABRICS_CONNECT_TIMEOUT_US", "66", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_HEADER_DIGEST", "false", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_DATA_DIGEST", "TRUE", 1), 0);

    const auto config = SpdkControllerConfig::FromEnvironment();

    EXPECT_EQ(config.num_io_queues, 11u);
    EXPECT_EQ(config.io_queue_size, 22u);
    EXPECT_EQ(config.io_queue_requests, 33u);
    EXPECT_EQ(config.transport_ack_timeout, 44u);
    EXPECT_EQ(config.admin_queue_size, 55u);
    EXPECT_EQ(config.fabrics_connect_timeout_us, 66u);
    EXPECT_EQ(config.header_digest, false);
    EXPECT_EQ(config.data_digest, true);
}

TEST_F(SpdkControllerConfigTest, UsesTypedIntegerAndBooleanSyntax) {
    ASSERT_EQ(setenv("MC_NVME_NUM_IO_QUEUES", "  +42 ", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_IO_QUEUE_SIZE", "010", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_IO_QUEUE_REQUESTS", "4294967295", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_TRANSPORT_ACK_TIMEOUT", "255", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_ADMIN_QUEUE_SIZE", "65535", 1), 0);
    ASSERT_EQ(
        setenv("MC_NVME_FABRICS_CONNECT_TIMEOUT_US", "18446744073709551615", 1),
        0);
    ASSERT_EQ(setenv("MC_NVME_HEADER_DIGEST", "OFF", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_DATA_DIGEST", "enable", 1), 0);

    const auto config = SpdkControllerConfig::FromEnvironment();

    EXPECT_EQ(config.num_io_queues, 42u);
    EXPECT_EQ(config.io_queue_size, 10u);
    EXPECT_EQ(config.io_queue_requests, std::numeric_limits<uint32_t>::max());
    EXPECT_EQ(config.transport_ack_timeout,
              std::numeric_limits<uint8_t>::max());
    EXPECT_EQ(config.admin_queue_size, std::numeric_limits<uint16_t>::max());
    EXPECT_EQ(config.fabrics_connect_timeout_us,
              std::numeric_limits<uint64_t>::max());
    EXPECT_EQ(config.header_digest, false);
    EXPECT_EQ(config.data_digest, true);
}

TEST_F(SpdkControllerConfigTest,
       RejectsNegativeOutOfRangeAndNonCanonicalValues) {
    ASSERT_EQ(setenv("MC_NVME_NUM_IO_QUEUES", "4294967296", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_IO_QUEUE_SIZE", "-1", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_IO_QUEUE_REQUESTS", "0x10", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_TRANSPORT_ACK_TIMEOUT", "256", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_ADMIN_QUEUE_SIZE", "65536", 1), 0);
    ASSERT_EQ(
        setenv("MC_NVME_FABRICS_CONNECT_TIMEOUT_US", "18446744073709551616", 1),
        0);
    ASSERT_EQ(setenv("MC_NVME_HEADER_DIGEST", "-0", 1), 0);
    ASSERT_EQ(setenv("MC_NVME_DATA_DIGEST", "2", 1), 0);

    testing::internal::CaptureStderr();
    const auto config = SpdkControllerConfig::FromEnvironment();
    const auto diagnostics = testing::internal::GetCapturedStderr();

    EXPECT_FALSE(config.num_io_queues.has_value());
    EXPECT_FALSE(config.io_queue_size.has_value());
    EXPECT_FALSE(config.io_queue_requests.has_value());
    EXPECT_FALSE(config.transport_ack_timeout.has_value());
    EXPECT_FALSE(config.admin_queue_size.has_value());
    EXPECT_FALSE(config.fabrics_connect_timeout_us.has_value());
    EXPECT_FALSE(config.header_digest.has_value());
    EXPECT_FALSE(config.data_digest.has_value());

    EXPECT_TRUE(diagnostics.empty()) << diagnostics;
}

TEST_F(SpdkControllerConfigTest, EachConstructionReadsCurrentEnvironment) {
    ASSERT_EQ(setenv("MC_NVME_NUM_IO_QUEUES", "1", 1), 0);
    const auto first = SpdkControllerConfig::FromEnvironment();
    ASSERT_EQ(setenv("MC_NVME_NUM_IO_QUEUES", "2", 1), 0);
    const auto second = SpdkControllerConfig::FromEnvironment();
    ASSERT_EQ(unsetenv("MC_NVME_NUM_IO_QUEUES"), 0);
    const auto third = SpdkControllerConfig::FromEnvironment();

    EXPECT_EQ(first.num_io_queues, 1u);
    EXPECT_EQ(second.num_io_queues, 2u);
    EXPECT_FALSE(third.num_io_queues.has_value());
}

}  // namespace
}  // namespace mooncake::test
