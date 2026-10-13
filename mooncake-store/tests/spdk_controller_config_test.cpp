#include "../src/config/spdk_controller_config.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>

#include "environ.h"

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
    }

    void TearDown() override { FLAGS_logtostderr = original_logtostderr_; }

    SpdkControllerConfig Load() const {
        return SpdkControllerConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
    bool original_logtostderr_ = false;
};

TEST_F(SpdkControllerConfigTest, UnsetAndEmptyValuesLeaveOverridesAbsent) {
    source_.Set("MC_NVME_NUM_IO_QUEUES", "");
    source_.Set("MC_NVME_HEADER_DIGEST", "");

    testing::internal::CaptureStderr();
    const auto config = Load();
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
    source_.Set("MC_NVME_NUM_IO_QUEUES", "11");
    source_.Set("MC_NVME_IO_QUEUE_SIZE", "22");
    source_.Set("MC_NVME_IO_QUEUE_REQUESTS", "33");
    source_.Set("MC_NVME_TRANSPORT_ACK_TIMEOUT", "44");
    source_.Set("MC_NVME_ADMIN_QUEUE_SIZE", "55");
    source_.Set("MC_NVME_FABRICS_CONNECT_TIMEOUT_US", "66");
    source_.Set("MC_NVME_HEADER_DIGEST", "false");
    source_.Set("MC_NVME_DATA_DIGEST", "TRUE");

    const auto config = Load();

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
    source_.Set("MC_NVME_NUM_IO_QUEUES", "  +42 ");
    source_.Set("MC_NVME_IO_QUEUE_SIZE", "010");
    source_.Set("MC_NVME_IO_QUEUE_REQUESTS", "4294967295");
    source_.Set("MC_NVME_TRANSPORT_ACK_TIMEOUT", "255");
    source_.Set("MC_NVME_ADMIN_QUEUE_SIZE", "65535");
    source_.Set("MC_NVME_FABRICS_CONNECT_TIMEOUT_US", "18446744073709551615");
    source_.Set("MC_NVME_HEADER_DIGEST", "OFF");
    source_.Set("MC_NVME_DATA_DIGEST", "enable");

    const auto config = Load();

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
    source_.Set("MC_NVME_NUM_IO_QUEUES", "4294967296");
    source_.Set("MC_NVME_IO_QUEUE_SIZE", "-1");
    source_.Set("MC_NVME_IO_QUEUE_REQUESTS", "0x10");
    source_.Set("MC_NVME_TRANSPORT_ACK_TIMEOUT", "256");
    source_.Set("MC_NVME_ADMIN_QUEUE_SIZE", "65536");
    source_.Set("MC_NVME_FABRICS_CONNECT_TIMEOUT_US", "18446744073709551616");
    source_.Set("MC_NVME_HEADER_DIGEST", "-0");
    source_.Set("MC_NVME_DATA_DIGEST", "2");

    testing::internal::CaptureStderr();
    const auto config = Load();
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
    source_.Set("MC_NVME_NUM_IO_QUEUES", "1");
    const auto first = Load();
    source_.Set("MC_NVME_NUM_IO_QUEUES", "2");
    const auto second = Load();
    source_.Unset("MC_NVME_NUM_IO_QUEUES");
    const auto third = Load();

    EXPECT_EQ(first.num_io_queues, 1u);
    EXPECT_EQ(second.num_io_queues, 2u);
    EXPECT_FALSE(third.num_io_queues.has_value());
}

}  // namespace
}  // namespace mooncake::test
