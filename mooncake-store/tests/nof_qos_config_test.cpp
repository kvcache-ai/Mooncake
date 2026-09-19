#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>
#include <utility>

#include "../src/config/nof_qos_config.h"
#ifdef USE_NOF
#include "transfer_task.h"
#endif

namespace mooncake {
namespace {

class NoFQosConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("NoFQosConfigTest");
        original_logtostderr_ = FLAGS_logtostderr;
        FLAGS_logtostderr = true;
        for (size_t i = 0; i < 2; ++i) {
            if (const char* value = std::getenv(names_[i])) {
                original_[i] = value;
            }
            ASSERT_EQ(unsetenv(names_[i]), 0);
        }
    }

    void TearDown() override {
        for (size_t i = 0; i < 2; ++i) {
            if (original_[i]) {
                EXPECT_EQ(setenv(names_[i], original_[i]->c_str(), 1), 0);
            } else {
                EXPECT_EQ(unsetenv(names_[i]), 0);
            }
        }
        FLAGS_logtostderr = original_logtostderr_;
        google::ShutdownGoogleLogging();
    }

   private:
    const char* names_[2] = {"MC_NOF_SUBMIT_CHUNK_BYTES",
                             "MC_NOF_INFLIGHT_BYTES_LIMIT"};
    std::optional<std::string> original_[2];
    bool original_logtostderr_ = false;
};

TEST_F(NoFQosConfigTest, UnsetAndEmptyKeepDefaultsSilently) {
    testing::internal::CaptureStderr();
    auto config = NoFQosConfig::FromEnvironment();
    EXPECT_EQ(config.submit_chunk_bytes, 131072);
    EXPECT_EQ(config.inflight_bytes_limit, 33554432);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());

    ASSERT_EQ(setenv("MC_NOF_SUBMIT_CHUNK_BYTES", "", 1), 0);
    ASSERT_EQ(setenv("MC_NOF_INFLIGHT_BYTES_LIMIT", "", 1), 0);
    testing::internal::CaptureStderr();
    config = NoFQosConfig::FromEnvironment();
    EXPECT_EQ(config.submit_chunk_bytes, 131072);
    EXPECT_EQ(config.inflight_bytes_limit, 33554432);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
}

TEST_F(NoFQosConfigTest, FieldsParseIndependentlyIncludingWhitespace) {
    ASSERT_EQ(setenv("MC_NOF_SUBMIT_CHUNK_BYTES", " +2048 ", 1), 0);
    ASSERT_EQ(setenv("MC_NOF_INFLIGHT_BYTES_LIMIT", "2147483647", 1), 0);
    testing::internal::CaptureStderr();
    const auto config = NoFQosConfig::FromEnvironment();
    EXPECT_EQ(config.submit_chunk_bytes, 2048);
    EXPECT_EQ(config.inflight_bytes_limit, 2147483647);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
}

TEST_F(NoFQosConfigTest, InvalidFieldsWarnInOriginalOrderAndFallBack) {
    ASSERT_EQ(setenv("MC_NOF_SUBMIT_CHUNK_BYTES", "0", 1), 0);
    ASSERT_EQ(setenv("MC_NOF_INFLIGHT_BYTES_LIMIT", "2147483648", 1), 0);
    testing::internal::CaptureStderr();
    auto config = NoFQosConfig::FromEnvironment();
    const auto log = testing::internal::GetCapturedStderr();
    EXPECT_EQ(config.submit_chunk_bytes, 131072);
    EXPECT_EQ(config.inflight_bytes_limit, 33554432);
    const auto chunk = log.find(
        "Invalid value for MC_NOF_SUBMIT_CHUNK_BYTES: 0, using default 131072");
    const auto inflight = log.find(
        "Invalid value for MC_NOF_INFLIGHT_BYTES_LIMIT: 2147483648, using "
        "default 33554432");
    ASSERT_NE(chunk, std::string::npos);
    ASSERT_NE(inflight, std::string::npos);
    EXPECT_LT(chunk, inflight);

    ASSERT_EQ(setenv("MC_NOF_SUBMIT_CHUNK_BYTES", "19x", 1), 0);
    ASSERT_EQ(setenv("MC_NOF_INFLIGHT_BYTES_LIMIT", "-1", 1), 0);
    config = NoFQosConfig::FromEnvironment();
    EXPECT_EQ(config.submit_chunk_bytes, 131072);
    EXPECT_EQ(config.inflight_bytes_limit, 33554432);
}

TEST_F(NoFQosConfigTest, OneInvalidFieldDoesNotOverrideTheOther) {
    ASSERT_EQ(setenv("MC_NOF_SUBMIT_CHUNK_BYTES", "-1", 1), 0);
    ASSERT_EQ(setenv("MC_NOF_INFLIGHT_BYTES_LIMIT", "4096", 1), 0);
    testing::internal::CaptureStderr();
    auto config = NoFQosConfig::FromEnvironment();
    const auto chunk_log = testing::internal::GetCapturedStderr();
    EXPECT_EQ(config.submit_chunk_bytes, 131072);
    EXPECT_EQ(config.inflight_bytes_limit, 4096);
    EXPECT_NE(chunk_log.find("Invalid value for MC_NOF_SUBMIT_CHUNK_BYTES: -1"),
              std::string::npos);
    EXPECT_EQ(chunk_log.find("Invalid value for MC_NOF_INFLIGHT_BYTES_LIMIT"),
              std::string::npos);

    ASSERT_EQ(setenv("MC_NOF_SUBMIT_CHUNK_BYTES", "2048", 1), 0);
    ASSERT_EQ(setenv("MC_NOF_INFLIGHT_BYTES_LIMIT", "bad", 1), 0);
    testing::internal::CaptureStderr();
    config = NoFQosConfig::FromEnvironment();
    const auto inflight_log = testing::internal::GetCapturedStderr();
    EXPECT_EQ(config.submit_chunk_bytes, 2048);
    EXPECT_EQ(config.inflight_bytes_limit, 33554432);
    EXPECT_EQ(inflight_log.find("Invalid value for MC_NOF_SUBMIT_CHUNK_BYTES"),
              std::string::npos);
    EXPECT_NE(
        inflight_log.find("Invalid value for MC_NOF_INFLIGHT_BYTES_LIMIT: bad"),
        std::string::npos);
}

TEST_F(NoFQosConfigTest, FreshReadsSeeUpdatesButFirstUseCachesBothFields) {
    ASSERT_EQ(setenv("MC_NOF_SUBMIT_CHUNK_BYTES", "1000", 1), 0);
    ASSERT_EQ(setenv("MC_NOF_INFLIGHT_BYTES_LIMIT", "4000", 1), 0);
    EXPECT_EQ(NoFQosConfig::AtFirstUse().submit_chunk_bytes, 1000);
    ASSERT_EQ(setenv("MC_NOF_SUBMIT_CHUNK_BYTES", "2000", 1), 0);
    ASSERT_EQ(setenv("MC_NOF_INFLIGHT_BYTES_LIMIT", "8000", 1), 0);
    const auto fresh = NoFQosConfig::FromEnvironment();
    EXPECT_EQ(fresh.submit_chunk_bytes, 2000);
    EXPECT_EQ(fresh.inflight_bytes_limit, 8000);
    EXPECT_EQ(NoFQosConfig::AtFirstUse().submit_chunk_bytes, 1000);
    EXPECT_EQ(NoFQosConfig::AtFirstUse().inflight_bytes_limit, 4000);
}

#ifdef USE_NOF
TEST_F(NoFQosConfigTest, QosDerivesLimitsFromBytesAndLiveBlockSize) {
    ASSERT_EQ(setenv("MC_NOF_SUBMIT_CHUNK_BYTES", "1000", 1), 0);
    ASSERT_EQ(setenv("MC_NOF_INFLIGHT_BYTES_LIMIT", "4000", 1), 0);
    const SpdkNofQos first(512);
    EXPECT_EQ(first.blocks_per_chunk, 1);
    EXPECT_EQ(first.inflight_blocks_limit, 7);
    ASSERT_EQ(setenv("MC_NOF_SUBMIT_CHUNK_BYTES", "2000", 1), 0);
    ASSERT_EQ(setenv("MC_NOF_INFLIGHT_BYTES_LIMIT", "8000", 1), 0);
    const SpdkNofQos second(1000);
    EXPECT_EQ(second.blocks_per_chunk, 1);
    EXPECT_EQ(second.inflight_blocks_limit, 4);
}
#endif

}  // namespace
}  // namespace mooncake
