#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>
#include <utility>

#include "../src/config/offload_parallel_worker_pool_config.h"

namespace mooncake {
namespace {

class OffloadParallelWorkerPoolConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OffloadParallelWorkerPoolConfigTest");
        FLAGS_logtostderr = 1;
        if (const char* value = std::getenv("MC_OFFLOAD_PARALLEL_WORKERS")) {
            original_ = value;
        }
        ASSERT_EQ(unsetenv("MC_OFFLOAD_PARALLEL_WORKERS"), 0);
    }

    void TearDown() override {
        if (original_) {
            EXPECT_EQ(
                setenv("MC_OFFLOAD_PARALLEL_WORKERS", original_->c_str(), 1),
                0);
        } else {
            EXPECT_EQ(unsetenv("MC_OFFLOAD_PARALLEL_WORKERS"), 0);
        }
        google::ShutdownGoogleLogging();
    }

   private:
    std::optional<std::string> original_;
};

TEST_F(OffloadParallelWorkerPoolConfigTest, DefaultsSilentlyForUnsetAndEmpty) {
    testing::internal::CaptureStderr();
    EXPECT_EQ(OffloadParallelWorkerPoolConfig::FromEnvironment().worker_count,
              8);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());

    ASSERT_EQ(setenv("MC_OFFLOAD_PARALLEL_WORKERS", "", 1), 0);
    testing::internal::CaptureStderr();
    EXPECT_EQ(OffloadParallelWorkerPoolConfig::FromEnvironment().worker_count,
              8);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
}

TEST_F(OffloadParallelWorkerPoolConfigTest,
       ParsesTypedPositiveIntegersOnEachRead) {
    for (const auto& [value, expected] :
         {std::pair{"2", 2}, std::pair{"+3", 3}, std::pair{" 4 ", 4},
          std::pair{"2147483647", 2147483647}}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MC_OFFLOAD_PARALLEL_WORKERS", value, 1), 0);
        testing::internal::CaptureStderr();
        EXPECT_EQ(
            OffloadParallelWorkerPoolConfig::FromEnvironment().worker_count,
            expected);
        EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
    }
}

TEST_F(OffloadParallelWorkerPoolConfigTest, InvalidValuesWarnOnceAndFallBack) {
    for (const char* value : {"0", "-1", "abc", "3x", "2147483648"}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MC_OFFLOAD_PARALLEL_WORKERS", value, 1), 0);
        testing::internal::CaptureStderr();
        EXPECT_EQ(
            OffloadParallelWorkerPoolConfig::FromEnvironment().worker_count, 8);
        const auto log = testing::internal::GetCapturedStderr();
        EXPECT_NE(
            log.find(
                std::string("Invalid value for MC_OFFLOAD_PARALLEL_WORKERS: ") +
                value + ", using default 8"),
            std::string::npos);
    }
}

}  // namespace
}  // namespace mooncake
