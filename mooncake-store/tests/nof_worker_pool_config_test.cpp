#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <iterator>
#include <optional>
#include <string>
#include <utility>

#include "../src/config/nof_worker_pool_config.h"
#ifdef USE_NOF
#include "transfer_task.h"
#endif

namespace mooncake {
namespace {

class NoFWorkerPoolConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("NoFWorkerPoolConfigTest");
        original_logtostderr_ = FLAGS_logtostderr;
        FLAGS_logtostderr = true;
        if (const char* value = std::getenv("MC_NOF_WORKERS")) {
            original_ = value;
        }
        ASSERT_EQ(unsetenv("MC_NOF_WORKERS"), 0);
    }

    void TearDown() override {
        if (original_) {
            EXPECT_EQ(setenv("MC_NOF_WORKERS", original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv("MC_NOF_WORKERS"), 0);
        }
        FLAGS_logtostderr = original_logtostderr_;
        google::ShutdownGoogleLogging();
    }

   private:
    std::optional<std::string> original_;
    bool original_logtostderr_ = false;
};

TEST_F(NoFWorkerPoolConfigTest, UnsetAndEmptyKeepFourWorkersSilently) {
    testing::internal::CaptureStderr();
    EXPECT_EQ(NoFWorkerPoolConfig::FromEnvironment().worker_count, 4);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());

    ASSERT_EQ(setenv("MC_NOF_WORKERS", "", 1), 0);
    testing::internal::CaptureStderr();
    EXPECT_EQ(NoFWorkerPoolConfig::FromEnvironment().worker_count, 4);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
}

TEST_F(NoFWorkerPoolConfigTest, AcceptsPositiveIntegersAndTrailingWhitespace) {
    for (const auto& [value, expected] :
         {std::pair{"2", 2}, std::pair{"+3", 3}, std::pair{" 4 ", 4},
          std::pair{"2147483647", 2147483647}}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MC_NOF_WORKERS", value, 1), 0);
        testing::internal::CaptureStderr();
        EXPECT_EQ(NoFWorkerPoolConfig::FromEnvironment().worker_count,
                  expected);
        EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
    }
}

TEST_F(NoFWorkerPoolConfigTest, InvalidValuesWarnWithUnchangedRawValue) {
    for (const char* value : {"0", "-1", "oops", "2x", "2147483648"}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MC_NOF_WORKERS", value, 1), 0);
        testing::internal::CaptureStderr();
        EXPECT_EQ(NoFWorkerPoolConfig::FromEnvironment().worker_count, 4);
        const auto log = testing::internal::GetCapturedStderr();
        EXPECT_NE(log.find(std::string("Invalid value for MC_NOF_WORKERS: ") +
                           value + ", using default 4"),
                  std::string::npos);
    }
}

TEST_F(NoFWorkerPoolConfigTest, FreshReadsSeeChangesButFirstUseIsCached) {
    ASSERT_EQ(setenv("MC_NOF_WORKERS", "2", 1), 0);
    EXPECT_EQ(NoFWorkerPoolConfig::FromEnvironment().worker_count, 2);
    EXPECT_EQ(NoFWorkerPoolConfig::AtFirstUse().worker_count, 2);
    ASSERT_EQ(setenv("MC_NOF_WORKERS", "3", 1), 0);
    EXPECT_EQ(NoFWorkerPoolConfig::FromEnvironment().worker_count, 3);
    EXPECT_EQ(NoFWorkerPoolConfig::AtFirstUse().worker_count, 2);
}

#ifdef USE_NOF
TEST_F(NoFWorkerPoolConfigTest, PoolConstructionUsesCachedWorkerCount) {
    ASSERT_EQ(setenv("MC_NOF_WORKERS", "2", 1), 0);
    const auto count_threads = [] {
        return std::distance(
            std::filesystem::directory_iterator("/proc/self/task"),
            std::filesystem::directory_iterator());
    };
    const auto before = count_threads();
    SpdkNofWorkerPool first;
    EXPECT_EQ(count_threads() - before, 2);
    ASSERT_EQ(setenv("MC_NOF_WORKERS", "3", 1), 0);
    SpdkNofWorkerPool second;
    EXPECT_EQ(count_threads() - before, 4);
    EXPECT_EQ(NoFWorkerPoolConfig::AtFirstUse().worker_count, 2);
}
#endif

}  // namespace
}  // namespace mooncake
