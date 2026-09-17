#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <optional>
#include <string>
#include <utility>

#include "../client/config/fileread_worker_pool_config.h"

namespace mooncake {
namespace {

class FilereadWorkerPoolConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("FilereadWorkerPoolConfigTest");
        FLAGS_logtostderr = 1;
        if (const char* value = std::getenv("MC_FILEREAD_WORKERS")) {
            original_ = value;
        }
        ASSERT_EQ(unsetenv("MC_FILEREAD_WORKERS"), 0);
    }

    void TearDown() override {
        if (original_) {
            EXPECT_EQ(setenv("MC_FILEREAD_WORKERS", original_->c_str(), 1), 0);
        } else {
            EXPECT_EQ(unsetenv("MC_FILEREAD_WORKERS"), 0);
        }
        google::ShutdownGoogleLogging();
    }

   private:
    std::optional<std::string> original_;
};

TEST_F(FilereadWorkerPoolConfigTest, DefaultsSilentlyForUnsetAndEmpty) {
    testing::internal::CaptureStderr();
    EXPECT_EQ(FilereadWorkerPoolConfig::FromEnvironment().worker_count, 10);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());

    ASSERT_EQ(setenv("MC_FILEREAD_WORKERS", "", 1), 0);
    testing::internal::CaptureStderr();
    EXPECT_EQ(FilereadWorkerPoolConfig::FromEnvironment().worker_count, 10);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
}

TEST_F(FilereadWorkerPoolConfigTest, ParsesTypedPositiveIntegersOnEachRead) {
    for (const auto& [value, expected] :
         {std::pair{"2", 2}, std::pair{"+3", 3}, std::pair{" 4 ", 4},
          std::pair{"2147483647", 2147483647}}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MC_FILEREAD_WORKERS", value, 1), 0);
        testing::internal::CaptureStderr();
        EXPECT_EQ(FilereadWorkerPoolConfig::FromEnvironment().worker_count,
                  expected);
        EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
    }
}

TEST_F(FilereadWorkerPoolConfigTest, InvalidValuesWarnOnceAndFallBack) {
    for (const char* value : {"0", "-1", "abc", "3x", "2147483648"}) {
        SCOPED_TRACE(value);
        ASSERT_EQ(setenv("MC_FILEREAD_WORKERS", value, 1), 0);
        testing::internal::CaptureStderr();
        EXPECT_EQ(FilereadWorkerPoolConfig::FromEnvironment().worker_count, 10);
        const auto log = testing::internal::GetCapturedStderr();
        EXPECT_NE(
            log.find(std::string("Invalid value for MC_FILEREAD_WORKERS: ") +
                     value + ", using default 10"),
            std::string::npos);
    }
}

}  // namespace
}  // namespace mooncake
