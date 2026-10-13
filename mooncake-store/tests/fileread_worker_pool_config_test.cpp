#include <glog/logging.h>
#include <gtest/gtest.h>

#include <string>
#include <utility>

#include "../src/config/fileread_worker_pool_config.h"
#include "environ.h"

namespace mooncake {
namespace {

class FilereadWorkerPoolConfigTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("FilereadWorkerPoolConfigTest");
        FLAGS_logtostderr = 1;
    }

    void TearDown() override { google::ShutdownGoogleLogging(); }

    FilereadWorkerPoolConfig Load() const {
        return FilereadWorkerPoolConfig::FromEnvironment(Environ(source_));
    }

    MapEnvironSource source_;
};

TEST_F(FilereadWorkerPoolConfigTest, DefaultsSilentlyForUnsetAndEmpty) {
    testing::internal::CaptureStderr();
    EXPECT_EQ(Load().worker_count, 10);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());

    source_.Set("MC_FILEREAD_WORKERS", "");
    testing::internal::CaptureStderr();
    EXPECT_EQ(Load().worker_count, 10);
    EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
}

TEST_F(FilereadWorkerPoolConfigTest, ParsesTypedPositiveIntegersOnEachRead) {
    for (const auto& [value, expected] :
         {std::pair{"2", 2}, std::pair{"+3", 3}, std::pair{" 4 ", 4},
          std::pair{"2147483647", 2147483647}}) {
        SCOPED_TRACE(value);
        source_.Set("MC_FILEREAD_WORKERS", value);
        testing::internal::CaptureStderr();
        EXPECT_EQ(Load().worker_count, expected);
        EXPECT_TRUE(testing::internal::GetCapturedStderr().empty());
    }
}

TEST_F(FilereadWorkerPoolConfigTest, InvalidValuesWarnOnceAndFallBack) {
    for (const char* value : {"0", "-1", "abc", "3x", "2147483648"}) {
        SCOPED_TRACE(value);
        source_.Set("MC_FILEREAD_WORKERS", value);
        testing::internal::CaptureStderr();
        EXPECT_EQ(Load().worker_count, 10);
        const auto log = testing::internal::GetCapturedStderr();
        EXPECT_NE(
            log.find(std::string("Invalid value for MC_FILEREAD_WORKERS: ") +
                     value + ", using default 10"),
            std::string::npos);
    }
}

}  // namespace
}  // namespace mooncake
