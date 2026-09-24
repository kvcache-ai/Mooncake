#include "ha/oplog/oplog_test_failpoint.h"

#include <gtest/gtest.h>

#include <chrono>
#include <atomic>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>

#include <unistd.h>

namespace mooncake::test {
namespace {

class OpLogTestFailPointTest : public ::testing::Test {
   protected:
    void SetUp() override {
        dir_ = "/tmp/mooncake-oplog-failpoint-" + std::to_string(::getpid());
        std::filesystem::create_directories(dir_);
        setenv("MOONCAKE_TEST_FAILPOINT_DIR", dir_.c_str(), 1);
        setenv("MOONCAKE_TEST_FAILPOINT_TIMEOUT_SEC", "1", 1);
    }

    void TearDown() override {
        unsetenv("MOONCAKE_TEST_FAILPOINT_DIR");
        unsetenv("MOONCAKE_TEST_FAILPOINT_TIMEOUT_SEC");
        std::filesystem::remove_all(dir_);
    }

    std::string dir_;
};

TEST_F(OpLogTestFailPointTest, UnarmedPointReturnsImmediately) {
    const auto started = std::chrono::steady_clock::now();
    EXPECT_FALSE(TestFailPoint::Wait("batch_txn_succeeded_before_callback"));
    EXPECT_LT(std::chrono::steady_clock::now() - started,
              std::chrono::milliseconds(100));
}

TEST_F(OpLogTestFailPointTest, CreatesHitAndWaitsForRelease) {
    const std::string name = "batch_txn_succeeded_before_callback";
    std::ofstream(dir_ + "/" + name + ".arm").put('\n');
    std::atomic<bool> completed{false};
    std::thread waiter([&] {
        EXPECT_TRUE(TestFailPoint::Wait(name));
        completed = true;
    });

    const auto hit = std::filesystem::path(dir_) / (name + ".hit");
    for (int i = 0; i < 100 && !std::filesystem::exists(hit); ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    ASSERT_TRUE(std::filesystem::exists(hit));
    const auto claim = std::filesystem::path(dir_) /
                       (name + ".claimed." + std::to_string(::getpid()));
    EXPECT_FALSE(std::filesystem::exists(dir_ + "/" + name + ".arm"));
    EXPECT_TRUE(std::filesystem::exists(claim));
    EXPECT_FALSE(completed.load());
    std::ofstream(dir_ + "/" + name + ".release").put('\n');
    waiter.join();
    EXPECT_TRUE(completed.load());
    EXPECT_FALSE(std::filesystem::exists(hit));
    EXPECT_FALSE(std::filesystem::exists(claim));
}

}  // namespace
}  // namespace mooncake::test
