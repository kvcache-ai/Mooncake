#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>

#include "process_handler.h"

namespace mooncake::testing {
namespace {

std::string SelfExecutable() {
    return std::filesystem::read_symlink("/proc/self/exe").string();
}

TEST(ProcessHandlerLifecycleTest, WaitSignalAndRepeatedStop) {
    const auto out_dir =
        "/tmp/mooncake-process-handler-" + std::to_string(::getpid());
    MasterRunnerConfig config;
    config.enable_ha = false;
    config.extra_args = {"--process-handler-helper"};
    MasterProcessHandler child(SelfExecutable(), config, 0, 0, out_dir);

    ASSERT_TRUE(child.start());
    EXPECT_GT(child.pid(), 0);
    EXPECT_TRUE(child.is_running());
    EXPECT_EQ(child.stdout_path(), out_dir + "/master_0.out");
    EXPECT_EQ(child.stderr_path(), out_dir + "/master_0.err");

    int status = 0;
    EXPECT_FALSE(child.wait_for_exit(std::chrono::milliseconds(20), &status));
    ASSERT_TRUE(child.signal(SIGTERM));
    ASSERT_TRUE(child.wait_for_exit(std::chrono::seconds(2), &status));
    EXPECT_TRUE(WIFSIGNALED(status));
    EXPECT_EQ(WTERMSIG(status), SIGTERM);
    EXPECT_FALSE(child.is_running());
    EXPECT_TRUE(child.stop(std::chrono::milliseconds(20)));
}

TEST(ProcessHandlerLifecycleTest, BuildsUniqueMetricsAndExtraArguments) {
    const auto out_dir =
        "/tmp/mooncake-process-handler-args-" + std::to_string(::getpid());
    MasterRunnerConfig config;
    config.enable_ha = false;
    config.extra_args = {"--process-handler-print-args"};
    MasterProcessHandler child(SelfExecutable(), config, 0, 7, out_dir);

    ASSERT_TRUE(child.start());
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (child.is_running() && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    EXPECT_FALSE(child.is_running());
    int status = 0;
    ASSERT_TRUE(child.wait_for_exit(std::chrono::seconds(2), &status));

    std::ifstream input(child.stdout_path());
    std::stringstream output;
    output << input.rdbuf();
    EXPECT_NE(output.str().find("--metrics-port=9010"), std::string::npos);
    EXPECT_TRUE(output.str().ends_with("--process-handler-print-args\n"));
}

}  // namespace
}  // namespace mooncake::testing

int main(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        if (std::string(argv[i]) == "--process-handler-helper") {
            std::this_thread::sleep_for(std::chrono::minutes(5));
            return 0;
        }
        if (std::string(argv[i]) == "--process-handler-print-args") {
            for (int j = 1; j < argc; ++j) {
                std::cout << argv[j] << '\n';
            }
            return 0;
        }
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
