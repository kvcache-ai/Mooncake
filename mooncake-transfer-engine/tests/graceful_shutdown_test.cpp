// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <memory>

#include "transfer_engine.h"

using namespace mooncake;

namespace {

void waitChildWithTimeout(pid_t pid, int* status) {
    for (int i = 0; i < 50; ++i) {
        pid_t ret = waitpid(pid, status, WNOHANG);
        ASSERT_NE(ret, -1) << "waitpid() failed";
        if (ret == pid) return;
        usleep(100000);
    }
    kill(pid, SIGKILL);
    waitpid(pid, status, 0);
    FAIL() << "child did not exit before timeout";
}

}  // namespace

TEST(GracefulShutdownTest, SigtermTriggersCleanExit) {
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork() failed";

    if (pid == 0) {
        auto engine = std::make_unique<TransferEngine>(false);
        engine->enableGracefulShutdown();
        pause();
        _exit(99);
    }

    usleep(100000);
    kill(pid, SIGTERM);

    int status;
    waitChildWithTimeout(pid, &status);
    ASSERT_TRUE(WIFEXITED(status))
        << "Child did not exit normally (signaled: " << WIFSIGNALED(status)
        << ")";
    EXPECT_EQ(WEXITSTATUS(status), 128 + SIGTERM);
}

TEST(GracefulShutdownTest, SigintTriggersCleanExit) {
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork() failed";

    if (pid == 0) {
        auto engine = std::make_unique<TransferEngine>(false);
        engine->enableGracefulShutdown();
        pause();
        _exit(99);
    }

    usleep(100000);
    kill(pid, SIGINT);

    int status;
    waitChildWithTimeout(pid, &status);
    ASSERT_TRUE(WIFEXITED(status)) << "Child did not exit normally";
    EXPECT_EQ(WEXITSTATUS(status), 128 + SIGINT);
}

TEST(GracefulShutdownTest, IdempotentEnable) {
    auto engine = std::make_unique<TransferEngine>(false);
    engine->enableGracefulShutdown();
    engine->enableGracefulShutdown();
    engine->enableGracefulShutdown();
}

TEST(GracefulShutdownTest, EngineDestroyedBeforeSignal) {
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork() failed";

    if (pid == 0) {
        {
            auto engine = std::make_unique<TransferEngine>(false);
            engine->enableGracefulShutdown();
        }
        pause();
        _exit(99);
    }

    usleep(100000);
    kill(pid, SIGTERM);

    int status;
    waitChildWithTimeout(pid, &status);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 128 + SIGTERM);
}

TEST(GracefulShutdownTest, ForkAfterInstallDoesNotHangChildSignal) {
    auto engine = std::make_unique<TransferEngine>(false);
    engine->enableGracefulShutdown();

    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork() failed";

    if (pid == 0) {
        pause();
        _exit(99);
    }

    usleep(100000);
    kill(pid, SIGTERM);

    int status;
    waitChildWithTimeout(pid, &status);
    ASSERT_TRUE(WIFEXITED(status))
        << "Child did not exit normally (signaled: " << WIFSIGNALED(status)
        << ")";
    EXPECT_EQ(WEXITSTATUS(status), 128 + SIGTERM);
}

TEST(GracefulShutdownTest, MultipleEngines) {
    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork() failed";

    if (pid == 0) {
        auto engine1 = std::make_unique<TransferEngine>(false);
        auto engine2 = std::make_unique<TransferEngine>(false);
        engine1->enableGracefulShutdown();
        engine2->enableGracefulShutdown();
        pause();
        _exit(99);
    }

    usleep(100000);
    kill(pid, SIGTERM);

    int status;
    waitChildWithTimeout(pid, &status);
    ASSERT_TRUE(WIFEXITED(status));
    EXPECT_EQ(WEXITSTATUS(status), 128 + SIGTERM);
}
