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
#include <poll.h>
#include <signal.h>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>

#include "transfer_engine.h"

extern char** environ;

using namespace mooncake;

namespace {

constexpr char kChildFlag[] = "--graceful-shutdown-child";
constexpr int kChildTimeoutMs = 5000;

enum class ChildMode { kSingle, kDestroyed, kMultiple };

const char* childModeName(ChildMode mode) {
    switch (mode) {
        case ChildMode::kSingle:
            return "single";
        case ChildMode::kDestroyed:
            return "destroyed";
        case ChildMode::kMultiple:
            return "multiple";
    }
    return "unknown";
}

bool parseChildMode(const char* value, ChildMode* mode) {
    if (strcmp(value, "single") == 0) {
        *mode = ChildMode::kSingle;
    } else if (strcmp(value, "destroyed") == 0) {
        *mode = ChildMode::kDestroyed;
    } else if (strcmp(value, "multiple") == 0) {
        *mode = ChildMode::kMultiple;
    } else {
        return false;
    }
    return true;
}

int runShutdownChild(ChildMode mode, int ready_fd) {
    std::unique_ptr<TransferEngine> engine1;
    std::unique_ptr<TransferEngine> engine2;

    if (mode == ChildMode::kDestroyed) {
        auto engine = std::make_unique<TransferEngine>(false);
        engine->enableGracefulShutdown();
    } else {
        engine1 = std::make_unique<TransferEngine>(false);
        engine1->enableGracefulShutdown();
        if (mode == ChildMode::kMultiple) {
            engine2 = std::make_unique<TransferEngine>(false);
            engine2->enableGracefulShutdown();
        }
    }

    const char ready = '1';
    ssize_t bytes_written;
    do {
        bytes_written = write(ready_fd, &ready, sizeof(ready));
    } while (bytes_written < 0 && errno == EINTR);
    if (bytes_written != static_cast<ssize_t>(sizeof(ready))) return 111;
    close(ready_fd);
    for (;;) pause();
}

void killAndReap(pid_t pid) {
    if (kill(pid, SIGKILL) != 0 && errno != ESRCH) {
        ADD_FAILURE() << "kill(SIGKILL) failed: " << strerror(errno);
    }

    int status = 0;
    pid_t ret;
    do {
        ret = waitpid(pid, &status, 0);
    } while (ret < 0 && errno == EINTR);
    if (ret < 0) {
        ADD_FAILURE() << "waitpid() failed while reaping child: "
                      << strerror(errno);
    }
}

bool waitChildWithTimeout(pid_t pid, int* status) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(kChildTimeoutMs);
    for (;;) {
        pid_t ret = waitpid(pid, status, WNOHANG);
        if (ret == pid) return true;
        if (ret < 0) {
            if (errno == EINTR) continue;
            int error = errno;
            if (error != ECHILD) killAndReap(pid);
            ADD_FAILURE() << "waitpid() failed: " << strerror(error);
            return false;
        }
        if (std::chrono::steady_clock::now() >= deadline) break;
        usleep(100000);
    }

    killAndReap(pid);
    ADD_FAILURE() << "child did not exit before timeout";
    return false;
}

bool waitForChildReady(int fd) {
    pollfd pfd{fd, POLLIN, 0};
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(kChildTimeoutMs);
    int ret;
    for (;;) {
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                             deadline - std::chrono::steady_clock::now())
                             .count();
        if (remaining <= 0) {
            ret = 0;
            break;
        }
        ret = poll(&pfd, 1, static_cast<int>(remaining));
        if (ret >= 0 || errno != EINTR) break;
    }

    if (ret == 0) {
        ADD_FAILURE() << "child did not report readiness before timeout";
        return false;
    }
    if (ret < 0) {
        ADD_FAILURE() << "poll() failed: " << strerror(errno);
        return false;
    }

    char ready = 0;
    ssize_t bytes_read;
    do {
        bytes_read = read(fd, &ready, sizeof(ready));
    } while (bytes_read < 0 && errno == EINTR);
    if (bytes_read == 0) {
        ADD_FAILURE() << "child exited before reporting readiness";
        return false;
    }
    if (bytes_read < 0) {
        ADD_FAILURE() << "read() failed while waiting for readiness: "
                      << strerror(errno);
        return false;
    }
    if (ready != '1') {
        ADD_FAILURE() << "unexpected readiness marker: "
                      << static_cast<int>(static_cast<unsigned char>(ready));
        return false;
    }
    return true;
}

pid_t spawnReadyChild(ChildMode mode) {
    int ready_pipe[2];
    if (pipe(ready_pipe) != 0) {
        ADD_FAILURE() << "pipe() failed: " << strerror(errno);
        return -1;
    }

    char ready_fd[32];
    snprintf(ready_fd, sizeof(ready_fd), "%d", ready_pipe[1]);
    char* child_argv[] = {
        const_cast<char*>("/proc/self/exe"), const_cast<char*>(kChildFlag),
        const_cast<char*>(childModeName(mode)), ready_fd, nullptr};

    pid_t pid = -1;
    int spawn_error = posix_spawn(&pid, "/proc/self/exe", nullptr, nullptr,
                                  child_argv, environ);
    close(ready_pipe[1]);
    if (spawn_error != 0) {
        close(ready_pipe[0]);
        ADD_FAILURE() << "posix_spawn() failed: " << strerror(spawn_error);
        return -1;
    }

    bool ready = waitForChildReady(ready_pipe[0]);
    close(ready_pipe[0]);
    if (!ready) {
        killAndReap(pid);
        return -1;
    }
    return pid;
}

void expectGracefulExit(ChildMode mode, int signo) {
    pid_t pid = spawnReadyChild(mode);
    ASSERT_GT(pid, 0);

    if (kill(pid, signo) != 0) {
        int error = errno;
        killAndReap(pid);
        FAIL() << "kill(" << signo << ") failed: " << strerror(error);
    }

    int status = 0;
    ASSERT_TRUE(waitChildWithTimeout(pid, &status));
    ASSERT_TRUE(WIFEXITED(status))
        << "Child did not exit normally (signaled: " << WIFSIGNALED(status)
        << ")";
    EXPECT_EQ(WEXITSTATUS(status), 128 + signo);
}

}  // namespace

TEST(GracefulShutdownTest, SigtermTriggersCleanExit) {
    expectGracefulExit(ChildMode::kSingle, SIGTERM);
}

TEST(GracefulShutdownTest, SigintTriggersCleanExit) {
    expectGracefulExit(ChildMode::kSingle, SIGINT);
}

TEST(GracefulShutdownTest, IdempotentEnable) {
    auto engine = std::make_unique<TransferEngine>(false);
    engine->enableGracefulShutdown();
    engine->enableGracefulShutdown();
    engine->enableGracefulShutdown();
}

TEST(GracefulShutdownTest, EngineDestroyedBeforeSignal) {
    expectGracefulExit(ChildMode::kDestroyed, SIGTERM);
}

TEST(GracefulShutdownTest, ForkAfterInstallDoesNotHangChildSignal) {
    auto engine = std::make_unique<TransferEngine>(false);
    engine->enableGracefulShutdown();

    pid_t pid = fork();
    ASSERT_NE(pid, -1) << "fork() failed";

    if (pid == 0) {
        for (;;) pause();
    }

    if (kill(pid, SIGTERM) != 0) {
        int error = errno;
        killAndReap(pid);
        FAIL() << "kill(SIGTERM) failed: " << strerror(error);
    }

    int status = 0;
    ASSERT_TRUE(waitChildWithTimeout(pid, &status));
    ASSERT_TRUE(WIFEXITED(status))
        << "Child did not exit normally (signaled: " << WIFSIGNALED(status)
        << ")";
    EXPECT_EQ(WEXITSTATUS(status), 128 + SIGTERM);
}

TEST(GracefulShutdownTest, MultipleEngines) {
    expectGracefulExit(ChildMode::kMultiple, SIGTERM);
}

int main(int argc, char** argv) {
    if (argc == 4 && strcmp(argv[1], kChildFlag) == 0) {
        ChildMode mode;
        char* end = nullptr;
        errno = 0;
        long ready_fd = strtol(argv[3], &end, 10);
        if (!parseChildMode(argv[2], &mode) || errno != 0 || *end != '\0' ||
            ready_fd < 0 || ready_fd > INT_MAX) {
            return 2;
        }
        return runShutdownChild(mode, static_cast<int>(ready_fd));
    }

    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
