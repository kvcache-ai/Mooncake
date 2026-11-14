#include "shutdown_request.h"

#include <fcntl.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <poll.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake {
namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

// Every case that installs handlers, latches or starts the watchdog runs in a
// forked child, because the flags and the dispositions are process-wide. The
// gtest parent never installs handlers, never latches and never starts the
// watchdog; the in-process cases start no threads, so the parent stays
// single-threaded and a child may run arbitrary code after fork.

void SignalReady(int ready_fd) {
    const char byte = 'r';
    ssize_t written;
    do {
        written = ::write(ready_fd, &byte, 1);
    } while (written < 0 && errno == EINTR);
}

// Forks; the child runs body(ready_fd) with stderr on a pipe and calls
// _exit(0) if body returns. The parent reads stderr only after the child
// exits, so a child that fills the pipe blocks, as case 8 needs.
class Child {
   public:
    explicit Child(const std::function<void(int ready_fd)>& body) {
        int ready[2];
        int err[2];
        if (::pipe2(ready, O_CLOEXEC) != 0 || ::pipe2(err, O_CLOEXEC) != 0) {
            ADD_FAILURE() << "pipe2: " << std::strerror(errno);
            return;
        }
        pid_ = ::fork();
        if (pid_ == 0) {
            ::close(ready[0]);
            ::close(err[0]);
            ::dup2(err[1], STDERR_FILENO);
            body(ready[1]);
            _exit(0);
        }
        ready_read_ = ready[0];
        err_read_ = err[0];
        ::close(ready[1]);
        ::close(err[1]);
        if (pid_ < 0) {
            ADD_FAILURE() << "fork: " << std::strerror(errno);
            return;
        }
        ::fcntl(err_read_, F_SETFL, O_NONBLOCK);
    }

    ~Child() {
        if (pid_ > 0 && IsRunning()) {
            ::kill(pid_, SIGKILL);
            ::waitpid(pid_, &status_, 0);
        }
        if (ready_read_ >= 0) ::close(ready_read_);
        if (err_read_ >= 0) ::close(err_read_);
    }

    Child(const Child&) = delete;
    Child& operator=(const Child&) = delete;

    pid_t pid() const { return pid_; }

    // Waits for one byte on the ready pipe.
    bool WaitReady(std::chrono::milliseconds timeout) {
        struct pollfd pfd{ready_read_, POLLIN, 0};
        if (::poll(&pfd, 1, static_cast<int>(timeout.count())) <= 0) {
            return false;
        }
        char byte;
        return ::read(ready_read_, &byte, 1) == 1;
    }

    bool IsRunning() {
        if (reaped_) return false;
        const pid_t result = ::waitpid(pid_, &status_, WNOHANG);
        if (result == pid_) {
            reaped_ = true;
            return false;
        }
        return true;
    }

    // Raw wait status, or nullopt on timeout (then SIGKILL + reap).
    std::optional<int> Wait(std::chrono::milliseconds timeout) {
        const auto deadline = Clock::now() + timeout;
        while (IsRunning()) {
            if (Clock::now() >= deadline) {
                ::kill(pid_, SIGKILL);
                ::waitpid(pid_, &status_, 0);
                reaped_ = true;
                return std::nullopt;
            }
            std::this_thread::sleep_for(5ms);
        }
        return status_;
    }

    // Non-blocking read of what the child wrote to stderr.
    std::string ReadStderr() {
        std::string text;
        char buffer[4096];
        ssize_t n;
        while ((n = ::read(err_read_, buffer, sizeof(buffer))) > 0) {
            text.append(buffer, static_cast<size_t>(n));
        }
        return text;
    }

   private:
    pid_t pid_ = -1;
    int ready_read_ = -1;
    int err_read_ = -1;
    int status_ = 0;
    bool reaped_ = false;
};

std::chrono::milliseconds Since(Clock::time_point start) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now() -
                                                                 start);
}

void ExpectExitedWith(const std::optional<int>& status, int code) {
    ASSERT_TRUE(status.has_value()) << "child did not exit in time";
    ASSERT_TRUE(WIFEXITED(*status))
        << "child was terminated by signal "
        << (WIFSIGNALED(*status) ? WTERMSIG(*status) : 0);
    EXPECT_EQ(WEXITSTATUS(*status), code);
}

// Blocks inside the sink to hold glog's mutex. glog calls sinks with its log
// mutex held, so FlushLogFiles on another thread blocks on that mutex.
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
class BlockingSink : public google::LogSink {
   public:
    using google::LogSink::send;
    void send(google::LogSeverity, const char*, const char*, int,
              const struct ::tm*, const char*, size_t) override {
        entered.store(true, std::memory_order_release);
        for (;;) {
            pause();
        }
    }
    std::atomic<bool> entered{false};
};
#pragma GCC diagnostic pop

// Parks a thread inside BlockingSink::send, or ends the child with status 3.
void BlockLogMutex(BlockingSink& sink) {
    google::AddLogSink(&sink);
    std::thread([] { LOG(INFO) << "block"; }).detach();
    const auto give_up = Clock::now() + 2s;
    while (!sink.entered.load(std::memory_order_acquire)) {
        if (Clock::now() > give_up) {
            _exit(3);
        }
        std::this_thread::sleep_for(1ms);
    }
}

class FirstSignalTest : public ::testing::TestWithParam<int> {};

TEST_P(FirstSignalTest, FirstSignalSetsFlagAndKeepsRunning) {
    Child child([](int ready_fd) {
        if (!InstallShutdownSignalHandlers()) _exit(5);
        SignalReady(ready_fd);
        const auto give_up = Clock::now() + 5s;
        while (!ShutdownRequested()) {
            if (Clock::now() > give_up) _exit(2);
            std::this_thread::sleep_for(1ms);
        }
        _exit(ShutdownLatched() ? 4 : 0);
    });
    ASSERT_TRUE(child.WaitReady(5s));
    ASSERT_EQ(::kill(child.pid(), GetParam()), 0);
    ExpectExitedWith(child.Wait(7s), 0);
    EXPECT_EQ(child.ReadStderr(), "") << "the handler must not do I/O";
}

INSTANTIATE_TEST_SUITE_P(Signals, FirstSignalTest,
                         ::testing::Values(SIGTERM, SIGINT));

struct SignalPair {
    int first;
    int second;
    int expected_status;
};

class SecondSignalTest : public ::testing::TestWithParam<SignalPair> {};

TEST_P(SecondSignalTest, SecondSignalExits128PlusSigno) {
    const SignalPair pair = GetParam();
    Child child([](int ready_fd) {
        if (!InstallShutdownSignalHandlers()) _exit(5);
        SignalReady(ready_fd);
        bool reported = false;
        const auto give_up = Clock::now() + 10s;
        while (Clock::now() < give_up) {
            if (!reported && ShutdownRequested()) {
                reported = true;
                SignalReady(ready_fd);
            }
            std::this_thread::sleep_for(1ms);
        }
        _exit(2);
    });
    ASSERT_TRUE(child.WaitReady(5s)) << "child never became ready";
    ASSERT_EQ(::kill(child.pid(), pair.first), 0);
    ASSERT_TRUE(child.WaitReady(5s)) << "first signal was not handled";
    ASSERT_EQ(::kill(child.pid(), pair.second), 0);
    ExpectExitedWith(child.Wait(2s), pair.expected_status);
    EXPECT_EQ(child.ReadStderr(), "");
}

INSTANTIATE_TEST_SUITE_P(Pairs, SecondSignalTest,
                         ::testing::Values(SignalPair{SIGTERM, SIGTERM, 143},
                                           SignalPair{SIGTERM, SIGINT, 130},
                                           SignalPair{SIGINT, SIGINT, 130}));

struct RecordedCall {
    int signo;
    int flags;
    void (*handler)(int);
};
std::vector<RecordedCall>& Calls() {
    static std::vector<RecordedCall> calls;
    return calls;
}

int RecordingSigaction(int signo, const struct sigaction* act,
                       struct sigaction*) {
    Calls().push_back({signo, act->sa_flags, act->sa_handler});
    return 0;
}

int FailingSigaction(int signo, const struct sigaction* act,
                     struct sigaction*) {
    Calls().push_back({signo, act->sa_flags, act->sa_handler});
    errno = EINVAL;
    return -1;
}

TEST(ShutdownRequestTest, InstallUsesSaOnstackForBothSignals) {
    Calls().clear();
    ASSERT_TRUE(detail::InstallShutdownSignalHandlersWith(&RecordingSigaction));
    ASSERT_EQ(Calls().size(), 2u);
    EXPECT_EQ(Calls()[0].signo, SIGINT);
    EXPECT_EQ(Calls()[1].signo, SIGTERM);
    for (const auto& call : Calls()) {
        EXPECT_TRUE(call.flags & SA_ONSTACK);
        EXPECT_TRUE(call.flags & SA_RESTART);
        EXPECT_FALSE(call.flags & SA_NODEFER);
        EXPECT_NE(call.handler, SIG_DFL);
        EXPECT_NE(call.handler, SIG_IGN);
    }
}

TEST(ShutdownRequestTest, InstallFailureReturnsFalse) {
    Calls().clear();
    errno = 0;
    EXPECT_FALSE(detail::InstallShutdownSignalHandlersWith(&FailingSigaction));
    EXPECT_EQ(errno, EINVAL);
    EXPECT_EQ(Calls().size(), 1u);
}

TEST(ShutdownRequestTest, RealInstallSetsSaOnstack) {
    Child child([](int) {
        if (!InstallShutdownSignalHandlers()) _exit(5);
        struct sigaction old{};
        ::sigaction(SIGTERM, nullptr, &old);
        _exit((old.sa_flags & SA_ONSTACK) ? 0 : 3);
    });
    ExpectExitedWith(child.Wait(5s), 0);
}

TEST(ShutdownRequestTest, WatchdogWithoutRequestNeverFires) {
    Child child([](int) {
        if (!StartShutdownWatchdog(200ms)) _exit(5);
        std::this_thread::sleep_for(1s);
        _exit(0);
    });
    ExpectExitedWith(child.Wait(5s), 0);
}

TEST(ShutdownRequestTest, WatchdogEndsBlockedCleanup) {
    const auto start = Clock::now();
    Child child([](int) {
        if (!InstallShutdownSignalHandlers()) _exit(5);
        if (!StartShutdownWatchdog(500ms)) _exit(5);
        ::raise(SIGTERM);
        for (;;) pause();
    });
    ExpectExitedWith(child.Wait(5s), 1);
    const auto elapsed = Since(start);
    EXPECT_GE(elapsed.count(), 500);
    EXPECT_LE(elapsed.count(), 2000);
}

TEST(ShutdownRequestTest, WatchdogEndsCleanupBlockedOnFullStderr) {
    const auto start = Clock::now();
    Child child([](int) {
        if (!InstallShutdownSignalHandlers()) _exit(5);
        if (!StartShutdownWatchdog(500ms)) _exit(5);
        const int flags = ::fcntl(STDERR_FILENO, F_GETFL);
        ::fcntl(STDERR_FILENO, F_SETFL, flags | O_NONBLOCK);
        char chunk[4096];
        std::memset(chunk, 'x', sizeof(chunk));
        while (::write(STDERR_FILENO, chunk, sizeof(chunk)) > 0) {
        }
        ::fcntl(STDERR_FILENO, F_SETFL, flags);
        ::raise(SIGTERM);
        (void)!::write(STDERR_FILENO, "x", 1);  // Blocks: the pipe is full.
        _exit(0);
    });
    ExpectExitedWith(child.Wait(5s), 1);
    EXPECT_LE(Since(start).count(), 2000);
}

TEST(ShutdownRequestTest, WatchdogEndsExitBlockedInLogFlush) {
    const auto start = Clock::now();
    Child child([](int) {
        if (!InstallShutdownSignalHandlers()) _exit(5);
        if (!StartShutdownWatchdog(500ms)) _exit(5);
        BlockingSink sink;
        BlockLogMutex(sink);
        ::raise(SIGTERM);
        ExitAfterShutdown(0);
    });
    auto status = child.Wait(5s);
    ASSERT_TRUE(status.has_value());
    ASSERT_TRUE(WIFEXITED(*status));
    EXPECT_NE(WEXITSTATUS(*status), 3) << "sink never entered";
    EXPECT_NE(WEXITSTATUS(*status), 0) << "fixture did not block FlushLogFiles";
    EXPECT_EQ(WEXITSTATUS(*status), 1);
    EXPECT_LE(Since(start).count(), 2000);
}

TEST(ShutdownRequestTest, WatchdogBoundsExitWithoutSignal) {
    const auto start = Clock::now();
    Child child([](int) {
        if (!StartShutdownWatchdog(500ms)) _exit(5);
        BlockingSink sink;
        BlockLogMutex(sink);
        ExitAfterShutdown(0);
    });
    auto status = child.Wait(5s);
    ASSERT_TRUE(status.has_value());
    ASSERT_TRUE(WIFEXITED(*status));
    EXPECT_NE(WEXITSTATUS(*status), 3) << "sink never entered";
    EXPECT_NE(WEXITSTATUS(*status), 0) << "fixture did not block FlushLogFiles";
    EXPECT_EQ(WEXITSTATUS(*status), 1);
    EXPECT_LE(Since(start).count(), 2000);
}

TEST(ShutdownRequestTest, LatchedRequestStartsDeadlineWithoutSignal) {
    const auto start = Clock::now();
    Child child([](int) {
        if (!StartShutdownWatchdog(500ms)) _exit(5);
        LatchShutdownRequest();
        if (!ShutdownLatched() || ShutdownRequested()) _exit(4);
        for (;;) pause();
    });
    ExpectExitedWith(child.Wait(5s), 1);
    const auto elapsed = Since(start);
    EXPECT_GE(elapsed.count(), 500);
    EXPECT_LE(elapsed.count(), 2000);
}

TEST(ShutdownRequestTest, LateBlockedFlushGetsNoNewDeadline) {
    Child child([](int ready_fd) {
        if (!InstallShutdownSignalHandlers()) _exit(5);
        if (!StartShutdownWatchdog(1000ms)) _exit(5);
        BlockingSink sink;
        BlockLogMutex(sink);
        SignalReady(ready_fd);
        ::raise(SIGTERM);
        std::this_thread::sleep_for(700ms);
        ExitAfterShutdown(0);
    });
    ASSERT_TRUE(child.WaitReady(5s));
    const auto ready_at = Clock::now();
    auto status = child.Wait(5s);
    const auto elapsed = Since(ready_at);
    ASSERT_TRUE(status.has_value());
    ASSERT_TRUE(WIFEXITED(*status));
    EXPECT_EQ(WEXITSTATUS(*status), 1);
    // A deadline restarted at the flush would end at 1700 ms or later.
    EXPECT_GE(elapsed.count(), 900);
    EXPECT_LE(elapsed.count(), 1500);
}

}  // namespace
}  // namespace mooncake
