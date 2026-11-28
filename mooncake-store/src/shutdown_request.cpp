#include "shutdown_request.h"

#include <glog/logging.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <initializer_list>
#include <system_error>
#include <thread>

namespace mooncake {
namespace {

constexpr auto kWatchdogPollInterval = std::chrono::milliseconds(50);

// 0 until the first SIGINT/SIGTERM, then that signal number.
std::atomic<int> g_shutdown_signal{0};
static_assert(std::atomic<int>::is_always_lock_free,
              "the signal handler needs a lock-free flag");

// A shutdown request seen outside the signal handler. The watchdog polls it.
std::atomic<bool> g_shutdown_latched{false};
static_assert(std::atomic<bool>::is_always_lock_free,
              "the watchdog polls a lock-free flag");

// Set only after the watchdog thread exists; gates the exit flush.
std::atomic<bool> g_watchdog_running{false};

void HandleShutdownSignal(int signo) {
    // Async-signal-safe: one lock-free exchange, or _exit on a repeat. No
    // logging, locking or allocation.
    if (g_shutdown_signal.exchange(signo, std::memory_order_acq_rel) != 0) {
        _exit(128 + signo);
    }
}

void RunWatchdog(std::chrono::milliseconds deadline) {
    // Sleep-polls two atomics only, so a stalled cleanup, a held glog mutex
    // or a full stderr pipe cannot stop it.
    while (!ShutdownRequested() && !ShutdownLatched()) {
        std::this_thread::sleep_for(kWatchdogPollInterval);
    }
    const auto expires_at = std::chrono::steady_clock::now() + deadline;
    for (auto now = std::chrono::steady_clock::now(); now < expires_at;
         now = std::chrono::steady_clock::now()) {
        std::this_thread::sleep_for(
            std::min<std::chrono::steady_clock::duration>(
                expires_at - now, kWatchdogPollInterval));
    }
    _exit(EXIT_FAILURE);
}

}  // namespace

namespace detail {

bool InstallShutdownSignalHandlersWith(SigactionFn install) {
    struct sigaction action{};
    action.sa_handler = HandleShutdownSignal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_ONSTACK | SA_RESTART;
    for (int signo : {SIGINT, SIGTERM}) {
        if (install(signo, &action, nullptr) != 0) {
            return false;
        }
    }
    return true;
}

}  // namespace detail

bool InstallShutdownSignalHandlers() {
    return detail::InstallShutdownSignalHandlersWith(&::sigaction);
}

bool ShutdownRequested() {
    return g_shutdown_signal.load(std::memory_order_acquire) != 0;
}

void LatchShutdownRequest() {
    g_shutdown_latched.store(true, std::memory_order_release);
}

bool ShutdownLatched() {
    return g_shutdown_latched.load(std::memory_order_acquire);
}

bool StartShutdownWatchdog(std::chrono::milliseconds deadline) {
    // Serial callers only: the check and the store are separate steps.
    if (g_watchdog_running.load(std::memory_order_acquire)) {
        return true;
    }
    try {
        std::thread(RunWatchdog, deadline).detach();
    } catch (const std::system_error&) {
        return false;
    }
    g_watchdog_running.store(true, std::memory_order_release);
    return true;
}

void ExitAfterShutdown(int status) {
    LatchShutdownRequest();
    if (g_watchdog_running.load(std::memory_order_acquire)) {
        google::FlushLogFiles(google::GLOG_INFO);
    }
    _exit(status);
}

}  // namespace mooncake
