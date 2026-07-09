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

#include "graceful_shutdown.h"

#include <errno.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <thread>
#include <utility>
#include <vector>

#include "transfer_engine_impl.h"

namespace mooncake {

namespace {

std::mutex g_registry_mutex;
std::vector<std::shared_ptr<ShutdownToken>> g_tokens;
std::atomic<bool> g_cleanup_started{false};

std::mutex g_install_mutex;
bool g_handlers_installed = false;
pid_t g_handlers_pid = 0;
bool g_atexit_registered = false;
int g_signal_pipe[2] = {-1, -1};
volatile sig_atomic_t g_signal_seen = 0;

class TransferEngineImplShutdownToken : public ShutdownToken {
   public:
    explicit TransferEngineImplShutdownToken(
        std::shared_ptr<TransferEngineImpl> impl)
        : impl_(std::move(impl)) {}

    void shutdown() override {
        if (auto impl = impl_.lock()) {
            impl->freeEngine();
        }
    }

    void detach() override { impl_.reset(); }

   private:
    std::weak_ptr<TransferEngineImpl> impl_;
};

std::vector<std::shared_ptr<ShutdownToken>> collectTokensForCleanup() {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    auto tokens = std::move(g_tokens);
    g_tokens.clear();
    return tokens;
}

void cleanupEngines() {
    if (g_cleanup_started.exchange(true)) return;

    auto tokens = collectTokensForCleanup();
    for (auto& token : tokens) {
        token->shutdown();
    }
}

void atexitCleanup() { cleanupEngines(); }

void signalWatcher() {
    unsigned char signal_byte = 0;
    ssize_t bytes_read = 0;
    do {
        bytes_read = read(g_signal_pipe[0], &signal_byte, sizeof(signal_byte));
    } while (bytes_read < 0 && errno == EINTR);

    if (bytes_read == static_cast<ssize_t>(sizeof(signal_byte))) {
        cleanupEngines();
        _Exit(128 + static_cast<int>(signal_byte));
    }

    _Exit(1);
}

bool startSignalWatcherLocked(pid_t current_pid) {
    if (g_signal_pipe[0] >= 0) close(g_signal_pipe[0]);
    if (g_signal_pipe[1] >= 0) close(g_signal_pipe[1]);
    g_signal_pipe[0] = -1;
    g_signal_pipe[1] = -1;
    g_signal_seen = 0;

    if (pipe(g_signal_pipe) != 0) return false;

    try {
        std::thread(signalWatcher).detach();
    } catch (...) {
        close(g_signal_pipe[0]);
        close(g_signal_pipe[1]);
        g_signal_pipe[0] = -1;
        g_signal_pipe[1] = -1;
        return false;
    }

    g_handlers_pid = current_pid;
    return true;
}

void shutdownSignalHandler(int signo) {
    if (g_signal_seen == 0) {
        g_signal_seen = signo;
        unsigned char signal_byte = static_cast<unsigned char>(signo);
        if (g_signal_pipe[1] < 0 ||
            write(g_signal_pipe[1], &signal_byte, sizeof(signal_byte)) !=
                static_cast<ssize_t>(sizeof(signal_byte))) {
            _Exit(128 + signo);
        }
    }

    for (;;) {
        pause();
    }
}

}  // namespace

void registerTokenForShutdown(std::shared_ptr<ShutdownToken> token) {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    g_tokens.push_back(std::move(token));
}

void registerEngineForShutdown(std::shared_ptr<TransferEngineImpl> impl) {
    registerTokenForShutdown(
        std::make_shared<TransferEngineImplShutdownToken>(std::move(impl)));
}

void installGracefulShutdownHandlers() {
    std::lock_guard<std::mutex> lock(g_install_mutex);
    pid_t current_pid = getpid();
    if (g_handlers_installed && g_handlers_pid == current_pid) return;

    if (!g_atexit_registered) {
        atexit(atexitCleanup);
        g_atexit_registered = true;
    }

    if (!startSignalWatcherLocked(current_pid)) return;

    struct sigaction sa{};
    sa.sa_handler = shutdownSignalHandler;
    sigemptyset(&sa.sa_mask);
    sigaddset(&sa.sa_mask, SIGTERM);
    sigaddset(&sa.sa_mask, SIGINT);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);

    g_handlers_installed = true;
}

}  // namespace mooncake
