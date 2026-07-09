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

#include <signal.h>
#include <stdlib.h>

#include <algorithm>
#include <atomic>
#include <mutex>
#include <vector>

#include "transfer_engine_impl.h"

namespace mooncake {

namespace {

std::mutex g_registry_mutex;
std::vector<std::weak_ptr<TransferEngineImpl>> g_engines;
std::atomic<bool> g_installed{false};

void atexitCleanup() {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    for (auto& weak : g_engines) {
        if (auto impl = weak.lock()) {
            impl->freeEngine();
        }
    }
    g_engines.clear();
}

void shutdownSignalHandler(int signo) { exit(128 + signo); }

}  // namespace

void registerEngineForShutdown(std::shared_ptr<TransferEngineImpl> impl) {
    std::lock_guard<std::mutex> lock(g_registry_mutex);
    g_engines.erase(
        std::remove_if(g_engines.begin(), g_engines.end(),
                       [](const std::weak_ptr<TransferEngineImpl>& w) {
                           return w.expired();
                       }),
        g_engines.end());
    g_engines.push_back(impl);
}

void installGracefulShutdownHandlers() {
    if (g_installed.exchange(true)) return;
    atexit(atexitCleanup);
    struct sigaction sa{};
    sa.sa_handler = shutdownSignalHandler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    sigaction(SIGTERM, &sa, nullptr);
    sigaction(SIGINT, &sa, nullptr);
    sigaction(SIGABRT, &sa, nullptr);
}

}  // namespace mooncake
