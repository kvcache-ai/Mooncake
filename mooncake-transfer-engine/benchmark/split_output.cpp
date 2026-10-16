// Copyright 2026 KVCache.AI
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

#include "split_output.h"

#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <mutex>

namespace mooncake {
namespace tent {

bool g_split_output_enabled = false;

namespace {

std::unique_ptr<std::ofstream> g_split_output;
std::mutex g_split_output_mu;

}  // namespace

bool formatSplitXferLine(char* buf, size_t cap, const SplitXferSample& sample,
                         size_t* written) {
    if (buf == nullptr || written == nullptr || cap == 0) return false;
    const int n = std::snprintf(
        buf, cap,
        "{\"batch_size\":%" PRIu64 ",\"submit_us\":%" PRIu64
        ",\"wait_us\":%" PRIu64 ",\"polls\":%" PRIu64 "}\n",
        sample.batch_size, sample.submit_us, sample.wait_us, sample.polls);
    if (n < 0 || static_cast<size_t>(n) >= cap) return false;
    *written = static_cast<size_t>(n);
    return true;
}

bool openSplitOutput(const std::string& path, std::string* error) {
    closeSplitOutput();
    auto out =
        std::make_unique<std::ofstream>(path, std::ios::out | std::ios::app);
    if (!*out) {
        if (error != nullptr) {
            *error = "failed to open split JSONL output: " + path;
        }
        return false;
    }
    std::lock_guard<std::mutex> lock(g_split_output_mu);
    g_split_output = std::move(out);
    g_split_output_enabled = true;
    static bool registered_atexit = false;
    if (!registered_atexit) {
        std::atexit(closeSplitOutput);
        registered_atexit = true;
    }
    return true;
}

void closeSplitOutput() {
    std::lock_guard<std::mutex> lock(g_split_output_mu);
    g_split_output_enabled = false;
    if (g_split_output) {
        g_split_output->flush();
        g_split_output.reset();
    }
}

void logSplitXfer(const SplitXferSample& sample) {
    if (!g_split_output_enabled) return;
    char buf[160];
    size_t n = 0;
    if (!formatSplitXferLine(buf, sizeof(buf), sample, &n)) return;
    std::lock_guard<std::mutex> lock(g_split_output_mu);
    if (!g_split_output || !*g_split_output) return;
    g_split_output->write(buf, static_cast<std::streamsize>(n));
}

}  // namespace tent
}  // namespace mooncake
