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

#include "bench_runner.h"

#include <glog/logging.h>
#include <signal.h>
#include <unistd.h>

#include <cstdlib>

namespace mooncake {
namespace tent {

namespace {

std::atomic<bool> g_stop_requested{false};
static_assert(std::atomic<bool>::is_always_lock_free,
              "the signal handler needs a lock-free flag");

void writeStderr(const char* msg, size_t len) {
    ssize_t rc = write(STDERR_FILENO, msg, len);
    (void)rc;
}

// Only async-signal-safe calls: a lock-free atomic, write(2), and _exit(2).
void onStopSignal(int) {
    if (g_stop_requested.exchange(true)) {
        static const char kMsg[] = "tebench: second signal, exiting now\n";
        writeStderr(kMsg, sizeof(kMsg) - 1);
        _exit(EXIT_FAILURE);
    }
    static const char kMsg[] =
        "tebench: stopping; send the signal again to exit immediately\n";
    writeStderr(kMsg, sizeof(kMsg) - 1);
}

}  // namespace

BenchRunner::BenchRunner() {
    struct sigaction action{};
    action.sa_handler = onStopSignal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_RESTART;
    sigaction(SIGINT, &action, nullptr);
    sigaction(SIGTERM, &action, nullptr);
}

bool BenchRunner::stopRequested() noexcept {
    return g_stop_requested.load(std::memory_order_relaxed);
}

void BenchRunner::startWorkers(int num_threads) {
    LOG_ASSERT(threads_.empty()) << "workers already started";
    shutdown_ = false;
    tasks_.assign(num_threads, nullptr);
    threads_.reserve(num_threads);
    for (int i = 0; i < num_threads; ++i)
        threads_.emplace_back(&BenchRunner::workerLoop, this, i);
}

void BenchRunner::stopInitiator() {
    {
        std::lock_guard<std::mutex> lk(mtx_);
        shutdown_ = true;
    }
    cv_task_.notify_all();
    for (auto& thread : threads_) thread.join();
    threads_.clear();
    tasks_.clear();
}

void BenchRunner::workerLoop(int thread_id) {
    std::unique_lock<std::mutex> lk(mtx_);
    while (true) {
        cv_task_.wait(lk, [&] { return shutdown_ || tasks_[thread_id]; });
        if (!tasks_[thread_id]) return;
        std::function<int(int)> task;
        std::swap(task, tasks_[thread_id]);
        lk.unlock();
        const int rc = task(thread_id);
        lk.lock();
        if (rc != 0) task_failed_ = true;
        if (--pending_ == 0) cv_done_.notify_all();
    }
}

int BenchRunner::runInitiatorTasks(
    const std::function<int(int /* thread_id */)>& func) {
    std::unique_lock<std::mutex> lk(mtx_);
    for (auto& task : tasks_) task = func;
    pending_ = static_cast<int>(tasks_.size());
    task_failed_ = false;
    cv_task_.notify_all();
    cv_done_.wait(lk, [&] { return pending_ == 0; });
    return (task_failed_ || !running()) ? -1 : 0;
}

size_t BenchRunner::getTargetIndex(int thread_id) const {
    LOG_ASSERT(!target_handles_.empty());
    return static_cast<size_t>(thread_id) % target_handles_.size();
}

int BenchRunner::localTargetThreadId(int thread_id) const {
    LOG_ASSERT(!target_handles_.empty());
    return thread_id / static_cast<int>(target_handles_.size());
}

uint64_t BenchRunner::getTargetSegmentId(int thread_id) const {
    return target_handles_[getTargetIndex(thread_id)];
}

uint64_t BenchRunner::targetSlotAddress(int thread_id, uint64_t base,
                                        uint64_t length, uint64_t bytes,
                                        uint64_t slot) {
    const uint64_t relative_offset =
        checkedMul(bytes, slot, "target relative offset");
    if (XferBenchConfig::target_range_size != 0 &&
        !rangeContains(relative_offset, bytes,
                       XferBenchConfig::target_range_size)) {
        LOG(FATAL) << "Target range too small for thread " << thread_id;
    }
    if (XferBenchConfig::target_offset > length ||
        !rangeContains(relative_offset, bytes,
                       length - XferBenchConfig::target_offset)) {
        LOG(FATAL) << "Target buffer too small for thread " << thread_id;
    }
    return checkedAdd(base,
                      checkedAdd(XferBenchConfig::target_offset,
                                 relative_offset, "target address offset"),
                      "target address");
}

}  // namespace tent
}  // namespace mooncake
