// Copyright 2025 KVCache.AI
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

#ifndef BENCH_RUNNER_H
#define BENCH_RUNNER_H

#include "utils.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace mooncake {
namespace tent {
enum class IntentType : int;

// Backend-independent parts of a benchmark runner: the initiator worker pool,
// stop/abort state, and the thread-to-target layout. Backends open targets,
// own the buffers, and issue transfers.
class BenchRunner {
   public:
    // Installs the SIGINT/SIGTERM handlers.
    BenchRunner();
    virtual ~BenchRunner() = default;

    BenchRunner(const BenchRunner&) = delete;
    BenchRunner& operator=(const BenchRunner&) = delete;

    virtual void pinThread(int thread_id) = 0;

    virtual int runTarget() = 0;

    // Opens the target segments, then calls startWorkers().
    virtual int startInitiator(int num_threads) = 0;

    // Joins the workers started by startInitiator().
    void stopInitiator();

    // Runs func(thread_id) on every worker and returns once all of them have
    // returned, including after a failed transfer or a stop request, so func
    // may capture the caller's stack. Returns 0 only if every call returned 0
    // and the run is still running().
    int runInitiatorTasks(const std::function<int(int /* thread_id */)>& func);

    virtual std::string getSegmentName() const = 0;

    // Start of worker thread_id's `stride`-byte region in its local buffer,
    // or nullopt if the region does not fit in that buffer.
    virtual std::optional<uint64_t> getLocalBufferBase(
        int thread_id, uint64_t stride) const = 0;

    size_t getTargetCount() const {
        return std::max<size_t>(target_handles_.size(), 1);
    }

    size_t getTargetIndex(int thread_id) const;

    uint64_t getTargetSegmentId(int thread_id) const;

    // Start of worker thread_id's `window`-byte region in its target buffer,
    // after --target_offset and --target_gpu_id, or nullopt if the region
    // does not fit in that buffer or in --target_range_size.
    virtual std::optional<uint64_t> getTargetBufferBase(
        int thread_id, uint64_t window) const = 0;

    // Returns elapsed microseconds on success, or a negative value if the
    // transfer failed or the run stopped. Implementations must not call
    // exit() from worker threads; use noteTransferFailed() instead.
    virtual double runSingleTransfer(uint64_t local_addr, uint64_t target_id,
                                     uint64_t target_addr, uint64_t block_size,
                                     uint64_t batch_size, OpCode opcode,
                                     uint64_t deadline_ns,
                                     IntentType intent_type) = 0;

    // True once SIGINT or SIGTERM has been received. Never reset.
    static bool stopRequested() noexcept;

   protected:
    // False after a stop request or a failed transfer. Transfers should not
    // start, and should give up waiting once the transports are done with
    // their buffers.
    bool running() const noexcept {
        return !aborted_.load(std::memory_order_relaxed) && !stopRequested();
    }

    // Ends the run from a worker thread. The remaining workers see
    // running() == false and return; runInitiatorTasks() then fails.
    void noteTransferFailed() noexcept {
        aborted_.store(true, std::memory_order_relaxed);
    }

    void startWorkers(int num_threads);

    // Index of this worker among the workers that share its target, shifted
    // by --target_gpu_id so target buffers are used round-robin from there.
    int localTargetThreadId(int thread_id) const;

    // base + slot * bytes if the slot-th `bytes`-sized region fits in
    // [base, base + length), else nullopt.
    static std::optional<uint64_t> slotAddress(uint64_t base, uint64_t length,
                                               uint64_t bytes, uint64_t slot);

    // slotAddress() for a target buffer: regions start after --target_offset
    // and must also fit in --target_range_size.
    static std::optional<uint64_t> targetSlotAddress(uint64_t base,
                                                     uint64_t length,
                                                     uint64_t bytes,
                                                     uint64_t slot);

    // Opened target segments, in --target_seg_name order.
    std::vector<uint64_t> target_handles_;

   private:
    void workerLoop(int thread_id);

    std::vector<std::thread> threads_;
    std::vector<std::function<int(int)>> tasks_;
    std::mutex mtx_;
    std::condition_variable cv_task_;
    std::condition_variable cv_done_;
    int pending_ = 0;
    bool shutdown_ = false;
    bool task_failed_ = false;
    std::atomic<bool> aborted_{false};
};

}  // namespace tent
}  // namespace mooncake

#endif  // BENCH_RUNNER_H
