// Copyright 2026 KVCache.AI
// SPDX-License-Identifier: Apache-2.0
#pragma once

#include <algorithm>
#include <atomic>
#include <charconv>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <cstdint>
#include <limits>
#include <exception>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <span>
#include <string>
#include <thread>
#include <vector>

namespace mooncake {

// A caller can use this before publishing sub-batches to preserve the legacy
// path for overlapping destinations. The common ordered case allocates no
// storage; shuffled input needs one interval index, never payload copies.
inline bool independentTransferDestinations(
    std::span<const uintptr_t> addresses, std::span<const size_t> lengths) {
    if (addresses.size() != lengths.size()) return false;
    bool ordered = true;
    uintptr_t previous_end = 0;
    for (size_t i = 0; i < addresses.size(); ++i) {
        if (lengths[i] == 0 ||
            lengths[i] > std::numeric_limits<uintptr_t>::max() - addresses[i])
            return false;
        if (i && addresses[i] < previous_end) ordered = false;
        previous_end = addresses[i] + lengths[i];
    }
    if (ordered) return true;
    std::vector<std::pair<uintptr_t, uintptr_t>> intervals;
    intervals.reserve(addresses.size());
    for (size_t i = 0; i < addresses.size(); ++i)
        intervals.emplace_back(addresses[i], addresses[i] + lengths[i]);
    std::sort(intervals.begin(), intervals.end());
    for (size_t i = 1; i < intervals.size(); ++i)
        if (intervals[i].first < intervals[i - 1].second) return false;
    return true;
}

// A bounded, engine-owned executor for independent synchronous sub-batches.
// All callbacks finish before execute() returns. Native cleanup follows the
// ordinary batch contract, including deferred cleanup on failure or timeout.
class TransferShardingExecutor {
   public:
    using Clock = std::chrono::steady_clock;
    struct Result {
        int code = 0;
        std::string error;
        static Result OK() { return {0, {}}; }
        bool ok() const { return code == 0; }
    };
    struct Range {
        size_t index;
        size_t begin;
        size_t end;
    };
    struct Cancellation {
        std::atomic<bool>& stopped;
        Clock::time_point deadline;
        const std::atomic<bool>* shutdown = nullptr;
        // A sibling failure stops new submissions, but accepted siblings keep
        // their ordinary polling lifecycle until terminal, deadline or
        // shutdown.
        bool requested() const {
            return stopped.load(std::memory_order_acquire) || expired();
        }
        bool expired() const {
            return Clock::now() >= deadline ||
                   (shutdown && shutdown->load(std::memory_order_acquire));
        }
        void requestStop() const {
            stopped.store(true, std::memory_order_release);
        }
    };
    using Work = std::function<Result(Range, const Cancellation&)>;

    struct Config {
        size_t threads = 0;
        size_t min_descriptors = 4096;
        size_t max_parents = 16;

        static Config fromEnvironment() {
            return {read("MC_BATCH_SUBMIT_THREADS", 0, 0, 32),
                    read("MC_BATCH_SUBMIT_MIN_SIZE", 4096, 1, SIZE_MAX), 16};
        }

       private:
        static size_t read(const char* name, size_t fallback, size_t low,
                           size_t high) {
            const char* value = std::getenv(name);
            if (!value) return fallback;
            const std::string text(value);
            size_t parsed = 0;
            auto [end, ec] =
                std::from_chars(text.data(), text.data() + text.size(), parsed);
            if (ec != std::errc{} || end != text.data() + text.size() ||
                parsed < low || parsed > high)
                throw std::invalid_argument(std::string("Invalid ") + name);
            return parsed;
        }
    };

    TransferShardingExecutor()
        : TransferShardingExecutor(Config::fromEnvironment()) {}

    explicit TransferShardingExecutor(Config config) : config_(config) {
        if (config.threads > 32 || config.min_descriptors == 0 ||
            config.max_parents == 0)
            throw std::invalid_argument("Invalid transfer sharding config");
        try {
            for (size_t i = 0; i < config_.threads; ++i)
                threads_.emplace_back([this] { worker(); });
        } catch (...) {
            shutdown();
            throw;
        }
    }

    ~TransferShardingExecutor() { shutdown(); }
    TransferShardingExecutor(const TransferShardingExecutor&) = delete;
    TransferShardingExecutor& operator=(const TransferShardingExecutor&) =
        delete;

    bool eligible(size_t count) const {
        return config_.threads != 0 && count >= config_.min_descriptors;
    }
    const Config& config() const { return config_; }

    static std::vector<Range> partition(size_t count, size_t threads) {
        std::vector<Range> ranges;
        const size_t k = std::min(count, threads);
        ranges.reserve(k);
        // Quotient/remainder form of floor(i*N/K), without i*N overflow.
        for (size_t i = 0; i < k; ++i) {
            auto boundary = [=](size_t j) {
                return j * (count / k) + j * (count % k) / k;
            };
            ranges.push_back({i, boundary(i), boundary(i + 1)});
        }
        return ranges;
    }

    Result execute(size_t count, Clock::time_point deadline, Work work) {
        if (!eligible(count)) return {-1, "Sharding executor not eligible"};
        if (active_executor_ == this)
            return {-1, "Recursive transfer sharding would deadlock"};
        auto ranges = partition(count, config_.threads);
        auto parent = std::make_shared<ParentBatch>(ranges.size(), deadline,
                                                    std::move(work));
        // Build every job before publishing any: allocation failure cannot
        // leave a partially published parent with an unpayable completion
        // count.
        std::list<Job> pending;
        for (auto range : ranges) pending.push_back({parent, range});
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (!admission_.wait_until(lock, deadline, [&] {
                    return stopping_ || parents_.size() < config_.max_parents;
                }))
                return {-1, "Transfer sharding admission timed out"};
            if (stopping_) return {-1, "Transfer sharding is shutting down"};
            parents_.push_back(parent);
            jobs_.splice(jobs_.end(), pending);
        }
        ready_.notify_all();
        {
            std::unique_lock<std::mutex> lock(parent->mutex);
            // Completion, not the caller deadline, owns the input lifetime.
            parent->finished.wait(
                lock, [&] { return parent->remaining_sub_batches == 0; });
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            parents_.remove(parent);
        }
        admission_.notify_one();
        idle_.notify_all();
        for (const auto& result : parent->sub_batch_results)
            if (result.code != 0 && result.code != kCanceled) return result;
        for (const auto& result : parent->sub_batch_results)
            if (!result.ok()) return result;
        return Result::OK();
    }

    void shutdown() {
        // Engine lifecycle must serialize shutdown/destruction. execute()
        // callers may still be waiting; their callbacks must all finish.
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
            for (auto& parent : parents_) {
                parent->stopped.store(true, std::memory_order_release);
                parent->shutdown.store(true, std::memory_order_release);
            }
        }
        ready_.notify_all();
        admission_.notify_all();
        for (auto& thread : threads_)
            if (thread.joinable()) thread.join();
        std::unique_lock<std::mutex> lock(mutex_);
        idle_.wait(lock, [&] { return parents_.empty(); });
    }

   private:
    static constexpr int kCanceled = -2;
    struct ParentBatch {
        ParentBatch(size_t count, Clock::time_point until, Work function)
            : remaining_sub_batches(count),
              deadline(until),
              work(std::move(function)),
              sub_batch_results(count) {}
        std::mutex mutex;
        std::condition_variable finished;
        size_t remaining_sub_batches;
        std::atomic<bool> stopped{false};
        std::atomic<bool> shutdown{false};
        Clock::time_point deadline;
        Work work;
        std::vector<Result> sub_batch_results;
    };
    struct Job {
        std::shared_ptr<ParentBatch> parent;
        Range range;
    };
    void worker() {
        active_executor_ = this;
        for (;;) {
            Job job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                ready_.wait(lock, [&] { return stopping_ || !jobs_.empty(); });
                if (jobs_.empty()) break;
                job = std::move(jobs_.front());
                jobs_.pop_front();
            }
            auto& parent = *job.parent;
            Cancellation cancellation{parent.stopped, parent.deadline,
                                      &parent.shutdown};
            Result result;
            if (cancellation.requested()) {
                result = {kCanceled, "Transfer sharding canceled or timed out"};
            } else {
                try {
                    result = parent.work(job.range, cancellation);
                } catch (const std::exception& error) {
                    result = {-1, error.what()};
                } catch (...) {
                    result = {-1, "Unknown transfer sharding exception"};
                }
            }
            if (!result.ok())
                parent.stopped.store(true, std::memory_order_release);
            {
                std::lock_guard<std::mutex> lock(parent.mutex);
                parent.sub_batch_results[job.range.index] = std::move(result);
                if (--parent.remaining_sub_batches == 0)
                    parent.finished.notify_one();
            }
        }
        active_executor_ = nullptr;
    }

    inline static thread_local TransferShardingExecutor* active_executor_ =
        nullptr;
    const Config config_;
    std::mutex mutex_;
    std::condition_variable ready_, admission_, idle_;
    bool stopping_ = false;
    std::list<Job> jobs_;
    std::list<std::shared_ptr<ParentBatch>> parents_;
    std::vector<std::thread> threads_;
};

struct SubBatchProgress {
    bool terminal = false;
    bool success = false;
    std::string error;
};

// Each adapter owns one native handle. The parent keeps request storage alive
// until every callback ends. Errors/timeouts use ordinary native free, which
// may defer cleanup and does not promise that payload access has stopped.
template <class Adapter>
TransferShardingExecutor::Result runSubBatch(
    Adapter& adapter,
    const TransferShardingExecutor::Cancellation& cancellation) {
    using Executor = TransferShardingExecutor;
    Executor::Result result;
    bool allocated = false;
    try {
        result = adapter.allocate();
        if (!result.ok()) return result;
        allocated = true;
        result = cancellation.requested()
                     ? Executor::Result{-1, "Parent canceled before submission"}
                     : adapter.submit();
    } catch (const std::exception& error) {
        result = {-1, error.what()};
    } catch (...) {
        result = {-1, "Unknown synchronous batch exception"};
    }
    if (!allocated) return result;
    while (result.ok()) {
        if (cancellation.expired()) {
            result = {-1, "Parent transfer canceled or timed out"};
            break;
        }
        try {
            auto progress = adapter.poll();
            if (progress.terminal) {
                if (!progress.success)
                    result = {-1, progress.error.empty()
                                      ? "Sub-batch transfer failed"
                                      : progress.error};
                break;
            }
        } catch (const std::exception& error) {
            if (result.ok()) result = {-1, error.what()};
        } catch (...) {
            if (result.ok()) result = {-1, "Unknown batch polling exception"};
        }
        if (result.ok()) std::this_thread::yield();
    }
    // Never retry a free that may already have invalidated the native handle.
    auto released = adapter.release();
    return result.ok() ? std::move(released) : result;
}

}  // namespace mooncake
