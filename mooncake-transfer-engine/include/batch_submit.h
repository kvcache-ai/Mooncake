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
#include <utility>
#include <vector>

#include <glog/logging.h>

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
class BatchSubmitExecutor {
   public:
    using Clock = std::chrono::steady_clock;
    struct Result {
        int code = 0;
        std::string error;
        bool submit_failed = false;
        bool retryable = false;
        static Result OK() { return {0, {}}; }
        static Result Error(std::string error) {
            return {kError, std::move(error)};
        }
        static Result SubmitError(std::string error) {
            auto result = Error(std::move(error));
            result.submit_failed = true;
            return result;
        }
        static Result TransferError(std::string error) {
            auto result = Error(std::move(error));
            result.retryable = true;
            return result;
        }
        static Result Canceled(std::string error = {}) {
            return {kCanceled, std::move(error)};
        }
        bool ok() const { return code == 0; }
        bool isCanceled() const { return code == kCanceled; }

       private:
        static constexpr int kError = -1;
        static constexpr int kCanceled = -2;
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
                parsed < low || parsed > high) {
                LOG(WARNING) << "Invalid " << name << " environment value: '"
                             << value << "', using default " << fallback;
                return fallback;
            }
            return parsed;
        }
    };

    BatchSubmitExecutor() : BatchSubmitExecutor(Config::fromEnvironment()) {}

    explicit BatchSubmitExecutor(Config config) : config_(config) {
        if (config.threads > 32 || config.min_descriptors == 0 ||
            config.max_parents == 0)
            throw std::invalid_argument("Invalid batch submit config");
        try {
            for (size_t i = 0; i < config_.threads; ++i)
                threads_.emplace_back([this] { worker(); });
        } catch (...) {
            shutdown();
            throw;
        }
    }

    ~BatchSubmitExecutor() { shutdown(); }
    BatchSubmitExecutor(const BatchSubmitExecutor&) = delete;
    BatchSubmitExecutor& operator=(const BatchSubmitExecutor&) = delete;

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
        if (!eligible(count))
            return Result::Error("Batch submit executor not eligible");
        if (active_executor_ == this)
            return Result::Error("Recursive batch submission would deadlock");
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
                return Result::Error("Parallel batch admission timed out");
            if (stopping_)
                return Result::Error("Batch submit executor is shutting down");
            parents_.push_back(parent);
            jobs_.splice(jobs_.end(), pending);
        }
        ready_.notify_all();
        bool timed_out;
        {
            std::unique_lock<std::mutex> lock(parent->mutex);
            timed_out = !parent->finished.wait_until(lock, deadline, [&] {
                return parent->remaining_sub_batches == 0;
            });
        }
        if (timed_out) {
            parent->stopped.store(true, std::memory_order_release);
            cancelQueued(parent);
            std::unique_lock<std::mutex> lock(parent->mutex);
            // Dequeued jobs still own caller input until their callbacks end.
            parent->finished.wait(
                lock, [&] { return parent->remaining_sub_batches == 0; });
        }
        {
            std::lock_guard<std::mutex> lock(mutex_);
            parents_.remove(parent);
        }
        admission_.notify_one();
        idle_.notify_all();
        Result outcome = Result::OK();
        bool submit_failed = false;
        bool retryable = true;
        for (const auto& result : parent->sub_batch_results) {
            submit_failed |= result.submit_failed;
            if (!result.ok() && !result.isCanceled()) {
                if (outcome.ok()) outcome = result;
                retryable &= result.retryable;
            }
        }
        const bool expired = timed_out || parent->deadline_expired;
        const bool shutdown = parent->shutdown.load(std::memory_order_acquire);
        // Keep the first actual error for diagnosis, but decide retry using
        // every sub-batch and the parent's termination reason.
        retryable &= !outcome.ok() && !submit_failed && !expired && !shutdown &&
                     Clock::now() < deadline;
        if (outcome.ok()) {
            if (expired)
                outcome = Result::Error("Parallel batch transfer timed out");
            else if (shutdown)
                outcome =
                    Result::Error("Batch submit executor is shutting down");
            else
                for (const auto& result : parent->sub_batch_results)
                    if (!result.ok()) {
                        outcome = result;
                        break;
                    }
        }
        outcome.submit_failed = submit_failed;
        outcome.retryable = retryable;
        return outcome;
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
    struct ParentBatch {
        ParentBatch(size_t count, Clock::time_point until, Work function)
            : remaining_sub_batches(count),
              deadline(until),
              work(std::move(function)),
              sub_batch_results(count) {}
        std::mutex mutex;
        std::condition_variable finished;
        size_t remaining_sub_batches;
        bool deadline_expired = false;
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

    static void finishJob(const Job& job, Result result) {
        auto& parent = *job.parent;
        if (!result.ok()) parent.stopped.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lock(parent.mutex);
        // Completion can satisfy wait_until's predicate after the deadline
        // without reporting a timeout. Record canceled jobs that reached it;
        // execute() reports the parent reason after checking actual errors.
        if (result.isCanceled() && Clock::now() >= parent.deadline)
            parent.deadline_expired = true;
        parent.sub_batch_results[job.range.index] = std::move(result);
        if (--parent.remaining_sub_batches == 0) parent.finished.notify_one();
    }

    void cancelQueued(const std::shared_ptr<ParentBatch>& parent) {
        std::list<Job> canceled;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            for (auto it = jobs_.begin(); it != jobs_.end();) {
                auto current = it++;
                if (current->parent == parent)
                    canceled.splice(canceled.end(), jobs_, current);
            }
        }
        // The queue lock gives either this caller or a worker each job, never
        // both. Settle without allocating error strings or holding the queue
        // lock; execute() supplies the timeout diagnostic after joining.
        for (const auto& job : canceled) finishJob(job, Result::Canceled());
    }

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
                result = Result::Canceled("Parent canceled before preparation");
            } else {
                try {
                    result = parent.work(job.range, cancellation);
                } catch (const std::exception& error) {
                    result = Result::Error(error.what());
                } catch (...) {
                    result =
                        Result::Error("Unknown batch submission exception");
                }
            }
            finishJob(job, std::move(result));
        }
        active_executor_ = nullptr;
    }

    inline static thread_local BatchSubmitExecutor* active_executor_ = nullptr;
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
    bool retryable = false;
};

// Each adapter owns one native handle. The parent keeps request storage alive
// until every callback ends. Errors/timeouts use ordinary native free, which
// may defer cleanup and does not promise that payload access has stopped.
template <class Adapter>
BatchSubmitExecutor::Result runSubBatch(
    Adapter& adapter, const BatchSubmitExecutor::Cancellation& cancellation) {
    using Executor = BatchSubmitExecutor;
    Executor::Result result;
    bool allocated = false;
    try {
        result = adapter.allocate();
        if (!result.ok()) return result;
        allocated = true;
        if (cancellation.requested()) {
            result =
                Executor::Result::Canceled("Parent canceled before submission");
        } else {
            result = adapter.submit();
        }
    } catch (const std::exception& error) {
        result = Executor::Result::Error(error.what());
    } catch (...) {
        result = Executor::Result::Error("Unknown synchronous batch exception");
    }
    if (!allocated) return result;
    while (result.ok()) {
        if (cancellation.expired()) {
            result = Executor::Result::Canceled("Parent transfer canceled");
            break;
        }
        try {
            auto progress = adapter.poll();
            if (progress.terminal) {
                if (!progress.success) {
                    auto error = progress.error.empty()
                                     ? "Sub-batch transfer failed"
                                     : progress.error;
                    result =
                        progress.retryable
                            ? Executor::Result::TransferError(std::move(error))
                            : Executor::Result::Error(std::move(error));
                }
                break;
            }
        } catch (const std::exception& error) {
            if (result.ok()) result = Executor::Result::Error(error.what());
        } catch (...) {
            if (result.ok())
                result =
                    Executor::Result::Error("Unknown batch polling exception");
        }
        if (result.ok()) std::this_thread::yield();
    }
    // Never retry a free that may already have invalidated the native handle.
    auto released = adapter.release();
    // A native release error must not be hidden by cooperative cancellation.
    if (result.isCanceled() && !released.ok()) return released;
    return result.ok() ? std::move(released) : result;
}

}  // namespace mooncake
