#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>
#include <utility>

namespace mooncake {

// Runs a callback on a dedicated thread when scheduled. Multiple Schedule()
// calls made before the worker consumes the pending request are coalesced into
// one callback invocation. Start() and Stop() must be called serially by the
// owner; Schedule() may be called concurrently from other threads.
class BackgroundWorker {
   public:
    using Callback = std::function<void()>;

    explicit BackgroundWorker(Callback callback)
        : callback_(std::move(callback)) {}

    ~BackgroundWorker() { Stop(); }

    BackgroundWorker(const BackgroundWorker&) = delete;
    BackgroundWorker& operator=(const BackgroundWorker&) = delete;

    void Start() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (running_) {
            return;
        }
        running_ = true;
        requested_ = false;
        next_run_at_ = Clock::time_point{};
        thread_ = std::thread(&BackgroundWorker::ThreadFunc, this);
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_ && !thread_.joinable()) {
                return;
            }
            running_ = false;
            requested_ = false;
        }
        cv_.notify_all();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    // Runs the callback once, as soon as the worker can.
    void Schedule() { ScheduleAfter(std::chrono::milliseconds(0)); }

    // Runs the callback once, no earlier than `delay` from now. Calls made
    // before the callback runs coalesce: the earliest deadline wins, so a
    // later ScheduleAfter() never postpones a request an earlier one made.
    void ScheduleAfter(std::chrono::milliseconds delay) {
        const auto deadline = Clock::now() + delay;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_) {
                return;
            }
            next_run_at_ =
                requested_ ? std::min(next_run_at_, deadline) : deadline;
            requested_ = true;
        }
        cv_.notify_one();
    }

   private:
    using Clock = std::chrono::steady_clock;

    void ThreadFunc() {
        std::unique_lock<std::mutex> lock(mutex_);
        while (true) {
            cv_.wait(lock, [this] { return !running_ || requested_; });
            if (!running_) {
                return;
            }
            if (next_run_at_ > Clock::now()) {
                // A later Schedule() or Stop() wakes this early; both are
                // re-checked when the loop continues.
                cv_.wait_until(lock, next_run_at_);
                continue;
            }
            requested_ = false;
            next_run_at_ = Clock::time_point{};
            lock.unlock();
            callback_();
            lock.lock();
        }
    }

    Callback callback_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread thread_;
    bool running_{false};
    bool requested_{false};
    Clock::time_point next_run_at_{};
};

}  // namespace mooncake
