#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

namespace mooncake {

// Runs a callback on a dedicated thread when scheduled. Multiple Schedule()
// calls made before the worker consumes the pending request are coalesced into
// one callback invocation. Start() and Stop() must be called serially by the
// owner; Schedule() may be called concurrently from other threads.
class BackgroundWorker {
   public:
    using Clock = std::chrono::steady_clock;
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
        requested_at_.reset();
        thread_ = std::thread(&BackgroundWorker::ThreadFunc, this);
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_ && !thread_.joinable()) {
                return;
            }
            running_ = false;
            requested_at_.reset();
        }
        cv_.notify_all();
        if (thread_.joinable()) {
            thread_.join();
        }
    }

    void Schedule() { ScheduleAt(Clock::now()); }

    // Keep the earliest pending wakeup; an immediate Schedule() can interrupt
    // a timed wait. Callbacks may schedule their next run without holding
    // locks.
    void ScheduleAt(Clock::time_point deadline) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_) {
                return;
            }
            if (!requested_at_ || deadline < *requested_at_)
                requested_at_ = deadline;
        }
        cv_.notify_one();
    }

    bool IsRunning() {
        std::lock_guard lock(mutex_);
        return running_;
    }

   private:
    void ThreadFunc() {
        while (true) {
            {
                std::unique_lock<std::mutex> lock(mutex_);
                while (running_) {
                    if (!requested_at_) {
                        cv_.wait(lock);
                    } else if (*requested_at_ > Clock::now()) {
                        const auto deadline = *requested_at_;
                        cv_.wait_until(lock, deadline);
                    } else {
                        break;
                    }
                }
                if (!running_) {
                    return;
                }
                requested_at_.reset();
            }
            callback_();
        }
    }

    Callback callback_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread thread_;
    bool running_{false};
    std::optional<Clock::time_point> requested_at_;
};

}  // namespace mooncake
