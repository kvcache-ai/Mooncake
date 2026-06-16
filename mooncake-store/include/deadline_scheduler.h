#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

namespace mooncake {

template <typename Id>
class DeadlineScheduler {
   public:
    using Clock = std::chrono::steady_clock;
    using Callback = std::function<void(const Id& id)>;

    explicit DeadlineScheduler(Callback callback)
        : callback_(std::move(callback)) {}

    ~DeadlineScheduler() { Stop(); }

    void Schedule(const Id& id, Clock::time_point deadline) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                return;
            }
            queue_.push(Record{id, deadline});
            if (!timer_running_.load()) {
                timer_running_.store(true);
                timer_thread_ = std::thread([this]() { this->TimerLoop(); });
            }
        }
        timer_cv_.notify_one();
    }

    template <typename Predicate>
    void RemoveIf(Predicate predicate) {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<Record> remaining;
        remaining.reserve(queue_.size());
        while (!queue_.empty()) {
            auto rec = queue_.top();
            queue_.pop();
            if (!predicate(rec.id)) {
                remaining.push_back(rec);
            }
        }
        for (auto& rec : remaining) {
            queue_.push(rec);
        }
        timer_cv_.notify_one();
    }

    void Stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
            timer_running_.store(false);
        }
        timer_cv_.notify_all();
        if (timer_thread_.joinable()) {
            timer_thread_.join();
        }
    }

   private:
    void TimerLoop() {
        while (timer_running_.load()) {
            std::vector<Record> expired;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                if (queue_.empty()) {
                    timer_cv_.wait(lock, [this]() {
                        return !timer_running_.load() || !queue_.empty();
                    });
                    if (!timer_running_.load()) break;
                    continue;
                }

                auto now = Clock::now();
                auto next_deadline = queue_.top().deadline;
                if (next_deadline > now) {
                    timer_cv_.wait_until(
                        lock, next_deadline, [this, next_deadline]() {
                            return !timer_running_.load() || queue_.empty() ||
                                   queue_.top().deadline < next_deadline;
                        });
                    if (!timer_running_.load()) break;
                    continue;
                }

                while (!queue_.empty() && queue_.top().deadline <= now) {
                    expired.push_back(queue_.top());
                    queue_.pop();
                }
            }

            // Run callbacks with the scheduler mutex released: callbacks may
            // acquire other locks, so holding mutex_ here risks lock-order
            // inversion deadlocks.
            for (auto& rec : expired) {
                callback_(rec.id);
            }
        }
    }

    struct Record {
        Id id;
        Clock::time_point deadline;

        bool operator>(const Record& other) const {
            return deadline > other.deadline;
        }
    };

    Callback callback_;
    std::mutex mutex_;
    std::priority_queue<Record, std::vector<Record>, std::greater<Record>>
        queue_;
    std::thread timer_thread_;
    std::atomic<bool> timer_running_{false};
    bool stopping_{false};
    std::condition_variable timer_cv_;
};

}  // namespace mooncake
