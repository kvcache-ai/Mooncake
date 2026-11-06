#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
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

    void Schedule(Id id, Clock::time_point deadline) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                return;
            }
            PushRecord(Record{std::move(id), deadline});
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
        auto erase_begin = std::remove_if(
            records_.begin(), records_.end(),
            [&](const Record& rec) { return predicate(rec.id); });
        if (erase_begin != records_.end()) {
            records_.erase(erase_begin, records_.end());
            std::make_heap(records_.begin(), records_.end(),
                           std::greater<Record>{});
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
                if (records_.empty()) {
                    timer_cv_.wait(lock, [this]() {
                        return !timer_running_.load() || !records_.empty();
                    });
                    if (!timer_running_.load()) break;
                    continue;
                }

                auto now = Clock::now();
                auto next_deadline = NextRecord().deadline;
                if (next_deadline > now) {
                    timer_cv_.wait_until(
                        lock, next_deadline, [this, next_deadline]() {
                            return !timer_running_.load() || records_.empty() ||
                                   NextRecord().deadline < next_deadline;
                        });
                    if (!timer_running_.load()) break;
                    continue;
                }

                while (!records_.empty() && NextRecord().deadline <= now) {
                    expired.push_back(PopNextRecord());
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

    void PushRecord(Record record) {
        records_.push_back(std::move(record));
        std::push_heap(records_.begin(), records_.end(),
                       std::greater<Record>{});
    }

    Record PopNextRecord() {
        std::pop_heap(records_.begin(), records_.end(), std::greater<Record>{});
        auto record = std::move(records_.back());
        records_.pop_back();
        return record;
    }

    const Record& NextRecord() const { return records_.front(); }

    Callback callback_;
    std::mutex mutex_;
    std::vector<Record> records_;
    std::thread timer_thread_;
    std::atomic<bool> timer_running_{false};
    bool stopping_{false};
    std::condition_variable timer_cv_;
};

}  // namespace mooncake
