#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

namespace mooncake {

template <typename Id>
class DeadlineScheduler {
   public:
    using Clock = std::chrono::steady_clock;
    using Callback = std::function<void(const Id& id)>;
    using Handle = uint64_t;

    explicit DeadlineScheduler(Callback callback)
        : callback_(std::move(callback)) {}

    ~DeadlineScheduler() { Stop(); }

    Handle Schedule(Id id, Clock::time_point deadline) {
        Handle handle;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (stopping_) {
                return 0;
            }
            if (next_handle_ == 0)
                throw std::overflow_error("deadline handles exhausted");
            if (!timer_running_.load()) {
                timer_running_.store(true);
                try {
                    timer_thread_ =
                        std::thread([this]() { this->TimerLoop(); });
                } catch (...) {
                    timer_running_.store(false);
                    throw;
                }
            }
            handle = next_handle_++;
            auto record =
                records_.emplace(deadline, Record{std::move(id), handle});
            try {
                handles_.emplace(handle, record);
            } catch (...) {
                records_.erase(record);
                throw;
            }
        }
        timer_cv_.notify_one();
        return handle;
    }

    // Zero, cancelled, and already-expired handles are harmless no-ops.
    bool Cancel(Handle handle) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto found = handles_.find(handle);
            if (found == handles_.end()) return false;
            records_.erase(found->second);
            handles_.erase(found);
        }
        timer_cv_.notify_one();
        return true;
    }

    template <typename Predicate>
    void RemoveIf(Predicate predicate) {
        std::lock_guard<std::mutex> lock(mutex_);
        for (auto it = records_.begin(); it != records_.end();) {
            if (predicate(it->second.id)) {
                handles_.erase(it->second.handle);
                it = records_.erase(it);
            } else {
                ++it;
            }
        }
        timer_cv_.notify_one();
    }

    void Stop() {
        // Release payloads and index storage after joining, outside mutex_.
        Records discarded;
        HandleIndex discarded_handles;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stopping_ = true;
            timer_running_.store(false);
            discarded.swap(records_);
            discarded_handles.swap(handles_);
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
                auto next_deadline = records_.begin()->first;
                if (next_deadline > now) {
                    timer_cv_.wait_until(
                        lock, next_deadline, [this, next_deadline]() {
                            return !timer_running_.load() || records_.empty() ||
                                   records_.begin()->first != next_deadline;
                        });
                    if (!timer_running_.load()) break;
                    continue;
                }

                while (!records_.empty() && records_.begin()->first <= now) {
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
        Handle handle;
    };
    using Records = std::multimap<Clock::time_point, Record>;
    using HandleIndex = std::unordered_map<Handle, typename Records::iterator>;

    Record PopNextRecord() {
        auto first = records_.begin();
        auto record = std::move(first->second);
        handles_.erase(record.handle);
        records_.erase(first);
        return record;
    }

    Callback callback_;
    std::mutex mutex_;
    Records records_;
    HandleIndex handles_;
    Handle next_handle_{1};
    std::thread timer_thread_;
    std::atomic<bool> timer_running_{false};
    bool stopping_{false};
    std::condition_variable timer_cv_;
};

}  // namespace mooncake
