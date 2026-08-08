#pragma once

#include <glog/logging.h>

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <string>
#include <utility>

namespace mooncake {

// Tracks in-flight work for graceful drain. Guard is thread-agnostic (safe to
// destroy after co_await on another executor).
// Drain protocol: Close() then Wait(). Wait() alone does not reject new Enter().
class InflightTracker {
   public:
    explicit InflightTracker(std::string name,
                             std::function<void()> on_entering = nullptr,
                             std::function<void()> on_leaving = nullptr)
        : name_(std::move(name)),
          on_entering_(std::move(on_entering)),
          on_leaving_(std::move(on_leaving)) {}

    // RAII guard for one in-flight operation.
    class Guard {
       public:
        explicit Guard(InflightTracker* tracker) {
            if (tracker->Admit()) {
                tracker_ = tracker;
            }
        }

        Guard(Guard&& other) noexcept : tracker_(other.tracker_) {
            other.tracker_ = nullptr;
        }
        Guard& operator=(Guard&& other) noexcept {
            if (this != &other) {
                if (tracker_) tracker_->Retire();
                tracker_ = other.tracker_;
                other.tracker_ = nullptr;
            }
            return *this;
        }

        Guard(const Guard&) = delete;
        Guard& operator=(const Guard&) = delete;

        ~Guard() {
            if (tracker_) {
                tracker_->Retire();
            }
        }
        bool is_valid() const { return tracker_ != nullptr; }

       private:
        InflightTracker* tracker_ = nullptr;  // non-null iff admitted
    };

   public:
    Guard Enter() { return Guard(this); }

    // Reject further Enter(); returns whether this call flipped running→false.
    bool Close() { return running_.exchange(false, std::memory_order_acq_rel); }

    // Block until inflight_ == 0. Does not stop new Enter() — call Close()
    // first, or Wait() may never return while callers keep entering.
    void Wait() {
        LOG(INFO) << name_ << ": draining, in-flight="
                  << inflight_.load(std::memory_order_acquire);
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] {
            return inflight_.load(std::memory_order_acquire) == 0;
        });
        LOG(INFO) << name_ << ": drained";
    }

    bool is_running() const { return running_.load(std::memory_order_acquire); }
    int inflight() const { return inflight_.load(std::memory_order_acquire); }

   private:
    bool Admit() {
        if (!running_.load(std::memory_order_acquire)) return false;
        inflight_.fetch_add(1, std::memory_order_acq_rel);
        // Close() may have raced after the first check; undo and reject.
        if (!running_.load(std::memory_order_acquire)) {
            NotifyIfDrained(
                inflight_.fetch_sub(1, std::memory_order_acq_rel) == 1);
            return false;
        }
        if (on_entering_) on_entering_();
        return true;
    }

    void Retire() {
        if (on_leaving_) on_leaving_();
        NotifyIfDrained(inflight_.fetch_sub(1, std::memory_order_acq_rel) == 1);
    }

    void NotifyIfDrained(bool became_zero) {
        if (!became_zero) return;
        std::lock_guard<std::mutex> lock(mu_);
        cv_.notify_all();
    }

    std::string name_;

    // on_entering / on_leaving:
    // 1. fired when an admitted operation enters / leaves;
    // 2. drive an in-flight gauge. Either may be null.
    std::function<void()> on_entering_;
    std::function<void()> on_leaving_;
    std::atomic<bool> running_{true};
    std::atomic<int> inflight_{0};
    std::mutex mu_;
    std::condition_variable cv_;
};

}  // namespace mooncake
