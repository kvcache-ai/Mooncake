#pragma once

#include <glog/logging.h>

#include <atomic>
#include <functional>
#include <mutex>
#include <shared_mutex>
#include <string>

namespace mooncake {

// Tracks in-flight operations and supports graceful draining via a rw lock.
class InflightTracker {
   public:
    // on_entering / on_leaving: fired when an admitted operation enters / leaves;
    // drive an in-flight gauge. Either may be null.
    explicit InflightTracker(std::string name,
                             std::function<void()> on_entering = nullptr,
                             std::function<void()> on_leaving = nullptr)
        : name_(std::move(name)),
          on_entering_(std::move(on_entering)),
          on_leaving_(std::move(on_leaving)) {}

    // RAII guard for one in-flight operation; holds the read lock while alive.
    class Guard {
       public:
        explicit Guard(InflightTracker* tracker) : lock_(tracker->rwlock_) {
            if (tracker->Admit()) {
                tracker_ = tracker;
            }
        }
        ~Guard() {
            if (tracker_) {
                tracker_->Retire();
            }
        }
        bool is_valid() const { return tracker_ != nullptr; }

       private:
        std::shared_lock<std::shared_mutex> lock_;
        InflightTracker* tracker_ = nullptr;  // non-null iff admitted
    };

   public:
    Guard Enter() { return Guard(this); }

    // Stop admitting new operations
    bool Close() { return running_.exchange(false, std::memory_order_acq_rel); }

    // Block until all in-flight operations have finished. Pure wait — does not
    // change the running state. Call Close() first so new operations stop
    // arriving, otherwise this may not converge.
    void Wait() {
        LOG(INFO) << name_ << ": draining, in-flight="
                  << inflight_.load(std::memory_order_acquire);
        std::lock_guard<std::shared_mutex> wait_for_inflight(rwlock_);
        LOG(INFO) << name_ << ": drained";
    }

    bool is_running() const { return running_.load(std::memory_order_acquire); }
    int inflight() const { return inflight_.load(std::memory_order_acquire); }

   private:
    // Admit() and Retire() run while the Guard holds the read lock.
    bool Admit() {
        if (!running_.load(std::memory_order_acquire)) return false;
        inflight_.fetch_add(1, std::memory_order_acq_rel);
        if (on_entering_) on_entering_();
        return true;
    }
    void Retire() {
        if (on_leaving_) on_leaving_();
        inflight_.fetch_sub(1, std::memory_order_acq_rel);
    }

    std::string name_;
    std::function<void()> on_entering_;
    std::function<void()> on_leaving_;
    std::atomic<bool> running_{true};
    std::atomic<int> inflight_{0};
    std::shared_mutex rwlock_;
};

}  // namespace mooncake
