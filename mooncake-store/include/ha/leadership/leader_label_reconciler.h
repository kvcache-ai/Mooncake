#pragma once

#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>

#include <glog/logging.h>

#include "types.h"

namespace mooncake {
namespace ha {

// Drives a single boolean "leader label" toward a desired state on a background
// thread. Callers only flip the desired flag (non-blocking); the worker runs
// the synchronous apply and retries until the actual state converges, so a
// transient failure cannot leave a stale leader label on a pod that is no
// longer serving.
class LeaderLabelReconciler {
   public:
    // Applies the label mutation: true sets the leader label, false clears it.
    // Must return ErrorCode::OK only when the cluster state reflects `desired`.
    using ApplyFn = std::function<ErrorCode(bool desired)>;

    LeaderLabelReconciler(bool enabled, ApplyFn apply,
                          std::chrono::milliseconds retry_interval)
        : enabled_(enabled),
          apply_(std::move(apply)),
          retry_interval_(retry_interval) {
        if (enabled_) {
            worker_ = std::thread([this] { Run(); });
        }
    }

    ~LeaderLabelReconciler() {
        if (!enabled_) return;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_ = true;
        }
        cv_.notify_all();
        if (worker_.joinable()) worker_.join();
    }

    LeaderLabelReconciler(const LeaderLabelReconciler&) = delete;
    LeaderLabelReconciler& operator=(const LeaderLabelReconciler&) = delete;

    void SetLeader(bool desired) {
        if (!enabled_) return;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            desired_leader_ = desired;
        }
        cv_.notify_all();
    }

   private:
    void Run() {
        std::optional<bool> applied;
        std::unique_lock<std::mutex> lock(mutex_);
        while (true) {
            cv_.wait(lock, [&] { return stop_ || applied != desired_leader_; });
            if (stop_) return;
            bool target = desired_leader_;
            lock.unlock();

            ErrorCode err = apply_(target);

            lock.lock();
            if (err == ErrorCode::OK) {
                applied = target;
                continue;
            }
            LOG(WARNING) << "Failed to " << (target ? "set" : "clear")
                         << " leader label: " << toString(err);
            cv_.wait_for(lock, retry_interval_,
                         [&] { return stop_ || desired_leader_ != target; });
            if (stop_) return;
        }
    }

    const bool enabled_;
    const ApplyFn apply_;
    const std::chrono::milliseconds retry_interval_;

    std::mutex mutex_;
    std::condition_variable cv_;
    bool desired_leader_ = false;
    bool stop_ = false;
    std::thread worker_;
};

}  // namespace ha
}  // namespace mooncake
