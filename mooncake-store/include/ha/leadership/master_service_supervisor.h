#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <utility>

#include "master_config.h"
#include "shutdown_request.h"

namespace mooncake {
namespace ha {

namespace detail {

class ServingStateGate {
   public:
    template <typename Action>
    bool RunIfActive(Action&& action) {
        std::lock_guard<std::mutex> lock(transition_mutex_);
        if (shutdown_requested_.load(std::memory_order_acquire)) {
            return false;
        }
        std::forward<Action>(action)();
        return true;
    }

    template <typename Action>
    bool RequestShutdown(Action&& action) {
        std::lock_guard<std::mutex> lock(transition_mutex_);
        const bool first_request =
            !shutdown_requested_.exchange(true, std::memory_order_acq_rel);
        if (!first_request) {
            return false;
        }
        std::forward<Action>(action)();
        return true;
    }

    bool IsActive() const {
        return !shutdown_requested_.load(std::memory_order_acquire);
    }

   private:
    std::atomic<bool> shutdown_requested_{false};
    std::mutex transition_mutex_;
};

}  // namespace detail

// Bound on an HA shutdown, from the first observation of the request to
// process exit. main() arms the shutdown watchdog with it.
inline constexpr std::chrono::seconds kHaShutdownDeadline{15};

class MasterServiceSupervisor {
   public:
    // shutdown_requested defaults to the process signal flag; tests inject
    // their own predicate. The predicate can run concurrently on the
    // supervisor and trigger threads, so it must be thread-safe and must not
    // throw.
    explicit MasterServiceSupervisor(
        const MasterServiceSupervisorConfig& config,
        std::function<bool()> shutdown_requested = ShutdownRequested);

    int Start();

   private:
    MasterServiceSupervisorConfig config_;
    std::function<bool()> shutdown_requested_;
};

}  // namespace ha
}  // namespace mooncake
