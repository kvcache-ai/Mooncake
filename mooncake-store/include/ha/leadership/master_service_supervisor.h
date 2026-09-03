#pragma once

#include <atomic>
#include <mutex>
#include <utility>

#include "master_config.h"

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
        const bool first_request =
            !shutdown_requested_.exchange(true, std::memory_order_acq_rel);
        if (!first_request) {
            return false;
        }
        std::lock_guard<std::mutex> lock(transition_mutex_);
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

class MasterServiceSupervisor {
   public:
    explicit MasterServiceSupervisor(
        const MasterServiceSupervisorConfig& config);

    int Start();

   private:
    MasterServiceSupervisorConfig config_;
};

}  // namespace ha
}  // namespace mooncake
