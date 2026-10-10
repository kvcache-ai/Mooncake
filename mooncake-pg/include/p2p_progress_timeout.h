#ifndef MOONCAKE_P2P_PROGRESS_TIMEOUT_H
#define MOONCAKE_P2P_PROGRESS_TIMEOUT_H

#include <chrono>

namespace mooncake {

class P2PProgressTimeout {
   public:
    using Clock = std::chrono::steady_clock;

    explicit P2PProgressTimeout(Clock::time_point last_progress = Clock::now())
        : last_progress_(last_progress) {}

    void markProgress(Clock::time_point now = Clock::now()) {
        last_progress_ = now;
    }

    template <typename Rep, typename Period>
    bool expired(std::chrono::duration<Rep, Period> timeout,
                 Clock::time_point now = Clock::now()) const {
        return now - last_progress_ > timeout;
    }

   private:
    Clock::time_point last_progress_;
};

}  // namespace mooncake

#endif
