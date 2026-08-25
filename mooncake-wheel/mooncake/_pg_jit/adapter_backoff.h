#ifndef MOONCAKE_PG_ADAPTER_BACKOFF_H
#define MOONCAKE_PG_ADAPTER_BACKOFF_H

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <thread>

namespace mooncake {

inline void adapterPause() noexcept {
#if defined(__x86_64__) || defined(__i386__)
    __asm__ __volatile__("pause" ::: "memory");
#elif defined(__aarch64__)
    __asm__ __volatile__("yield" ::: "memory");
#endif
}

class AdapterBackoffWaiter final {
   public:
    explicit AdapterBackoffWaiter(std::chrono::microseconds initial_sleep,
                                  std::chrono::microseconds max_sleep)
        : initial_sleep_(initial_sleep),
          max_sleep_(max_sleep),
          current_sleep_(initial_sleep) {}

    template <typename Predicate, typename Rep, typename Period>
    bool wait_for(std::chrono::duration<Rep, Period> timeout,
                  Predicate predicate) {
        auto start = std::chrono::steady_clock::now();
        uint32_t spin_count = 0;
        while (!predicate()) {
            if (std::chrono::steady_clock::now() - start > timeout) {
                return false;
            }
            if (spin_count++ < 200) {
                adapterPause();
            } else {
                std::this_thread::sleep_for(current_sleep_);
                current_sleep_ = std::min(current_sleep_ * 2, max_sleep_);
            }
        }
        return true;
    }

   private:
    std::chrono::microseconds initial_sleep_;
    std::chrono::microseconds max_sleep_;
    std::chrono::microseconds current_sleep_;
};

}  // namespace mooncake

#endif  // MOONCAKE_PG_ADAPTER_BACKOFF_H
