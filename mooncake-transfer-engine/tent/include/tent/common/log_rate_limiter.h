// Copyright 2024 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include "tent/common/utils/os.h"

namespace mooncake {
namespace tent {

// Simple rate limiter for logging to avoid log spam
// Usage:
//   thread_local LogRateLimiter limiter(1000000000ull); // 1 second
//   if (limiter.shouldLog()) {
//       LOG(INFO) << "This will be logged at most once per second";
//   }
class LogRateLimiter {
public:
    explicit LogRateLimiter(uint64_t interval_ns)
        : interval_ns_(interval_ns), last_log_ts_(0) {}

    // Check if logging is allowed based on time interval
    bool shouldLog() {
        uint64_t current_ts = getCurrentTimeInNano();
        if (current_ts < last_log_ts_ || (current_ts - last_log_ts_ >= interval_ns_)) {
            last_log_ts_ = current_ts;
            return true;
        }
        return false;
    }

    // Reset the timer (e.g., after a successful operation)
    void reset() { last_log_ts_ = 0; }

    // Get the configured interval
    uint64_t interval() const { return interval_ns_; }

private:
    const uint64_t interval_ns_;
    uint64_t last_log_ts_;
};

// Convenience macros for common time intervals
#define LOG_RATE_LIMIT_1S 1000000000ull   // 1 second
#define LOG_RATE_LIMIT_2S 2000000000ull   // 2 seconds
#define LOG_RATE_LIMIT_5S 5000000000ull   // 5 seconds
#define LOG_RATE_LIMIT_10S 10000000000ull // 10 seconds

}  // namespace tent
}  // namespace mooncake
