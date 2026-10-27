/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <map>
#include <string>
#include <thread>

#include "Time.h"

namespace facebook {
namespace cachelib {
namespace util {

class Throttler {
 public:
  // Throttler is used to do certain amount of work before yielding. We call
  // throttle every time we work() and throttler will sleep if we work() too
  // much.  To do this, we have two options. 1) sleepMs 2) workMs.  SleepMs
  // indicates the time we yield when we decide to be throttled.  WorkMs
  // indicates the time we run un-throttled.

  // The callback to be called when the time is checked for throttling
  using ThrottleCb = std::function<void(std::chrono::milliseconds curTime)>;

  // this config indicates that we sleep for sleepMs time every
  // workMs time at least.
  struct Config {
    // time we yield when throttled
    uint64_t sleepMs = 10;

    // time period of uninterrupted work.
    uint64_t workMs = 5;

    // return true if the config indicates we need to run throttling logic.
    bool needsThrottling() const noexcept { return sleepMs != 0; }

    static Config makeNoThrottleConfig() {
      // setting to 0 on sleepMs means we dont need to throttle.
      return Config{.sleepMs = 0, .workMs = 0};
    }

    std::map<std::string, std::string> serialize() const {
      std::map<std::string, std::string> configMap;
      configMap["sleepMs"] = std::to_string(sleepMs);
      configMap["workMs"] = std::to_string(workMs);
      return configMap;
    }
  };

  // returns true if throttled, false otherwise
  bool throttle() {
    if (!config_.needsThrottling() || ++counter_ % kSpinLimit) {
      return false;
    }

    uint64_t curr = util::getCurrentTimeMs();
    if (throttleCb_) {
      throttleCb_(std::chrono::milliseconds(curr));
    }
    if (curr - currWorkStartMs_ > config_.workMs) {
      /* sleep override */
      std::this_thread::sleep_for(std::chrono::milliseconds(config_.sleepMs));
      // start the time period when we don't throttle
      currWorkStartMs_ = util::getCurrentTimeMs();
      ++throttleCounter_;
      return true;
    }
    return false;
  }

  uint64_t numThrottles() const noexcept { return throttleCounter_; }

  explicit Throttler(Config config, ThrottleCb&& throttleCb = nullptr)
      : config_(std::move(config)),
        currWorkStartMs_(util::getCurrentTimeMs()),
        throttleCb_(std::move(throttleCb)) {}
  explicit Throttler() : Throttler(Config{}) {}

 private:
  // number of spins before we attempt to call time
  static constexpr const uint64_t kSpinLimit = 1024;

  Config config_;
  uint64_t currWorkStartMs_;    // time when we started to not throttle
  uint64_t counter_{0};         // counter to track the calls.
  uint64_t throttleCounter_{0}; // number of times we've throttled
  ThrottleCb throttleCb_;
};

} // namespace util
} // namespace cachelib
} // namespace facebook
