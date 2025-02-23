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
#include <ctime>
#include <stdexcept>

namespace facebook {
namespace cachelib {
namespace util {

// ::time is the fastest for getting the second granularity system clock
// through the vdso. This is faster than std::chrono::system_clock::now and
// counting it as seconds since epoch.
inline uint32_t getCurrentTimeSec() {
  // time in seconds since epoch will fit in 32 bit. We use this primarily for
  // storing in cache.
  return static_cast<uint32_t>(std::time(nullptr));
}

// For nano second granularity, std::chrono::steady_clock seems to do a fine
// job.
inline uint64_t getCurrentTimeMs() {
  auto ret = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::milliseconds>(ret).count();
}

inline uint64_t getCurrentTimeNs() {
  auto ret = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::nanoseconds>(ret).count();
}

inline uint32_t getSteadyCurrentTimeSec() {
  auto ret = std::chrono::steady_clock::now().time_since_epoch();
  return std::chrono::duration_cast<std::chrono::seconds>(ret).count();
}

class Timer {
  using steady_clock = std::chrono::steady_clock;
  class Finish {
   public:
    explicit Finish(Timer* t) : timer_(t) {}
    ~Finish() { timer_->pause(); }
    Timer* timer_;
  };

 public:
  steady_clock::duration getDuration() const { return duration_; }

  uint32_t getDurationSec() const {
    return std::chrono::duration_cast<std::chrono::seconds>(duration_).count();
  }

  uint64_t getDurationMs() const {
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration_)
        .count();
  }

  void startOrResume() {
    if (started_) {
      throw std::runtime_error("already stated");
    }
    started_ = true;
    start_ = steady_clock::now();
  }

  // automatically call pause() when out of scope
  Finish scopedStartOrResume() {
    startOrResume();
    return Finish{this};
  }

  steady_clock::duration pause() {
    if (!started_) {
      throw std::runtime_error("not stated yet");
    }
    started_ = false;
    auto d = steady_clock::now() - start_;
    duration_ += d;
    return d;
  }

 private:
  steady_clock::duration duration_ = steady_clock::duration::zero();
  steady_clock::time_point start_;
  bool started_{false};
};

} // namespace util
} // namespace cachelib
} // namespace facebook
