/*
 * Copyright (c) 2023, Alibaba Group Holding Limited;
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

#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/SpinLock.h>
#include <async_simple/coro/SyncAwait.h>

#include <algorithm>
#include <chrono>
#include <iostream>
#include <ylt/coro_io/coro_io.hpp>
#include <ylt/easylog.hpp>

namespace coro_io {
class rate_limiter {
 public:
  async_simple::coro::Lazy<std::chrono::milliseconds> acquire(int permits) {
    std::chrono::milliseconds wait_mills;
    {
      auto scope = co_await this->lock_.coScopedLock();
      wait_mills = reserve_and_get_wait_length(permits, current_time_mills());
    }
    co_await coro_io::sleep_for(wait_mills);
    co_return wait_mills;
  }
  async_simple::coro::Lazy<void> set_rate(double permitsPerSecond) {
    auto scope = co_await this->lock_.coScopedLock();
    do_set_rate(permitsPerSecond, current_time_mills());
  }

  virtual ~rate_limiter() {}

 protected:
  virtual void do_set_rate(
      double permitsPerSecond,
      std::chrono::steady_clock::time_point now_micros) = 0;
  virtual std::chrono::steady_clock::time_point reserve_earliest_available(
      int permits, std::chrono::steady_clock::time_point now_micros) = 0;
  std::chrono::steady_clock::time_point current_time_mills() {
    return std::chrono::steady_clock::now();
  }

 private:
  std::chrono::milliseconds reserve_and_get_wait_length(
      int permits, std::chrono::steady_clock::time_point now_micros) {
    std::chrono::steady_clock::time_point moment_available =
        reserve_earliest_available(permits, now_micros);
    std::chrono::milliseconds diff_mills =
        std::chrono::duration_cast<std::chrono::milliseconds>(moment_available -
                                                              now_micros);
    return std::max(diff_mills, std::chrono::milliseconds(0));
  }

  async_simple::coro::SpinLock lock_;
};

class abstract_smooth_rate_limiter : public rate_limiter {
 public:
  virtual ~abstract_smooth_rate_limiter() {}

 protected:
  virtual void do_set_rate(double permits_per_second,
                           double stable_internal_micros) = 0;
  virtual std::chrono::milliseconds stored_permits_to_wait_time(
      double stored_permits, double permits_to_take) = 0;
  virtual double cool_down_internal_micros() = 0;
  void resync(std::chrono::steady_clock::time_point now_micros) {
    // if next_free_ticket is in the past, resync to now
    ELOG_DEBUG << "now micros: "
               << std::chrono::duration_cast<std::chrono::milliseconds>(
                      now_micros.time_since_epoch())
                      .count()
               << ", next_free_ticket_micros_: "
               << std::chrono::duration_cast<std::chrono::milliseconds>(
                      this->next_free_ticket_micros_.time_since_epoch())
                      .count();
    if (now_micros > this->next_free_ticket_micros_) {
      std::chrono::milliseconds diff_mills =
          std::chrono::duration_cast<std::chrono::milliseconds>(
              now_micros - this->next_free_ticket_micros_);
      double newPermits = diff_mills.count() / cool_down_internal_micros();
      this->stored_permits_ =
          std::min(this->max_permits_, this->stored_permits_ + newPermits);
      this->next_free_ticket_micros_ = now_micros;
    }
  }
  void do_set_rate(double permits_per_second,
                   std::chrono::steady_clock::time_point now_micros) override {
    resync(now_micros);
    double stable_internal_micros = 1000 / permits_per_second;
    this->stable_internal_micros_ = stable_internal_micros;
    do_set_rate(permits_per_second, stable_internal_micros);
  }
  std::chrono::steady_clock::time_point reserve_earliest_available(
      int required_permits,
      std::chrono::steady_clock::time_point now_micros) override {
    resync(now_micros);
    std::chrono::steady_clock::time_point return_value =
        this->next_free_ticket_micros_;
    double stored_permits_to_spend =
        std::min((double)required_permits, this->stored_permits_);
    double fresh_permits = required_permits - stored_permits_to_spend;
    std::chrono::milliseconds wait_micros =
        stored_permits_to_wait_time(this->stored_permits_,
                                    stored_permits_to_spend) +
        std::chrono::milliseconds(
            (int64_t)(fresh_permits * this->stable_internal_micros_));
    this->next_free_ticket_micros_ += wait_micros;
    this->stored_permits_ -= stored_permits_to_spend;
    return return_value;
  }

  /**
   * The currently stored permits.
   */
  double stored_permits_ = 0;
  /**
   * The maximum number of stored permits.
   */
  double max_permits_ = 0;
  /**
   * The interval between two unit requests, at our stable rate. E.g., a stable
   * rate of 5 permits per second has a stable internal of 200ms.
   */
  double stable_internal_micros_ = 0;
  /**
   * The time when the next request (no matter its size) will be granted. After
   * granting a request, this is pushed further in the future. Large requests
   * push this further than small requests.
   */
  std::chrono::steady_clock::time_point next_free_ticket_micros_;
};

class smooth_bursty_rate_limiter : public abstract_smooth_rate_limiter {
 public:
  smooth_bursty_rate_limiter(double permits_per_second) {
    this->max_burst_seconds_ = 1.0;
    async_simple::coro::syncAwait(set_rate(permits_per_second));
  }

 protected:
  void do_set_rate(double permits_per_second, double stable_internal_micros) {
    double old_max_permits = this->max_permits_;
    this->max_permits_ = this->max_burst_seconds_ * permits_per_second;
    this->stored_permits_ =
        (0 == old_max_permits)
            ? 0
            : this->stored_permits_ * this->max_permits_ / old_max_permits;
    ELOG_DEBUG << "max_permits_: " << this->max_permits_
               << ", stored_permits_:" << this->stored_permits_;
  }

  std::chrono::milliseconds stored_permits_to_wait_time(
      double stored_permits, double permits_to_take) {
    return std::chrono::milliseconds(0);
  }

  double cool_down_internal_micros() { return this->stable_internal_micros_; }

 private:
  /**
   * The work(permits) of how many seconds can be saved up if the rate_limiter
   * is unused.
   */
  double max_burst_seconds_ = 0;
};
}  // namespace coro_io