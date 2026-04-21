/*
 * Copyright (c) 2025, Alibaba Group Holding Limited;
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

#include <infiniband/verbs.h>
#include <sys/types.h>

#include <atomic>
#include <cerrno>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <new>
#include <stdexcept>
#include <system_error>

#include "async_simple/coro/Lazy.h"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/detail/client_queue.hpp"
#include "ylt/easylog.hpp"

namespace coro_io {

class ib_buffer_pool_t;

class ib_device_t;

struct ib_deleter {
  void operator()(ibv_pd* pd) const noexcept {
    if (pd) {
      auto ret = ibv_dealloc_pd(pd);
      if (ret != 0) {
        ELOG_ERROR << "ibv_dealloc_pd failed: "
                   << std::make_error_code(std::errc{ret}).message();
      }
    }
  }
  void operator()(ibv_context* context) const noexcept {
    if (context) {
      auto ret = ibv_close_device(context);
      if (ret != 0) {
        ELOG_ERROR << "ibv_close_device failed "
                   << std::make_error_code(std::errc{ret}).message();
      }
    }
  }
  void operator()(ibv_cq* cq) const noexcept {
    if (cq) {
      auto ret = ibv_destroy_cq(cq);
      if (ret != 0) {
        ELOG_ERROR << "ibv_destroy_cq failed "
                   << std::make_error_code(std::errc{ret}).message();
      }
    }
  }
  void operator()(ibv_qp* qp) const noexcept {
    if (qp) {
      auto ret = ibv_destroy_qp(qp);
      if (ret != 0) {
        ELOG_ERROR << "ibv_destroy_qp failed "
                   << std::make_error_code(std::errc{ret}).message();
      }
    }
  }
  void operator()(ibv_comp_channel* channel) const noexcept {
    if (channel) {
      auto ret = ibv_destroy_comp_channel(channel);
      if (ret != 0) {
        ELOG_ERROR << "ibv_destroy_comp_channel failed "
                   << std::make_error_code(std::errc{ret}).message();
      }
    }
  }
  void operator()(ibv_mr* ptr) const noexcept {
    if (ptr) {
      if (auto ret = ibv_dereg_mr(ptr); ret) [[unlikely]] {
        ELOG_ERROR << "ibv_dereg_mr failed: "
                   << std::make_error_code(std::errc{ret}).message();
      }
    }
  }
};

struct ib_buffer_t {
 private:
  std::unique_ptr<ibv_mr, ib_deleter> mr_;
  std::weak_ptr<ib_buffer_pool_t> owner_pool_;
  std::unique_ptr<char[]> memory_owner_;
  ib_buffer_t(std::unique_ptr<ibv_mr, ib_deleter> mr,
              std::unique_ptr<char[]> memory_owner,
              ib_buffer_pool_t& owner_pool) noexcept;
  void release_resource();

 public:
  ib_buffer_t() = default;
  friend class ib_buffer_pool_t;
  ibv_sge subview(std::size_t start = 0) const {
    return ibv_sge{.addr = (uintptr_t)mr_->addr + start,
                   .length = (uint32_t)(mr_->length - start),
                   .lkey = mr_->lkey};
  }
  ibv_sge subview(std::size_t start, uint32_t length) const {
    return ibv_sge{.addr = (uintptr_t)mr_->addr + start,
                   .length = length,
                   .lkey = mr_->lkey};
  }
  ibv_mr* operator->() noexcept { return mr_.get(); }
  ibv_mr* operator->() const noexcept { return mr_.get(); }
  ibv_mr& operator*() noexcept { return *mr_.get(); }
  ibv_mr& operator*() const noexcept { return *mr_.get(); }
  operator bool() const noexcept { return mr_.get() != nullptr; }
  static std::unique_ptr<ibv_mr, ib_deleter> regist(
      ib_device_t& dev, void* ptr, uint32_t size,
      int ib_flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ |
                     IBV_ACCESS_REMOTE_WRITE);
  static ib_buffer_t regist(ib_buffer_pool_t& pool,
                            std::unique_ptr<char[]> data, std::size_t size,
                            int ib_flags = IBV_ACCESS_LOCAL_WRITE |
                                           IBV_ACCESS_REMOTE_READ |
                                           IBV_ACCESS_REMOTE_WRITE);
  ib_buffer_t(ib_buffer_t&&) = default;
  ib_buffer_t& operator=(ib_buffer_t&&);
  ~ib_buffer_t();
};

class ib_buffer_pool_t : public std::enable_shared_from_this<ib_buffer_pool_t> {
  struct ib_buffer_mem_control_t {
    std::atomic<std::size_t> now_usage;
    std::atomic<std::size_t> history_max_usage;
  };

 private:
  static std::shared_ptr<ib_buffer_mem_control_t> g_memory_usage_recorder() {
    static auto used_memory = std::make_shared<ib_buffer_mem_control_t>();
    return used_memory;
  }

  struct ib_buffer_impl_t {
    std::unique_ptr<ibv_mr, ib_deleter> mr_;
    std::unique_ptr<char[]> memory_owner_;
    ib_buffer_t convert_to_ib_buffer(ib_buffer_pool_t& pool) && {
      return ib_buffer_t{std::move(mr_), std::move(memory_owner_), pool};
    }
    ib_buffer_impl_t& operator=(ib_buffer_impl_t&& o) noexcept = default;
    ib_buffer_impl_t(ib_buffer_impl_t&& o) noexcept = default;
    ib_buffer_impl_t() noexcept = default;
    ib_buffer_impl_t(std::unique_ptr<ibv_mr, ib_deleter>&& mr,
                     std::unique_ptr<char[]>&& memory_owner) noexcept
        : mr_(std::move(mr)), memory_owner_(std::move(memory_owner)) {}
  };
  struct private_construct_token {};
  static async_simple::coro::Lazy<void> collect_idle_timeout_client(
      std::weak_ptr<ib_buffer_pool_t> self_weak,
      std::chrono::milliseconds sleep_time) {
    std::shared_ptr<ib_buffer_pool_t> self = self_weak.lock();
    if (self == nullptr) {
      co_return;
    }
    while (true) {
      self->free_buffers_.reselect();
      self = nullptr;
      co_await coro_io::sleep_for(sleep_time);
      if ((self = self_weak.lock()) == nullptr) {
        break;
      }
      while (true) {
        ELOG_TRACE << "start ib_buffer timeout free of pool{" << self.get()
                   << "}, now ib_buffer count: " << self->free_buffers_.size();
        auto [is_all_cleared, clear_cnt] = self->free_buffers_.clear_old(1000);
        self->modify_memory_usage(-1 * (ssize_t)clear_cnt *
                                  (ssize_t)self->buffer_size());
        ELOG_INFO << "finish ib_buffer timeout free of pool{" << self.get()
                  << "}, now ib_buffer cnt: " << self->free_buffers_.size()
                  << " mem usage:"
                  << (int64_t)(std::round(self->memory_usage() /
                                          (1.0 * 1024 * 1024)))
                  << " MB";
        if (!is_all_cleared) {
          try {
            co_await async_simple::coro::Yield{};
          } catch (std::exception& e) {
            ELOG_ERROR << "unexcepted yield exception: " << e.what();
          }
        }
        else {
          break;
        }
      }
    }
    co_return;
  }
  void enqueue(ib_buffer_t& buffer) {
    auto impl = std::make_unique<ib_buffer_impl_t>(
        std::move(buffer.mr_), std::move(buffer.memory_owner_));
    if (free_buffers_.enqueue(std::move(impl)) == 1) {
      std::size_t expected = 0;
      if (free_buffers_.collecter_cnt_.compare_exchange_strong(expected, 1))
          [[unlikely]] {
        ELOG_TRACE << "start timeout client collecter of client_pool{" << this
                   << "}";
        collect_idle_timeout_client(this->weak_from_this(),
                                    (std::max)(pool_config_.idle_timeout,
                                               std::chrono::milliseconds{50}))
            .directlyStart(
                [](auto&&) {
                },
                coro_io::get_global_executor());
      }
    }
  }
  void collect_free(ib_buffer_t& buffer) {
    if (buffer) {
      if (!memory_out_of_limit()) {
        ELOG_TRACE << "collect free buffer{data:" << buffer->addr << ",len"
                   << buffer->length << "} enqueue";
        enqueue(buffer);
      }
      else {
        ELOG_TRACE << "out of max buffer pool limit " << max_memory_usage()
                   << "now usage:" << buffer_size()
                   << ",buffer{data:" << buffer->addr << ",len"
                   << buffer->length << "} wont be collect";
      }
    }
    return;
  };

 public:
  static std::size_t global_memory_usage() {
    return g_memory_usage_recorder()->now_usage.load(std::memory_order_acquire);
  }
  static std::size_t global_history_max_memory_usage() {
    return g_memory_usage_recorder()->history_max_usage.load(
        std::memory_order_acquire);
  }

  struct config_t {
    size_t buffer_size = 256 * 1024;  // 256KB
    uint64_t max_memory_usage = UINT32_MAX;
    std::shared_ptr<ib_buffer_mem_control_t> memory_usage_recorder =
        nullptr;  // nullopt means use global memory_usage_recorder
    std::chrono::milliseconds idle_timeout = std::chrono::milliseconds{5000};
  };
  std::size_t max_memory_usage() { return pool_config_.max_memory_usage; }

 private:
  static std::shared_ptr<ib_buffer_pool_t> create(ib_device_t& device,
                                                  const config_t& pool_config) {
    return std::make_shared<ib_buffer_pool_t>(private_construct_token{}, device,
                                              pool_config);
  }

 public:
  friend struct ib_buffer_t;
  friend struct ib_device_t;
  std::size_t buffer_size() const noexcept {
    return this->pool_config_.buffer_size;
  }
  void modify_memory_usage(ssize_t count) {
    auto ret = memory_usage_recorder_->now_usage.fetch_add(
        count, std::memory_order_release);
    auto old = memory_usage_recorder_->history_max_usage.load(
        std::memory_order_acquire);
    while (ret + count > old) [[likely]] {
      if (memory_usage_recorder_->history_max_usage.compare_exchange_strong(
              old, ret + count, std::memory_order_acq_rel)) [[likely]] {
        break;
      }
    }
    return;
  }
  bool memory_out_of_limit() {
    return max_memory_usage() < buffer_size() + memory_usage();
  }
  ib_buffer_t get_buffer() {
    std::unique_ptr<ib_buffer_impl_t> buffer;
    ib_buffer_t ib_buffer;
    free_buffers_.try_dequeue(buffer);
    if (!buffer) {
      ELOG_TRACE
          << "There is no free buffer. Allocate and regist new buffer now";
      if (memory_out_of_limit()) [[unlikely]] {
        ELOG_WARN << "Memory out of pool limit";
        return ib_buffer_t{};
      }
      std::unique_ptr<char[]> data;
      data.reset(new char[buffer_size()]);
      ib_buffer = ib_buffer_t::regist(*this, std::move(data), buffer_size());
      if (!ib_buffer) {
        ELOG_ERROR << "regist buffer failed";
        return ib_buffer_t{};
      }
    }
    else {
      ib_buffer = std::move(*buffer).convert_to_ib_buffer(*this);
    }
    ELOG_TRACE << "get buffer{data:" << ib_buffer->addr << ",len"
               << ib_buffer->length << "} from queue";
    return ib_buffer;
  }
  ib_buffer_pool_t(private_construct_token, ib_device_t& device,
                   const config_t& pool_config)
      : device_(device),
        memory_usage_recorder_(pool_config.memory_usage_recorder
                                   ? pool_config.memory_usage_recorder
                                   : g_memory_usage_recorder()),
        pool_config_(std::move(pool_config)) {}
  std::size_t memory_usage() const noexcept {
    return memory_usage_recorder_->now_usage.load(std::memory_order_relaxed);
  }
  std::size_t max_recorded_memory_usage() const noexcept {
    return memory_usage_recorder_->history_max_usage.load(
        std::memory_order_relaxed);
  }
  std::size_t free_buffer_size() const noexcept { return free_buffers_.size(); }

 private:
  coro_io::detail::client_queue<std::unique_ptr<ib_buffer_impl_t>>
      free_buffers_;
  std::shared_ptr<ib_buffer_mem_control_t> memory_usage_recorder_;
  ib_device_t& device_;
  config_t pool_config_;
};

inline ib_buffer_t::ib_buffer_t(std::unique_ptr<ibv_mr, ib_deleter> mr,
                                std::unique_ptr<char[]> memory_owner,
                                ib_buffer_pool_t& owner_pool) noexcept
    : mr_(std::move(mr)),
      memory_owner_(std::move(memory_owner)),
      owner_pool_(owner_pool.weak_from_this()) {}

inline ib_buffer_t& ib_buffer_t::operator=(ib_buffer_t&& o) {
  release_resource();
  mr_ = std::move(o.mr_);
  owner_pool_ = std::move(o.owner_pool_);
  memory_owner_ = std::move(o.memory_owner_);
  return *this;
}

inline ib_buffer_t::~ib_buffer_t() { release_resource(); }

inline void ib_buffer_t::release_resource() {
  if (mr_) {
    if (auto ptr = owner_pool_.lock(); ptr) {
      ptr->collect_free(*this);
      // if buffer not collect by pool, we need dec total memory here.
      if (mr_) [[unlikely]] {
        ptr->modify_memory_usage(-1 * (ssize_t)mr_->length);
      }
    }
  }
}

}  // namespace coro_io