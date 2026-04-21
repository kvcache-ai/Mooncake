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
#include <netinet/in.h>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>
#include <system_error>
#include <unordered_map>

#include "asio/ip/address.hpp"
#include "asio/posix/stream_descriptor.hpp"
#include "ib_buffer.hpp"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/easylog.hpp"
#include "ylt/util/type_traits.h"

namespace coro_io {
namespace detail {
class ib_devices_t {
 public:
  static auto& instance() {
    static ib_devices_t inst{};
    return inst;
  }
  int size() { return size_; }
  ibv_device* operator[](int i) {
    if (i >= size_ || i < 0) {
      throw std::out_of_range("out of range");
    }
    return dev_list_[i];
  }

  ibv_device* at(const std::string& dev_name) {
    if (dev_name.empty()) {
      return (*this)[0];
    }

    auto list = get_devices();

    auto it = std::find_if(list.begin(), list.end(), [&dev_name](auto dev) {
      return dev->name == dev_name;
    });
    size_t i = std::distance(list.begin(), it);
    return (*this)[i];
  }

  std::span<ibv_device*> get_devices() { return {dev_list_, (size_t)size_}; }

 private:
  ib_devices_t() {
    dev_list_ = ibv_get_device_list(&size_);
    if (dev_list_ == nullptr) {
      size_ = 0;
    }
  }

  ~ib_devices_t() {
    if (dev_list_ != nullptr) {
      ibv_free_device_list(dev_list_);
    }
  }

  int size_;
  ibv_device** dev_list_;
};
inline std::string gid_to_string(uint8_t (&a)[16]) noexcept {
  std::string ret;
  ret.resize(40);
  sprintf(ret.data(),
          "%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%"
          "02x%02x",
          a[0], a[1], a[2], a[3], a[4], a[5], a[6], a[7], a[8], a[9], a[10],
          a[11], a[12], a[13], a[14], a[15]);
  return ret;
}

inline std::string mtu_str(ibv_mtu mtu) {
  std::string str;
  switch (mtu) {
    case IBV_MTU_256:
      str = "IBV_MTU_256";
      break;
    case IBV_MTU_512:
      str = "IBV_MTU_512";
      break;
    case IBV_MTU_1024:
      str = "IBV_MTU_1024";
      break;
    case IBV_MTU_2048:
      str = "IBV_MTU_2048";
      break;
    case IBV_MTU_4096:
      str = "IBV_MTU_4096";
      break;
    default:
      break;
  }
  return str;
}

/* helper function to print the content of the async event */
inline void print_async_event(struct ibv_context* ctx,
                              struct ibv_async_event* event) {
  switch (event->event_type) {
    /* QP events */
    case IBV_EVENT_QP_FATAL:
      ELOG_WARN << "QP fatal event for QP number:" << event->element.qp->qp_num;
      break;
    case IBV_EVENT_QP_REQ_ERR:
      ELOG_WARN << "QP Requestor error for QP number:"
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_QP_ACCESS_ERR:
      ELOG_WARN << "QP access error event for QP number:"
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_COMM_EST:
      ELOG_INFO << "QP communication established event for QP number:"
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_SQ_DRAINED:
      ELOG_INFO << "QP Send Queue drained event for QP number:"
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_PATH_MIG:
      ELOG_INFO << "QP Path migration loaded event for QP number:"
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_PATH_MIG_ERR:
      ELOG_WARN << "QP Path migration error event for QP number:"
                << event->element.qp->qp_num;
      break;
    case IBV_EVENT_QP_LAST_WQE_REACHED:
      ELOG_INFO << "QP last WQE reached event for QP number:"
                << event->element.qp->qp_num;
      break;

    /* CQ events */
    case IBV_EVENT_CQ_ERR:
      ELOG_WARN << "CQ error for CQ with handle " << event->element.cq;
      break;

    /* SRQ events */
    case IBV_EVENT_SRQ_ERR:
      ELOG_WARN << "SRQ error for SRQ with handle " << event->element.srq;
      break;
    case IBV_EVENT_SRQ_LIMIT_REACHED:
      ELOG_INFO << "SRQ limit reached event for SRQ with handle "
                << event->element.srq;
      break;

    /* Port events */
    case IBV_EVENT_PORT_ACTIVE:
      ELOG_INFO << "Port active event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_PORT_ERR:
      ELOG_WARN << "Port error event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_LID_CHANGE:
      ELOG_INFO << "LID change event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_PKEY_CHANGE:
      ELOG_INFO << "P_Key table change event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_GID_CHANGE:
      ELOG_INFO << "GID table change event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_SM_CHANGE:
      ELOG_INFO << "SM change event for port number "
                << event->element.port_num;
      break;
    case IBV_EVENT_CLIENT_REREGISTER:
      ELOG_INFO << "Client reregister event for port number "
                << event->element.port_num;
      break;

    /* RDMA device events */
    case IBV_EVENT_DEVICE_FATAL:
      ELOG_WARN << "Fatal error event for device "
                << ibv_get_device_name(ctx->device);
      break;

    default:
      ELOG_WARN << "Unknown event (" << event->event_type << ")";
  }
}

}  // namespace detail

class ib_device_t : public std::enable_shared_from_this<ib_device_t> {
 public:
  struct config_t {
    std::string dev_name;
    uint16_t port = 1;               // dev gid
    uint16_t gid_index = 0;          // dev gid_index
    bool use_best_gid_index = true;  // automatically find best gid index. If
                                     // failed, it will use gid_index.
    ib_buffer_pool_t::config_t buffer_pool_config;
  };
  static std::shared_ptr<ib_device_t> create(const config_t& conf,
                                             ibv_device* dev = nullptr) {
    auto ret =
        std::make_shared<ib_device_t>(private_construct_token{}, conf, dev);
    ret->start_async_event_watcher();
    return ret;
  }

 private:
  struct private_construct_token {};

  void poll_async_events() {
    ibv_async_event event;
    while (ibv_get_async_event(ctx_.get(), &event) == 0) {
      ELOG_INFO << "IBDevice(" << name()
                << ") get async event: " << event.event_type;
      detail::print_async_event(ctx_.get(), &event);
      ibv_ack_async_event(&event);
    }
  }

 public:
  ib_device_t(private_construct_token, const config_t& conf,
              ibv_device* dev = nullptr) {
    auto& inst = detail::ib_devices_t::instance();
    port_ = conf.port;
    if (dev == nullptr) {
      dev = inst.at(conf.dev_name);
    }
    name_ = ibv_get_device_name(dev);
    ctx_.reset(ibv_open_device(dev));
    pd_.reset(ibv_alloc_pd(ctx_.get()));

    if (auto ret = ibv_query_port(ctx_.get(), conf.port, &attr_); ret != 0) {
      auto ec = std::make_error_code((std::errc)ret);
      ELOG_ERROR << "IBDevice failed to query port " << conf.port
                 << " of device " << name_ << " error msg: " << ec.message();
      throw std::system_error(ec);
    }

    gid_index_ = conf.use_best_gid_index ? find_best_gid_index(conf.gid_index)
                                         : conf.gid_index;

    ELOG_INFO << name_ << " Active MTU: " << detail::mtu_str(attr_.active_mtu)
              << ", "
              << "Max MTU: " << detail::mtu_str(attr_.max_mtu)
              << ", gid index: " << gid_index_;

    if (gid_index_ >= 0) {
      if (auto ec = ibv_query_gid(ctx_.get(), conf.port, gid_index_, &gid_);
          ec) {
        auto err_code = std::make_error_code(std::errc{ec});
        ELOG_ERROR << "IBDevice failed to query port " << conf.port
                   << " of device " << name_ << " by gid_index:" << gid_index_
                   << ", error msg: " << err_code.message();
        throw std::system_error(err_code);
      }
      std::error_code err_code;
      gid_address_ =
          asio::ip::make_address(detail::gid_to_string(gid_.raw), err_code);
      if (err_code) {
        ELOG_ERROR << "IBDevice failed to convert gid to ip address of device "
                   << name_ << " by gid_index:" << gid_index_
                   << ", error msg: " << err_code.message();
        throw std::system_error(err_code);
      }
    }
    else {
      ELOG_ERROR << "gid index should greater than zero, now is: "
                 << gid_index_;
      throw std::invalid_argument{
          "gid index should greater than zero, now is: " +
          std::to_string(gid_index_)};
    }
    ibv_device_attr attr;
    auto pool_config = conf.buffer_pool_config;
    if (pool_config.max_memory_usage == 0) {
      pool_config.max_memory_usage = std::numeric_limits<uint64_t>::max();
    }
    if (ibv_query_device(ctx_.get(), &attr) == 0) {
      ELOG_INFO << "max mr size of device:" << attr.max_mr_size
                << ", user config max memory usage:"
                << pool_config.max_memory_usage
                << ". we will use min value as limit";
      pool_config.max_memory_usage =
          std::min<uint64_t>(attr.max_mr_size, pool_config.max_memory_usage);
    }
    else {
      ELOG_WARN
          << "query device info failed! We dont know the max_mr_size of device";
    }
    buffer_pool_ = ib_buffer_pool_t::create(*this, pool_config);
  }

  std::string_view name() const noexcept { return name_; }

  uint16_t port() const noexcept { return port_; }

  ibv_context* context() const noexcept { return ctx_.get(); }
  ibv_pd* pd() const noexcept { return pd_.get(); }
  int gid_index() const noexcept { return gid_index_; }
  const ibv_gid& gid() const noexcept { return gid_; }
  asio::ip::address gid_address() const noexcept { return gid_address_; }
  const ibv_port_attr& attr() const noexcept { return attr_; }

  bool is_support_inline_data() const noexcept { return support_inline_data_; }
  void set_support_inline_data(bool flag) noexcept {
    support_inline_data_ = flag;
  }

  std::shared_ptr<ib_buffer_pool_t> get_buffer_pool() const noexcept {
    return buffer_pool_;
  }

 private:
  int ipv6_addr_v4mapped(const struct in6_addr* a) {
    return ((a->s6_addr32[0] | a->s6_addr32[1]) |
            (a->s6_addr32[2] ^ htonl(0x0000ffff))) == 0UL ||
           /* IPv4 encoded multicast addresses */
           (a->s6_addr32[0] == htonl(0xff0e0000) &&
            ((a->s6_addr32[1] | (a->s6_addr32[2] ^ htonl(0x0000ffff))) == 0UL));
  }

  int find_best_gid_index(int default_gid_index) {
#ifndef YLT_IBVERBS_DONT_SUPPORT_FIND_GID_INDEX
    ibv_gid_entry gid_entry;
    for (int i = 0; i < attr_.gid_tbl_len; i++) {
      if (auto ret = ibv_query_gid_ex(ctx_.get(), port_, i, &gid_entry, 0)) {
        continue;
      }
      if ((ipv6_addr_v4mapped((struct in6_addr*)gid_entry.gid.raw) &&
           gid_entry.gid_type == IBV_GID_TYPE_ROCE_V2) ||
          gid_entry.gid_type == IBV_GID_TYPE_IB) {
        return i;
      }
    }
#endif
    ELOG_DEBUG << "selected best device failed, maybe the platform don't "
                  "support it. return default rdma device gid";
    return default_gid_index;
  }

  void start_async_event_watcher() {
    if (!ctx_) {
      return;
    }
    int flags = fcntl(ctx_->async_fd, F_GETFL);
    int ret = fcntl(ctx_->async_fd, F_SETFL, flags | O_NONBLOCK);
    if (ret < 0) {
      ELOG_ERROR
          << "Error, failed to change file descriptor of async event queue\n";
      return;
    }

    auto fd_deleter = [](asio::posix::stream_descriptor* fd) {
      fd->release();
      delete fd;
    };
    auto fd =
        std::unique_ptr<asio::posix::stream_descriptor, decltype(fd_deleter)>(
            new asio::posix::stream_descriptor(
                coro_io::get_global_executor()->get_asio_executor(),
                ctx_->async_fd));
    auto listen_event = [](std::weak_ptr<ib_device_t> dev,
                           auto fd) -> async_simple::coro::Lazy<void> {
      std::error_code ec;
      auto name = std::string{dev.lock()->name()};
      ELOG_INFO << "start_async_event_watcher of device:" << name << " start";
      while (!ec) {
        coro_io::callback_awaitor<std::error_code> awaitor;
        ec = co_await awaitor.await_resume([&fd](auto handler) {
          fd->async_wait(asio::posix::stream_descriptor::wait_read,
                         [handler](const std::error_code& ec) mutable {
                           handler.set_value_then_resume(ec);
                         });
        });

        if (!ec) {
          auto self = dev.lock();
          if (!self) {
            ELOG_DEBUG
                << "ib_device async event stop listening by close device:"
                << name;
            break;
          }
          self->poll_async_events();
        }
        else {
          ELOG_DEBUG << "ib_device async event stop listening by error:"
                     << ec.message() << ",device:" << name;
        }
      }
    };
    listen_event(shared_from_this(), std::move(fd)).start([](auto&& ec) {
    });
  }

  std::string name_;
  std::unique_ptr<ibv_context, ib_deleter> ctx_;
  std::unique_ptr<ibv_pd, ib_deleter> pd_;
  std::shared_ptr<ib_buffer_pool_t> buffer_pool_;
  std::atomic<bool> support_inline_data_ = true;
  ibv_port_attr attr_;
  ibv_gid gid_;
  asio::ip::address gid_address_;
  int gid_index_;

  uint16_t port_;
};

inline std::unique_ptr<ibv_mr, ib_deleter> ib_buffer_t::regist(ib_device_t& dev,
                                                               void* ptr,
                                                               uint32_t size,
                                                               int ib_flags) {
  auto mr = ibv_reg_mr(dev.pd(), ptr, size, ib_flags);
  ELOG_TRACE << "ibv_reg_mr regist: " << mr << " with pd:" << dev.pd();
  if (mr != nullptr) [[unlikely]] {
    ELOG_TRACE << "regist sge.lkey: " << mr->lkey << ", sge.addr: " << mr->addr
               << ", sge.length: " << mr->length;
    return std::unique_ptr<ibv_mr, ib_deleter>{mr};
  }
  else {
    ELOG_WARN << "regist memory failed! "
              << std::make_error_code(std::errc{errno}).message();
    return nullptr;
  }
};

inline ib_buffer_t ib_buffer_t::regist(ib_buffer_pool_t& pool,
                                       std::unique_ptr<char[]> data,
                                       std::size_t size, int ib_flags) {
  auto mr = ibv_reg_mr(pool.device_.pd(), data.get(), size, ib_flags);
  if (mr != nullptr) [[unlikely]] {
    ELOG_DEBUG << "ibv_reg_mr regist: " << mr
               << " with pd:" << pool.device_.pd();
    pool.modify_memory_usage(size);
    return ib_buffer_t{std::unique_ptr<ibv_mr, ib_deleter>{mr}, std::move(data),
                       pool};
  }
  else {
    std::error_code ec = std::make_error_code(std::errc{errno});
    ELOG_WARN << "allocate ibverbs memory region failed! " << ec.message();
    return ib_buffer_t{};
  }
};
class ib_device_manager_t {
  ib_device_manager_t(const ib_device_manager_t&) = delete;
  ib_device_manager_t& operator=(const ib_device_manager_t&) = delete;
  std::unordered_map<std::string, std::shared_ptr<ib_device_t>> device_map_;
  std::shared_ptr<ib_device_t> default_device_;

 public:
  std::unordered_map<std::string, std::shared_ptr<ib_device_t>>&
  get_dev_list() {
    return device_map_;
  }
  ib_device_manager_t(const ib_device_t::config_t& conf = {}) {
    auto& devices = detail::ib_devices_t::instance();
    for (auto& native_dev : devices.get_devices()) {
      try {
        auto dev = ib_device_t::create(conf, native_dev);
        auto [iter, is_ok] = device_map_.emplace(dev->name(), dev);
        if (is_ok && default_device_ == nullptr) {
          default_device_ = iter->second;
        }
      } catch (const std::system_error& error) {
        // discard the exception if ib_device_t construct failed
      }
    }
    if (!default_device_) {
      ELOG_ERROR << "get rdma device failed, there is no available device";
    }
  }
  std::shared_ptr<ib_device_t> find_device(std::string name) {
    if (!default_device_) [[unlikely]] {
      ELOG_ERROR << "get rdma device failed, there is no available device";
      return nullptr;
    }
    if (name.empty()) {
      return default_device_;
    }
    else {
      auto iter = device_map_.find(name);
      if (iter != device_map_.end()) {
        return iter->second;
      }
      else {
        ELOG_ERROR << "get device failed, cant found device:" << name;
        return nullptr;
      }
    }
  }
};

inline std::shared_ptr<ib_device_manager_t> g_ib_device_manager(
    const ib_device_t::config_t& conf = {}) {
  static auto dev_map = std::make_shared<ib_device_manager_t>(conf);
  return dev_map;
}

inline std::shared_ptr<ib_device_t> get_global_ib_device(
    const ib_device_t::config_t& conf = {}) {
  static auto dev_map = g_ib_device_manager(conf);
  return dev_map->find_device(conf.dev_name);
}

}  // namespace coro_io