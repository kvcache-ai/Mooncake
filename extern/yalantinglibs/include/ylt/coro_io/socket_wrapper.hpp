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
#include <exception>
#ifdef YLT_ENABLE_IBV
#include "ibverbs/ib_io.hpp"
#include "ibverbs/ib_socket.hpp"
#endif
#include "io_context_pool.hpp"
namespace coro_io {
struct socket_wrapper_t {
  // construct by listen tcp
  socket_wrapper_t(){};
  socket_wrapper_t(coro_io::ExecutorWrapper<> *executor,
                   const std::string &local_ip = "")
      : executor_(executor), local_ip_(local_ip){};
  socket_wrapper_t(asio::ip::tcp::socket &&soc,
                   coro_io::ExecutorWrapper<> *executor)
      : socket_(std::make_unique<asio::ip::tcp::socket>(std::move(soc))),
        executor_(executor) {}
#ifdef YLT_ENABLE_SSL
  // construct by listen tcp & make ssl
  socket_wrapper_t(asio::ip::tcp::socket &&soc,
                   coro_io::ExecutorWrapper<> *executor,
                   asio::ssl::context &ssl_ctx)
      : socket_(std::make_unique<asio::ip::tcp::socket>(std::move(soc))),
        executor_(executor) {
    init_ssl(ssl_ctx);
  }
#endif
#ifdef YLT_ENABLE_IBV
  socket_wrapper_t(asio::ip::tcp::socket &&soc,
                   coro_io::ExecutorWrapper<> *executor,
                   const coro_io::ib_socket_t::config_t &config)
      : executor_(executor),
        ib_socket_(std::make_unique<ib_socket_t>(executor_, config)) {
    ib_socket_->prepare_accpet(std::move(soc));
  }
#endif
  void init_tcp_socket() {
    asio::ip::address addr;
    if (!local_ip_.empty()) {
      addr = asio::ip::address::from_string(local_ip_);
    }
    auto ep = asio::ip::tcp::endpoint(std::move(addr), 0);
    if (!socket_) {
      socket_ = std::make_unique<asio::ip::tcp::socket>(
          executor_->get_asio_executor());
    }
    else {
      *socket_ = asio::ip::tcp::socket{executor_->get_asio_executor()};
    }
    socket_->open(ep.protocol());
    if (!local_ip_.empty()) {
      socket_->bind(ep);
    }
  }
  // tcp client init
  bool init_client(bool enable_tcp_no_delay) {
    try {
      init_tcp_socket();
      socket_->set_option(asio::ip::tcp::no_delay(enable_tcp_no_delay));
    } catch (const std::exception &e) {
      ELOG_WARN << "init client failed:" << e.what();
      init_ok_ = false;
      return false;
    }
    return true;
  }
#ifdef YLT_ENABLE_SSL
  // ssl client init
  bool init_client(asio::ssl::context &ssl_ctx, bool enable_tcp_no_delay) {
    try {
      init_tcp_socket();
      socket_->set_option(asio::ip::tcp::no_delay(enable_tcp_no_delay));
      init_ssl(ssl_ctx);
    } catch (const std::exception &e) {
      ELOG_WARN << "init client failed:" << e.what();
      init_ok_ = false;
      return false;
    }
    return true;
  }
#endif
#ifdef YLT_ENABLE_IBV
  bool init_client(const coro_io::ib_socket_t::config_t &config) {
    try {
      init_tcp_socket();
      if (ib_socket_) {
        *ib_socket_ = ib_socket_t(executor_, config);
      }
      else {
        ib_socket_ = std::make_unique<ib_socket_t>(executor_, config);
      }
    } catch (const std::exception &e) {
      ELOG_WARN << "init client failed:" << e.what();
      init_ok_ = false;
      return false;
    }
    return true;
  }
#endif

  void set_local_ip(const std::string &local_ip) { local_ip_ = local_ip; }

 private:
  std::unique_ptr<asio::ip::tcp::socket> socket_;
  coro_io::ExecutorWrapper<> *executor_;
  std::string local_ip_;
#ifdef YLT_ENABLE_SSL
  std::unique_ptr<asio::ssl::stream<asio::ip::tcp::socket &>> ssl_stream_;
#endif

#ifdef YLT_ENABLE_IBV
  std::unique_ptr<ib_socket_t> ib_socket_;
#endif
  bool init_ok_ = true;

 public:
  bool use_ssl() const noexcept {
#ifdef YLT_ENABLE_SSL
    return ssl_stream_ != nullptr;
#else
    return false;
#endif
  }
  template <typename T>
  auto visit(T &&op) noexcept {
#ifdef YLT_ENABLE_IBV
    if (ib_socket_) {
      return op(*ib_socket_);
    }
#endif
#ifdef YLT_ENABLE_SSL
    if (use_ssl()) {
      return op(*ssl_stream_);
    }
#endif
    return op(*socket_);
  }

  bool init_ok() const { return init_ok_; }

  void close() noexcept {
    std::error_code ignored_ec;
#ifdef YLT_ENABLE_IBV
    if (ib_socket_) {
      ib_socket_->close();
      return;
    }
#endif
    if (socket_) {
      socket_->shutdown(asio::ip::tcp::socket::shutdown_both, ignored_ec);
      socket_->close(ignored_ec);
    }
  }

  coro_io::endpoint remote_endpoint() {
#ifdef YLT_ENABLE_IBV
    if (ib_socket_) {
      return {ib_socket_->get_remote_address(), ib_socket_->get_remote_qp_num(),
              coro_io::endpoint::rdma};
    }
#endif
    return {socket_->remote_endpoint().address(),
            socket_->remote_endpoint().port(), coro_io::endpoint::tcp};
  }
  coro_io::endpoint local_endpoint() {
#ifdef YLT_ENABLE_IBV
    if (ib_socket_) {
      return {ib_socket_->get_local_address(), ib_socket_->get_local_qp_num(),
              coro_io::endpoint::rdma};
    }
#endif
    return {socket_->local_endpoint().address(),
            socket_->local_endpoint().port(), coro_io::endpoint::tcp};
  }

  auto get_executor() const noexcept { return executor_; }
  std::unique_ptr<asio::ip::tcp::socket> &socket() noexcept { return socket_; }
  using tcp_socket_t = asio::ip::tcp::socket;
#ifdef YLT_ENABLE_SSL
  void init_ssl(asio::ssl::context &ssl_ctx) {
    ssl_stream_ = std::make_unique<asio::ssl::stream<asio::ip::tcp::socket &>>(
        *socket_, ssl_ctx);
  }
  std::unique_ptr<asio::ssl::stream<asio::ip::tcp::socket &>>
      &ssl_stream() noexcept {
    return ssl_stream_;
  }
  using tcp_socket_with_ssl_t = asio::ssl::stream<asio::ip::tcp::socket &>;
#endif
#ifdef YLT_ENABLE_IBV
  using ibv_socket_t = coro_io::ib_socket_t;
#endif
};
}  // namespace coro_io