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
#include <async_simple/Executor.h>
#include <async_simple/coro/SyncAwait.h>

#include <any>
#include <array>
#include <asio/buffer.hpp>
#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <utility>
#include <ylt/easylog.hpp>

#include "asio/dispatch.hpp"
#include "async_simple/Common.h"
#include "async_simple/util/move_only_function.h"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/socket_wrapper.hpp"
#include "ylt/coro_rpc/impl/errno.h"
#include "ylt/util/utils.hpp"
#ifdef UNIT_TEST_INJECT
#include "inject_action.hpp"
#endif
namespace coro_rpc {
class coro_connection;
using rpc_conn = std::shared_ptr<coro_connection>;

enum class context_status : int { init, start_response, finish_response };
template <typename rpc_protocol>
struct context_info_t {
#ifndef CORO_RPC_TEST
 private:
#endif
  typename rpc_protocol::route_key_t key_;
  typename rpc_protocol::router &router_;
  std::shared_ptr<coro_connection> conn_;
  typename rpc_protocol::req_header req_head_;
  std::string req_body_;
  std::string req_attachment_;
  std::function<std::string_view()> resp_attachment_ = [] {
    return std::string_view{};
  };
  std::function<void(const std::error_code &, std::size_t)> complete_handler_;
  std::atomic<context_status> status_ = context_status::init;

 public:
  template <typename, typename>
  friend class context_base;
  friend class coro_connection;
  context_info_t(typename rpc_protocol::router &r,
                 std::shared_ptr<coro_connection> &&conn)
      : router_(r), conn_(std::move(conn)) {}
  context_info_t(typename rpc_protocol::router &r,
                 std::shared_ptr<coro_connection> &&conn,
                 std::string &&req_body_buf, std::string &&req_attachment_buf)
      : router_(r),
        conn_(std::move(conn)),
        req_body_(std::move(req_body_buf)),
        req_attachment_(std::move(req_attachment_buf)) {}
  uint64_t get_connection_id() noexcept;
  uint64_t has_closed() const noexcept;
  void close();
  uint64_t get_connection_id() const noexcept;
  void set_response_attachment(std::string_view attachment);
  void set_response_attachment(std::string attachment);
  void set_response_attachment(std::function<std::string_view()> attachment);
  /* set a handler which will be called when data was serialized and write to
   * socket*/
  /* std::error_code: socket write result*/
  /* std::size_t : write length*/
  void set_complete_handler(
      std::function<void(const std::error_code &, std::size_t)> &&handler) {
    complete_handler_ = std::move(handler);
  }
  std::string_view get_request_attachment() const;
  std::string release_request_attachment();
  std::any &tag() noexcept;
  const std::any &tag() const noexcept;
  coro_io::endpoint get_local_endpoint() const noexcept;
  coro_io::endpoint get_remote_endpoint() const noexcept;
  uint64_t get_request_id() const noexcept;
  std::string_view get_rpc_function_name() const {
    return router_.get_name(key_);
  }
};

namespace detail {
template <typename rpc_protocol>
context_info_t<rpc_protocol> *&set_context();
}

/*!
 * TODO: add doc
 */

[[noreturn]] inline void unreachable() {
  // Uses compiler specific extensions if possible.
  // Even if no extension is used, undefined behavior is still raised by
  // an empty function body and the noreturn attribute.
#ifdef __GNUC__  // GCC, Clang, ICC
  __builtin_unreachable();
#elif defined(_MSC_VER)  // msvc
  __assume(false);
#endif
}

class coro_connection : public std::enable_shared_from_this<coro_connection> {
  using TransferCallback = async_simple::util::move_only_function<void(
      coro_io::socket_wrapper_t &&socket, std::string_view magic_number)>;

 public:
  template <typename rpc_protocol_t>
  struct connection_lazy_ctx : public async_simple::coro::LazyLocalBase {
    inline static char tag;
    // init LazyLocalBase by unique address
    connection_lazy_ctx(std::shared_ptr<context_info_t<rpc_protocol_t>> info)
        : LazyLocalBase(&tag), info_(std::move(info)) {}
    static bool classof(const LazyLocalBase *base) {
      return base->getTypeTag() == &tag;
    }
    std::shared_ptr<context_info_t<rpc_protocol_t>> info_;
  };
  /*!
   *
   * @param io_context
   * @param socket
   * @param timeout_duration
   */
  using executor_t = coro_io::ExecutorWrapper<>;
  coro_connection(coro_io::socket_wrapper_t socket,
                  std::chrono::steady_clock::duration timeout_duration =
                      std::chrono::seconds(0))
      : socket_wrapper_(std::move(socket)),
        timer_(socket_wrapper_.get_executor()->get_asio_executor()) {
    if (timeout_duration == std::chrono::seconds(0)) {
      return;
    }

    enable_check_timeout_ = true;
    keep_alive_timeout_duration_ = timeout_duration;
  }

  ~coro_connection() {
    if (!has_closed_) {
#ifdef UNIT_TEST_INJECT
      ELOG_INFO << "~async_connection conn_id " << conn_id_ << ", client_id "
                << client_id_;
#endif
      close();
    }
  }

  template <typename rpc_protocol>
  struct handshake_result_t {
    std::error_code ec;
    std::string magic_number;
  };

  template <typename rpc_protocol>
  async_simple::coro::Lazy<void> start(typename rpc_protocol::router &router,
                                       std::string_view magic_number) noexcept {
    co_await socket_wrapper_.visit([this, &router, magic_number](auto &socket) {
      return start_impl<rpc_protocol>(router, socket, magic_number);
    });
  }
  template <typename rpc_protocol, typename Socket>
  async_simple::coro::Lazy<void> start_impl(
      typename rpc_protocol::router &router, Socket &socket,
      std::string_view magic_number) noexcept {
#ifdef YLT_ENABLE_IBV
    if constexpr (std::is_same_v<Socket,
                                 coro_io::socket_wrapper_t::ibv_socket_t>) {
      reset_timer();  // TODO: test ibverbs accept timeout
      auto ec = co_await socket.accept(magic_number);
      magic_number = "";
      cancel_timer();
      if (ec) [[unlikely]] {
        ELOG_ERROR << "ibverbs init qp failed: " << ec.message() << " conn_id  "
                   << conn_id_;
        close();
        co_return;
      }
    }
#endif
    auto context_info = std::make_shared<context_info_t<rpc_protocol>>(
        router, shared_from_this());
    reset_timer();
    for (int64_t req_count = 0;; req_count++) {
      typename rpc_protocol::req_header req_head_tmp;
      std::error_code ec;
      // timer will be reset after rpc call response
      if (req_count == 0) {
        ec = co_await rpc_protocol::read_first_head(socket, req_head_tmp,
                                                    magic_number);
      }
      else {
        ec = co_await rpc_protocol::read_head(socket, req_head_tmp);
      }
      // `co_await async_read` uses asio::async_read underlying.
      // If eof occurred, the bytes_transferred of `co_await async_read` must
      // less than RPC_HEAD_LEN. Incomplete data will be discarded.
      // So, no special handling of eof is required.
      if (ec) [[unlikely]] {
        ELOG_INFO << "connection " << conn_id_ << " close: " << ec.message();
        break;
      }
      // we won't transfer connection after first check magic number
#ifdef UNIT_TEST_INJECT
      client_id_ = req_head_tmp.seq_num;
      ELOG_INFO << "conn_id " << conn_id_ << " client_id " << client_id_;
#endif
#ifdef UNIT_TEST_INJECT
      if (g_action == inject_action::close_socket_after_read_header) {
        ELOG_WARN << "inject action: close_socket_after_read_header, conn_id "
                  << conn_id_ << ", client_id " << client_id_;
        break;
      }
#endif

      // try to reuse context
      if (is_rpc_return_by_callback_) {
        // cant reuse context,make shared new one
        is_rpc_return_by_callback_ = false;
        if (context_info->status_ != context_status::finish_response) {
          // cant reuse buffer
          context_info = std::make_shared<context_info_t<rpc_protocol>>(
              router, shared_from_this());
        }
        else {
          // reuse string buffer
          context_info = std::make_shared<context_info_t<rpc_protocol>>(
              router, shared_from_this(), std::move(context_info->req_body_),
              std::move(context_info->req_attachment_));
        }
      }
      auto &req_head = context_info->req_head_;
      auto &body = context_info->req_body_;
      auto &req_attachment = context_info->req_attachment_;
      auto &key = context_info->key_;
      req_head = std::move(req_head_tmp);
      auto serialize_proto = rpc_protocol::get_serialize_protocol(req_head);

      if (!serialize_proto.has_value())
        AS_UNLIKELY {
          ELOG_ERROR << "bad serialize protocol type, conn_id " << conn_id_;
          break;
        }

      std::string_view payload;
      // rpc_protocol::buffer_type maybe from user, default from framework.

      ec = co_await rpc_protocol::read_payload(socket, req_head, body,
                                               req_attachment);
      cancel_timer();
      payload = std::string_view{body};

      if (ec)
        AS_UNLIKELY {
          ELOG_ERROR << "read error: " << ec.message() << ", conn_id "
                     << conn_id_;
          break;
        }

      key = rpc_protocol::get_route_key(req_head);
      auto handler = router.get_handler(key);
      ++rpc_processing_cnt_;
      if (!handler) {
        auto coro_handler = router.get_coro_handler(key);
        set_rpc_return_by_callback();
        router.route_coro(coro_handler, payload, serialize_proto.value(), key)
            .template setLazyLocal<connection_lazy_ctx<rpc_protocol>>(
                context_info)
            .directlyStart(
                [context_info](auto &&result) mutable {
                  std::pair<coro_rpc::err_code, std::string> &ret =
                      result.value();
                  if (ret.first)
                    AS_UNLIKELY {
                      ELOGI << "rpc error in function:"
                            << context_info->get_rpc_function_name()
                            << ". error code:" << ret.first.ec
                            << ". message : " << ret.second;
                    }
                  auto executor = context_info->conn_->get_executor();
                  executor->schedule([context_info = std::move(context_info),
                                      ret = std::move(ret)]() mutable {
                    context_info->conn_
                        ->template direct_response_msg<rpc_protocol>(
                            ret.first, ret.second, context_info->req_head_,
                            std::move(context_info->resp_attachment_),
                            std::move(context_info->complete_handler_));
                  });
                },
                socket_wrapper_.get_executor());
      }
      else {
        coro_rpc::detail::set_context<rpc_protocol>() = context_info.get();
        auto &&[resp_err, resp_buf] = router.route(
            handler, payload, context_info, serialize_proto.value(), key);
        if (is_rpc_return_by_callback_) {
          if (!resp_err) {
            continue;
          }
          else {
            ELOGI << "rpc error in function:"
                  << context_info->get_rpc_function_name()
                  << ". error code:" << resp_err.ec
                  << ". message : " << resp_buf;
            is_rpc_return_by_callback_ = false;
          }
        }
#ifdef UNIT_TEST_INJECT
        if (g_action == inject_action::close_socket_after_send_length) {
          ELOG_WARN
              << "inject action: close_socket_after_send_length , conn_id "
              << conn_id_ << ", client_id " << client_id_;
          std::string header_buf = rpc_protocol::prepare_response(
              resp_buf, req_head, 0, resp_err, "");
          co_await coro_io::async_write(socket, asio::buffer(header_buf));
          break;
        }
        if (g_action == inject_action::server_send_bad_rpc_result) {
          ELOG_WARN << "inject action: server_send_bad_rpc_result , conn_id "
                    << conn_id_ << ", client_id " << client_id_;
          resp_buf[0] = resp_buf[0] + 1;
        }
#endif
        direct_response_msg<rpc_protocol>(
            resp_err, resp_buf, req_head,
            std::move(context_info->resp_attachment_),
            std::move(context_info->complete_handler_));
        context_info->resp_attachment_ = [] {
          return std::string_view{};
        };
      }
    }
    close();
    cancel_timer();
  }
  /*!
   * send `ret` to RPC client
   *
   * @tparam R message type
   * @param ret object of message type
   */
  template <typename rpc_protocol>
  void direct_response_msg(
      coro_rpc::err_code &resp_err, std::string &resp_buf,
      const typename rpc_protocol::req_header &req_head,
      std::function<std::string_view()> &&attachment,
      std::function<void(const std::error_code &, std::size_t)>
          &&complete_handler) {
    std::string resp_error_msg;
    if (resp_err) {
      resp_error_msg = std::move(resp_buf);
      resp_buf = {};
      ELOG_WARN << "rpc route/execute error, error msg: " << resp_error_msg;
    }
    std::string header_buf = rpc_protocol::prepare_response(
        resp_buf, req_head, attachment().length(), resp_err, resp_error_msg);

    response(std::move(header_buf), std::move(resp_buf), std::move(attachment),
             std::move(complete_handler), nullptr)
        .start([](auto &&) {
        });
  }

  template <typename rpc_protocol>
  void response_msg(std::string &&body_buf,
                    std::function<std::string_view()> &&resp_attachment,
                    const typename rpc_protocol::req_header &req_head,
                    std::function<void(const std::error_code &, std::size_t)>
                        &&complete_handler) {
    std::string header_buf = rpc_protocol::prepare_response(
        body_buf, req_head, resp_attachment().size());
    asio::dispatch(
        socket_wrapper_.get_executor()->get_asio_executor(),
        [watcher = weak_from_this(), header_buf = std::move(header_buf),
         body_buf = std::move(body_buf),
         resp_attachment = std::move(resp_attachment),
         handler = std::move(complete_handler)]() mutable {
          if (auto self = watcher.lock()) {
            self->response(std::move(header_buf), std::move(body_buf),
                           std::move(resp_attachment), std::move(handler), self)
                .start([](auto &&) {
                });
          }
        });
  }

  template <typename rpc_protocol>
  void response_error(coro_rpc::errc ec, std::string_view error_msg,
                      const typename rpc_protocol::req_header &req_head,
                      std::function<void(const std::error_code &, std::size_t)>
                          &&complete_handler) {
    std::string body_buf;
    std::string header_buf =
        rpc_protocol::prepare_response(body_buf, req_head, 0, ec, error_msg);
    asio::dispatch(
        socket_wrapper_.get_executor()->get_asio_executor(),
        [watcher = weak_from_this(), header_buf = std::move(header_buf),
         body_buf = std::move(body_buf),
         handler = std::move(complete_handler)]() mutable {
          if (auto self = watcher.lock()) {
            self->response(
                    std::move(header_buf), std::move(body_buf),
                    []() -> std::string_view {
                      return {};
                    },
                    std::move(handler), self)
                .start([](auto &&) {
                });
          }
        });
  }

  void set_rpc_return_by_callback() { is_rpc_return_by_callback_ = true; }

  /*!
   * Check the connection has closed or not
   *
   * @return true if closed, otherwise false.
   */
  bool has_closed() const { return has_closed_; }

  /*!
   * close connection asynchronously
   *
   * If the connection has already closed, nothing will happen.
   *
   * @param close_ssl whether to close the ssl stream
   */
  void async_close() {
    if (has_closed_) {
      return;
    }
    socket_wrapper_.get_executor()->schedule([this, self = shared_from_this()] {
      this->close();
    });
  }

  using QuitCallback = std::function<void(const uint64_t &conn_id)>;
  void set_quit_callback(QuitCallback callback, uint64_t conn_id) {
    quit_callback_ = std::move(callback);
    conn_id_ = conn_id;
  }

  uint64_t get_connection_id() const noexcept { return conn_id_; }

  std::any &tag() { return tag_; }
  const std::any &tag() const { return tag_; }

  executor_t *get_executor() { return socket_wrapper_.get_executor(); }

  coro_io::endpoint get_remote_endpoint() {
    return socket_wrapper_.remote_endpoint();
  }

  coro_io::endpoint get_local_endpoint() {
    return socket_wrapper_.local_endpoint();
  }

  coro_io::socket_wrapper_t &socket_wrapper() { return socket_wrapper_; }

  template <typename rpc_protocol>
  async_simple::coro::Lazy<handshake_result_t<rpc_protocol>> handshake() {
    co_return co_await socket_wrapper_.visit(
        [this]<typename Socket>(Socket &socket)
            -> async_simple::coro::Lazy<handshake_result_t<rpc_protocol>> {
          handshake_result_t<rpc_protocol> result;
#ifdef YLT_ENABLE_SSL
          if constexpr (std::is_same_v<
                            Socket,
                            coro_io::socket_wrapper_t::tcp_socket_with_ssl_t>) {
            ELOG_INFO << "begin to handshake conn_id " << conn_id_;
            reset_timer();
            result.ec = co_await coro_io::async_handshake(
                &socket, asio::ssl::stream_base::server);
            cancel_timer();
            if (result.ec) {
              ELOG_ERROR << "handshake failed: " << result.ec.message()
                         << " conn_id  " << conn_id_;
              co_return result;
            }
            else {
              ELOG_INFO << "handshake ok conn_id " << conn_id_;
            }
          }
#endif
          result.ec =
              co_await rpc_protocol::read_magic(socket, result.magic_number);
          co_return result;
        });
  }

 private:
  template <typename Socket>
  async_simple::coro::Lazy<void> send_data(Socket &socket) {
    std::pair<std::error_code, size_t> ret;
    while (!write_queue_.empty()) {
      auto &msg = write_queue_.front();
#ifdef UNIT_TEST_INJECT
      if (g_action == inject_action::force_inject_connection_close_socket) {
        ELOG_WARN
            << "inject action: force_inject_connection_close_socket , conn_id "
            << conn_id_ << ", client_id " << client_id_;
        close();
        co_return;
      }
#endif
      auto attachment = std::get<2>(msg)();
      if (attachment.empty()) {
        std::array<asio::const_buffer, 2> buffers{
            asio::buffer(std::get<0>(msg)), asio::buffer(std::get<1>(msg))};
        ret = co_await coro_io::async_write(socket, buffers);
      }
      else {
        std::array<asio::const_buffer, 3> buffers{
            asio::buffer(std::get<0>(msg)), asio::buffer(std::get<1>(msg)),
            asio::buffer(attachment)};
        ret = co_await coro_io::async_write(socket, buffers);
      }
      auto &complete_handler = std::get<3>(msg);
      if (complete_handler) {
        complete_handler(ret.first, ret.second);
      }
      if (ret.first)
        AS_UNLIKELY {
          ELOG_INFO << ret.first.message() << ", "
                    << "async_write error";
          close();
          co_return;
        }
      write_queue_.pop_front();
    }
#ifdef UNIT_TEST_INJECT
    if (g_action == inject_action::close_socket_after_send_length) {
      ELOG_INFO << "inject action: close_socket_after_send_length , conn_id "
                << conn_id_ << ", client_id " << client_id_;
      // Attention: close ssl stream after read error
      // otherwise, server will crash
      close();
      co_return;
    }
#endif
  }

  async_simple::coro::Lazy<void> response(
      std::string header_buf, std::string body_buf,
      std::function<std::string_view()> resp_attachment,
      std::function<void(const std::error_code, std::size_t)> complete_handler,
      rpc_conn self) noexcept {
    if (has_closed())
      AS_UNLIKELY {
        ELOG_DEBUG << "response_msg failed: connection has been closed";
        co_return;
      }
#ifdef UNIT_TEST_INJECT
    if (g_action == inject_action::close_socket_after_send_length) {
      ELOG_WARN << "inject action: close_socket_after_send_length";
      body_buf.clear();
    }
#endif
    write_queue_.emplace_back(std::move(header_buf), std::move(body_buf),
                              std::move(resp_attachment),
                              std::move(complete_handler));
    --rpc_processing_cnt_;
    assert(rpc_processing_cnt_ >= 0);
    reset_timer();
    if (write_queue_.size() == 1) {
      if (self == nullptr) {
        self = shared_from_this();
      }
      co_await socket_wrapper_.visit([this](auto &socket) {
        return send_data(socket);
      });
    }
  }

  void close() {
    ELOG_TRACE << "connection closed";
    if (has_closed_) {
      return;
    }
    has_closed_ = true;
    socket_wrapper_.close();

    if (quit_callback_) {
      quit_callback_(conn_id_);
    }
  }
  void reset_timer() {
    if (!enable_check_timeout_ || rpc_processing_cnt_ != 0) {
      return;
    }

    timer_.expires_from_now(keep_alive_timeout_duration_);
    timer_.async_wait(
        [this, self = shared_from_this()](asio::error_code const &ec) {
          if (!ec) {
#ifdef UNIT_TEST_INJECT
            ELOG_INFO << "close timeout client client_id " << client_id_
                      << ", conn_id " << conn_id_;
#else
            ELOG_INFO << "close timeout client conn_id " << conn_id_;
#endif

            close();
          }
        });
  }

  void cancel_timer() {
    if (!enable_check_timeout_) {
      return;
    }

    asio::error_code ec;
    timer_.cancel(ec);
  }
  coro_io::socket_wrapper_t socket_wrapper_;
  // FIXME: queue's performance can be imporved.
  std::deque<
      std::tuple<std::string, std::string, std::function<std::string_view()>,
                 std::function<void(const std::error_code, std::size_t)>>>
      write_queue_;
  bool is_rpc_return_by_callback_{false};

  // if don't get any message in keep_alive_timeout_duration_, the connection
  // will be closed when enable_check_timeout_ is true.
  std::chrono::steady_clock::duration keep_alive_timeout_duration_;
  bool enable_check_timeout_{false};
  asio::steady_timer timer_;
  std::atomic<bool> has_closed_{false};

  QuitCallback quit_callback_{nullptr};
  uint64_t conn_id_{0};
  uint64_t rpc_processing_cnt_{0};

  std::any tag_;

#ifdef UNIT_TEST_INJECT
  uint32_t client_id_ = 0;
#endif
};

template <typename rpc_protocol>
uint64_t context_info_t<rpc_protocol>::get_connection_id() noexcept {
  return conn_->get_connection_id();
}

template <typename rpc_protocol>
uint64_t context_info_t<rpc_protocol>::has_closed() const noexcept {
  return conn_->has_closed();
}

template <typename rpc_protocol>
void context_info_t<rpc_protocol>::close() {
  return conn_->async_close();
}

template <typename rpc_protocol>
uint64_t context_info_t<rpc_protocol>::get_connection_id() const noexcept {
  return conn_->get_connection_id();
}

template <typename rpc_protocol>
void context_info_t<rpc_protocol>::set_response_attachment(
    std::string attachment) {
  set_response_attachment([attachment = std::move(attachment)] {
    return std::string_view{attachment};
  });
}

template <typename rpc_protocol>
void context_info_t<rpc_protocol>::set_response_attachment(
    std::string_view attachment) {
  set_response_attachment([attachment] {
    return attachment;
  });
}

template <typename rpc_protocol>
void context_info_t<rpc_protocol>::set_response_attachment(
    std::function<std::string_view()> attachment) {
  resp_attachment_ = std::move(attachment);
}

template <typename rpc_protocol>
std::string_view context_info_t<rpc_protocol>::get_request_attachment() const {
  return req_attachment_;
}

template <typename rpc_protocol>
std::string context_info_t<rpc_protocol>::release_request_attachment() {
  return std::move(req_attachment_);
}

template <typename rpc_protocol>
std::any &context_info_t<rpc_protocol>::tag() noexcept {
  return conn_->tag();
}

template <typename rpc_protocol>
const std::any &context_info_t<rpc_protocol>::tag() const noexcept {
  return conn_->tag();
}

template <typename rpc_protocol>
coro_io::endpoint context_info_t<rpc_protocol>::get_local_endpoint()
    const noexcept {
  return conn_->get_local_endpoint();
}

template <typename rpc_protocol>
coro_io::endpoint context_info_t<rpc_protocol>::get_remote_endpoint()
    const noexcept {
  return conn_->get_remote_endpoint();
}
namespace protocol {
template <typename rpc_protocol>
uint64_t get_request_id(const typename rpc_protocol::req_header &) noexcept;
}
template <typename rpc_protocol>
uint64_t context_info_t<rpc_protocol>::get_request_id() const noexcept {
  return coro_rpc::protocol::get_request_id<rpc_protocol>(req_head_);
}
}  // namespace coro_rpc
