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
#include <async_simple/coro/Collect.h>
#include <async_simple/coro/Lazy.h>
#include <async_simple/coro/Sleep.h>
#include <async_simple/coro/SyncAwait.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <system_error>
#include <thread>
#include <type_traits>
#include <utility>

#include "asio/dispatch.hpp"
#include "asio/io_context.hpp"
#include "asio/ip/address.hpp"
#include "async_simple/Signal.h"
#include "ylt/easylog.hpp"
#include "ylt/util/type_traits.h"

#if defined(YLT_ENABLE_SSL) || defined(CINATRA_ENABLE_SSL)
#include <asio/ssl.hpp>
#endif

#include <asio/connect.hpp>
#include <asio/experimental/channel.hpp>
#include <asio/ip/tcp.hpp>
#include <asio/read.hpp>
#include <asio/read_at.hpp>
#include <asio/read_until.hpp>
#include <asio/write.hpp>
#include <asio/write_at.hpp>

#include "io_context_pool.hpp"
#if __has_include("ylt/util/type_traits.h")
#include "ylt/util/type_traits.h"
#else
#include "../util/type_traits.h"
#endif
#ifdef __linux__
#include <sys/sendfile.h>
#endif
namespace coro_io {
template <typename T>
constexpr inline bool is_lazy_v =
    util::is_specialization_v<std::remove_cvref_t<T>, async_simple::coro::Lazy>;

template <typename Arg, typename Derived>
class callback_awaitor_base {
 private:
  template <typename Op>
  class callback_awaitor_impl {
   public:
    callback_awaitor_impl(Derived &awaitor, Op &op) noexcept
        : awaitor(awaitor), op(op) {}
    constexpr bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> handle) noexcept {
      awaitor.coro_ = handle;
      op(awaitor_handler{&awaitor});
    }
    auto coAwait(async_simple::Executor *executor) const noexcept {
      return *this;
    }
    decltype(auto) await_resume() noexcept {
      if constexpr (std::is_void_v<Arg>) {
        return;
      }
      else {
        return std::move(awaitor.arg_);
      }
    }

   private:
    Derived &awaitor;
    Op &op;
  };

 public:
  class awaitor_handler {
   public:
    awaitor_handler(Derived *obj) : obj(obj) {}
    awaitor_handler(awaitor_handler &&) = default;
    awaitor_handler(const awaitor_handler &) = default;
    awaitor_handler &operator=(const awaitor_handler &) = default;
    awaitor_handler &operator=(awaitor_handler &&) = default;
    template <typename... Args>
    void set_value_then_resume(Args &&...args) const {
      set_value(std::forward<Args>(args)...);
      resume();
    }
    template <typename Args>
    void set_value(std::error_code ec, Args &&arg) const {
      if constexpr (!std::is_same_v<Arg, std::error_code>) {
        std::get<0>(obj->arg_) = std::move(ec);
        if constexpr (requires { std::get<1>(obj->arg_) = std::move(arg); }) {
          std::get<1>(obj->arg_) = std::move(arg);
        }
      }
      else {
        obj->arg_ = std::move(ec);
      }
    }
    template <typename Args>
    void set_value(Args &&args) const {
      obj->arg_ = std::move(args);
    }
    void set_value(std::error_code ec) const {
      if constexpr (std::is_same_v<Arg, std::error_code>) {
        obj->arg_ = std::move(ec);
      }
      else {
        std::get<0>(obj->arg_) = std::move(ec);
      }
    }
    void resume() const { obj->coro_.resume(); }

    auto handler() const { return (std::size_t)obj; }

   private:
    Derived *obj;
  };
  template <typename Op>
  callback_awaitor_impl<Op> await_resume(Op &&op) noexcept {
    return callback_awaitor_impl<Op>{static_cast<Derived &>(*this), op};
  }

 private:
  std::coroutine_handle<> coro_;
};

template <typename Arg>
class callback_awaitor
    : public callback_awaitor_base<Arg, callback_awaitor<Arg>> {
  friend class callback_awaitor_base<Arg, callback_awaitor<Arg>>;

 private:
  Arg arg_;
};

template <>
class callback_awaitor<void>
    : public callback_awaitor_base<void, callback_awaitor<void>> {};

template <typename R, typename Func, typename Executor>
struct post_helper {
  void operator()(auto handler) {
    asio::post(e, [this, handler]() {
      try {
        if constexpr (std::is_same_v<R, async_simple::Try<void>>) {
          func();
          handler.resume();
        }
        else {
          auto r = func();
          handler.set_value_then_resume(std::move(r));
        }
      } catch (const std::exception &e) {
        R er;
        er.setException(std::current_exception());
        handler.set_value_then_resume(std::move(er));
      }
    });
  }
  Executor e;
  Func func;
};

template <typename R, typename Func, typename Executor>
struct dispatch_helper {
  void operator()(auto handler) {
    asio::dispatch(e, [this, handler]() {
      try {
        if constexpr (std::is_same_v<R, async_simple::Try<void>>) {
          func();
          handler.resume();
        }
        else {
          auto r = func();
          handler.set_value_then_resume(std::move(r));
        }
      } catch (const std::exception &e) {
        R er;
        er.setException(std::current_exception());
        handler.set_value_then_resume(std::move(er));
      }
    });
  }
  Executor e;
  Func func;
};

template <typename Func, typename Executor>
inline async_simple::coro::Lazy<
    async_simple::Try<typename util::function_traits<Func>::return_type>>
post(Func func, Executor executor) {
  using R =
      async_simple::Try<typename util::function_traits<Func>::return_type>;

  callback_awaitor<R> awaitor;
  post_helper<R, Func, Executor> helper{executor, std::move(func)};
  co_return co_await awaitor.await_resume(helper);
}

template <typename Func>
inline async_simple::coro::Lazy<
    async_simple::Try<typename util::function_traits<Func>::return_type>>
post(Func func,
     coro_io::ExecutorWrapper<> *e = coro_io::get_global_block_executor()) {
  return post(std::move(func), e->get_asio_executor());
}

template <typename Func, typename Executor>
inline async_simple::coro::Lazy<
    async_simple::Try<typename util::function_traits<Func>::return_type>>
dispatch(Func func, Executor executor) {
  using R =
      async_simple::Try<typename util::function_traits<Func>::return_type>;
  callback_awaitor<R> awaitor;
  dispatch_helper<R, Func, Executor> helper{executor, std::move(func)};
  co_return co_await awaitor.await_resume(helper);
}

template <typename Executor>
inline async_simple::coro::Lazy<async_simple::Try<void>> dispatch(
    Executor executor) {
  if (executor.running_in_this_thread()) {
    co_return async_simple::Try<void>{};
  }
  co_return co_await dispatch(
      []() {
      },
      executor);
}

namespace detail {

template <typename T>
void cancel(T &io_object) {
  if constexpr (requires { io_object.cancel(); }) {
    io_object.cancel();
  }
  else {
    io_object.lowest_layer().cancel();
  }
}
constexpr int no_cancel_flag = 0;
constexpr int could_cancel_flag = 1;
constexpr int start_cancel_flag = 2;
constexpr int finish_cancel_flag = 3;
}  // namespace detail

template <typename ret_type, typename IO_func, typename io_object>
inline async_simple::coro::Lazy<ret_type> async_io(IO_func io_func,
                                                   io_object &obj) noexcept {
  callback_awaitor<ret_type> awaitor;
  auto slot = co_await async_simple::coro::CurrentSlot{};
  if (!slot) {
    co_return co_await awaitor.await_resume([&](auto handler) {
      io_func([&, handler](auto &&...args) mutable {
        handler.set_value(std::forward<decltype(args)>(args)...);
        handler.resume();
      });
    });
  }
  else {
    auto executor = obj.get_executor();
    auto lock = std::make_shared<std::atomic<int>>();
    bool hasCanceled;
    auto result = co_await awaitor.await_resume([&, &lock_ref = lock](
                                                    auto handler) mutable {
      auto lock = lock_ref;
      hasCanceled = !slot->emplace(
          async_simple::SignalType::Terminate,
          [&obj, lock](async_simple::SignalType signalType,
                       async_simple::Signal *signal) {
            int expected = detail::no_cancel_flag;
            if (!lock->compare_exchange_strong(expected,
                                               detail::could_cancel_flag,
                                               std::memory_order_acq_rel)) {
              if (expected == detail::could_cancel_flag) {
                if (lock->compare_exchange_strong(expected,
                                                  detail::start_cancel_flag,
                                                  std::memory_order_release)) {
                  detail::cancel(obj);
                  lock->store(detail::finish_cancel_flag,
                              std::memory_order_release);
                }
              }
            }
          });
      if (hasCanceled) [[unlikely]] {
        asio::dispatch(executor, [handler]() {
          handler.set_value(
              std::make_error_code(std::errc::operation_canceled));
          handler.resume();
        });
      }
      else {
        io_func([&, handler](auto &&...args) mutable {
          slot->clear(async_simple::Terminate);
          handler.set_value(std::forward<decltype(args)>(args)...);
          handler.resume();
        });
        int expected = detail::no_cancel_flag;
        if (!lock->compare_exchange_strong(expected, detail::could_cancel_flag,
                                           std::memory_order_acq_rel)) {
          if (expected == detail::could_cancel_flag) {
            if (lock->compare_exchange_strong(expected,
                                              detail::start_cancel_flag,
                                              std::memory_order_release)) {
              detail::cancel(obj);
              lock->store(detail::finish_cancel_flag,
                          std::memory_order_release);
            }
          }
        }
      }
    });
    if (!hasCanceled) {
      int expected = detail::no_cancel_flag;
      if (!lock->compare_exchange_strong(expected, detail::finish_cancel_flag,
                                         std::memory_order_acq_rel)) {
        if (expected != detail::finish_cancel_flag) {
          do {
            if (expected == detail::could_cancel_flag) {
              if (lock->compare_exchange_strong(expected,
                                                detail::finish_cancel_flag,
                                                std::memory_order_acq_rel) ||
                  expected == detail::finish_cancel_flag) {
                break;
              }
            }
            // flag is start_cancel_flag now.
            // wait cancel finish to make sure io object's life-time
            for (; lock->load(std::memory_order_acquire) ==
                   detail::start_cancel_flag;) {
              co_await coro_io::post(
                  []() {
                  },
                  executor);
            }
          } while (0);
        }
      }
    }
    co_return result;
  }
}

inline async_simple::coro::Lazy<std::error_code> async_accept(
    asio::ip::tcp::acceptor &acceptor, asio::ip::tcp::socket &socket) noexcept {
  return async_io<std::error_code>(
      [&](auto &&cb) {
        ELOG_TRACE << "call asio acceptor.async_accept";
        acceptor.async_accept(socket, cb);
      },
      acceptor);
}

template <typename Socket, typename AsioBuffer>
inline async_simple::coro::Lazy<std::pair<std::error_code, size_t>>
async_read_some(Socket &socket, AsioBuffer buffer) noexcept {
  return async_io<std::pair<std::error_code, size_t>>(
      [&, buffer](auto &&cb) {
        socket.async_read_some(buffer, std::move(cb));
      },
      socket);
}

template <typename Socket, typename AsioBuffer>
inline async_simple::coro::Lazy<std::pair<std::error_code, size_t>>
async_read_at(uint64_t offset, Socket &socket, AsioBuffer buffer) noexcept {
  return async_io<std::pair<std::error_code, size_t>>(
      [&socket, buffer, offset](auto &&cb) {
        asio::async_read_at(socket, offset, buffer, std::move(cb));
      },
      socket);
}

template <typename Socket, typename AsioBuffer>
inline async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_read(
    Socket &socket, AsioBuffer buffer) noexcept {
  return async_io<std::pair<std::error_code, size_t>>(
      [&socket, buffer](auto &&cb) {
        asio::async_read(socket, buffer, std::move(cb));
      },
      socket);
}

template <typename Socket, typename AsioBuffer>
inline async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_read(
    Socket &socket, AsioBuffer &buffer, size_t size_to_read) noexcept {
  return async_io<std::pair<std::error_code, size_t>>(
      [&, size_to_read](auto &&cb) {
        asio::async_read(socket, buffer, asio::transfer_exactly(size_to_read),
                         std::move(cb));
      },
      socket);
}

template <typename Socket, typename AsioBuffer>
inline async_simple::coro::Lazy<std::pair<std::error_code, size_t>>
async_read_until(Socket &socket, AsioBuffer &buffer,
                 asio::string_view delim) noexcept {
  return async_io<std::pair<std::error_code, size_t>>(
      [&, delim](auto &&cb) {
        asio::async_read_until(socket, buffer, delim, std::move(cb));
      },
      socket);
}

template <typename Socket, typename AsioBuffer>
inline async_simple::coro::Lazy<std::pair<std::error_code, size_t>> async_write(
    Socket &socket, AsioBuffer buffer) noexcept {
  return async_io<std::pair<std::error_code, size_t>>(
      [&, buffer](auto &&cb) {
        asio::async_write(socket, buffer, std::move(cb));
      },
      socket);
}

template <typename Socket, typename AsioBuffer>
inline async_simple::coro::Lazy<std::pair<std::error_code, size_t>>
async_write_some(Socket &socket, AsioBuffer buffer) noexcept {
  return async_io<std::pair<std::error_code, size_t>>(
      [&, buffer](auto &&cb) {
        socket.async_write_some(buffer, std::move(cb));
      },
      socket);
}

template <typename Socket, typename AsioBuffer>
inline async_simple::coro::Lazy<std::pair<std::error_code, size_t>>
async_write_at(uint64_t offset, Socket &socket, AsioBuffer buffer) noexcept {
  return async_io<std::pair<std::error_code, size_t>>(
      [&, offset, buffer](auto &&cb) {
        asio::async_write_at(socket, offset, buffer, std::move(cb));
      },
      socket);
}

template <typename executor_t>
inline async_simple::coro::Lazy<std::error_code> async_connect(
    executor_t *executor, asio::ip::tcp::socket &socket,
    const std::string &host, const std::string &port) noexcept {
  std::error_code ec;
  auto address = asio::ip::make_address(host, ec);
  std::pair<std::error_code, asio::ip::tcp::resolver::iterator> result;
  if (ec) {
    asio::ip::tcp::resolver resolver(executor->get_asio_executor());
    result = co_await async_io<
        std::pair<std::error_code, asio::ip::tcp::resolver::iterator>>(
        [&](auto &&cb) {
          ELOG_INFO << "call asio resolver.async_resolve";
          resolver.async_resolve(host, port, std::move(cb));
          ELOG_INFO << "call asio resolver.async_resolve over, waiting cb";
        },
        resolver);
    ELOG_INFO << "call asio resolver.async_resolve cbover";
    if (result.first) {
      co_return result.first;
    }
    co_return co_await async_io<std::error_code>(
        [&](auto &&cb) {
          ELOG_INFO << "call asio socket.async_connect";
          asio::async_connect(socket, result.second, std::move(cb));
        },
        socket);
  }
  else {
    ELOG_INFO << "direct call without resolve";
    uint16_t port_v;
    auto result =
        std::from_chars(port.data(), port.data() + port.size(), port_v);
    if (result.ec != std::errc{}) {
      co_return std::make_error_code(result.ec);
    }
    asio::ip::tcp::endpoint ep{address, port_v};
    co_return co_await async_io<std::error_code>(
        [&](auto &&cb) {
          ELOG_INFO << "call asio socket.async_connect";
          asio::async_connect(socket, std::span{&ep, 1}, std::move(cb));
        },
        socket);
  }
}

template <typename EndPointSeq>
inline async_simple::coro::Lazy<std::error_code> async_connect(
    asio::ip::tcp::socket &socket, const EndPointSeq &endpoint) noexcept {
  std::error_code ec;
  auto local_ep = socket.local_endpoint(ec);
  if (!ec && local_ep.address() != asio::ip::address()) {
    ELOG_INFO << "get local ip: " << local_ep.address().to_string();
    std::error_code result{};
    for (auto &ep : endpoint) {
      result = co_await async_io<std::error_code>(
          [&](auto &&cb) {
            socket.async_connect(ep, std::move(cb));
          },
          socket);
      if (!result) {
        break;
      }
    }
    co_return result;
  }

  auto result = co_await async_io<std::error_code>(
      [&](auto &&cb) {
        asio::async_connect(socket, endpoint, std::move(cb));
      },
      socket);
  co_return result;
}

template <typename executor_t>
inline async_simple::coro::Lazy<
    std::pair<std::error_code, asio::ip::tcp::resolver::iterator>>
async_resolve(executor_t *executor, asio::ip::tcp::socket &socket,
              const std::string &host, const std::string &port) noexcept {
  asio::ip::tcp::resolver resolver(executor->get_asio_executor());
  co_return co_await async_io<
      std::pair<std::error_code, asio::ip::tcp::resolver::iterator>>(
      [&](auto &&cb) {
        resolver.async_resolve(host, port, std::move(cb));
      },
      resolver);
}

template <typename executor_t>
inline async_simple::coro::Lazy<
    std::pair<std::error_code, asio::ip::tcp::resolver::iterator>>
async_resolve(executor_t *executor, const std::string &host,
              const std::string &port) noexcept {
  asio::ip::tcp::resolver resolver(executor->get_asio_executor());
  co_return co_await async_io<
      std::pair<std::error_code, asio::ip::tcp::resolver::iterator>>(
      [&](auto &&cb) {
        resolver.async_resolve(host, port, std::move(cb));
      },
      resolver);
}

template <typename Socket>
inline async_simple::coro::Lazy<void> async_close(Socket &socket) noexcept {
  callback_awaitor<void> awaitor;
  auto executor = socket.get_executor();
  co_return co_await awaitor.await_resume([&](auto handler) {
    asio::dispatch(executor, [&, handler]() {
      asio::error_code ignored_ec;
      socket.shutdown(asio::ip::tcp::socket::shutdown_both, ignored_ec);
      socket.close(ignored_ec);
      handler.resume();
    });
  });
}

#if defined(YLT_ENABLE_SSL) || defined(CINATRA_ENABLE_SSL)
inline async_simple::coro::Lazy<std::error_code> async_handshake(
    auto &&ssl_stream, asio::ssl::stream_base::handshake_type type) noexcept {
  return async_io<std::error_code>(
      [&, type](auto &&cb) {
        ssl_stream->async_handshake(type, std::move(cb));
      },
      *ssl_stream);
}
template <typename executor_t>
inline async_simple::coro::Lazy<std::error_code> async_connect(
    executor_t *executor, asio::ssl::stream<asio::ip::tcp::socket &> &socket,
    const std::string &host, const std::string &port) noexcept {
  auto ec = co_await async_connect(executor, socket, host, port);
  if (ec) [[unlikely]] {
    co_return ec;
  }
  ec = co_await coro_io::async_handshake(&socket,
                                         asio::ssl::stream_base::client);
  co_return ec;
}
template <typename EndPointSeq>
inline async_simple::coro::Lazy<std::error_code> async_connect(
    asio::ssl::stream<asio::ip::tcp::socket &> &socket,
    const EndPointSeq &endpoint) noexcept {
  auto ec = co_await async_connect(socket.next_layer(), endpoint);
  if (ec) [[unlikely]] {
    co_return ec;
  }
  ec = co_await coro_io::async_handshake(&socket,
                                         asio::ssl::stream_base::client);
  co_return ec;
}
#endif
class period_timer : public asio::steady_timer {
 public:
  using asio::steady_timer::steady_timer;
  template <typename T>
  period_timer(coro_io::ExecutorWrapper<T> *executor)
      : asio::steady_timer(executor->get_asio_executor()) {}

  async_simple::coro::Lazy<bool> async_await() noexcept {
    auto ec = co_await async_io<std::error_code>(
        [&](auto &&cb) {
          this->async_wait(std::move(cb));
        },
        *this);
    co_return !ec;
  }
};

template <typename Duration, typename Executor>
inline async_simple::coro::Lazy<bool> sleep_for(Duration d, Executor *e) {
  coro_io::period_timer timer(e);
  timer.expires_after(d);
  co_return co_await timer.async_await();
}
template <typename Duration>
inline async_simple::coro::Lazy<bool> sleep_for(Duration d) {
  if (auto executor = co_await async_simple::CurrentExecutor();
      executor != nullptr) {
    try {
      co_await async_simple::coro::sleep(d);
    } catch (const async_simple::SignalException &e) {
      co_return false;
    }
    co_return true;
  }
  else if (auto ioc = coro_io::get_current();
           ioc != nullptr && *ioc != nullptr) {
    coro_io::ExecutorWrapper<> e((**ioc).get_executor());
    co_return co_await sleep_for(d, &e);
  }
  else {
    co_return co_await sleep_for(d,
                                 coro_io::g_io_context_pool().get_executor());
  }
}

template <typename R>
struct channel : public asio::experimental::channel<void(std::error_code, R)> {
  using return_type = R;
  using value_type = std::pair<std::error_code, R>;
  using base_type = asio::experimental::channel<void(std::error_code, R)>;
  using base_type::base_type;
  channel(coro_io::ExecutorWrapper<> *executor, size_t capacity)
      : executor_(executor),
        asio::experimental::channel<void(std::error_code, R)>(
            executor->get_asio_executor(), capacity) {}
  auto get_executor() { return executor_; }

 private:
  coro_io::ExecutorWrapper<> *executor_;
};

template <typename R>
inline channel<R> create_channel(
    size_t capacity,
    coro_io::ExecutorWrapper<> *executor = coro_io::get_global_executor()) {
  return channel<R>(executor, capacity);
}

template <typename R>
inline auto create_shared_channel(
    size_t capacity,
    coro_io::ExecutorWrapper<> *executor = coro_io::get_global_executor()) {
  return std::make_shared<channel<R>>(executor, capacity);
}

template <typename T>
inline async_simple::coro::Lazy<std::error_code> async_send(
    asio::experimental::channel<void(std::error_code, T)> &channel, T val) {
  bool r = channel.try_send(std::error_code{}, val);
  if (r) {
    co_return std::error_code{};
  }
  co_return co_await async_io<std::error_code>(
      [&](auto &&cb) {
        channel.async_send({}, std::move(val), std::move(cb));
      },
      channel);
}

template <typename Channel>
async_simple::coro::Lazy<std::pair<
    std::error_code,
    typename Channel::return_type>> inline async_receive(Channel &channel) {
  using value_type = typename Channel::return_type;
  value_type val;
  bool r = channel.try_receive([&val](std::error_code, value_type result) {
    val = result;
  });
  if (r) {
    co_return std::make_pair(std::error_code{}, val);
  }
  co_return co_await async_io<
      std::pair<std::error_code, typename Channel::return_type>>(
      [&](auto &&cb) {
        channel.async_receive(std::move(cb));
      },
      *(typename Channel::base_type *)&channel);
}

template <typename Socket, typename AsioBuffer>
std::pair<asio::error_code, size_t> read_some(Socket &sock,
                                              AsioBuffer &&buffer) {
  asio::error_code error;
  size_t length = sock.read_some(std::forward<AsioBuffer>(buffer), error);
  return std::make_pair(error, length);
}

template <typename Socket, typename AsioBuffer>
std::pair<asio::error_code, size_t> read(Socket &sock, AsioBuffer &&buffer) {
  asio::error_code error;
  size_t length = asio::read(sock, buffer, error);
  return std::make_pair(error, length);
}

template <typename Socket, typename AsioBuffer>
std::pair<asio::error_code, size_t> write(Socket &sock, AsioBuffer &&buffer) {
  asio::error_code error;
  auto length = asio::write(sock, std::forward<AsioBuffer>(buffer), error);
  return std::make_pair(error, length);
}

inline std::error_code accept(asio::ip::tcp::acceptor &a,
                              asio::ip::tcp::socket &socket) {
  std::error_code error;
  a.accept(socket, error);
  return error;
}

template <typename executor_t>
inline std::error_code connect(executor_t &executor,
                               asio::ip::tcp::socket &socket,
                               const std::string &host,
                               const std::string &port) {
  asio::ip::tcp::resolver resolver(executor);
  std::error_code error;
  auto endpoints = resolver.resolve(host, port, error);
  if (error) {
    return error;
  }
  asio::connect(socket, endpoints, error);
  return error;
}

#ifdef __linux__
// this help us ignore SIGPIPE when send data to a unexpected closed socket.
inline auto pipe_signal_handler = [] {
  std::signal(SIGPIPE, SIG_IGN);
  return 0;
}();

// FIXME: this function may not thread-safe if it not running in socket's
// executor
inline async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
async_sendfile(asio::ip::tcp::socket &socket, int fd, off_t offset,
               size_t size) noexcept {
  std::error_code ec;
  std::size_t least_bytes = size;
  if (!ec) [[likely]] {
    if (!socket.native_non_blocking()) {
      socket.native_non_blocking(true, ec);
      if (ec) {
        co_return std::pair{ec, 0};
      }
    }
    while (true) {
      // Try the system call.
      errno = 0;
      ssize_t n = ::sendfile(socket.native_handle(), fd, &offset,
                             std::min(std::size_t{65536}, least_bytes));
      ec = asio::error_code(n < 0 ? errno : 0,
                            asio::error::get_system_category());
      least_bytes -= ec ? 0 : n;
      // total_bytes_transferred += ec ? 0 : n;
      // Retry operation immediately if interrupted by signal.
      if (ec == asio::error::interrupted) [[unlikely]]
        continue;
      // Check if we need to run the operation again.
      if (ec == asio::error::would_block || ec == asio::error::try_again)
          [[unlikely]] {
        // We have to wait for the socket to become ready again.
        ec = co_await async_io<std::error_code>(
            [&](auto &&cb) {
              socket.async_wait(asio::ip::tcp::socket::wait_write,
                                std::move(cb));
            },
            socket);
      }
      if (ec || n == 0 || least_bytes == 0) [[unlikely]] {  // End of File
        break;
      }
      // Loop around to try calling sendfile again.
    }
  }
  co_return std::pair{ec, size - least_bytes};
}
#endif

struct endpoint {
  enum protocal { tcp, rdma };
  asio::ip::address address;
  uint32_t port;
  protocal proto;
};

inline std::ostream &operator<<(std::ostream &stream, const endpoint &ep) {
  return stream << ep.address.to_string() << ":" << ep.port;
}

}  // namespace coro_io
