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
#include <infiniband/verbs.h>
#include <sys/socket.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <future>
#include <memory>
#include <optional>
#include <span>
#include <string_view>
#include <system_error>
#include <type_traits>
#include <utility>

#include "asio/buffer.hpp"
#include "async_simple/Executor.h"
#include "async_simple/Future.h"
#include "async_simple/Promise.h"
#include "async_simple/Signal.h"
#include "async_simple/coro/Collect.h"
#include "async_simple/coro/FutureAwaiter.h"
#include "async_simple/coro/Lazy.h"
#include "ib_buffer.hpp"
#include "ib_socket.hpp"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_io/ibverbs/ib_device.hpp"
#include "ylt/coro_io/io_context_pool.hpp"
#include "ylt/easylog.hpp"
#include "ylt/struct_pack.hpp"
#include "ylt/struct_pack/reflection.hpp"
namespace coro_io {

inline async_simple::coro::Lazy<std::error_code> async_accept(
    asio::ip::tcp::acceptor& acceptor, coro_io::ib_socket_t& ib_socket) {
  asio::ip::tcp::socket soc(ib_socket.get_executor());
  auto ec = co_await async_io<std::error_code>(
      [&](auto cb) {
        acceptor.async_accept(soc, cb);
      },
      acceptor);

  if (ec) [[unlikely]] {
    co_return std::move(ec);
  }
  auto ret = co_await ib_socket.accept(std::move(soc));
  ELOGV(INFO, "accept over:%s", ret.message().data());
  if (ret) [[unlikely]] {
    ib_socket.close();
    co_return std::make_error_code(std::errc::protocol_error);
  }
  co_return ret;
}

inline async_simple::coro::Lazy<std::error_code> async_connect(
    coro_io::ib_socket_t& ib_socket, const std::string& host,
    const std::string& port) {
  return ib_socket.connect(host, port);
}

template <typename EndPointSeq>
inline async_simple::coro::Lazy<std::error_code> async_connect(
    coro_io::ib_socket_t& ib_socket, const EndPointSeq& endpoint) noexcept {
  return ib_socket.connect(endpoint);
}

namespace detail {

template <typename T>
inline std::size_t consume_buffer(coro_io::ib_socket_t& ib_socket,
                                  std::span<T>& sge_buffer) {
  std::size_t transfer_total = 0;
  if (ib_socket.remain_read_buffer_size()) {
    while (sge_buffer.size()) {
      auto length = ib_socket.consume((char*)sge_buffer.front().addr,
                                      sge_buffer.front().length);

      transfer_total += length;
      if (length < sge_buffer.front().length) {
        sge_buffer.front().addr = sge_buffer.front().addr + length;
        sge_buffer.front().length -= length;
        break;
      }
      sge_buffer = sge_buffer.subspan(1);
    }
  }
  if (transfer_total) {
    ELOG_TRACE << "has completed size:" << transfer_total;
  }
  return transfer_total;
}

inline std::size_t copy(std::span<ibv_sge> from, ibv_sge to) {
  std::size_t transfer_total = 0;
  for (auto& sge : from) {
    memcpy((void*)(to.addr + transfer_total), (void*)sge.addr, sge.length);
    transfer_total += sge.length;
  }
  return transfer_total;
}

inline void copy(ibv_sge from, std::span<ibv_sge> to) {
  std::size_t transfer_total = 0;
  for (auto& sge : to) {
    memcpy((void*)sge.addr, (void*)(from.addr + transfer_total), sge.length);
    transfer_total += sge.length;
  }
  return;
}

async_simple::coro::
    Lazy<std::pair<std::error_code, std::size_t>> inline async_recv_impl(
        coro_io::ib_socket_t& ib_socket, std::span<ibv_sge> sge_list,
        std::size_t io_size) {
  std::span<ibv_sge> io_buffer;
  auto result =
      co_await coro_io::async_io<std::pair<std::error_code, std::size_t>>(
          [&](auto&& cb) {
            ib_socket.post_recv(std::move(cb));
          },
          ib_socket);
  if (result.first) [[unlikely]] {
    co_return std::pair{result.first, 0};
  }
  ibv_sge socket_buffer = ib_socket.get_recv_buffer();
  socket_buffer.length = result.second;
  copy(socket_buffer, sge_list);
  size_t recved_len = std::min(result.second, io_size);
  ib_socket.set_read_buffer_len(recved_len, result.second - recved_len);

  co_return std::pair{result.first, recved_len};
}

struct async_send_callback_helper {
  ib_buffer_t buffer;
  std::unique_ptr<char[]> zero_copy_buffer;
  ib_socket_shared_state_t* state;
  void operator()(std::pair<std::error_code, std::size_t> result) {
    if (buffer) {
      state->return_send_buffer(std::move(buffer));
    }
    zero_copy_buffer = {};
    auto sz = state->send_buffer_data_size_;
    if (!result.first && sz) {  // write small package data
      buffer = state->release_send_buffer();
      auto sge = buffer.subview(0, sz);
      state->post_send_impl(sge, std::move(*this));
    }
    else {
      state->wake_up_if_is_waiting(result.first);
    }
  }
};

async_simple::coro::
    Lazy<std::pair<std::error_code, std::size_t>> inline async_send_impl(
        coro_io::ib_socket_t& ib_socket, std::span<ibv_sge> sge_list,
        std::size_t io_size) {
  if (io_size == 0) [[unlikely]] {
    co_return std::pair{std::error_code{}, 0};
  }
  if (!ib_socket.is_open()) {
    co_return std::pair{std::make_error_code(std::errc::not_connected), 0};
  }
  ibv_sge socket_buffer;
  std::unique_ptr<char[]> zero_copy_buffer;
  std::size_t max_buffer_length =
      std::min<std::size_t>(256 * 1024, ib_socket.get_buffer_size());
  std::size_t send_request_count = ib_socket.sent_request_count();
  auto now_buffer_data = ib_socket.get_buffer_size() -
                         ib_socket.get_free_send_buffer_size() + io_size;
  bool enable_small_message_combine =
      now_buffer_data < max_buffer_length &&
      send_request_count >= ib_socket.get_config().send_buffer_cnt;
  bool is_enable_inline_send =
      ib_socket.get_config().cap.max_inline_data >= io_size &&
      !enable_small_message_combine;
  ib_buffer_t send_buffer;
  if (is_enable_inline_send) {
    zero_copy_buffer = std::make_unique<char[]>(io_size);
    socket_buffer = {.addr = (uintptr_t)zero_copy_buffer.get(),
                     .length = (uint32_t)io_size,
                     .lkey = 0};
  }
  else {
    auto sv = ib_socket.get_send_buffer_view();
    if (!sv) [[unlikely]] {
      co_return std::pair{std::make_error_code(std::errc::no_buffer_space),
                          std::size_t{0}};
    }
    socket_buffer = *sv;
    // we make sure it when split
    assert(socket_buffer.length >= io_size);
    socket_buffer.length = io_size;
    ib_socket.consume_send_buffer(io_size);
  }
  auto len = copy(sge_list, socket_buffer);
  assert(len == io_size);
  if (enable_small_message_combine) {
    ELOG_TRACE << "combine small message, now buffer size:" << now_buffer_data;
    co_return std::pair{std::error_code{}, io_size};
  }
  if (!zero_copy_buffer) {
    send_buffer = std::move(ib_socket.release_send_buffer());
    socket_buffer.length += socket_buffer.addr - (size_t)send_buffer->addr;
    socket_buffer.addr = (size_t)send_buffer->addr;
  }
  std::error_code ec{};
  if (send_request_count >= ib_socket.get_config().send_buffer_cnt) {
    bool is_canceled = false;
    try {
      ec = co_await ib_socket.waiting_write_over();
    } catch (const async_simple::SignalException& e) {
      is_canceled = true;
    }
    if (is_canceled) [[unlikely]] {
      ib_socket.close();
      co_return std::pair{std::make_error_code(std::errc::operation_canceled),
                          std::size_t{0}};
    }
  }
  ib_socket.post_send(socket_buffer,
                      async_send_callback_helper{std::move(send_buffer),
                                                 std::move(zero_copy_buffer),
                                                 ib_socket.get_raw_state()});
  if (ec) {
    co_return std::pair{ec, 0};
  }
  else {
    co_return std::pair{ec, io_size};
  }
}

template <typename T>
void make_sge_impl(std::vector<ibv_sge>& sge, std::span<T> buffers) {
  constexpr bool is_ibv_sge = requires { buffers.begin()->lkey; };
  sge.reserve(buffers.size());
  for (auto& buffer : buffers) {
    if constexpr (is_ibv_sge) {
      if (buffer.length == 0) [[unlikely]] {
        continue;
      }
      sge.push_back(ibv_sge{buffer.addr, buffer.length, buffer.lkey});
    }
    else {
      if (buffer.size() == 0) [[unlikely]] {
        continue;
      }
      for (std::size_t i = 0; i < buffer.size(); i += UINT32_MAX) {
        sge.push_back(ibv_sge{(uintptr_t)buffer.data() + i,
                              std::min<uint32_t>(buffer.size() - i, UINT32_MAX),
                              0});
      }
    }
  }
}

template <typename T>
inline void make_sge(std::vector<ibv_sge>& sge, T& buffer) {
  if constexpr (requires { buffer.data(); }) {
    using pointer_t = decltype(buffer.data());
    if constexpr (std::is_same_v<pointer_t, void*> ||
                  std::is_same_v<pointer_t, char*> ||
                  std::is_same_v<pointer_t, const char*>) {
      make_sge_impl(sge, std::span{&buffer, 1});
    }
    else {
      // multiple buffers
      make_sge_impl(sge, std::span<typename T::value_type>{buffer});
    }
  }
  else {
    make_sge_impl(sge, std::span<T>{&buffer, 1});
  }
}

inline void reset_buffer(std::vector<ibv_sge>& buffer, std::size_t read_size) {
  if (read_size) {
    for (auto& e : buffer) {
      if (e.length <= read_size) {
        read_size -= e.length;
        e.length = 0;
      }
      else {
        e.addr += read_size;
        e.length -= read_size;
        break;
      }
    }
    auto it = std::find_if(buffer.begin(), buffer.end(), [](const ibv_sge& x) {
      return x.length != 0;
    });
    buffer.erase(buffer.begin(), it);
  }
}

template <ib_socket_t::io_type io, typename Buffer>
async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
async_io_split_impl(coro_io::ib_socket_t& ib_socket, Buffer&& raw_buffer,
                    bool read_some) {
  std::vector<ibv_sge> sge_list;
  make_sge(sge_list, raw_buffer);
  std::span<ibv_sge> sge_span = sge_list;
  if (sge_span.size() == 0) [[unlikely]] {
    co_return std::pair{std::error_code{}, std::size_t{0}};
  }

  std::vector<ibv_sge> split_sge_block;
  split_sge_block.reserve(sge_span.size());
  std::size_t io_completed_size = 0;
  if constexpr (io == ib_socket_t::io_type::recv) {
    io_completed_size = consume_buffer(ib_socket, sge_span);
    if (sge_span.empty()) {
      co_return std::pair{std::error_code{}, io_completed_size};
    }
  }

  std::size_t block_size;
  uint32_t now_split_size = 0;
  uint32_t max_size = ib_socket.get_buffer_size();
  if constexpr (io == ib_socket_t::io_type::send) {
    max_size = ib_socket.get_free_send_buffer_size();
  }
  for (auto& sge : sge_span) {
    for (std::size_t i = 0; i < sge.length; i += block_size) {
      block_size =
          std::min<uint32_t>(max_size - now_split_size, sge.length - i);

      if (split_sge_block.size() &&
          split_sge_block.back().addr + split_sge_block.back().length ==
              sge.addr + i &&
          split_sge_block.back().lkey == sge.lkey) {  // try combine iov
        split_sge_block.back().length += block_size;
      }
      else {
        split_sge_block.push_back(
            ibv_sge{sge.addr + i, (uint32_t)block_size, sge.lkey});
      }

      now_split_size += block_size;
      if (now_split_size == max_size) {
        std::error_code ec;
        std::size_t len = 0;
        if constexpr (io == ib_socket_t::io_type::recv) {
          std::tie(ec, len) = co_await async_recv_impl(
              ib_socket, split_sge_block, now_split_size);
        }
        else {
          std::tie(ec, len) = co_await async_send_impl(
              ib_socket, split_sge_block, now_split_size);
          max_size = ib_socket.get_buffer_size();
        }
        io_completed_size += len;
        ELOG_TRACE << "has completed size:" << io_completed_size;
        if (ec) {
          co_return std::pair{ec, io_completed_size};
        }
        if constexpr (io == ib_socket_t::io_type::recv) {
          if (read_some) {
            co_return std::pair{ec, io_completed_size};
          }
        }
        if (len < now_split_size) [[unlikely]] {
          reset_buffer(split_sge_block, len);
        }
        else {
          split_sge_block.clear();
        }
        now_split_size -= len;
      }
    }
  }

  std::error_code ec;
  for (std::size_t len = 0; now_split_size > 0;) {
    if constexpr (io == ib_socket_t::io_type::recv) {
      reset_buffer(split_sge_block, len);
      std::tie(ec, len) =
          co_await async_recv_impl(ib_socket, split_sge_block, now_split_size);
    }
    else {
      std::tie(ec, len) =
          co_await async_send_impl(ib_socket, split_sge_block, now_split_size);
    }
    ELOG_TRACE << "now piece io_size:" << len;

    io_completed_size += len;
    now_split_size -= len;
    if (ec) {
      co_return std::pair{ec, io_completed_size};
    }
    if constexpr (io == ib_socket_t::io_type::recv) {
      if (read_some) {
        co_return std::pair{ec, io_completed_size};
      }
    }
  }
  ELOG_TRACE << "has completed size:" << io_completed_size;
  co_return std::pair{ec, io_completed_size};
}

template <ib_socket_t::io_type io, typename Buffer>
async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
async_io_split(coro_io::ib_socket_t& ib_socket, Buffer&& raw_buffer,
               bool read_some = false) {
  if (!ib_socket.get_executor().running_in_this_thread()) {
    // switch to io_thread
    co_await dispatch(ib_socket.get_executor());
  }
  auto ret = co_await async_io_split_impl<io>(
      ib_socket, std::forward<Buffer>(raw_buffer), read_some);
  co_return ret;
}

}  // namespace detail

template <typename buffer_t>
async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>> async_write(
    coro_io::ib_socket_t& ib_socket, buffer_t&& buffer) {
  return detail::async_io_split<ib_socket_t::send>(
      ib_socket, std::forward<buffer_t>(buffer));
}

template <typename buffer_t>
async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>> async_read(
    coro_io::ib_socket_t& ib_socket, buffer_t&& buffer) {
  return detail::async_io_split<ib_socket_t::recv>(
      ib_socket, std::forward<buffer_t>(buffer));
}

template <typename buffer_t>
async_simple::coro::Lazy<std::pair<std::error_code, std::size_t>>
async_read_some(coro_io::ib_socket_t& ib_socket, buffer_t&& buffer) {
  auto result = co_await detail::async_io_split<ib_socket_t::recv>(
      ib_socket, std::forward<buffer_t>(buffer), true);
  co_return result;
}

}  // namespace coro_io