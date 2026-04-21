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

#include <array>
#include <cstdint>
#include <optional>
#include <string>
#include <system_error>
#include <type_traits>
#include <variant>
#include <ylt/easylog.hpp>
#include <ylt/struct_pack.hpp>

#include "asio/buffer.hpp"
#include "struct_pack_protocol.hpp"
#include "ylt/coro_io/coro_io.hpp"
#include "ylt/coro_rpc/impl/context.hpp"
#include "ylt/coro_rpc/impl/errno.h"
#include "ylt/coro_rpc/impl/expected.hpp"
#include "ylt/coro_rpc/impl/router.hpp"
#include "ylt/struct_pack/reflection.hpp"

namespace coro_rpc {
namespace protocol {

struct coro_rpc_protocol {
 public:
  constexpr static inline uint8_t VERSION_NUMBER = 0;
  /*!
   * RPC header
   *
   * memory layout
   * ```
   * ┌────────┬───────────┬────────────────┬──────────┬─────────┬─────────┐─────────┐
   * │  magic │  version  │ serialize_type │ msg_type │ seq_num │ length
   * │reserved
   * ├────────┼───────────┼────────────────┼──────────┼─────────┼─────────┤─────────┤
   * │   1    │     1     │       1        │    1     │    4    │    4    │    4
   * │
   * └────────┴───────────┴────────────────┴──────────┴─────────┴─────────┘─────────┘
   * ```
   */
  struct req_header {
    uint8_t magic;           //!< magic number
    uint8_t version;         //!< rpc protocol version
    uint8_t serialize_type;  //!< serialization type
    uint8_t msg_type;        //!< message type
    uint32_t seq_num;        //!< sequence number
    uint32_t function_id;    //!< rpc function ID
    uint32_t length;         //!< length of RPC body
    uint32_t attach_length;  //!< attachment length
  };

  struct resp_header {
    uint8_t magic;           //!< magic number
    uint8_t version;         //!< rpc protocol version
    uint8_t err_code;        //!< rpc error type
    uint8_t msg_type;        //!< message type
    uint32_t seq_num;        //!< sequence number
    uint32_t length;         //!< length of RPC body
    uint32_t attach_length;  //!< attachment length
  };

  using supported_serialize_protocols = std::variant<struct_pack_protocol>;
  using route_key_t = uint32_t;
  using router = coro_rpc::protocol::router<coro_rpc_protocol>;

  static std::optional<supported_serialize_protocols> get_serialize_protocol(
      req_header& req_header) {
    if (req_header.serialize_type == 0)
      AS_LIKELY { return struct_pack_protocol{}; }
    else {
      return std::nullopt;
    }
  };

  static route_key_t get_route_key(req_header& req_header) {
    return req_header.function_id;
  };

  template <typename Socket>
  static async_simple::coro::Lazy<std::error_code> read_head(
      Socket& socket, req_header& req_head) {
    // TODO: add a connection-level buffer in parameter to reuse memory
    char head_buffer[sizeof(req_header)];
    auto [ec, _] = co_await coro_io::async_read(
        socket, asio::buffer(head_buffer, sizeof(req_header)));
    if (ec) [[unlikely]] {
      co_return std::move(ec);
    }
    auto i = struct_pack::get_needed_size<
        struct_pack::sp_config::DISABLE_ALL_META_INFO>(req_head);
    auto ec2 = struct_pack::deserialize_to<
        struct_pack::sp_config::DISABLE_ALL_META_INFO>(
        req_head,
        std::string_view{head_buffer, head_buffer + sizeof(head_buffer)});
    if (ec2 || req_head.magic != magic_number ||
        req_head.version > VERSION_NUMBER) [[unlikely]] {
      co_return std::make_error_code(std::errc::protocol_error);
    }
    co_return std::error_code{};
  }

  // this function is used for check connection type when read first rpc head
  template <typename Socket>
  static async_simple::coro::Lazy<std::error_code> read_magic(
      Socket& socket, std::string& magic) {
    char head_buffer[sizeof(req_header)];
    auto [ec, _] =
        co_await coro_io::async_read(socket, asio::buffer(head_buffer, 1));
    if (ec) [[unlikely]] {
      co_return std::move(ec);
    }
    magic = head_buffer[0];
    if (head_buffer[0] != magic_number) {
      co_return std::make_error_code(std::errc::protocol_error);
    }
    co_return std::error_code{};
  }

  template <typename Socket>
  static async_simple::coro::Lazy<std::error_code> read_payload(
      Socket& socket, req_header& req_head, std::string& buffer,
      std::string& attchment) {
    struct_pack::detail::resize(buffer, req_head.length);
    if (req_head.attach_length > 0) {
      struct_pack::detail::resize(attchment, req_head.attach_length);

      if (req_head.length > 0) {
        std::array<asio::mutable_buffer, 2> buffers{asio::buffer(buffer),
                                                    asio::buffer(attchment)};
        auto [ec, _] = co_await coro_io::async_read(socket, buffers);
        co_return ec;
      }

      auto [ec, _] =
          co_await coro_io::async_read(socket, asio::buffer(attchment));
      co_return ec;
    }

    auto [ec, _] = co_await coro_io::async_read(socket, asio::buffer(buffer));
    co_return ec;
  }

  template <typename Socket>
  static async_simple::coro::Lazy<std::error_code> read_first_head(
      Socket& socket, req_header& req_head, std::string_view magic) {
    // TODO: add a connection-level buffer in parameter to reuse memory
    assert(magic.size() < sizeof(req_header));
    char head_buffer[sizeof(req_header)];
    memcpy(head_buffer, magic.data(), magic.size());
    auto [ec, _] = co_await coro_io::async_read(
        socket, asio::buffer(head_buffer + magic.size(),
                             sizeof(req_header) - magic.size()));
    if (ec) [[unlikely]] {
      co_return std::move(ec);
    }
    auto i = struct_pack::get_needed_size<
        struct_pack::sp_config::DISABLE_ALL_META_INFO>(req_head);
    auto ec2 = struct_pack::deserialize_to<
        struct_pack::sp_config::DISABLE_ALL_META_INFO>(
        req_head,
        std::string_view{head_buffer, head_buffer + sizeof(head_buffer)});
    if (ec2 || req_head.magic != magic_number ||
        req_head.version > VERSION_NUMBER) [[unlikely]] {
      co_return std::make_error_code(std::errc::protocol_error);
    }
    co_return std::error_code{};
  }

  static std::string prepare_response(std::string& rpc_result,
                                      const req_header& req_header,
                                      std::size_t attachment_len,
                                      coro_rpc::errc rpc_err_code = {},
                                      std::string_view err_msg = {}) {
    std::string err_msg_buf;
    std::string header_buf;
    resp_header resp_head;
    resp_head.magic = magic_number;
    resp_head.version = VERSION_NUMBER;
    resp_head.seq_num = req_header.seq_num;
    resp_head.attach_length = attachment_len;
    resp_head.msg_type = 0;
    resp_head.err_code = 0;
    if (attachment_len > UINT32_MAX)
      AS_UNLIKELY {
        ELOG_ERROR << "attachment larger than 4G: " << attachment_len;
        rpc_err_code = coro_rpc::errc::message_too_large;
        err_msg_buf =
            "attachment larger than 4G:" + std::to_string(attachment_len) + "B";
        err_msg = err_msg_buf;
      }
    else if (rpc_result.size() > UINT32_MAX)
      AS_UNLIKELY {
        auto sz = rpc_result.size();
        ELOG_ERROR << "body larger than 4G: " << sz;
        rpc_err_code = coro_rpc::errc::message_too_large;
        err_msg_buf =
            "body larger than 4G:" + std::to_string(attachment_len) + "B";
        err_msg = err_msg_buf;
      }
    if (rpc_err_code != coro_rpc::errc{})
      AS_UNLIKELY {
        rpc_result.clear();
        if (static_cast<uint16_t>(rpc_err_code) > UINT8_MAX) {
          struct_pack::serialize_to(
              rpc_result,
              std::pair{static_cast<uint16_t>(rpc_err_code), err_msg});
          resp_head.err_code = static_cast<uint16_t>(255);
        }
        else {
          struct_pack::serialize_to(rpc_result, err_msg);
          resp_head.err_code = static_cast<uint16_t>(rpc_err_code);
        }
      }
    resp_head.length = rpc_result.size();
    struct_pack::serialize_to<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
        header_buf, resp_head);
    return header_buf;
  }

  /*!
   * The RPC error for client
   *
   * The `rpc_error` struct holds the error code `code` and error message
   * `msg`.
   */

  // internal variable
  constexpr static inline int8_t magic_number = 21;

  static constexpr auto REQ_HEAD_LEN = sizeof(req_header{});
  static_assert(REQ_HEAD_LEN == 20);

  static constexpr auto RESP_HEAD_LEN = sizeof(resp_header{});
  static_assert(RESP_HEAD_LEN == 16);
};

template <typename rpc_protocol = coro_rpc::protocol::coro_rpc_protocol>
uint64_t get_request_id(
    const typename rpc_protocol::req_header& header) noexcept {
  if constexpr (std::is_same_v<rpc_protocol,
                               coro_rpc::protocol::coro_rpc_protocol>) {
    return header.seq_num;
  }
  else {
    return 0;
  }
}
}  // namespace protocol
template <typename return_msg_type>
using context = coro_rpc::context_base<return_msg_type,
                                       coro_rpc::protocol::coro_rpc_protocol>;

template <typename rpc_protocol = coro_rpc::protocol::coro_rpc_protocol>
async_simple::coro::Lazy<context_info_t<rpc_protocol>*> get_context_in_coro() {
  auto* ctx = co_await async_simple::coro::CurrentLazyLocals<
      coro_connection::connection_lazy_ctx<rpc_protocol>>{};
  assert(ctx != nullptr);
  co_return ctx->info_.get();
}

namespace detail {
template <typename rpc_protocol>
context_info_t<rpc_protocol>*& set_context() {
  thread_local static context_info_t<rpc_protocol>* ctx;
  return ctx;
}
}  // namespace detail

template <typename rpc_protocol = coro_rpc::protocol::coro_rpc_protocol>
context_info_t<rpc_protocol>* get_context() {
  return detail::set_context<rpc_protocol>();
}

}  // namespace coro_rpc